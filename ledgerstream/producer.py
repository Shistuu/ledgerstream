"""Emit fake loan payment events to Kafka.

Usage:
    python -m ledgerstream.producer --count 10000 --malformed-pct 5 --duplicate-pct 10
"""
import argparse
import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from confluent_kafka import Producer

from ledgerstream import config

CURRENCIES = ["USD", "USD", "USD", "CAD", "EUR"]


@dataclass
class ProduceStats:
    sent: int = 0          # total messages sent
    unique_valid: int = 0  # distinct valid event_ids
    duplicates: int = 0    # re-sends of an already-sent valid event
    malformed: int = 0     # messages that should end up in the DLQ
    valid_amount_total: int = 0  # sum of amount_cents over unique valid events


def fake_event(rng: random.Random) -> dict:
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "loan_id": f"loan-{rng.randint(1, 50_000)}",
        "merchant_id": f"m-{rng.randint(1, 500)}",
        "amount_cents": rng.randint(500, 250_000),
        "currency": rng.choice(CURRENCIES),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }


def break_event(event: dict, rng: random.Random) -> bytes:
    """Return a malformed version of an event (one of several kinds of bad)."""
    kind = rng.randrange(5)
    bad = dict(event)
    if kind == 0:
        return b"{not json"
    if kind == 1:
        del bad["amount_cents"]
    elif kind == 2:
        bad["amount_cents"] = -abs(bad["amount_cents"])
    elif kind == 3:
        bad["currency"] = "dollars"
    else:
        bad["event_id"] = "not-a-uuid"
    return json.dumps(bad).encode()


def make_producer(bootstrap: str = config.KAFKA_BOOTSTRAP) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
    })


def produce(count: int, topic: str = config.PAYMENTS_TOPIC, malformed_pct: float = 0.0,
            duplicate_pct: float = 0.0, rate: float = 0.0, seed: int = None,
            bootstrap: str = config.KAFKA_BOOTSTRAP) -> ProduceStats:
    """Send `count` messages. `rate` is messages/second (0 means as fast as possible)."""
    rng = random.Random(seed)
    producer = make_producer(bootstrap)
    stats = ProduceStats()
    sent_valid = []  # (key, value) of valid events, for picking duplicates
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(err)

    start = time.monotonic()
    for i in range(count):
        roll = rng.random() * 100
        if sent_valid and roll < duplicate_pct:
            key, value = rng.choice(sent_valid)
            stats.duplicates += 1
        elif roll < duplicate_pct + malformed_pct:
            event = fake_event(rng)
            key, value = event["loan_id"].encode(), break_event(event, rng)
            stats.malformed += 1
        else:
            event = fake_event(rng)
            key, value = event["loan_id"].encode(), json.dumps(event).encode()
            sent_valid.append((key, value))
            stats.unique_valid += 1
            stats.valid_amount_total += event["amount_cents"]

        while True:
            try:
                producer.produce(topic, value=value, key=key, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)  # local queue full, wait for deliveries
        producer.poll(0)
        stats.sent += 1

        if rate > 0:
            ahead = (i + 1) / rate - (time.monotonic() - start)
            if ahead > 0:
                time.sleep(ahead)

    remaining = producer.flush(60)
    if remaining or errors:
        raise RuntimeError(f"{remaining} undelivered, {len(errors)} delivery errors: {errors[:3]}")
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--topic", default=config.PAYMENTS_TOPIC)
    parser.add_argument("--malformed-pct", type=float, default=0.0, help="percent of messages to corrupt")
    parser.add_argument("--duplicate-pct", type=float, default=0.0, help="percent of messages that re-send an earlier event")
    parser.add_argument("--rate", type=float, default=0.0, help="messages per second, 0 = unthrottled")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    t0 = time.monotonic()
    stats = produce(args.count, args.topic, args.malformed_pct, args.duplicate_pct, args.rate, args.seed)
    elapsed = time.monotonic() - t0
    print(f"sent={stats.sent} unique_valid={stats.unique_valid} duplicates={stats.duplicates} "
          f"malformed={stats.malformed} in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
