"""Consume payment events from Kafka and write them to MySQL.

Delivery is at-least-once: for each batch the consumer writes valid events to
MySQL (one transaction), sends invalid ones to the DLQ and waits for the ack,
and only then commits the Kafka offsets. A crash anywhere before the offset
commit means the batch is read again, and the event_id primary key turns the
replayed inserts into no-ops.

Usage:
    python -m ledgerstream.consumer --metrics-port 8001
"""
import argparse
import json
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

from ledgerstream import config, db
from ledgerstream.models import InvalidEvent, parse_event, to_row

log = logging.getLogger("ledgerstream.consumer")

PROCESSED = Counter("ledgerstream_events_processed_total", "Valid events written (new rows)")
DUPLICATES = Counter("ledgerstream_duplicates_skipped_total", "Valid events skipped because event_id already existed")
DEAD_LETTERED = Counter("ledgerstream_dead_lettered_total", "Invalid events sent to the DLQ")
LAG = Gauge("ledgerstream_consumer_lag", "High watermark minus committed offset", ["topic", "partition"])

LAG_REFRESH_SECONDS = 2.0


class PaymentConsumer:
    def __init__(self, topic, dlq_topic, table, group, bootstrap=config.KAFKA_BOOTSTRAP, batch_size=500):
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.table = table
        self.batch_size = batch_size
        self.running = True
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            # Notice dead members quickly so their partitions move to live ones.
            "session.timeout.ms": 6000,
            "heartbeat.interval.ms": 2000,
        })
        self.dlq = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
        self.conn = db.connect()
        self._last_lag_refresh = 0.0

    def stop(self, *_):
        log.info("shutdown requested, finishing current batch")
        self.running = False

    def run(self):
        self.consumer.subscribe([self.topic], on_revoke=self._on_revoke)
        try:
            while self.running:
                msgs = self.consumer.consume(num_messages=self.batch_size, timeout=0.5)
                if msgs:
                    self.handle_batch(msgs)
                self._maybe_refresh_lag()
        finally:
            self.consumer.close()  # leaves the group so partitions rebalance right away
            self.conn.close()
            log.info("consumer closed")

    def handle_batch(self, msgs):
        rows, dead, next_offsets = [], [], {}
        for msg in msgs:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consume error: %s", msg.error())
                continue
            next_offsets[msg.partition()] = msg.offset() + 1
            try:
                rows.append(to_row(parse_event(msg.value())))
            except InvalidEvent as e:
                dead.append((msg, str(e)))

        if not next_offsets:
            return

        # 1. Database first. If this raises, nothing is committed and the batch is retried.
        inserted = db.insert_payments(self.conn, rows, self.table)

        # 2. Dead letters, waiting for the broker ack.
        for msg, reason in dead:
            self.dlq.produce(self.dlq_topic, key=msg.key(), value=json.dumps({
                "error": reason,
                "source_topic": msg.topic(),
                "source_partition": msg.partition(),
                "source_offset": msg.offset(),
                "raw": msg.value().decode("utf-8", errors="replace") if msg.value() else None,
            }).encode())
        if dead and self.dlq.flush(30) > 0:
            raise RuntimeError("DLQ messages not acknowledged; not committing offsets")

        # 3. Only now commit offsets.
        offsets = [TopicPartition(self.topic, p, o) for p, o in next_offsets.items()]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as e:
            # Usually a rebalance took the partition away. The new owner will
            # re-read these messages and the writes will be no-ops.
            log.warning("offset commit failed, batch will be redelivered: %s", e)

        PROCESSED.inc(inserted)
        DUPLICATES.inc(len(rows) - inserted)
        DEAD_LETTERED.inc(len(dead))

    def _on_revoke(self, _consumer, partitions):
        for tp in partitions:
            try:
                LAG.remove(tp.topic, str(tp.partition))
            except KeyError:
                pass

    def _maybe_refresh_lag(self):
        now = time.monotonic()
        if now - self._last_lag_refresh < LAG_REFRESH_SECONDS:
            return
        self._last_lag_refresh = now
        assigned = self.consumer.assignment()
        if not assigned:
            return
        try:
            committed = self.consumer.committed(assigned, timeout=5)
            for tp in committed:
                # cached=True reads the high watermark from the last fetch response.
                # Asking the broker (cached=False) took ~0.5s per partition here,
                # which slowed a single consumer down by more than 10x.
                _low, high = self.consumer.get_watermark_offsets(tp, cached=True)
                if high < 0:
                    continue  # nothing fetched from this partition yet
                done = tp.offset if tp.offset >= 0 else 0  # no commit yet -> whole partition is lag
                LAG.labels(tp.topic, str(tp.partition)).set(max(high - done, 0))
        except KafkaException as e:
            log.warning("lag refresh failed: %s", e)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--topic", default=config.PAYMENTS_TOPIC)
    parser.add_argument("--dlq-topic", default=config.DLQ_TOPIC)
    parser.add_argument("--table", default=config.PAYMENTS_TABLE)
    parser.add_argument("--group", default=config.CONSUMER_GROUP)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--metrics-port", type=int, default=8001)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(process)d %(levelname)s %(message)s")
    start_http_server(args.metrics_port)
    pc = PaymentConsumer(args.topic, args.dlq_topic, args.table, args.group, batch_size=args.batch_size)
    signal.signal(signal.SIGTERM, pc.stop)
    signal.signal(signal.SIGINT, pc.stop)
    log.info("consuming %s -> table %s, metrics on :%d", args.topic, args.table, args.metrics_port)
    pc.run()


if __name__ == "__main__":
    main()
