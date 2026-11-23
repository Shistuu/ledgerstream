"""Measure how fast 1 vs 3 consumers write a backlog of events into MySQL.

For each run: create a fresh topic (6 partitions) and table, pre-load the topic
with --events valid events, start N consumer processes, and poll MySQL until
every event is in the table.

Two numbers are printed per run:
  wall   = events / (time from starting the consumers to the last row landing),
           which includes process start-up and the consumer group join
  steady = events / (time from the first row landing to the last row landing)

Usage:
    python scripts/loadtest.py --events 200000 --consumers 1 3 --repeats 3
"""
import argparse
import platform
import signal
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ledgerstream import db, kafka_admin  # noqa: E402
from ledgerstream.producer import produce  # noqa: E402

POLL_SECONDS = 0.25


def one_run(n_consumers, n_events, batch_size, first_port):
    run = uuid.uuid4().hex[:8]
    topic, dlq = f"payments-load-{run}", f"payments-load-{run}.dlq"
    table, group = f"payments_load_{run}", f"load-{run}"
    kafka_admin.create_topics({topic: 6, dlq: 1})
    conn = db.connect()
    db.create_table(conn, table)
    procs = []
    try:
        t = time.monotonic()
        produce(n_events, topic, seed=1)
        produce_secs = time.monotonic() - t

        start = time.monotonic()
        for i in range(n_consumers):
            procs.append(subprocess.Popen(
                [sys.executable, "-m", "ledgerstream.consumer", "--topic", topic, "--dlq-topic", dlq,
                 "--table", table, "--group", group, "--batch-size", str(batch_size),
                 "--metrics-port", str(first_port + i)],
                cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

        first_row = None
        while True:
            rows = db.count_rows(conn, table)
            now = time.monotonic()
            if rows > 0 and first_row is None:
                first_row = now
            if rows >= n_events:
                break
            if now - start > 600:
                raise RuntimeError(f"timed out with {rows}/{n_events} rows")
            time.sleep(POLL_SECONDS)
        done = time.monotonic()
        return {
            "produce_rate": n_events / produce_secs,
            "wall": n_events / (done - start),
            "steady": n_events / (done - first_row),
            "seconds": done - start,
        }
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            p.wait(timeout=30)
        db.drop_table(conn, table)
        conn.close()
        kafka_admin.delete_topics([topic, dlq])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--events", type=int, default=200_000)
    parser.add_argument("--consumers", type=int, nargs="+", default=[1, 3])
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    print(f"machine: {platform.platform()} / {platform.machine()} / python {platform.python_version()}")
    print(f"events per run: {args.events}, batch size: {args.batch_size}, topic partitions: 6")
    for k in args.consumers:
        results = []
        for r in range(args.repeats):
            res = one_run(k, args.events, args.batch_size, first_port=9100)
            results.append(res)
            print(f"  consumers={k} run={r + 1}: wall={res['wall']:,.0f} ev/s  steady={res['steady']:,.0f} ev/s  "
                  f"({res['seconds']:.1f}s; producer loaded topic at {res['produce_rate']:,.0f} ev/s)")
        if args.repeats > 1:
            walls = [x["wall"] for x in results]
            steadies = [x["steady"] for x in results]
            print(f"consumers={k} median: wall={statistics.median(walls):,.0f} ev/s  "
                  f"steady={statistics.median(steadies):,.0f} ev/s  "
                  f"(min/max steady {min(steadies):,.0f}/{max(steadies):,.0f})")


if __name__ == "__main__":
    main()
