"""Integration test: SIGKILL consumers mid-stream and check nothing is lost or doubled.

Needs Kafka on localhost:9092 and MySQL set up by scripts/setup.sh.
Run with:  pytest -s tests/test_crash_recovery.py
"""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path

import pytest

from ledgerstream import db, kafka_admin
from ledgerstream.producer import produce

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parent.parent

N_EVENTS = 20_000
DUPLICATE_PCT = 10
MALFORMED_PCT = 5
PRODUCE_RATE = 2_000  # msgs/s, so the stream lasts ~10s and the kills land mid-stream


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_consumer(topic, dlq, table, group, log_dir, name):
    port = free_port()
    log = open(log_dir / f"{name}.log", "w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "ledgerstream.consumer", "--topic", topic, "--dlq-topic", dlq,
         "--table", table, "--group", group, "--metrics-port", str(port)],
        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
    )
    return proc, port


def wait_for(predicate, timeout, interval=1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def run_names(mysql_conn):
    run = uuid.uuid4().hex[:8]
    names = {
        "topic": f"payments-crash-{run}",
        "dlq": f"payments-crash-{run}.dlq",
        "table": f"payments_crash_{run}",
        "group": f"crash-test-{run}",
    }
    try:
        kafka_admin.create_topics({names["topic"]: 6, names["dlq"]: 1})
    except Exception as e:
        pytest.skip(f"Kafka not reachable: {e}")
    db.create_table(mysql_conn, names["table"])
    yield names
    db.drop_table(mysql_conn, names["table"])
    kafka_admin.delete_topics([names["topic"], names["dlq"]])


def test_sigkill_consumers_mid_stream(mysql_conn, run_names, tmp_path):
    n = run_names
    procs = {}
    for i in range(3):
        procs[f"c{i}"] = start_consumer(n["topic"], n["dlq"], n["table"], n["group"], tmp_path, f"c{i}")
    time.sleep(5)  # let the group form before data arrives

    stats_box = {}
    producer = threading.Thread(target=lambda: stats_box.update(stats=produce(
        N_EVENTS, n["topic"], MALFORMED_PCT, DUPLICATE_PCT, rate=PRODUCE_RATE, seed=7)))
    producer.start()

    killed = []
    rows_at_kill = []
    try:
        started = time.monotonic()
        for victim, at in (("c0", 3), ("c1", 6)):  # seconds after the producer starts
            time.sleep(max(0, at - (time.monotonic() - started)))
            procs[victim][0].send_signal(signal.SIGKILL)
            procs[victim][0].wait()
            killed.append(victim)
            rows_at_kill.append(db.count_rows(mysql_conn, n["table"]))

        time.sleep(2)
        for victim in killed:  # restart the killed ones
            procs[victim + "-restarted"] = start_consumer(
                n["topic"], n["dlq"], n["table"], n["group"], tmp_path, victim + "-restarted")
            del procs[victim]

        producer.join()
        stats = stats_box["stats"]

        assert wait_for(lambda: kafka_admin.group_lag(n["topic"], n["group"]) == 0, timeout=180), \
            "consumer lag did not reach 0"
        time.sleep(3)  # give a surviving consumer time to refresh its lag gauge
        metrics = urllib.request.urlopen(f"http://127.0.0.1:{procs['c2'][1]}/metrics").read().decode()

        # Graceful shutdown: SIGTERM, each process should finish its batch and exit 0.
        for proc, _ in procs.values():
            proc.send_signal(signal.SIGTERM)
        exit_codes = {name: proc.wait(timeout=30) for name, (proc, _) in procs.items()}
    finally:
        for proc, _ in procs.values():
            if proc.poll() is None:
                proc.kill()
        producer.join()

    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT event_id), COALESCE(SUM(amount_cents), 0) FROM {n['table']}")
        rows, distinct_ids, amount_total = cur.fetchone()
    mysql_conn.commit()

    dlq_msgs = [json.loads(v) for v in kafka_admin.read_all(n["dlq"])]
    dlq_sources = {(m["source_partition"], m["source_offset"]) for m in dlq_msgs}

    print(f"\nproduced={stats.sent} unique_valid={stats.unique_valid} duplicates={stats.duplicates} "
          f"malformed={stats.malformed}")
    print(f"killed={killed} rows_in_mysql_at_each_kill={rows_at_kill}")
    print(f"rows={rows} distinct_event_ids={distinct_ids} amount_sum_matches={amount_total == stats.valid_amount_total}")
    print(f"dlq_messages={len(dlq_msgs)} dlq_distinct_source_offsets={len(dlq_sources)}")
    print(f"exit_codes_after_sigterm={exit_codes}")
    print("lag gauge on c2:", [l for l in metrics.splitlines() if l.startswith("ledgerstream_consumer_lag{")])

    assert stats.sent == N_EVENTS
    assert all(r < stats.unique_valid for r in rows_at_kill), "kills did not happen mid-stream"
    assert rows == stats.unique_valid
    assert distinct_ids == rows
    assert amount_total == stats.valid_amount_total
    # The DLQ is at-least-once too: a crash after the DLQ ack but before the
    # offset commit can resend a dead letter. Count by source offset.
    assert len(dlq_sources) == stats.malformed
    lag_lines = [l for l in metrics.splitlines() if l.startswith("ledgerstream_consumer_lag{")]
    assert lag_lines and all(l.endswith(" 0.0") for l in lag_lines), lag_lines
    assert all(code == 0 for code in exit_codes.values())
