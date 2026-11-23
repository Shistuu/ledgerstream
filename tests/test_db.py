import json
import random

import pytest

from ledgerstream import db
from ledgerstream.models import parse_event, to_row
from ledgerstream.producer import fake_event


def make_rows(n, seed=0):
    rng = random.Random(seed)
    return [to_row(parse_event(json.dumps(fake_event(rng)).encode())) for _ in range(n)]


def test_insert_counts_new_rows(mysql_conn, temp_table):
    rows = make_rows(50)
    assert db.insert_payments(mysql_conn, rows, temp_table) == 50
    assert db.count_rows(mysql_conn, temp_table) == 50


def test_replaying_a_batch_is_a_noop(mysql_conn, temp_table):
    rows = make_rows(50)
    db.insert_payments(mysql_conn, rows, temp_table)
    assert db.insert_payments(mysql_conn, rows, temp_table) == 0
    assert db.count_rows(mysql_conn, temp_table) == 50


def test_partial_overlap_only_inserts_new(mysql_conn, temp_table):
    rows = make_rows(60)
    db.insert_payments(mysql_conn, rows[:40], temp_table)
    assert db.insert_payments(mysql_conn, rows[20:], temp_table) == 20
    assert db.count_rows(mysql_conn, temp_table) == 60


def test_duplicates_inside_one_batch(mysql_conn, temp_table):
    rows = make_rows(10)
    assert db.insert_payments(mysql_conn, rows + rows[:3], temp_table) == 10
    assert db.count_rows(mysql_conn, temp_table) == 10


def test_replay_does_not_overwrite_first_write(mysql_conn, temp_table):
    row = make_rows(1)[0]
    db.insert_payments(mysql_conn, [row], temp_table)
    changed = (row[0], row[1], row[2], row[3] + 1, row[4], row[5])
    db.insert_payments(mysql_conn, [changed], temp_table)
    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT amount_cents FROM {temp_table} WHERE event_id = %s", (row[0],))
        assert cur.fetchone()[0] == row[3]


def test_failed_batch_rolls_back(mysql_conn, temp_table):
    rows = make_rows(5)
    bad = (rows[0][0][:-1] + "x", None, "m-1", 100, "USD", rows[0][5])  # loan_id NULL violates NOT NULL
    with pytest.raises(Exception):
        db.insert_payments(mysql_conn, rows[1:] + [bad], temp_table)
    assert db.count_rows(mysql_conn, temp_table) == 0


def test_rejects_unsafe_table_name(mysql_conn):
    with pytest.raises(ValueError):
        db.insert_payments(mysql_conn, make_rows(1), "payments; DROP TABLE x")
