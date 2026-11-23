"""MySQL access: table creation and idempotent batch inserts."""
import argparse
import re

import pymysql

from ledgerstream import config

_TABLE_NAME = re.compile(r"^[A-Za-z0-9_]{1,64}$")


def _check_table(table: str) -> str:
    # Table names can't be bound as query parameters, so whitelist them.
    if not _TABLE_NAME.match(table):
        raise ValueError(f"bad table name: {table!r}")
    return table


def connect(**overrides):
    params = dict(config.MYSQL)
    params.update(overrides)
    return pymysql.connect(autocommit=False, **params)


def create_table(conn, table: str = config.PAYMENTS_TABLE) -> None:
    table = _check_table(table)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                event_id     CHAR(36)    NOT NULL,
                loan_id      VARCHAR(64) NOT NULL,
                merchant_id  VARCHAR(64) NOT NULL,
                amount_cents BIGINT      NOT NULL,
                currency     CHAR(3)     NOT NULL,
                occurred_at  DATETIME(6) NOT NULL,
                ingested_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                PRIMARY KEY (event_id),
                KEY idx_loan (loan_id)
            ) ENGINE=InnoDB
            """
        )
    conn.commit()


def drop_table(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {_check_table(table)}")
    conn.commit()


def insert_payments(conn, rows, table: str = config.PAYMENTS_TABLE) -> int:
    """Insert rows in one transaction. Returns how many rows were new.

    Rows whose event_id already exists are skipped by the primary key. I use
    ON DUPLICATE KEY UPDATE with a no-op instead of INSERT IGNORE because
    IGNORE also turns real errors (bad data, truncation) into warnings.
    """
    if not rows:
        return 0
    table = _check_table(table)
    sql = (
        f"INSERT INTO {table} "
        "(event_id, loan_id, merchant_id, amount_cents, currency, occurred_at) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE event_id = event_id"
    )
    try:
        with conn.cursor() as cur:
            inserted = cur.executemany(sql, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    # With a no-op update, MySQL reports 1 per new row and 0 per duplicate.
    return inserted


def count_rows(conn, table: str = config.PAYMENTS_TABLE) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {_check_table(table)}")
        (n,) = cur.fetchone()
    conn.commit()  # end the read snapshot so the next call sees new rows
    return n


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create the payments table")
    parser.add_argument("--table", default=config.PAYMENTS_TABLE)
    args = parser.parse_args()
    c = connect()
    create_table(c, args.table)
    print(f"table {args.table} ready")
