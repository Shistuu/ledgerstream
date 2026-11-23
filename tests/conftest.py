import uuid

import pymysql
import pytest

from ledgerstream import db


@pytest.fixture
def mysql_conn():
    try:
        conn = db.connect()
    except pymysql.err.OperationalError as e:
        pytest.skip(f"MySQL not reachable ({e}); run scripts/setup.sh first")
    yield conn
    conn.close()


@pytest.fixture
def temp_table(mysql_conn):
    name = f"payments_test_{uuid.uuid4().hex[:10]}"
    db.create_table(mysql_conn, name)
    yield name
    db.drop_table(mysql_conn, name)
