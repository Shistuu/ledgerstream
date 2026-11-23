"""Settings read from environment variables, with defaults for a local setup."""
import os

KAFKA_BOOTSTRAP = os.environ.get("LEDGER_KAFKA_BOOTSTRAP", "localhost:9092")
PAYMENTS_TOPIC = os.environ.get("LEDGER_TOPIC", "payments")
DLQ_TOPIC = os.environ.get("LEDGER_DLQ_TOPIC", "payments.dlq")
CONSUMER_GROUP = os.environ.get("LEDGER_GROUP", "ledgerstream-writer")

MYSQL = {
    "host": os.environ.get("LEDGER_MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("LEDGER_MYSQL_PORT", "3306")),
    "user": os.environ.get("LEDGER_MYSQL_USER", "ledger"),
    "password": os.environ.get("LEDGER_MYSQL_PASSWORD", "ledger"),
    "database": os.environ.get("LEDGER_MYSQL_DB", "ledgerstream"),
}
PAYMENTS_TABLE = os.environ.get("LEDGER_TABLE", "payments")
