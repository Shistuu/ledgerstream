#!/usr/bin/env bash
# Create the Kafka topics and the MySQL database, user and table.
# Assumes Kafka (localhost:9092) and MySQL are already running, e.g. via
#   brew services start kafka && brew services start mysql
# and that the local MySQL root user has no password (Homebrew's default).
set -euo pipefail

cd "$(dirname "$0")/.."

BOOTSTRAP="${LEDGER_KAFKA_BOOTSTRAP:-localhost:9092}"
DB_NAME="${LEDGER_MYSQL_DB:-ledgerstream}"
DB_USER="${LEDGER_MYSQL_USER:-ledger}"
DB_PASS="${LEDGER_MYSQL_PASSWORD:-ledger}"

echo "== Kafka topics"
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic payments --partitions 6 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic payments.dlq --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic payments | head -1

echo "== MySQL database and user"
mysql -u root <<SQL
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASS}';
CREATE USER IF NOT EXISTS '${DB_USER}'@'127.0.0.1' IDENTIFIED BY '${DB_PASS}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'127.0.0.1';
SQL

echo "== payments table"
PY="${PYTHON:-.venv/bin/python}"
"$PY" -m ledgerstream.db --table payments
