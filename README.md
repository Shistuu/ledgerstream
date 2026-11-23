# Ledgerstream

A small pipeline for loan payment events: a Python producer writes events to
Kafka, a consumer group reads them and writes to MySQL. Replays and consumer
restarts don't double-count a payment, bad events go to a dead-letter topic,
and each consumer exports its lag as a Prometheus metric.

```
producer.py --> Kafka "payments" (6 partitions, keyed by loan_id)
                    |
              consumer group (consumer.py x N)
                 |                  |
          MySQL `payments`     Kafka "payments.dlq"
      (PRIMARY KEY event_id)   (invalid events + reason)
```

## Event

```json
{"event_id": "uuid", "loan_id": "loan-123", "merchant_id": "m-7",
 "amount_cents": 12345, "currency": "USD", "occurred_at": "2026-11-01T12:00:00+00:00"}
```

Validation (pydantic, `ledgerstream/models.py`): `event_id` must be a UUID,
`amount_cents` a positive integer (strings like `"100"` are rejected, not
coerced), `currency` three uppercase letters, `occurred_at` must carry a
timezone, and unknown fields are rejected.

## Design choices

**At-least-once delivery plus idempotent writes, not Kafka transactions.**
Kafka's exactly-once (transactions + `read_committed`) only covers writes that
go back into Kafka. The output here is MySQL, which can't take part in a Kafka
transaction, so EOS wouldn't cover the part that matters. Instead the
consumer may see an event more than once, and the table makes the second
write a no-op: `event_id` is the primary key and inserts use
`ON DUPLICATE KEY UPDATE event_id = event_id`. I used that instead of
`INSERT IGNORE` because IGNORE also downgrades real errors (truncation, bad
values) to warnings.

**Offsets are committed after the DB commit.** `enable.auto.commit=false`.
For each batch (up to 500 messages) the consumer:

1. writes all valid rows in one MySQL transaction,
2. sends invalid events to `payments.dlq` and waits for the broker ack,
3. commits the Kafka offsets for the batch.

If the process dies before step 3, the batch is read again by whoever owns
the partition next. Step 1 is then a no-op for rows already written. If the
order were reversed (commit offsets, then write), a crash in between would
lose those events for good.

The DLQ has the same at-least-once property: a crash between steps 2 and 3
can send the same dead letter twice. Each DLQ message carries the source
partition and offset so it can be de-duplicated downstream.

Other details: `session.timeout.ms=6000` so a killed consumer's partitions
move to live members within a few seconds; SIGTERM sets a flag, the current
batch finishes and commits, and `close()` leaves the group cleanly.

## Metrics

Each consumer serves `/metrics` (`--metrics-port`, default 8001):

- `ledgerstream_consumer_lag{topic,partition}`: high watermark minus
  committed offset, for the partitions this consumer owns, refreshed every 2s
- `ledgerstream_events_processed_total`: new rows written
- `ledgerstream_duplicates_skipped_total`: valid events whose `event_id` was
  already in the table
- `ledgerstream_dead_lettered_total`

The high watermark comes from librdkafka's cache (updated on every fetch). My
first version asked the broker for it on every refresh, and each query took
about 0.5s per partition on this setup. That cut single-consumer throughput
to around 700-2,400 events/s until I found it.

## Running it

Needs Homebrew Kafka (KRaft mode, which is the default config) and MySQL.

```sh
brew install kafka mysql python@3.12
brew services start kafka
brew services start mysql

python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
./scripts/setup.sh          # topics, database, user, table

.venv/bin/python -m ledgerstream.consumer --metrics-port 8001 &
.venv/bin/python -m ledgerstream.producer --count 10000 --malformed-pct 5 --duplicate-pct 10
curl -s localhost:8001/metrics | grep ^ledgerstream
```

Connection settings come from `LEDGER_*` environment variables
(`ledgerstream/config.py`); defaults match `setup.sh`.

## Tests

```sh
.venv/bin/pytest -v                                  # everything
.venv/bin/pytest -s tests/test_crash_recovery.py     # just the failure test, with output
```

- `test_validation.py`: schema rules, garbage input, producer's malformed events
- `test_db.py` (real MySQL, throwaway table): replaying a batch inserts
  nothing, partial overlap only inserts new rows, duplicates inside one batch,
  replay doesn't overwrite the first write, a failed batch rolls back
- `test_crash_recovery.py` (real Kafka + MySQL, unique topic/table/group per run):
  1. start 3 consumer processes
  2. produce 20,000 messages at 2,000/s (10% re-sent duplicates, 5% malformed)
  3. SIGKILL consumer 0 at 3s and consumer 1 at 6s, restart both 2s later
  4. wait for group lag (from Kafka) to reach 0
  5. SIGTERM everything and check:
     - rows in MySQL equal the number of unique valid events
     - `SUM(amount_cents)` matches what the producer sent
     - distinct DLQ source offsets equal the malformed count
     - the lag gauge reads 0
     - every consumer exits 0

A typical run:

```
produced=20000 unique_valid=16991 duplicates=2025 malformed=984
killed=['c0', 'c1'] rows_in_mysql_at_each_kill=[4297, 7777]
rows=16991 distinct_event_ids=16991 amount_sum_matches=True
dlq_messages=984 dlq_distinct_source_offsets=984
exit_codes_after_sigterm={'c2': 0, 'c0-restarted': 0, 'c1-restarted': 0}
```

## Throughput

`scripts/loadtest.py` pre-loads a fresh 6-partition topic with valid events,
starts N consumer processes, and polls `COUNT(*)` every 0.25s until all rows
are in MySQL. It reports two rates:

- **wall**: start of consumers to last row, including process start-up and
  the consumer group join (several seconds)
- **steady**: first row to last row

Machine: Apple M5 Pro (15 cores), 24 GB RAM, macOS 26.7.3. Kafka 4.4.0
(single KRaft broker) and MySQL 26.10.0 (default Homebrew config,
`innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`) on the same laptop.
Python 3.12, batch size 500.

`python scripts/loadtest.py --events 500000 --consumers 1 3 --repeats 3`:

| consumers | wall (events/s), 3 runs | steady (events/s), 3 runs |
|-----------|-------------------------|---------------------------|
| 1         | 38,341 / 38,090 / 39,687 | 52,909 / 52,348 / 55,460 |
| 3         | 45,742 / 45,127 / 44,036 | 117,412 / 113,213 / 105,979 |

Caveats: this measures draining a backlog, not a live stream at a fixed
input rate. With 3 consumers the steady window is only about 4-5 seconds, so
the 0.25s polling interval adds a few percent of error. The wall number for 3
consumers is mostly group-join time. Everything shares one laptop, so these
numbers say little about a real cluster.

## Layout

```
ledgerstream/config.py       env-based settings
ledgerstream/models.py       event schema + parse/validate
ledgerstream/db.py           table DDL, idempotent batch insert
ledgerstream/producer.py     fake event generator (--malformed-pct, --duplicate-pct, --rate)
ledgerstream/consumer.py     consumer loop, DLQ, metrics, SIGTERM handling
ledgerstream/kafka_admin.py  topic create/delete, group lag, read a topic
scripts/setup.sh             topics + MySQL database/user/table
scripts/loadtest.py          throughput measurement
tests/                       unit + crash-recovery tests
```
