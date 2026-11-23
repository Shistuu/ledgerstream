import json
import random
import uuid
from datetime import datetime, timezone

import pytest

from ledgerstream.models import InvalidEvent, parse_event, to_row
from ledgerstream.producer import break_event, fake_event


def good_event(**changes):
    event = {
        "event_id": str(uuid.uuid4()),
        "loan_id": "loan-42",
        "merchant_id": "m-7",
        "amount_cents": 12_345,
        "currency": "USD",
        "occurred_at": "2026-11-01T12:00:00+00:00",
    }
    event.update(changes)
    return event


def encode(event):
    return json.dumps(event).encode()


def test_valid_event_parses():
    e = parse_event(encode(good_event()))
    assert e.amount_cents == 12_345
    assert e.currency == "USD"


def test_row_converts_time_to_utc():
    e = parse_event(encode(good_event(occurred_at="2026-11-01T07:00:00-05:00")))
    row = to_row(e)
    assert row[5] == datetime(2026, 11, 1, 12, 0, 0)


@pytest.mark.parametrize("changes, field", [
    ({"amount_cents": -5}, "amount_cents"),
    ({"amount_cents": 0}, "amount_cents"),
    ({"amount_cents": "100"}, "amount_cents"),     # strings are not silently coerced
    ({"amount_cents": 10.5}, "amount_cents"),
    ({"currency": "usd"}, "currency"),
    ({"currency": "dollars"}, "currency"),
    ({"event_id": "abc"}, "event_id"),
    ({"loan_id": ""}, "loan_id"),
    ({"occurred_at": "2026-11-01T12:00:00"}, "occurred_at"),  # no timezone
    ({"extra": 1}, "extra"),
])
def test_bad_fields_rejected_with_reason(changes, field):
    with pytest.raises(InvalidEvent) as exc:
        parse_event(encode(good_event(**changes)))
    assert field in str(exc.value)


def test_missing_field_rejected():
    event = good_event()
    del event["merchant_id"]
    with pytest.raises(InvalidEvent, match="merchant_id"):
        parse_event(encode(event))


@pytest.mark.parametrize("raw", [b"{not json", b"\xff\xfe", b"[1, 2]", b"null", None])
def test_garbage_rejected(raw):
    with pytest.raises(InvalidEvent):
        parse_event(raw)


def test_producer_events_are_valid_and_broken_ones_are_not():
    rng = random.Random(1)
    for _ in range(200):
        event = fake_event(rng)
        parse_event(encode(event))
        with pytest.raises(InvalidEvent):
            parse_event(break_event(event, rng))
