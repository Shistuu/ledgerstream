"""Payment event schema and validation."""
import json
from datetime import timezone
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StrictInt, ValidationError


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    loan_id: str = Field(min_length=1, max_length=64)
    merchant_id: str = Field(min_length=1, max_length=64)
    amount_cents: StrictInt = Field(gt=0)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    occurred_at: AwareDatetime


class InvalidEvent(Exception):
    """Raised when a message can't be turned into a PaymentEvent."""


def parse_event(raw: bytes) -> PaymentEvent:
    """Decode and validate one Kafka message value. Raises InvalidEvent with a reason."""
    if raw is None:
        raise InvalidEvent("empty message value")
    try:
        data = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidEvent(f"not valid JSON: {e}") from None
    if not isinstance(data, dict):
        raise InvalidEvent("JSON value is not an object")
    try:
        return PaymentEvent.model_validate(data)
    except ValidationError as e:
        reasons = "; ".join(
            f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}" for err in e.errors()
        )
        raise InvalidEvent(reasons) from None


def to_row(event: PaymentEvent) -> tuple:
    """Row tuple in the column order used by db.insert_payments."""
    occurred_utc = event.occurred_at.astimezone(timezone.utc).replace(tzinfo=None)
    return (
        str(event.event_id),
        event.loan_id,
        event.merchant_id,
        event.amount_cents,
        event.currency,
        occurred_utc,
    )
