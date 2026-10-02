"""Helpers for writing events to the outbox table. relay.py publishes them to Kafka."""
import uuid
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from kafka_client import PAYMENT_EVENTS_TOPIC
from models import OutboxEvent


def add_payment_event(db: Session, event_type: str, payment_id: uuid.UUID, **fields: str) -> OutboxEvent:
    """Adds the event to the session; it is saved by the caller's commit, together with the change."""
    event = OutboxEvent(
        topic=PAYMENT_EVENTS_TOPIC,
        key=str(payment_id),
        payload={
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "payment_id": str(payment_id),
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            **fields,
        },
    )
    db.add(event)
    return event
