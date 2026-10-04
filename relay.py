"""Publishes events from the outbox table to Kafka.

The API never talks to Kafka directly: it saves each event in the outbox, in the same
transaction as the payment. This process sends those events to Kafka and marks them as
published. If Kafka is down the events wait in PostgreSQL and are sent once it is back.

If the relay crashes after Kafka accepted an event but before marking it, the event is sent
again. That's fine because the worker handles duplicate events safely.
"""
import logging
import time

from sqlalchemy import select
from sqlalchemy.orm import Session

import aws_services
import heartbeat
import kafka_client
from database import SessionLocal
from models import OutboxEvent, utc_now

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
BATCH_SIZE = 100
IDLE_SECONDS = 0.2
MAX_BACKOFF_SECONDS = 30


def publish_pending(db: Session) -> int:
    """Publishes up to BATCH_SIZE unpublished events in order and returns how many were sent."""
    # SKIP LOCKED lets several relays run at once without sending the same rows.
    events = db.scalars(
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at)
        .limit(BATCH_SIZE)
        .with_for_update(skip_locked=True)
    ).all()
    published = 0
    try:
        for event in events:
            kafka_client.publish(event.topic, event.key, event.payload)
            event.published_at = utc_now()
            published += 1
    except Exception as exc:
        # Stop at the first failure so events keep their order; the rest are retried later.
        events[published].attempts += 1
        events[published].last_error = str(exc)[:500]
        raise
    finally:
        db.commit()
    return published


def run_relay() -> None:
    logger.info("Outbox relay started")
    kafka_client.ensure_topics()
    backoff = 1
    while True:
        heartbeat.beat()
        with SessionLocal() as db:
            try:
                published = publish_pending(db)
                backoff = 1
            except Exception as exc:
                logger.warning("Publishing outbox events failed, retrying in %ss: %s", backoff, exc)
                aws_services.put_metric("OutboxPublishFailures")
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                continue
        if published < BATCH_SIZE:
            time.sleep(IDLE_SECONDS)


if __name__ == "__main__":
    run_relay()
