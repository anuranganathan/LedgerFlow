"""Kafka consumer that settles payments.

Delivery guarantees:
  - The Kafka offset is committed only after an event has been fully handled, so an event is
    redelivered if the worker crashes (at-least-once delivery).
  - Handling an event is idempotent: money moves at most once per payment, and receipts and
    notifications are only created if they are missing. A redelivered event is harmless.
  - A failing event is retried with backoff, then sent to the dead-letter topic so it can't
    block the events behind it.
"""
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka.structs import OffsetAndMetadata, TopicPartition
from sqlalchemy import select
from sqlalchemy.orm import Session

import aws_services
import kafka_client
from database import SessionLocal
from models import Account, EntryType, LedgerEntry, Notification, Payment, PaymentStatus, utc_now
from redis_client import set_payment_status

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
MAX_ATTEMPTS = int(os.getenv("WORKER_MAX_ATTEMPTS", "3"))
RETRY_BASE_SECONDS = float(os.getenv("WORKER_RETRY_BASE_SECONDS", "1"))


def lock(db: Session, model, ids: list[uuid.UUID]) -> list:
    """SELECT ... FOR UPDATE, always in ID order so two transactions can't deadlock.

    populate_existing makes SQLAlchemy use the values read under the lock, not older copies
    already loaded in the session.
    """
    return db.scalars(
        select(model).where(model.id.in_(ids)).order_by(model.id)
        .with_for_update().execution_options(populate_existing=True)
    ).all()


def settle(db: Session, payment: Payment) -> None:
    """Moves the money (or fails the payment) in the caller's transaction."""
    accounts = {account.id: account for account in lock(
        db, Account, [payment.customer_account_id, payment.merchant_account_id]
    )}
    customer = accounts[payment.customer_account_id]
    merchant = accounts[payment.merchant_account_id]
    if customer.balance < payment.amount:
        payment.status = PaymentStatus.FAILED
        payment.failure_reason = "Insufficient balance"
        message = "Payment failed because of insufficient balance"
    else:
        customer.balance -= payment.amount
        merchant.balance += payment.amount
        db.add_all([
            LedgerEntry(payment_id=payment.id, account_id=customer.id,
                        entry_type=EntryType.DEBIT, amount=payment.amount),
            LedgerEntry(payment_id=payment.id, account_id=merchant.id,
                        entry_type=EntryType.CREDIT, amount=payment.amount),
        ])
        payment.status = PaymentStatus.SUCCESS
        message = "Payment processed successfully"
    payment.updated_at = utc_now()
    # Saved in the same transaction as the result, so a notification is never forgotten.
    db.add(Notification(payment_id=payment.id, message=message))


def as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def process_payment(payment_id: uuid.UUID | str, db: Session) -> Payment | None:
    """Brings a payment to its final state. Safe to call any number of times."""
    payment_id = uuid.UUID(str(payment_id))
    # Locking the payment first means two workers can't settle the same payment at once:
    # the second one waits, then sees the final status and skips the money movement.
    payments = lock(db, Payment, [payment_id])
    if not payments:
        db.rollback()
        logger.warning("Payment %s does not exist", payment_id)
        return None
    payment = payments[0]

    if payment.status == PaymentStatus.PENDING:
        settle(db, payment)
        db.commit()
        set_payment_status(payment.id, payment.status.value)
        aws_services.put_metric(
            "PaymentsSucceeded" if payment.status == PaymentStatus.SUCCESS else "PaymentsFailed"
        )
        waited = datetime.now(timezone.utc) - as_utc(payment.created_at)
        aws_services.put_metric("PaymentProcessingTime", waited.total_seconds() * 1000, "Milliseconds")
        logger.info("Payment %s completed with status %s", payment.id, payment.status.value)
    else:
        db.commit()  # release the lock
        logger.info("Payment %s is already %s; finishing side effects only",
                    payment.id, payment.status.value)

    complete_side_effects(db, payment)
    return payment


def complete_side_effects(db: Session, payment: Payment) -> None:
    """Queues the notification and stores the receipt, if that hasn't happened yet.

    These run after the money transaction. If one fails, the exception makes the event be
    retried; the money step is then skipped and only the missing side effects are redone.
    """
    # The notification goes first: telling the customer matters more than the receipt.
    notification = db.scalar(select(Notification).where(Notification.payment_id == payment.id))
    if notification is not None and notification.enqueued_at is None:
        aws_services.send_notification({
            "notification_id": str(notification.id),
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency,
            "reason": payment.failure_reason or "",
        })
        notification.enqueued_at = utc_now()
        db.commit()

    if payment.status == PaymentStatus.SUCCESS and payment.receipt_s3_key is None:
        payment.receipt_s3_key = aws_services.store_receipt(str(payment.id), {
            "payment_id": str(payment.id),
            "customer_account_id": str(payment.customer_account_id),
            "merchant_account_id": str(payment.merchant_account_id),
            "amount": str(payment.amount),
            "currency": payment.currency,
            "status": payment.status.value,
            "processed_at": as_utc(payment.updated_at).isoformat(),
        })
        db.commit()


def invalid_reason(event: dict[str, Any]) -> str | None:
    if "_invalid" in event:
        return "Message is not a JSON object"
    if event.get("event_type") != "PAYMENT_CREATED":
        return f"Unknown event type: {event.get('event_type')!r}"
    try:
        uuid.UUID(str(event.get("payment_id")))
    except ValueError:
        return f"Invalid payment_id: {event.get('payment_id')!r}"
    return None


def handle_event(event: dict[str, Any]) -> None:
    """Processes one event with retries. Returns only when the event is done or dead-lettered."""
    reason = invalid_reason(event)
    if reason:
        dead_letter(event, reason, attempts=0)
        return
    for attempt in range(1, MAX_ATTEMPTS + 1):
        with SessionLocal() as db:
            try:
                process_payment(event["payment_id"], db)
                return
            except Exception as exc:
                db.rollback()
                error = f"{type(exc).__name__}: {exc}"
                logger.exception("Attempt %d/%d failed for event %s", attempt, MAX_ATTEMPTS, event)
        if attempt < MAX_ATTEMPTS:
            time.sleep(RETRY_BASE_SECONDS * 2 ** (attempt - 1))
    dead_letter(event, error, MAX_ATTEMPTS)


def dead_letter(event: dict[str, Any], error: str, attempts: int) -> None:
    """Moves an event to the dead-letter topic.

    If this publish fails the exception stops the worker before the offset is committed, so the
    event is not lost: it is processed again when the worker restarts.
    """
    kafka_client.publish(kafka_client.PAYMENT_EVENTS_DLQ_TOPIC, str(event.get("payment_id", "")), {
        "event": event,
        "error": error[:1000],
        "attempts": attempts,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    })
    aws_services.put_metric("PaymentEventsDeadLettered")
    logger.error("Event sent to %s: %s (%s)", kafka_client.PAYMENT_EVENTS_DLQ_TOPIC, event, error)


def consume_batch(consumer) -> int:
    """Handles one poll() batch, committing each record's offset right after it is handled."""
    batch = consumer.poll(timeout_ms=1000)
    handled = 0
    for partition, records in batch.items():
        for record in records:
            handle_event(record.value)
            consumer.commit({
                TopicPartition(partition.topic, partition.partition):
                    OffsetAndMetadata(record.offset + 1, "", -1)
            })
            handled += 1
    return handled


def run_worker() -> None:
    logger.info("Payment worker started")
    consumer = kafka_client.create_consumer()
    try:
        while True:
            consume_batch(consumer)
    finally:
        consumer.close()


if __name__ == "__main__":
    run_worker()
