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
import events
import kafka_client
import webhooks
from database import SessionLocal
from ledger import lock, transfer
from models import Account, Notification, Payment, PaymentStatus, Refund, RefundStatus, utc_now

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
MAX_ATTEMPTS = int(os.getenv("WORKER_MAX_ATTEMPTS", "3"))
RETRY_BASE_SECONDS = float(os.getenv("WORKER_RETRY_BASE_SECONDS", "1"))


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
        transfer(db, debit=customer, credit=merchant, amount=payment.amount, payment_id=payment.id)
        payment.status = PaymentStatus.SUCCESS
        message = "Payment processed successfully"
    payment.updated_at = utc_now()
    # Saved in the same transaction as the result, so a notification is never forgotten.
    db.add(Notification(payment_id=payment.id, message=message))
    webhooks.add_delivery(db, payment, None)


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
    """Sends updates, queues the notification and webhook, and stores the receipt, if not done yet.

    These run after the money transaction. If one fails, the exception makes the event be
    retried; the money step is then skipped and only the missing side effects are redone.
    """
    # Telling people comes first: the live update, Slack, then the merchant's webhook.
    push_live_update(db, payment, None)
    queue_notification(db, payment, None)
    webhooks.queue_deliveries(db, payment.id, None)

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


def push_live_update(db: Session, payment: Payment, refund: Refund | None) -> None:
    """Tells the customer's and merchant's open dashboards that this payment changed."""
    owners = db.scalars(select(Account.owner_id).where(
        Account.id.in_([payment.customer_account_id, payment.merchant_account_id])
    )).all()
    subject = refund or payment
    events.publish(owners, {
        "type": "refund.updated" if refund else "payment.updated",
        "payment_id": str(payment.id),
        "refund_id": str(refund.id) if refund else None,
        "status": subject.status.value,
        "amount": str(subject.amount),
        "failure_reason": subject.failure_reason,
    })


def queue_notification(db: Session, payment: Payment, refund: Refund | None) -> None:
    """Puts the notification on SQS unless that already happened."""
    notification = db.scalar(select(Notification).where(
        Notification.payment_id == payment.id,
        Notification.refund_id == refund.id if refund else Notification.refund_id.is_(None),
    ))
    if notification is None or notification.enqueued_at is not None:
        return
    subject = refund or payment
    aws_services.send_notification({
        "notification_id": str(notification.id),
        "kind": "REFUND" if refund else "PAYMENT",
        "payment_id": str(payment.id),
        "refund_id": str(refund.id) if refund else "",
        "status": subject.status.value,
        "amount": str(subject.amount),
        "currency": payment.currency,
        "reason": subject.failure_reason or "",
    })
    notification.enqueued_at = utc_now()
    db.commit()


def process_refund(refund_id: uuid.UUID | str, db: Session) -> Refund | None:
    """Sends a refund's money back from the merchant to the customer. Safe to repeat."""
    refund_id = uuid.UUID(str(refund_id))
    found = db.get(Refund, refund_id)
    if found is None:
        db.rollback()
        logger.warning("Refund %s does not exist", refund_id)
        return None
    (payment,) = lock(db, Payment, [found.payment_id])  # payment first, then refund, then accounts
    (refund,) = lock(db, Refund, [refund_id])

    if refund.status == RefundStatus.PENDING:
        accounts = {account.id: account for account in lock(
            db, Account, [payment.customer_account_id, payment.merchant_account_id]
        )}
        merchant = accounts[payment.merchant_account_id]
        if merchant.balance < refund.amount:
            refund.status = RefundStatus.FAILED
            refund.failure_reason = "Merchant has insufficient balance"
            message = "Refund failed because the merchant has insufficient balance"
        else:
            transfer(db, debit=merchant, credit=accounts[payment.customer_account_id],
                     amount=refund.amount, payment_id=payment.id, refund_id=refund.id)
            payment.refunded_amount += refund.amount
            refund.status = RefundStatus.SUCCESS
            message = "Refund processed successfully"
        refund.updated_at = utc_now()
        db.add(Notification(payment_id=payment.id, refund_id=refund.id, message=message))
        webhooks.add_delivery(db, payment, refund)
        db.commit()
        aws_services.put_metric(
            "RefundsSucceeded" if refund.status == RefundStatus.SUCCESS else "RefundsFailed"
        )
        logger.info("Refund %s completed with status %s", refund.id, refund.status.value)
    else:
        db.commit()

    push_live_update(db, payment, refund)
    queue_notification(db, payment, refund)
    webhooks.queue_deliveries(db, payment.id, refund.id)
    return refund


# Each event type, the field holding the ID it's about, and the function that processes it.
HANDLERS = {
    "PAYMENT_CREATED": ("payment_id", process_payment),
    "REFUND_REQUESTED": ("refund_id", process_refund),
}


def invalid_reason(event: dict[str, Any]) -> str | None:
    if "_invalid" in event:
        return "Message is not a JSON object"
    if event.get("event_type") not in HANDLERS:
        return f"Unknown event type: {event.get('event_type')!r}"
    field, _ = HANDLERS[event["event_type"]]
    try:
        uuid.UUID(str(event.get(field)))
    except ValueError:
        return f"Invalid {field}: {event.get(field)!r}"
    return None


def handle_event(event: dict[str, Any]) -> None:
    """Processes one event with retries. Returns only when the event is done or dead-lettered."""
    reason = invalid_reason(event)
    if reason:
        dead_letter(event, reason, attempts=0)
        return
    for attempt in range(1, MAX_ATTEMPTS + 1):
        field, process = HANDLERS[event["event_type"]]
        with SessionLocal() as db:
            try:
                process(event[field], db)
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
