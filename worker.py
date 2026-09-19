import logging
import time
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

import aws_services
from database import SessionLocal
from kafka_client import create_consumer
from models import EntryType, LedgerEntry, Notification, Payment, PaymentStatus
from redis_client import set_payment_status

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def process_payment(payment_id: uuid.UUID | str, db: Session) -> Payment | None:
    payment_id = uuid.UUID(str(payment_id))
    payment = db.get(Payment, payment_id)
    if payment is None:
        logger.warning("Payment %s does not exist", payment_id)
        return None
    if payment.status in (PaymentStatus.SUCCESS, PaymentStatus.FAILED):
        logger.info("Payment %s is already complete; ignoring event", payment_id)
        return payment

    started = time.perf_counter()
    payment.status = PaymentStatus.PROCESSING
    db.commit()
    set_payment_status(payment.id, PaymentStatus.PROCESSING.value)

    # Row locks make the balance check and both balance updates one transaction.
    payment = db.execute(
        select(Payment).where(Payment.id == payment_id).with_for_update()
    ).scalar_one()
    customer = payment.customer
    merchant = payment.merchant

    if customer.balance < payment.amount:
        payment.status = PaymentStatus.FAILED
        payment.failure_reason = "Insufficient balance"
        message = "Payment failed because of insufficient balance"
    else:
        customer.balance -= payment.amount
        merchant.balance += payment.amount
        db.add_all(
            [
                LedgerEntry(
                    payment_id=payment.id,
                    account_id=customer.id,
                    entry_type=EntryType.DEBIT,
                    amount=payment.amount,
                ),
                LedgerEntry(
                    payment_id=payment.id,
                    account_id=merchant.id,
                    entry_type=EntryType.CREDIT,
                    amount=payment.amount,
                ),
            ]
        )
        payment.status = PaymentStatus.SUCCESS
        message = "Payment processed successfully"

    payment.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(payment)
    set_payment_status(payment.id, payment.status.value)

    if payment.status == PaymentStatus.SUCCESS:
        receipt = {
            "payment_id": str(payment.id),
            "customer_account_id": str(payment.customer_account_id),
            "merchant_account_id": str(payment.merchant_account_id),
            "amount": str(payment.amount),
            "currency": payment.currency,
            "status": payment.status.value,
            "processed_at": payment.updated_at.isoformat(),
        }
        try:
            payment.receipt_s3_key = aws_services.store_receipt(str(payment.id), receipt)
            db.commit()
        except Exception:
            logger.exception("Receipt storage failed for payment %s", payment.id)

    notification = Notification(payment_id=payment.id, message=message)
    db.add(notification)
    db.commit()
    try:
        aws_services.send_notification(
            {
                "notification_id": str(notification.id),
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "amount": str(payment.amount),
                "currency": payment.currency,
                "reason": payment.failure_reason or "",
            }
        )
    except Exception:
        logger.exception("Notification queueing failed for payment %s", payment.id)

    succeeded = payment.status == PaymentStatus.SUCCESS
    aws_services.put_metric("PaymentsSucceeded" if succeeded else "PaymentsFailed")
    aws_services.put_metric(
        "PaymentProcessingTime", (time.perf_counter() - started) * 1000, "Milliseconds"
    )
    logger.info("Payment %s completed with status %s", payment.id, payment.status.value)
    return payment


def run_worker() -> None:
    logger.info("Payment worker started")
    for kafka_message in create_consumer():
        event = kafka_message.value
        if event.get("event_type") != "PAYMENT_CREATED":
            continue
        with SessionLocal() as db:
            try:
                process_payment(uuid.UUID(event["payment_id"]), db)
            except Exception:
                db.rollback()
                logger.exception("Could not process event %s", event)


if __name__ == "__main__":
    run_worker()
