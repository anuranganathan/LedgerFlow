"""Checks that the books balance, and flags anything stuck. Runs every few minutes.

Checks:
  1. Every account's balance equals its ledger entries (credits minus debits).
  2. For each currency, total debits equal total credits.
  3. Every successful payment has exactly one DEBIT and one CREDIT of its amount; failed and
     pending payments have none.
  4. Every payment's refunded_amount equals the sum of its successful refunds.
  5. Nothing is stuck: payments/refunds PENDING, or outbox events unpublished, for over 5 minutes.

Problems are published as CloudWatch metrics (LedgerMismatches, StuckItems) with alarms, so a
person is told. The job never "fixes" money by itself: a mismatch needs investigation.
"""
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import case, delete, func, select
from sqlalchemy.orm import Session

import aws_services
import heartbeat
from database import SessionLocal
import webhooks
from models import (
    Account, DeliveryStatus, EntryType, IdempotencyKey, LedgerEntry, OutboxEvent, Payment, PaymentStatus, RefreshToken,
    Refund, RefundStatus, WebhookDelivery,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
INTERVAL_SECONDS = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "300"))
STUCK_AFTER = timedelta(minutes=5)
KEEP_PUBLISHED_EVENTS = timedelta(days=7)
KEEP_IDEMPOTENCY_KEYS = timedelta(hours=24)
MAX_LISTED = 20
CENT = Decimal("0.01")


def money(value) -> Decimal:
    # SQLite returns sums as floats; PostgreSQL returns exact decimals.
    return Decimal(str(value or 0)).quantize(CENT)


def signed_amount():
    return case((LedgerEntry.entry_type == EntryType.CREDIT, LedgerEntry.amount), else_=-LedgerEntry.amount)


def account_mismatches(db: Session) -> list[dict]:
    rows = db.execute(
        select(Account.id, Account.balance, func.sum(signed_amount()))
        .outerjoin(LedgerEntry, LedgerEntry.account_id == Account.id)
        .group_by(Account.id, Account.balance)
    ).all()
    return [
        {"account_id": str(account_id), "balance": str(money(balance)), "ledger": str(money(total))}
        for account_id, balance, total in rows if money(balance) != money(total)
    ]


def currency_imbalances(db: Session) -> list[dict]:
    totals: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {"DEBIT": Decimal("0.00"), "CREDIT": Decimal("0.00")}
    )
    for currency, entry_type, total in db.execute(
        select(Account.currency, LedgerEntry.entry_type, func.sum(LedgerEntry.amount))
        .join(Account, Account.id == LedgerEntry.account_id)
        .group_by(Account.currency, LedgerEntry.entry_type)
    ):
        totals[currency][entry_type.value] = money(total)
    return [
        {"currency": currency, "debits": str(sides["DEBIT"]), "credits": str(sides["CREDIT"])}
        for currency, sides in totals.items() if sides["DEBIT"] != sides["CREDIT"]
    ]


def payment_mismatches(db: Session) -> list[dict]:
    # The payment's own entries, not its refunds'.
    entries = (
        select(
            LedgerEntry.payment_id,
            func.sum(case((LedgerEntry.entry_type == EntryType.DEBIT, LedgerEntry.amount), else_=0)).label("debits"),
            func.sum(case((LedgerEntry.entry_type == EntryType.CREDIT, LedgerEntry.amount), else_=0)).label("credits"),
            func.count().label("count"),
        )
        .where(LedgerEntry.payment_id.is_not(None), LedgerEntry.refund_id.is_(None))
        .group_by(LedgerEntry.payment_id)
        .subquery()
    )
    problems = []
    for payment_id, status, amount, debits, credits, count in db.execute(
        select(Payment.id, Payment.status, Payment.amount, entries.c.debits, entries.c.credits, entries.c.count)
        .outerjoin(entries, entries.c.payment_id == Payment.id)
    ):
        if status == PaymentStatus.SUCCESS:
            ok = count == 2 and money(debits) == money(credits) == money(amount)
        else:
            ok = not count
        if not ok:
            problems.append({"payment_id": str(payment_id), "status": status.value,
                             "entries": count or 0, "amount": str(money(amount))})
    return problems


def refund_mismatches(db: Session) -> list[dict]:
    refunded = (
        select(Refund.payment_id, func.sum(Refund.amount).label("total"))
        .where(Refund.status == RefundStatus.SUCCESS)
        .group_by(Refund.payment_id)
        .subquery()
    )
    return [
        {"payment_id": str(payment_id), "refunded_amount": str(money(recorded)),
         "successful_refunds": str(money(total))}
        for payment_id, recorded, total in db.execute(
            select(Payment.id, Payment.refunded_amount, refunded.c.total)
            .outerjoin(refunded, refunded.c.payment_id == Payment.id)
        )
        if money(recorded) != money(total)
    ]


def stuck_counts(db: Session, now: datetime) -> dict[str, int]:
    cutoff = now - STUCK_AFTER
    count = lambda query: db.scalar(select(func.count()).select_from(query.subquery()))  # noqa: E731
    return {
        "stuck_payments": count(select(Payment.id).where(
            Payment.status == PaymentStatus.PENDING, Payment.created_at < cutoff)),
        "stuck_refunds": count(select(Refund.id).where(
            Refund.status == RefundStatus.PENDING, Refund.created_at < cutoff)),
        "stale_outbox_events": count(select(OutboxEvent.id).where(
            OutboxEvent.published_at.is_(None), OutboxEvent.created_at < cutoff)),
    }


def run_checks(db: Session) -> dict:
    now = datetime.now(timezone.utc)
    mismatches = {
        "account_mismatches": account_mismatches(db),
        "currency_imbalances": currency_imbalances(db),
        "payment_mismatches": payment_mismatches(db),
        "refund_mismatches": refund_mismatches(db),
    }
    stuck = stuck_counts(db, now)
    db.rollback()  # read-only; end the transaction
    ledger_problems = sum(len(items) for items in mismatches.values())
    return {
        "ok": ledger_problems == 0 and not any(stuck.values()),
        "checked_at": now.isoformat(),
        "ledger_problems": ledger_problems,
        **{name: items[:MAX_LISTED] for name, items in mismatches.items()},
        **stuck,
    }


def delete_expired_rows(db: Session) -> int:
    """Keeps housekeeping tables small: old published events, idempotency keys and sessions."""
    now = datetime.now(timezone.utc)
    deleted = sum(db.execute(statement).rowcount for statement in [
        delete(OutboxEvent).where(
            OutboxEvent.published_at.is_not(None), OutboxEvent.published_at < now - KEEP_PUBLISHED_EVENTS),
        delete(IdempotencyKey).where(IdempotencyKey.created_at < now - KEEP_IDEMPOTENCY_KEYS),
        delete(RefreshToken).where(RefreshToken.expires_at < now),
    ])
    db.commit()
    return deleted


def queue_forgotten_webhooks(db: Session) -> int:
    """Queues webhook deliveries saved but never put on SQS (e.g. SQS was down during a retry)."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=1)
    deliveries = db.scalars(select(WebhookDelivery).where(
        WebhookDelivery.status == DeliveryStatus.PENDING, WebhookDelivery.enqueued_at.is_(None),
        WebhookDelivery.created_at < cutoff,
    ).limit(100)).all()
    for delivery in deliveries:
        webhooks.queue(db, delivery)
    return len(deliveries)


def run_once() -> dict:
    with SessionLocal() as db:
        report = run_checks(db)
        deleted = delete_expired_rows(db)
        requeued = queue_forgotten_webhooks(db)
    if requeued:
        logger.warning("Queued %d webhook deliveries that were never queued", requeued)
    stuck = report["stuck_payments"] + report["stuck_refunds"] + report["stale_outbox_events"]
    aws_services.put_metric("LedgerMismatches", report["ledger_problems"])
    aws_services.put_metric("StuckItems", stuck)
    if report["ok"]:
        logger.info("Reconciliation passed (deleted %d expired rows)", deleted)
    else:
        logger.error("Reconciliation found problems: %s", report)
    return report


def run_reconciler() -> None:
    logger.info("Reconciler started; checking every %ds", INTERVAL_SECONDS)
    while True:
        heartbeat.beat()
        try:
            run_once()
        except Exception:
            logger.exception("Reconciliation run failed")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    run_reconciler()
