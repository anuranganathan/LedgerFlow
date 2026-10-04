import enum
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    JSON, CheckConstraint, DateTime, Enum, ForeignKey, Index, Integer, Numeric, String, Uuid, text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def enum_column(enum_class: type[enum.Enum]) -> Enum:
    # A fixed length keeps the column the same size on SQLite when enum values change.
    return Enum(enum_class, length=20)


class AccountType(str, enum.Enum):
    CUSTOMER = "CUSTOMER"
    MERCHANT = "MERCHANT"
    # The other side of every top-up: money entering LedgerFlow from outside (a bank, a card).
    # Its balance is negative and equals minus all the money ever added, so the books balance.
    SYSTEM = "SYSTEM"


class PaymentStatus(str, enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class EntryType(str, enum.Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class RefundStatus(str, enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class NotificationStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    SENT = "SENT"


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (
        # A last line of defence: even a bug can't overdraw a customer or merchant.
        CheckConstraint("account_type = 'SYSTEM' OR balance >= 0", name="ck_accounts_balance_non_negative"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100))
    account_type: Mapped[AccountType] = mapped_column(enum_column(AccountType))
    balance: Mapped[Decimal] = mapped_column(Numeric(14, 2), default=Decimal("0.00"))
    currency: Mapped[str] = mapped_column(String(3), default="INR")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint("refunded_amount >= 0 AND refunded_amount <= amount",
                        name="ck_payments_refunded_amount_valid"),
        Index("ix_payments_customer_created", "customer_account_id", "created_at"),
        Index("ix_payments_merchant_created", "merchant_account_id", "created_at"),
        Index("ix_payments_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    customer_account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id"))
    merchant_account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id"))
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str | None] = mapped_column(String(200), nullable=True)
    status: Mapped[PaymentStatus] = mapped_column(enum_column(PaymentStatus), default=PaymentStatus.PENDING)
    refunded_amount: Mapped[Decimal] = mapped_column(
        Numeric(14, 2), default=Decimal("0.00"), server_default="0"
    )
    failure_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    receipt_s3_key: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
    customer: Mapped[Account] = relationship(foreign_keys=[customer_account_id])
    merchant: Mapped[Account] = relationship(foreign_keys=[merchant_account_id])


class TopUp(Base):
    """Money added to an account from outside LedgerFlow."""

    __tablename__ = "top_ups"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_top_ups_amount_positive"),
        Index("ix_top_ups_account_id", "account_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id"))
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    description: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Refund(Base):
    __tablename__ = "refunds"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_refunds_amount_positive"),
        Index("ix_refunds_payment_id", "payment_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.id"))
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    status: Mapped[RefundStatus] = mapped_column(enum_column(RefundStatus), default=RefundStatus.PENDING)
    failure_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class LedgerEntry(Base):
    """One side of a money movement. Every movement writes a DEBIT and a CREDIT of the same amount.

    Each entry belongs to exactly one payment or one top-up. Refund entries also point at the
    payment they refund, so a payment's ledger shows its refunds too.
    """

    __tablename__ = "ledger_entries"
    __table_args__ = (
        CheckConstraint(
            "(payment_id IS NULL) <> (top_up_id IS NULL) AND (refund_id IS NULL OR payment_id IS NOT NULL)",
            name="ck_ledger_entries_one_source",
        ),
        CheckConstraint("amount > 0", name="ck_ledger_entries_amount_positive"),
        Index("ix_ledger_entries_payment_id", "payment_id"),
        Index("ix_ledger_entries_account_created", "account_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("payments.id"), nullable=True)
    refund_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("refunds.id"), nullable=True)
    top_up_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("top_ups.id"), nullable=True)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id"))
    entry_type: Mapped[EntryType] = mapped_column(enum_column(EntryType))
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (Index("ix_notifications_payment_id", "payment_id"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.id"))
    # Set for a refund's notification; empty for the payment's own notification.
    refund_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("refunds.id"), nullable=True)
    message: Mapped[str] = mapped_column(String(300))
    status: Mapped[NotificationStatus] = mapped_column(
        enum_column(NotificationStatus), default=NotificationStatus.QUEUED
    )
    # Set once the message is on the SQS queue, so a retried worker doesn't queue it twice.
    enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class OutboxEvent(Base):
    """An event waiting to be published to Kafka (the transactional outbox pattern).

    It is saved in the same database transaction as the change it describes, so the change
    and its event are stored together or not at all. relay.py publishes it afterwards, so an
    event is never lost when Kafka is down.
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index(
            "ix_outbox_events_unpublished", "created_at",
            postgresql_where=text("published_at IS NULL"), sqlite_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    topic: Mapped[str] = mapped_column(String(100))
    key: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSON)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
