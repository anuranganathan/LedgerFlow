import enum
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, Enum, ForeignKey, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AccountType(str, enum.Enum):
    CUSTOMER = "CUSTOMER"
    MERCHANT = "MERCHANT"


class PaymentStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class EntryType(str, enum.Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class NotificationStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    SENT = "SENT"


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100))
    account_type: Mapped[AccountType] = mapped_column(Enum(AccountType))
    balance: Mapped[Decimal] = mapped_column(Numeric(14, 2), default=Decimal("0.00"))
    currency: Mapped[str] = mapped_column(String(3), default="INR")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    customer_account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id"))
    merchant_account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id"))
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str | None] = mapped_column(String(200), nullable=True)
    status: Mapped[PaymentStatus] = mapped_column(Enum(PaymentStatus), default=PaymentStatus.PENDING)
    failure_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    receipt_s3_key: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
    customer: Mapped[Account] = relationship(foreign_keys=[customer_account_id])
    merchant: Mapped[Account] = relationship(foreign_keys=[merchant_account_id])


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.id"))
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id"))
    entry_type: Mapped[EntryType] = mapped_column(Enum(EntryType))
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.id"))
    message: Mapped[str] = mapped_column(String(300))
    status: Mapped[NotificationStatus] = mapped_column(
        Enum(NotificationStatus), default=NotificationStatus.QUEUED
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
