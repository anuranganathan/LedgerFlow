import os
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from models import AccountType, EntryType, NotificationStatus, PaymentStatus, RefundStatus

SUPPORTED_CURRENCIES = set(os.getenv("SUPPORTED_CURRENCIES", "INR,USD,EUR,GBP").split(","))
MAX_TOP_UP = Decimal("1000000.00")


def supported_currency(value: str) -> str:
    value = value.upper()
    if value not in SUPPORTED_CURRENCIES:
        raise ValueError(f"currency must be one of {', '.join(sorted(SUPPORTED_CURRENCIES))}")
    return value


# At most 2 decimal places (paise/cents) and small enough for the NUMERIC(14, 2) columns.
Money = Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2)]
Currency = Annotated[str, AfterValidator(supported_currency)]


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


def user_account_type(value: AccountType) -> AccountType:
    if value == AccountType.SYSTEM:
        raise ValueError("account_type must be CUSTOMER or MERCHANT")
    return value


class AccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    account_type: Annotated[AccountType, AfterValidator(user_account_type)]
    currency: Currency = "INR"


class FundRequest(BaseModel):
    amount: Annotated[Money, Field(le=MAX_TOP_UP)]
    description: str = Field(default="Top-up", min_length=1, max_length=200)


class AccountResponse(ORMModel):
    id: uuid.UUID
    name: str
    account_type: AccountType
    balance: Decimal
    currency: str
    created_at: datetime


class PaymentCreate(BaseModel):
    customer_account_id: uuid.UUID
    merchant_account_id: uuid.UUID
    amount: Money
    currency: Currency = "INR"
    description: str | None = Field(default=None, max_length=200)


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    status_url: str
    message: str


class PaymentResponse(ORMModel):
    id: uuid.UUID
    customer_account_id: uuid.UUID
    merchant_account_id: uuid.UUID
    amount: Decimal
    refunded_amount: Decimal
    currency: str
    description: str | None
    status: PaymentStatus
    failure_reason: str | None
    receipt_s3_key: str | None
    created_at: datetime
    updated_at: datetime


class RefundCreate(BaseModel):
    # Leave out for a full refund of whatever hasn't been refunded yet.
    amount: Money | None = None
    reason: str | None = Field(default=None, max_length=200)


class RefundResponse(ORMModel):
    id: uuid.UUID
    payment_id: uuid.UUID
    amount: Decimal
    reason: str | None
    status: RefundStatus
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime


class LedgerResponse(ORMModel):
    id: uuid.UUID
    payment_id: uuid.UUID | None
    refund_id: uuid.UUID | None
    top_up_id: uuid.UUID | None
    account_id: uuid.UUID
    entry_type: EntryType
    amount: Decimal
    created_at: datetime


class NotificationResponse(ORMModel):
    id: uuid.UUID
    payment_id: uuid.UUID
    refund_id: uuid.UUID | None
    message: str
    status: NotificationStatus
    created_at: datetime


class Page(BaseModel):
    """Query parameters for list endpoints."""

    limit: int = Field(default=50, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
