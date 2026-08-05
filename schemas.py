import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from models import AccountType, EntryType, NotificationStatus, PaymentStatus


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class AccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    account_type: AccountType
    currency: str = Field(default="INR", min_length=3, max_length=3)


class FundRequest(BaseModel):
    amount: Decimal

    @field_validator("amount")
    @classmethod
    def positive_amount(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("amount must be greater than zero")
        return value


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
    amount: Decimal
    currency: str = Field(default="INR", min_length=3, max_length=3)
    description: str | None = Field(default=None, max_length=200)

    @field_validator("amount")
    @classmethod
    def positive_amount(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("amount must be greater than zero")
        return value


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    message: str


class PaymentResponse(ORMModel):
    id: uuid.UUID
    customer_account_id: uuid.UUID
    merchant_account_id: uuid.UUID
    amount: Decimal
    currency: str
    description: str | None
    status: PaymentStatus
    failure_reason: str | None
    receipt_s3_key: str | None
    created_at: datetime
    updated_at: datetime


class LedgerResponse(ORMModel):
    id: uuid.UUID
    payment_id: uuid.UUID
    account_id: uuid.UUID
    entry_type: EntryType
    amount: Decimal
    created_at: datetime


class NotificationResponse(ORMModel):
    id: uuid.UUID
    payment_id: uuid.UUID
    message: str
    status: NotificationStatus
    created_at: datetime
