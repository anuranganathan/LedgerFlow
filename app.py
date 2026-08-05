import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

import aws_services
from database import Base, engine, get_db
from kafka_client import publish_payment_created
from models import Account, AccountType, LedgerEntry, Notification, NotificationStatus, Payment
from redis_client import get_payment_status, set_payment_status
from schemas import (
    AccountCreate, AccountResponse, FundRequest, LedgerResponse, NotificationResponse,
    PaymentAccepted, PaymentCreate, PaymentResponse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="LedgerFlow",
    description="Educational event-driven payment processing simulator",
    version="1.0.0",
    lifespan=lifespan,
)


def get_or_404(db: Session, model, object_id: uuid.UUID):
    item = db.get(model, object_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"{model.__name__} not found")
    return item


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/accounts", response_model=AccountResponse, status_code=201)
def create_account(data: AccountCreate, db: Session = Depends(get_db)):
    account = Account(name=data.name, account_type=data.account_type, currency=data.currency.upper())
    db.add(account)
    db.commit()
    db.refresh(account)
    return account


@app.get("/accounts", response_model=list[AccountResponse])
def list_accounts(db: Session = Depends(get_db)):
    return db.scalars(select(Account).order_by(Account.created_at)).all()


@app.get("/accounts/{account_id}", response_model=AccountResponse)
def get_account(account_id: uuid.UUID, db: Session = Depends(get_db)):
    return get_or_404(db, Account, account_id)


@app.post("/accounts/{account_id}/fund", response_model=AccountResponse)
def fund_account(account_id: uuid.UUID, data: FundRequest, db: Session = Depends(get_db)):
    account = get_or_404(db, Account, account_id)
    account.balance += data.amount
    db.commit()
    db.refresh(account)
    return account


@app.post("/payments", response_model=PaymentAccepted, status_code=202)
def create_payment(data: PaymentCreate, db: Session = Depends(get_db)):
    customer = get_or_404(db, Account, data.customer_account_id)
    merchant = get_or_404(db, Account, data.merchant_account_id)
    if customer.account_type != AccountType.CUSTOMER:
        raise HTTPException(status_code=400, detail="customer_account_id must be a CUSTOMER")
    if merchant.account_type != AccountType.MERCHANT:
        raise HTTPException(status_code=400, detail="merchant_account_id must be a MERCHANT")
    currency = data.currency.upper()
    if customer.currency != currency or merchant.currency != currency:
        raise HTTPException(status_code=400, detail="Payment and account currencies must match")
    payment = Payment(
        customer_account_id=customer.id, merchant_account_id=merchant.id,
        amount=data.amount, currency=currency, description=data.description,
    )
    db.add(payment)
    db.commit()
    db.refresh(payment)
    set_payment_status(payment.id, payment.status.value)
    try:
        publish_payment_created(payment.id)
    except Exception as exc:
        logging.exception("Kafka publish failed for payment %s", payment.id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Payment saved as PENDING, but Kafka is unavailable: {payment.id}",
        ) from exc
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status,
        message="Payment accepted for asynchronous processing",
    )


@app.get("/payments", response_model=list[PaymentResponse])
def list_payments(db: Session = Depends(get_db)):
    return db.scalars(select(Payment).order_by(Payment.created_at.desc())).all()


@app.get("/payments/{payment_id}", response_model=PaymentResponse)
def get_payment(payment_id: uuid.UUID, db: Session = Depends(get_db)):
    payment = get_or_404(db, Payment, payment_id)
    cached_status = get_payment_status(payment_id)
    response = PaymentResponse.model_validate(payment)
    return response.model_copy(update={"status": cached_status}) if cached_status else response


@app.get("/payments/{payment_id}/ledger", response_model=list[LedgerResponse])
def get_ledger(payment_id: uuid.UUID, db: Session = Depends(get_db)):
    get_or_404(db, Payment, payment_id)
    return db.scalars(select(LedgerEntry).where(LedgerEntry.payment_id == payment_id)).all()


@app.get("/payments/{payment_id}/receipt")
def get_receipt(payment_id: uuid.UUID, db: Session = Depends(get_db)):
    payment = get_or_404(db, Payment, payment_id)
    if not payment.receipt_s3_key:
        raise HTTPException(status_code=404, detail="Receipt is not available")
    return aws_services.receipt_location(payment.receipt_s3_key)


@app.get("/notifications", response_model=list[NotificationResponse])
def list_notifications(db: Session = Depends(get_db)):
    return db.scalars(select(Notification).order_by(Notification.created_at.desc())).all()


@app.get("/notifications/process-one")
def process_one_notification(db: Session = Depends(get_db)):
    queued = aws_services.receive_notification()
    if queued is None:
        return {"message": "No notification available"}
    body = queued["body"]
    print(body)
    notification = db.scalar(
        select(Notification).where(
            Notification.payment_id == uuid.UUID(body["payment_id"]),
            Notification.status == NotificationStatus.QUEUED,
        ).order_by(Notification.created_at)
    )
    if notification:
        notification.status = NotificationStatus.SENT
        db.commit()
    aws_services.delete_notification(queued.get("receipt_handle"))
    return body
