import logging
import uuid

from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from sqlalchemy import select, text
from sqlalchemy.orm import Session

import aws_services
from database import get_db
from models import Account, AccountType, LedgerEntry, Notification, Payment, PaymentStatus
from outbox import add_payment_event
from redis_client import get_payment_status, set_payment_status
from schemas import (
    AccountCreate, AccountResponse, FundRequest, LedgerResponse, NotificationResponse,
    PaymentAccepted, PaymentCreate, PaymentResponse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# The database schema is created and upgraded by Alembic migrations (alembic upgrade head),
# which run before the app starts. See migrations/.
app = FastAPI(
    title="LedgerFlow",
    description="Educational event-driven payment processing simulator",
    version="1.0.0",
)


def get_or_404(db: Session, model, object_id: uuid.UUID):
    item = db.get(model, object_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"{model.__name__} not found")
    return item


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse("static/index.html")


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, str]:
    # Used by Docker and the deploy step to check the API can reach the database.
    db.execute(text("SELECT 1"))
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
    # Lock the row so a payment settling at the same moment can't overwrite this update.
    account = db.scalar(
        select(Account).where(Account.id == account_id).with_for_update()
        .execution_options(populate_existing=True)
    )
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account.balance += data.amount
    db.commit()
    db.refresh(account)
    return account


@app.post("/payments", response_model=PaymentAccepted, status_code=202)
def create_payment(data: PaymentCreate, response: Response, db: Session = Depends(get_db)):
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
        id=uuid.uuid4(), customer_account_id=customer.id, merchant_account_id=merchant.id,
        amount=data.amount, currency=currency, description=data.description,
        status=PaymentStatus.PENDING,
    )
    db.add(payment)
    # The payment and its PAYMENT_CREATED event are committed together (transactional outbox).
    # relay.py publishes the event to Kafka, so the payment is processed even if Kafka is down now.
    add_payment_event(db, "PAYMENT_CREATED", payment.id)
    db.commit()
    set_payment_status(payment.id, payment.status.value)
    status_url = f"/payments/{payment.id}"
    response.headers["Location"] = status_url
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, status_url=status_url,
        message="Payment accepted for asynchronous processing. Poll status_url for the result.",
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
    return aws_services.read_receipt(payment.receipt_s3_key)


@app.get("/notifications", response_model=list[NotificationResponse])
def list_notifications(db: Session = Depends(get_db)):
    return db.scalars(select(Notification).order_by(Notification.created_at.desc())).all()
