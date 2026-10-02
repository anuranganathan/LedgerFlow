import logging
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Annotated

from fastapi import Cookie, Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, or_, select, text, true
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import auth
import aws_services
import events
import idempotency
import ledger
import reconcile
import redis_client
import webhooks
from database import SessionLocal, get_db
from models import (
    Account, AccountType, DeliveryStatus, LedgerEntry, Notification, Payment, PaymentStatus, Refund,
    RefundStatus, TopUp, User, UserRole, WebhookDelivery, WebhookEndpoint,
)
from outbox import add_payment_event
from schemas import (
    AccountCreate, AccountResponse, FundRequest, LedgerResponse, MeResponse, MerchantResponse,
    NotificationResponse, Page, PaymentAccepted, PaymentCreate, PaymentResponse, RefundCreate,
    RefundResponse, RegisterRequest, TokenResponse, UserResponse, WebhookDeliveryResponse,
    WebhookEndpointRequest, WebhookEndpointResponse, WebhookSecretResponse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# The database schema is created and upgraded by Alembic migrations (alembic upgrade head),
# which run before the app starts. See migrations/.
app = FastAPI(
    title="LedgerFlow",
    description="Educational event-driven payment processing simulator. "
                "Register at POST /auth/register, then use Authorize with your email and password.",
    version="2.0.0",
)
app.mount("/static", StaticFiles(directory="static"), name="static")

PageQuery = Annotated[Page, Query()]
CurrentUser = Annotated[User, Depends(auth.current_user)]
IdempotencyKeyHeader = Annotated[str | None, Header(alias="Idempotency-Key", min_length=1, max_length=100)]
# Customers top up their own account to try the demo; in a real system this money would come
# from a card or bank transfer. Admins aren't limited.
CUSTOMER_TOP_UP_MAX = Decimal("10000.00")
CUSTOMER_TOP_UP_DAILY_MAX = Decimal("50000.00")


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    if not request.url.path.startswith(("/docs", "/redoc", "/openapi.json")):
        # The dashboard only runs its own script file; injected inline scripts are blocked.
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        )
    return response


def rate_limit(name: str, identity: str, limit: int, window_seconds: int) -> None:
    retry_after = redis_client.hit(name, identity, limit, window_seconds)
    if retry_after is not None:
        raise HTTPException(429, "Too many requests; try again later",
                            headers={"Retry-After": str(retry_after)})


def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def paginate(query, page: Page):
    return query.limit(page.limit).offset(page.offset)


# ---------- Access control ----------
# Anything a user may not see returns 404, not 403, so IDs of other users' data aren't confirmed.

def owned_account_ids(user: User):
    return select(Account.id).where(Account.owner_id == user.id)


def visible_payments(user: User):
    if user.role == UserRole.ADMIN:
        return true()
    owned = owned_account_ids(user)
    return or_(Payment.customer_account_id.in_(owned), Payment.merchant_account_id.in_(owned))


def get_account_for(db: Session, account_id: uuid.UUID, user: User) -> Account:
    account = db.get(Account, account_id)
    if account is None or (user.role != UserRole.ADMIN and account.owner_id != user.id):
        raise HTTPException(404, "Account not found")
    return account


def get_payment_for(db: Session, payment_id: uuid.UUID, user: User) -> Payment:
    payment = db.scalar(select(Payment).where(Payment.id == payment_id, visible_payments(user)))
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return payment


# ---------- Pages and health ----------

@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse("static/index.html")


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, str]:
    # Used by Docker and the deploy step to check the API can reach the database.
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


# ---------- Authentication ----------

@app.post("/auth/register", response_model=UserResponse, status_code=201, tags=["auth"])
def register(data: RegisterRequest, request: Request, db: Session = Depends(get_db)):
    rate_limit("register", client_ip(request), limit=20, window_seconds=3600)
    role = UserRole(data.role)
    user = User(email=data.email.lower(), name=data.name, password_hash=auth.hash_password(data.password), role=role)
    db.add(user)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "An account with this email already exists")
    db.add(Account(owner_id=user.id, name=data.name, account_type=AccountType(role.value), currency=data.currency))
    db.commit()
    return user


@app.post("/auth/login", response_model=TokenResponse, tags=["auth"])
def login(
    request: Request, response: Response, form: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: Session = Depends(get_db),
):
    """OAuth2 password login: send `username` (your email) and `password` as form fields."""
    rate_limit("login-ip", client_ip(request), limit=20, window_seconds=60)
    rate_limit("login-email", form.username.lower(), limit=5, window_seconds=300)
    user = auth.authenticate(db, form.username, form.password)
    if user is None:
        raise HTTPException(401, "Incorrect email or password", headers={"WWW-Authenticate": "Bearer"})
    auth.issue_refresh_token(db, user, response)
    db.commit()
    return TokenResponse(access_token=auth.create_access_token(user),
                         expires_in=int(auth.ACCESS_TOKEN_TTL.total_seconds()))


@app.post("/auth/refresh", response_model=TokenResponse, tags=["auth"])
def refresh(
    request: Request, response: Response, db: Session = Depends(get_db),
    refresh_token: Annotated[str | None, Cookie(alias=auth.REFRESH_COOKIE)] = None,
):
    """Swaps the refresh-token cookie for a new access token and a new refresh token."""
    rate_limit("refresh", client_ip(request), limit=30, window_seconds=60)
    if not refresh_token:
        raise HTTPException(401, "Not logged in")
    user = auth.use_refresh_token(db, refresh_token)
    auth.issue_refresh_token(db, user, response)
    db.commit()
    return TokenResponse(access_token=auth.create_access_token(user),
                         expires_in=int(auth.ACCESS_TOKEN_TTL.total_seconds()))


@app.post("/auth/logout", status_code=204, tags=["auth"])
def logout(
    response: Response, db: Session = Depends(get_db),
    refresh_token: Annotated[str | None, Cookie(alias=auth.REFRESH_COOKIE)] = None,
):
    if refresh_token:
        auth.revoke_refresh_token(db, refresh_token)
        db.commit()
    auth.clear_refresh_cookie(response)
    response.status_code = 204
    return response


@app.post("/auth/logout-everywhere", status_code=204, tags=["auth"])
def logout_everywhere(user: CurrentUser, response: Response, db: Session = Depends(get_db)):
    auth.revoke_all_sessions(db, user.id)
    db.commit()
    auth.clear_refresh_cookie(response)
    response.status_code = 204
    return response


@app.get("/auth/me", response_model=MeResponse, tags=["auth"])
def me(user: CurrentUser, db: Session = Depends(get_db)):
    accounts = db.scalars(select(Account).where(Account.owner_id == user.id).order_by(Account.created_at)).all()
    return MeResponse(**UserResponse.model_validate(user).model_dump(),
                      accounts=[AccountResponse.model_validate(account) for account in accounts])


# ---------- Accounts ----------

@app.post("/accounts", response_model=AccountResponse, status_code=201)
def create_account(data: AccountCreate, user: CurrentUser, db: Session = Depends(get_db)):
    if user.role == UserRole.ADMIN:
        raise HTTPException(403, "Admins don't hold accounts")
    account = Account(owner_id=user.id, name=data.name, account_type=AccountType(user.role.value),
                      currency=data.currency)
    db.add(account)
    db.commit()
    return account


@app.get("/accounts", response_model=list[AccountResponse])
def list_accounts(user: CurrentUser, page: PageQuery, db: Session = Depends(get_db)):
    query = select(Account).where(Account.account_type != AccountType.SYSTEM).order_by(Account.created_at, Account.id)
    if user.role != UserRole.ADMIN:
        query = query.where(Account.owner_id == user.id)
    return db.scalars(paginate(query, page)).all()


@app.get("/accounts/{account_id}", response_model=AccountResponse)
def get_account(account_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    return get_account_for(db, account_id, user)


@app.post("/accounts/{account_id}/fund", response_model=AccountResponse)
def fund_account(
    account_id: uuid.UUID, data: FundRequest, user: CurrentUser, db: Session = Depends(get_db)
):
    """Top up an account: customers their own (demo limits apply), admins any account."""
    rate_limit("top-up", str(user.id), limit=10, window_seconds=60)
    account = get_account_for(db, account_id, user)
    if user.role == UserRole.MERCHANT or account.account_type == AccountType.SYSTEM:
        raise HTTPException(403, "Only customers and admins can top up")
    if user.role == UserRole.CUSTOMER:
        if data.amount > CUSTOMER_TOP_UP_MAX:
            raise HTTPException(400, f"A top-up can be at most {CUSTOMER_TOP_UP_MAX}")
        ledger.lock(db, Account, [account_id])  # so parallel top-ups can't both pass the daily check
        since = datetime.now(timezone.utc) - timedelta(days=1)
        today = db.scalar(select(func.coalesce(func.sum(TopUp.amount), 0))
                          .where(TopUp.account_id == account_id, TopUp.created_at >= since))
        if today + data.amount > CUSTOMER_TOP_UP_DAILY_MAX:
            raise HTTPException(400, f"Top-ups are limited to {CUSTOMER_TOP_UP_DAILY_MAX} per 24 hours")
    # Recorded in the ledger as a transfer from the currency's system account, so every rupee
    # in an account can be traced to a top-up or a payment.
    account = ledger.top_up(db, account_id, data.amount, data.description)
    db.commit()
    return account


@app.get("/accounts/{account_id}/ledger", response_model=list[LedgerResponse])
def get_account_statement(
    account_id: uuid.UUID, user: CurrentUser, page: PageQuery, db: Session = Depends(get_db)
):
    get_account_for(db, account_id, user)
    query = (select(LedgerEntry).where(LedgerEntry.account_id == account_id)
             .order_by(LedgerEntry.created_at.desc(), LedgerEntry.id))
    return db.scalars(paginate(query, page)).all()


@app.get("/merchants", response_model=list[MerchantResponse])
def list_merchants(_: CurrentUser, page: PageQuery, db: Session = Depends(get_db)):
    """Merchants a customer can pay."""
    query = (select(Account).where(Account.account_type == AccountType.MERCHANT, Account.owner_id.is_not(None))
             .order_by(Account.name, Account.id))
    return db.scalars(paginate(query, page)).all()


# ---------- Payments ----------

@app.post("/payments", response_model=PaymentAccepted, status_code=202)
def create_payment(
    data: PaymentCreate, user: CurrentUser, idempotency_key: IdempotencyKeyHeader = None,
    db: Session = Depends(get_db),
):
    """Accepts a payment for asynchronous processing.

    Send a unique `Idempotency-Key` header: if the request is retried (for example after a
    timeout), the original response is returned and no second payment is created.
    """
    if user.role != UserRole.CUSTOMER:
        raise HTTPException(403, "Only customers can make payments")
    rate_limit("payment", str(user.id), limit=30, window_seconds=60)
    request_hash = idempotency.fingerprint("/payments", data)
    if idempotency_key and (saved := idempotency.find(db, user.id, idempotency_key)):
        return idempotency.replay(saved, request_hash)

    customer = db.get(Account, data.customer_account_id)
    if customer is None or customer.owner_id != user.id or customer.account_type != AccountType.CUSTOMER:
        raise HTTPException(404, "Customer account not found")
    merchant = db.get(Account, data.merchant_account_id)
    if merchant is None or merchant.account_type != AccountType.MERCHANT:
        raise HTTPException(404, "Merchant not found")
    if customer.currency != data.currency or merchant.currency != data.currency:
        raise HTTPException(400, "Payment and account currencies must match")
    payment = Payment(
        id=uuid.uuid4(), customer_account_id=customer.id, merchant_account_id=merchant.id,
        amount=data.amount, currency=data.currency, description=data.description,
        status=PaymentStatus.PENDING,
    )
    db.add(payment)
    # The payment and its PAYMENT_CREATED event are committed together (transactional outbox).
    # relay.py publishes the event to Kafka, so the payment is processed even if Kafka is down now.
    add_payment_event(db, "PAYMENT_CREATED", payment.id)
    status_url = f"/payments/{payment.id}"
    body = PaymentAccepted(
        payment_id=payment.id, status=payment.status, status_url=status_url,
        message="Payment accepted for asynchronous processing. Poll status_url for the result.",
    ).model_dump(mode="json")
    response = JSONResponse(body, status_code=202, headers={"Location": status_url})
    return commit_idempotent(db, user, idempotency_key, request_hash, response, body)


def commit_idempotent(db: Session, user: User, key: str | None, request_hash: str,
                      response: JSONResponse, body: dict) -> JSONResponse:
    if key:
        idempotency.save(db, user.id, key, request_hash, response, body)
    try:
        db.commit()
    except IntegrityError:
        # The same key was used by a request that committed a moment ago: return its response.
        db.rollback()
        saved = idempotency.find(db, user.id, key) if key else None
        if saved is None:
            raise
        return idempotency.replay(saved, request_hash)
    return response


@app.get("/payments", response_model=list[PaymentResponse])
def list_payments(user: CurrentUser, page: PageQuery, db: Session = Depends(get_db)):
    query = select(Payment).where(visible_payments(user)).order_by(Payment.created_at.desc(), Payment.id)
    return db.scalars(paginate(query, page)).all()


@app.get("/payments/{payment_id}", response_model=PaymentResponse)
def get_payment(payment_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    return get_payment_for(db, payment_id, user)


@app.get("/payments/{payment_id}/ledger", response_model=list[LedgerResponse])
def get_ledger(payment_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    get_payment_for(db, payment_id, user)
    query = (select(LedgerEntry).where(LedgerEntry.payment_id == payment_id)
             .order_by(LedgerEntry.created_at, LedgerEntry.entry_type.desc()))
    return db.scalars(query).all()


@app.get("/payments/{payment_id}/receipt")
def get_receipt(payment_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    payment = get_payment_for(db, payment_id, user)
    if not payment.receipt_s3_key:
        raise HTTPException(404, "Receipt is not available")
    return aws_services.read_receipt(payment.receipt_s3_key)


# ---------- Refunds ----------

@app.post("/payments/{payment_id}/refunds", response_model=RefundResponse, status_code=202)
def create_refund(
    payment_id: uuid.UUID, data: RefundCreate, user: CurrentUser,
    idempotency_key: IdempotencyKeyHeader = None, db: Session = Depends(get_db),
):
    """Refunds all or part of a payment. Only the merchant who was paid (or an admin) can."""
    rate_limit("refund", str(user.id), limit=30, window_seconds=60)
    request_hash = idempotency.fingerprint(f"/payments/{payment_id}/refunds", data)
    if idempotency_key and (saved := idempotency.find(db, user.id, idempotency_key)):
        return idempotency.replay(saved, request_hash)

    visible = get_payment_for(db, payment_id, user)
    merchant = db.get(Account, visible.merchant_account_id)
    if user.role != UserRole.ADMIN and merchant.owner_id != user.id:
        raise HTTPException(403, "Only the merchant who was paid can refund this payment")
    # Locking the payment stops two refund requests from both passing the limit check below.
    (payment,) = ledger.lock(db, Payment, [payment_id])
    if payment.status != PaymentStatus.SUCCESS:
        raise HTTPException(409, "Only successful payments can be refunded")
    # Pending refunds count too, so their total can never exceed the payment.
    reserved = db.scalar(
        select(func.coalesce(func.sum(Refund.amount), 0)).where(
            Refund.payment_id == payment_id, Refund.status != RefundStatus.FAILED
        )
    )
    refundable = payment.amount - reserved
    if refundable <= 0:
        raise HTTPException(409, "Payment is already fully refunded")
    amount = data.amount or refundable
    if amount > refundable:
        raise HTTPException(400, f"Refund exceeds the refundable amount ({refundable})")
    refund = Refund(id=uuid.uuid4(), payment_id=payment_id, amount=amount, reason=data.reason,
                    status=RefundStatus.PENDING)
    db.add(refund)
    # Same outbox and worker as payments; keyed by payment ID so it's processed after the payment.
    add_payment_event(db, "REFUND_REQUESTED", payment_id, refund_id=str(refund.id))
    db.flush()
    body = RefundResponse.model_validate(refund).model_dump(mode="json")
    response = JSONResponse(body, status_code=202, headers={"Location": f"/refunds/{refund.id}"})
    return commit_idempotent(db, user, idempotency_key, request_hash, response, body)


@app.get("/payments/{payment_id}/refunds", response_model=list[RefundResponse])
def list_refunds(payment_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    get_payment_for(db, payment_id, user)
    return db.scalars(select(Refund).where(Refund.payment_id == payment_id).order_by(Refund.created_at)).all()


@app.get("/refunds/{refund_id}", response_model=RefundResponse)
def get_refund(refund_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    refund = db.get(Refund, refund_id)
    if refund is None:
        raise HTTPException(404, "Refund not found")
    get_payment_for(db, refund.payment_id, user)
    return refund


# ---------- Notifications and operations ----------

@app.get("/notifications", response_model=list[NotificationResponse])
def list_notifications(user: CurrentUser, page: PageQuery, db: Session = Depends(get_db)):
    query = (select(Notification).join(Payment, Payment.id == Notification.payment_id)
             .where(visible_payments(user)).order_by(Notification.created_at.desc(), Notification.id))
    return db.scalars(paginate(query, page)).all()


@app.get("/reconciliation", tags=["operations"])
def run_reconciliation(
    _: Annotated[User, Depends(auth.require_role(UserRole.ADMIN))], db: Session = Depends(get_db)
) -> dict:
    """Checks that the ledger balances (admins only). Also run every few minutes by reconcile.py."""
    return reconcile.run_checks(db)


# ---------- Live updates ----------

@app.get("/events", tags=["live updates"])
async def live_events(request: Request, token: Annotated[str, Depends(auth.oauth2_scheme)]):
    """Server-Sent Events: an `update` event whenever one of your payments or refunds changes.

    The stream ends after 10 minutes; reconnect with a current access token.
    """
    def load_user() -> uuid.UUID:
        # A short session, so the open stream doesn't hold a database connection.
        with SessionLocal() as db:
            return auth.user_for_token(db, token).id

    user_id = await run_in_threadpool(load_user)
    return StreamingResponse(
        events.stream(user_id, request.is_disconnected), media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


# ---------- Merchant webhooks ----------

def merchant_account_for(db: Session, account_id: uuid.UUID, user: User) -> Account:
    account = get_account_for(db, account_id, user)
    if account.account_type != AccountType.MERCHANT:
        raise HTTPException(400, "Webhooks are for merchant accounts")
    return account


def endpoint_for(db: Session, account_id: uuid.UUID) -> WebhookEndpoint | None:
    return db.scalar(select(WebhookEndpoint).where(WebhookEndpoint.account_id == account_id))


def validate_webhook_url(url: str) -> None:
    try:
        webhooks.resolve(url)
    except webhooks.InvalidWebhookUrl as exc:
        raise HTTPException(400, str(exc))


@app.get("/accounts/{account_id}/webhook-endpoint", response_model=WebhookEndpointResponse, tags=["webhooks"])
def get_webhook_endpoint(account_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    merchant_account_for(db, account_id, user)
    endpoint = endpoint_for(db, account_id)
    if endpoint is None:
        raise HTTPException(404, "No webhook endpoint")
    return endpoint


@app.put("/accounts/{account_id}/webhook-endpoint", tags=["webhooks"],
         response_model=WebhookSecretResponse | WebhookEndpointResponse)
def set_webhook_endpoint(
    account_id: uuid.UUID, data: WebhookEndpointRequest, user: CurrentUser, db: Session = Depends(get_db)
):
    """Sets the URL that receives this merchant's events. The signing secret is returned the first
    time; keep it to verify the LedgerFlow-Signature header."""
    merchant_account_for(db, account_id, user)
    validate_webhook_url(data.url)
    endpoint = endpoint_for(db, account_id)
    if endpoint is None:
        endpoint = WebhookEndpoint(account_id=account_id, url=data.url, secret=webhooks.new_secret(), enabled=True)
        db.add(endpoint)
        db.commit()
        return WebhookSecretResponse.model_validate(endpoint)
    endpoint.url, endpoint.enabled = data.url, True
    db.commit()
    return WebhookEndpointResponse.model_validate(endpoint)


@app.post("/accounts/{account_id}/webhook-endpoint/rotate-secret", response_model=WebhookSecretResponse,
          tags=["webhooks"])
def rotate_webhook_secret(account_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    merchant_account_for(db, account_id, user)
    endpoint = endpoint_for(db, account_id)
    if endpoint is None:
        raise HTTPException(404, "No webhook endpoint")
    endpoint.secret = webhooks.new_secret()
    db.commit()
    return endpoint


@app.delete("/accounts/{account_id}/webhook-endpoint", status_code=204, tags=["webhooks"])
def disable_webhook_endpoint(account_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    """Stops sending events. Delivery history is kept."""
    merchant_account_for(db, account_id, user)
    endpoint = endpoint_for(db, account_id)
    if endpoint is not None:
        endpoint.enabled = False
        db.commit()
    return Response(status_code=204)


@app.get("/accounts/{account_id}/webhook-deliveries", response_model=list[WebhookDeliveryResponse],
         tags=["webhooks"])
def list_webhook_deliveries(
    account_id: uuid.UUID, user: CurrentUser, page: PageQuery, db: Session = Depends(get_db)
):
    merchant_account_for(db, account_id, user)
    query = (select(WebhookDelivery).join(WebhookEndpoint, WebhookEndpoint.id == WebhookDelivery.endpoint_id)
             .where(WebhookEndpoint.account_id == account_id)
             .order_by(WebhookDelivery.created_at.desc(), WebhookDelivery.id))
    return db.scalars(paginate(query, page)).all()


@app.post("/webhook-deliveries/{delivery_id}/retry", response_model=WebhookDeliveryResponse, status_code=202,
          tags=["webhooks"])
def retry_webhook_delivery(delivery_id: uuid.UUID, user: CurrentUser, db: Session = Depends(get_db)):
    """Sends a failed delivery again (for example after fixing your endpoint)."""
    delivery = db.get(WebhookDelivery, delivery_id)
    endpoint = db.get(WebhookEndpoint, delivery.endpoint_id) if delivery else None
    if endpoint is None:
        raise HTTPException(404, "Delivery not found")
    merchant_account_for(db, endpoint.account_id, user)
    if delivery.status != DeliveryStatus.FAILED:
        raise HTTPException(409, "Only failed deliveries can be retried")
    if not endpoint.enabled:
        raise HTTPException(409, "The webhook endpoint is disabled")
    delivery.status, delivery.attempts, delivery.enqueued_at = DeliveryStatus.PENDING, 0, None
    db.commit()
    try:
        webhooks.queue(db, delivery)
    except Exception:
        # Saved as PENDING and not queued: the reconciler queues it within a few minutes.
        logging.exception("Could not queue webhook delivery %s now", delivery.id)
    return delivery
