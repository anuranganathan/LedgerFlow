"""Tests that need real PostgreSQL: row locking under concurrency and the migrations.

SQLite (used by tests.py) ignores SELECT ... FOR UPDATE, so these run against PostgreSQL:

    docker run -d --name ledgerflow-test-db -p 55432:5432 \\
        -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test postgres:16-alpine
    TEST_DATABASE_URL=postgresql+psycopg2://test:test@localhost:55432/test pytest -q tests_postgres.py

The tests are skipped when TEST_DATABASE_URL is not set.
"""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest

if not os.getenv("TEST_DATABASE_URL"):
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)
os.environ["DATABASE_URL"] = os.environ["TEST_DATABASE_URL"]
os.environ.pop("AWS_ENDPOINT_URL", None)
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["JWT_SECRET"] = "test-secret-that-is-long-enough-for-hs256"

import fakeredis
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from moto import mock_aws
from sqlalchemy import func, select, text

import app as app_module
import auth
import aws_services
import ledger
import redis_client
import reconcile
import setup_aws
import worker
from database import Base, SessionLocal, engine
from models import (
    Account, AccountType, EntryType, LedgerEntry, OutboxEvent, Payment, PaymentStatus, User, UserRole,
)


def reset_database() -> None:
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public"))


def migrate(revision: str = "head") -> None:
    command.upgrade(Config("alembic.ini"), revision)


@pytest.fixture(autouse=True)
def database(monkeypatch):
    reset_database()
    migrate()
    monkeypatch.setattr(redis_client, "client", fakeredis.FakeRedis(decode_responses=True))
    monkeypatch.setattr(app_module, "rate_limit", lambda *args, **kwargs: None)  # tests hammer on purpose
    aws_services.client.cache_clear()
    aws_services.queue_url.cache_clear()
    with mock_aws():
        setup_aws.create_bucket()
        setup_aws.create_queues()
        yield


def add_user(role: UserRole) -> User:
    with SessionLocal() as db:
        user = User(email=f"{uuid.uuid4().hex[:8]}@example.com", name=role.value, role=role,
                    password_hash=auth.hash_password("irrelevant password"))
        db.add(user)
        db.commit()
        return user


def headers_for(user: User) -> dict[str, str]:
    return {"Authorization": f"Bearer {auth.create_access_token(user)}"}


def add_accounts(balance: str, owner: User | None = None) -> tuple[uuid.UUID, uuid.UUID]:
    with SessionLocal() as db:
        customer = Account(name="C", account_type=AccountType.CUSTOMER, balance=Decimal("0"), currency="INR",
                           owner_id=owner.id if owner else None)
        merchant = Account(name="M", account_type=AccountType.MERCHANT, balance=Decimal("0"), currency="INR")
        db.add_all([customer, merchant])
        db.commit()
        if Decimal(balance):
            ledger.top_up(db, customer.id, Decimal(balance), "Test funds")
            db.commit()
        return customer.id, merchant.id


def assert_books_balance() -> None:
    with SessionLocal() as db:
        report = reconcile.run_checks(db)
    assert report["ledger_problems"] == 0, report


def add_payments(customer_id, merchant_id, amount: str, count: int) -> list[uuid.UUID]:
    with SessionLocal() as db:
        payments = [
            Payment(customer_account_id=customer_id, merchant_account_id=merchant_id,
                    amount=Decimal(amount), currency="INR", status=PaymentStatus.PENDING)
            for _ in range(count)
        ]
        db.add_all(payments)
        db.commit()
        return [payment.id for payment in payments]


def process_in_parallel(payment_ids: list[uuid.UUID]) -> None:
    def run(payment_id):
        with SessionLocal() as db:
            worker.process_payment(payment_id, db)

    with ThreadPoolExecutor(max_workers=len(payment_ids)) as pool:
        list(pool.map(run, payment_ids))


def balance(account_id) -> Decimal:
    with SessionLocal() as db:
        return db.get(Account, account_id).balance


def test_concurrent_payments_never_overdraw_the_customer():
    customer, merchant = add_accounts("100.00")
    payment_ids = add_payments(customer, merchant, "30.00", 10)
    process_in_parallel(payment_ids)
    with SessionLocal() as db:
        statuses = db.scalars(select(Payment.status)).all()
    assert statuses.count(PaymentStatus.SUCCESS) == 3
    assert statuses.count(PaymentStatus.FAILED) == 7
    assert balance(customer) == Decimal("10.00")
    assert balance(merchant) == Decimal("90.00")
    assert_books_balance()


def test_same_payment_processed_by_many_workers_moves_money_once():
    customer, merchant = add_accounts("100.00")
    (payment_id,) = add_payments(customer, merchant, "40.00", 1)
    process_in_parallel([payment_id] * 8)
    assert balance(customer) == Decimal("60.00")
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).where(LedgerEntry.payment_id.is_not(None))) == 2
    assert_books_balance()


def test_funding_during_payments_loses_no_update(client_factory=None):
    from fastapi.testclient import TestClient

    from app import app

    customer, merchant = add_accounts("0.00")
    payment_ids = add_payments(customer, merchant, "10.00", 20)
    admin = headers_for(add_user(UserRole.ADMIN))
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=40) as pool:
        funding = [pool.submit(client.post, f"/accounts/{customer}/fund", json={"amount": "10.00"}, headers=admin)
                   for _ in range(20)]
        processing = [pool.submit(process_in_parallel, [payment_id]) for payment_id in payment_ids]
        assert all(future.result().status_code == 200 for future in funding)
        [future.result() for future in processing]
    with SessionLocal() as db:
        succeeded = db.scalar(select(func.count()).where(Payment.status == PaymentStatus.SUCCESS))
        debits = db.scalar(select(func.coalesce(func.sum(LedgerEntry.amount), 0)).where(
            LedgerEntry.entry_type == EntryType.DEBIT, LedgerEntry.payment_id.is_not(None)))
    # 20 top-ups of 10, minus whatever was paid: no update was overwritten.
    assert balance(customer) == Decimal("200.00") - Decimal("10.00") * succeeded
    assert debits == Decimal("10.00") * succeeded
    assert balance(merchant) == debits
    assert_books_balance()


def test_concurrent_refund_requests_cannot_refund_more_than_the_payment():
    from fastapi.testclient import TestClient

    from app import app

    customer, merchant = add_accounts("100.00")
    (payment_id,) = add_payments(customer, merchant, "100.00", 1)
    process_in_parallel([payment_id])
    admin = headers_for(add_user(UserRole.ADMIN))
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(
            lambda _: client.post(f"/payments/{payment_id}/refunds", json={"amount": "40.00"}, headers=admin),
            range(10),
        ))
    assert sorted(r.status_code for r in responses).count(202) == 2  # 40 + 40 <= 100 < 120
    refund_ids = [r.json()["id"] for r in responses if r.status_code == 202]

    def run(refund_id):
        with SessionLocal() as db:
            worker.process_refund(refund_id, db)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(run, refund_ids * 2))  # each refund processed twice, concurrently
    assert balance(customer) == Decimal("80.00")
    assert balance(merchant) == Decimal("20.00")
    assert_books_balance()


def test_parallel_retries_with_one_idempotency_key_create_one_payment():
    from fastapi.testclient import TestClient

    from app import app

    user = add_user(UserRole.CUSTOMER)
    customer, merchant = add_accounts("100.00", owner=user)
    headers = {**headers_for(user), "Idempotency-Key": "checkout-7"}
    body = {"customer_account_id": str(customer), "merchant_account_id": str(merchant), "amount": "5.00"}
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(lambda _: client.post("/payments", json=body, headers=headers), range(10)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(Payment)) == 1
        assert db.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_migrations_match_the_models_on_postgresql():
    with engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"compare_type": True})
        assert compare_metadata(context, Base.metadata) == []


def test_migrations_adopt_an_existing_database_and_requeue_stuck_payments():
    # A database created by the old app (create_all, no alembic_version table) with a payment
    # stuck in PROCESSING and one stuck in PENDING.
    reset_database()
    migrate("0001")
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE alembic_version"))
        connection.execute(text(
            "INSERT INTO accounts VALUES "
            "('11111111-1111-1111-1111-111111111111','C','CUSTOMER',50,'INR',now()),"
            "('22222222-2222-2222-2222-222222222222','M','MERCHANT',0,'INR',now())"
        ))
        for status in ["PROCESSING", "PENDING", "FAILED"]:
            connection.execute(text(
                "INSERT INTO payments (id, customer_account_id, merchant_account_id, amount, currency,"
                " status, created_at, updated_at) VALUES (:id, '11111111-1111-1111-1111-111111111111',"
                " '22222222-2222-2222-2222-222222222222', 10, 'INR', :status, now(), now())"
            ), {"id": uuid.uuid4(), "status": status})
    migrate()
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(OutboxEvent)) == 2
        assert sorted(db.scalars(select(Payment.status)).all()) == sorted(
            [PaymentStatus.PENDING, PaymentStatus.PENDING, PaymentStatus.FAILED]
        )
        # The balance funded without a ledger entry became an opening-balance top-up.
        assert db.get(Account, ledger.system_account_id("INR")).balance == Decimal("-50.00")
    assert_books_balance()
