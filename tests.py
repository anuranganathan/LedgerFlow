import os
import uuid
from decimal import Decimal

os.environ["DATABASE_URL"] = "sqlite:///./test_payflow.db"
os.environ["USE_AWS"] = "false"

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

import app as app_module
import worker
from app import app
from database import Base, SessionLocal, engine
from models import Account, AccountType, EntryType, LedgerEntry, Payment, PaymentStatus


@pytest.fixture(autouse=True)
def clean_database(monkeypatch, tmp_path):
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(app_module, "publish_payment_created", lambda _: None)
    monkeypatch.setattr(app_module, "set_payment_status", lambda *_: None)
    monkeypatch.setattr(worker, "set_payment_status", lambda *_: None)
    monkeypatch.setattr(worker.aws_services, "RECEIPTS_DIR", tmp_path / "receipts")
    monkeypatch.setattr(worker.aws_services, "LOCAL_QUEUE_FILE", tmp_path / "queue.json")
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def create_accounts(client: TestClient):
    customer = client.post(
        "/accounts", json={"name": "Customer", "account_type": "CUSTOMER", "currency": "INR"}
    ).json()
    merchant = client.post(
        "/accounts", json={"name": "Merchant", "account_type": "MERCHANT", "currency": "INR"}
    ).json()
    return customer, merchant


def create_payment(client: TestClient, amount: str = "500.00"):
    customer, merchant = create_accounts(client)
    response = client.post(
        "/payments",
        json={
            "customer_account_id": customer["id"],
            "merchant_account_id": merchant["id"],
            "amount": amount,
            "currency": "INR",
            "description": "Test purchase",
        },
    )
    return response, customer, merchant


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_create_customer_account(client):
    response = client.post(
        "/accounts", json={"name": "Ananya", "account_type": "CUSTOMER", "currency": "INR"}
    )
    assert response.status_code == 201
    assert response.json()["balance"] == "0.00"


def test_fund_account(client):
    customer, _ = create_accounts(client)
    response = client.post(f"/accounts/{customer['id']}/fund", json={"amount": "5000.00"})
    assert response.status_code == 200
    assert response.json()["balance"] == "5000.00"


def test_create_payment_returns_pending(client):
    response, _, _ = create_payment(client)
    assert response.status_code == 202
    assert response.json()["status"] == "PENDING"


def test_successful_payment_processing(client):
    response, customer, merchant = create_payment(client)
    client.post(f"/accounts/{customer['id']}/fund", json={"amount": "1000.00"})
    with SessionLocal() as db:
        payment = worker.process_payment(response.json()["payment_id"], db)
        assert payment.status == PaymentStatus.SUCCESS
        assert db.get(Account, uuid.UUID(customer["id"])).balance == Decimal("500.00")
        assert db.get(Account, uuid.UUID(merchant["id"])).balance == Decimal("500.00")


def test_failed_payment_for_insufficient_balance(client):
    response, customer, merchant = create_payment(client)
    with SessionLocal() as db:
        payment = worker.process_payment(response.json()["payment_id"], db)
        assert payment.status == PaymentStatus.FAILED
        assert payment.failure_reason == "Insufficient balance"
        assert db.get(Account, uuid.UUID(customer["id"])).balance == Decimal("0.00")
        assert db.get(Account, uuid.UUID(merchant["id"])).balance == Decimal("0.00")


def test_success_creates_debit_and_credit_ledger_entries(client):
    response, customer, _ = create_payment(client, "250.00")
    client.post(f"/accounts/{customer['id']}/fund", json={"amount": "1000.00"})
    with SessionLocal() as db:
        worker.process_payment(response.json()["payment_id"], db)
        entries = db.scalars(select(LedgerEntry)).all()
        assert {entry.entry_type for entry in entries} == {EntryType.DEBIT, EntryType.CREDIT}
        assert all(entry.amount == Decimal("250.00") for entry in entries)


def test_redis_failure_falls_back_to_postgresql(client, monkeypatch):
    response, _, _ = create_payment(client)
    monkeypatch.setattr(app_module, "get_payment_status", lambda _: None)
    result = client.get(f"/payments/{response.json()['payment_id']}")
    assert result.status_code == 200
    assert result.json()["status"] == "PENDING"
