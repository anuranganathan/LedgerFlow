import os
import uuid
from decimal import Decimal

os.environ["DATABASE_URL"] = "sqlite:///./test_payflow.db"
os.environ.pop("AWS_ENDPOINT_URL", None)
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"

import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import select

import app as app_module
import aws_services
import notifier
import setup_aws
import worker
from app import app
from database import Base, SessionLocal, engine
from models import Account, EntryType, LedgerEntry, Notification, NotificationStatus, PaymentStatus


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(app_module, "publish_payment_created", lambda _: None)
    monkeypatch.setattr(app_module, "set_payment_status", lambda *_: None)
    monkeypatch.setattr(worker, "set_payment_status", lambda *_: None)
    # moto fakes AWS in memory, so the real setup script creates the bucket and queues.
    with mock_aws():
        setup_aws.create_bucket()
        setup_aws.create_queues()
        setup_aws.create_monitoring()
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


def test_successful_payment_stores_receipt_in_s3(client):
    response, customer, _ = create_payment(client, "300.00")
    client.post(f"/accounts/{customer['id']}/fund", json={"amount": "1000.00"})
    with SessionLocal() as db:
        worker.process_payment(response.json()["payment_id"], db)
    receipt = client.get(f"/payments/{response.json()['payment_id']}/receipt")
    assert receipt.status_code == 200
    assert receipt.json()["amount"] == "300.00"
    assert receipt.json()["status"] == "SUCCESS"


def test_failed_payment_sends_sqs_message_and_metric(client):
    response, _, _ = create_payment(client)
    with SessionLocal() as db:
        worker.process_payment(response.json()["payment_id"], db)
    messages = aws_services.receive_notifications()
    assert len(messages) == 1
    assert messages[0]["body"]["status"] == "FAILED"
    assert messages[0]["body"]["reason"] == "Insufficient balance"
    metrics = aws_services.client("cloudwatch").list_metrics(Namespace="LedgerFlow")["Metrics"]
    assert "PaymentsFailed" in {metric["MetricName"] for metric in metrics}


def test_notifier_posts_to_slack_and_marks_notification_sent(client, monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_slack_message", sent.append)
    response, _, _ = create_payment(client)
    with SessionLocal() as db:
        worker.process_payment(response.json()["payment_id"], db)
    for message in aws_services.receive_notifications():
        notifier.handle_message(message)
    assert "Payment FAILED" in sent[0]
    assert "Insufficient balance" in sent[0]
    assert aws_services.receive_notifications() == []
    with SessionLocal() as db:
        assert db.scalars(select(Notification)).one().status == NotificationStatus.SENT


def test_slack_failure_keeps_message_in_queue(client, monkeypatch):
    def slack_down(_):
        raise RuntimeError("Slack unavailable")

    monkeypatch.setattr(notifier, "send_slack_message", slack_down)
    response, _, _ = create_payment(client)
    with SessionLocal() as db:
        worker.process_payment(response.json()["payment_id"], db)
    message = aws_services.receive_notifications()[0]
    with pytest.raises(RuntimeError):
        notifier.handle_message(message)
    # The message was not deleted, so SQS will deliver it again (and later move it to the DLQ).
    sqs = aws_services.client("sqs")
    sqs.change_message_visibility(
        QueueUrl=aws_services.queue_url(), ReceiptHandle=message["receipt_handle"], VisibilityTimeout=0
    )
    assert len(aws_services.receive_notifications()) == 1


def test_setup_aws_is_safe_to_run_twice():
    setup_aws.create_bucket()
    setup_aws.create_queues()
    setup_aws.create_monitoring()
    alarms = aws_services.client("cloudwatch").describe_alarms()["MetricAlarms"]
    assert [alarm["AlarmName"] for alarm in alarms] == ["ledgerflow-failed-payments"]
