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
import kafka_client
import notifier
import relay
import setup_aws
import worker
from app import app
from database import Base, SessionLocal, engine
from models import (
    Account, EntryType, LedgerEntry, Notification, NotificationStatus, OutboxEvent, Payment,
    PaymentStatus,
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(app_module, "set_payment_status", lambda *_: None)
    monkeypatch.setattr(worker, "set_payment_status", lambda *_: None)
    monkeypatch.setattr(worker, "RETRY_BASE_SECONDS", 0)
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
    # Running setup twice updates the alarms in place instead of creating duplicates.
    assert sorted(alarm["AlarmName"] for alarm in alarms) == [
        "ledgerflow-dead-lettered-events", "ledgerflow-failed-payments",
    ]


# ---------- Reliable event processing ----------

class FakeKafka:
    """Records published messages, or fails like an unreachable broker."""

    def __init__(self, down: bool = False):
        self.down = down
        self.messages: list[tuple[str, str, dict]] = []

    def publish(self, topic, key, value, timeout=10):
        if self.down:
            raise ConnectionError("Kafka unavailable")
        self.messages.append((topic, key, value))


def funded_payment(client: TestClient, amount: str = "500.00", funds: str = "1000.00") -> str:
    response, customer, _ = create_payment(client, amount)
    client.post(f"/accounts/{customer['id']}/fund", json={"amount": funds})
    return response.json()["payment_id"]


def test_payment_and_event_are_saved_together_even_when_kafka_is_down(client, monkeypatch):
    monkeypatch.setattr(kafka_client, "publish", FakeKafka(down=True).publish)
    response, _, _ = create_payment(client)
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]
    assert response.json()["status_url"] == f"/payments/{payment_id}"
    assert response.headers["Location"] == f"/payments/{payment_id}"
    with SessionLocal() as db:
        event = db.scalars(select(OutboxEvent)).one()
        assert event.published_at is None
        assert event.payload["event_type"] == "PAYMENT_CREATED"
        assert event.payload["payment_id"] == payment_id


def test_relay_publishes_outbox_events_once(client, monkeypatch):
    kafka = FakeKafka()
    monkeypatch.setattr(kafka_client, "publish", kafka.publish)
    payment_id = create_payment(client)[0].json()["payment_id"]
    with SessionLocal() as db:
        assert relay.publish_pending(db) == 1
        assert relay.publish_pending(db) == 0
        assert db.scalars(select(OutboxEvent)).one().published_at is not None
    assert kafka.messages == [(kafka_client.PAYMENT_EVENTS_TOPIC, payment_id, kafka.messages[0][2])]


def test_relay_keeps_events_while_kafka_is_down_and_sends_them_later(client, monkeypatch):
    kafka = FakeKafka(down=True)
    monkeypatch.setattr(kafka_client, "publish", kafka.publish)
    create_payment(client)
    with SessionLocal() as db, pytest.raises(ConnectionError):
        relay.publish_pending(db)
    with SessionLocal() as db:
        event = db.scalars(select(OutboxEvent)).one()
        assert (event.published_at, event.attempts) == (None, 1)
    kafka.down = False
    with SessionLocal() as db:
        assert relay.publish_pending(db) == 1
    assert len(kafka.messages) == 1


def test_duplicate_event_moves_money_only_once(client):
    payment_id = funded_payment(client)
    with SessionLocal() as db:
        worker.process_payment(payment_id, db)
        worker.process_payment(payment_id, db)
        assert len(db.scalars(select(LedgerEntry)).all()) == 2
        assert len(db.scalars(select(Notification)).all()) == 1
        payment = db.get(Payment, uuid.UUID(payment_id))
        assert db.get(Account, payment.customer_account_id).balance == Decimal("500.00")
    assert len(aws_services.receive_notifications()) == 1


def test_side_effects_resume_after_a_failure_without_moving_money_again(client, monkeypatch):
    payment_id = funded_payment(client)
    real_store = aws_services.store_receipt

    def s3_down(*_):
        raise ConnectionError("S3 unavailable")

    monkeypatch.setattr(aws_services, "store_receipt", s3_down)
    with SessionLocal() as db, pytest.raises(ConnectionError):
        worker.process_payment(payment_id, db)
    monkeypatch.setattr(aws_services, "store_receipt", real_store)
    with SessionLocal() as db:
        payment = worker.process_payment(payment_id, db)
        assert payment.receipt_s3_key == f"receipts/{payment_id}.json"
        assert db.get(Account, payment.customer_account_id).balance == Decimal("500.00")
        assert len(db.scalars(select(LedgerEntry)).all()) == 2
    assert len(aws_services.receive_notifications()) == 1


class FakeRecord:
    def __init__(self, offset, value):
        self.offset, self.value = offset, value


class FakeConsumer:
    def __init__(self, events):
        partition = worker.TopicPartition("payment-events", 0)
        self.batch = {partition: [FakeRecord(offset, event) for offset, event in enumerate(events)]}
        self.log: list[str] = []

    def poll(self, timeout_ms):
        batch, self.batch = self.batch, {}
        return batch

    def commit(self, offsets):
        (position,) = offsets.values()
        self.log.append(f"commit {position.offset}")


def test_worker_commits_each_offset_only_after_handling_it(client, monkeypatch):
    payment_id = funded_payment(client)
    consumer = FakeConsumer([{"event_type": "PAYMENT_CREATED", "payment_id": payment_id}] * 2)
    real_handle = worker.handle_event
    monkeypatch.setattr(worker, "handle_event", lambda event: (consumer.log.append("handled"), real_handle(event)))
    assert worker.consume_batch(consumer) == 2
    assert consumer.log == ["handled", "commit 1", "handled", "commit 2"]


def test_failing_event_is_retried_then_dead_lettered(client, monkeypatch):
    kafka = FakeKafka()
    monkeypatch.setattr(kafka_client, "publish", kafka.publish)
    calls = []

    def broken(payment_id, db):
        calls.append(payment_id)
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(worker, "process_payment", broken)
    event = {"event_type": "PAYMENT_CREATED", "payment_id": str(uuid.uuid4())}
    consumer = FakeConsumer([event])
    worker.consume_batch(consumer)
    assert len(calls) == worker.MAX_ATTEMPTS
    ((topic, _, dead),) = kafka.messages
    assert topic == kafka_client.PAYMENT_EVENTS_DLQ_TOPIC
    assert dead["event"] == event and "database unavailable" in dead["error"]
    assert consumer.log == ["commit 1"]


def test_malformed_event_goes_straight_to_dead_letter_topic(client, monkeypatch):
    kafka = FakeKafka()
    monkeypatch.setattr(kafka_client, "publish", kafka.publish)
    worker.handle_event(kafka_client.decode_event(b"not json"))
    worker.handle_event({"event_type": "PAYMENT_CREATED", "payment_id": "abc"})
    assert [message[0] for message in kafka.messages] == [kafka_client.PAYMENT_EVENTS_DLQ_TOPIC] * 2


def test_offset_is_not_committed_when_dead_lettering_fails(client, monkeypatch):
    monkeypatch.setattr(kafka_client, "publish", FakeKafka(down=True).publish)
    consumer = FakeConsumer([{"event_type": "UNKNOWN"}])
    with pytest.raises(ConnectionError):
        worker.consume_batch(consumer)
    assert consumer.log == []


def test_notifier_does_not_resend_a_notification_already_sent(client, monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_slack_message", sent.append)
    payment_id = create_payment(client)[0].json()["payment_id"]
    with SessionLocal() as db:
        worker.process_payment(payment_id, db)
    message = aws_services.receive_notifications()[0]
    notifier.handle_message(message)
    notifier.handle_message(message)  # SQS delivered the same message again
    assert len(sent) == 1


def test_migrations_create_the_same_schema_as_the_models(tmp_path):
    from alembic import command
    from alembic.autogenerate import compare_metadata
    from alembic.config import Config
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import create_engine

    migrated = create_engine(f"sqlite:///{tmp_path / 'migrated.db'}")
    config = Config("alembic.ini")
    with migrated.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
    with migrated.connect() as connection:
        context = MigrationContext.configure(connection, opts={"compare_type": True})
        assert compare_metadata(context, Base.metadata) == []
