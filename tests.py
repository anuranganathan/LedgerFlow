import os
import uuid
from decimal import Decimal

os.environ["DATABASE_URL"] = "sqlite:///./test_payflow.db"
os.environ.pop("AWS_ENDPOINT_URL", None)
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["JWT_SECRET"] = "test-secret-that-is-long-enough-for-hs256"

import fakeredis
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import select

import aws_services
import kafka_client
import notifier
import reconcile
import redis_client
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
    monkeypatch.setattr(redis_client, "client", fakeredis.FakeRedis(decode_responses=True))
    monkeypatch.setattr(worker, "RETRY_BASE_SECONDS", 0)
    ACTORS.clear()
    # moto fakes AWS in memory, so the real setup script creates the bucket and queues.
    aws_services.client.cache_clear()
    aws_services.queue_url.cache_clear()
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


PASSWORD = "correct horse battery"
ACTORS: dict[str, dict] = {}  # account ID -> the user who owns it (with their auth headers)


def signup(client: TestClient, role: str = "CUSTOMER") -> dict:
    """Registers and logs in a user. Returns their account, plus "headers" and "email"."""
    email = f"{role.lower()}-{uuid.uuid4().hex[:8]}@example.com"
    assert client.post("/auth/register", json={
        "email": email, "password": PASSWORD, "name": role.title(), "role": role,
    }).status_code == 201
    token = client.post("/auth/login", data={"username": email, "password": PASSWORD}).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    account = client.get("/auth/me", headers=headers).json()["accounts"][0]
    actor = {**account, "headers": headers, "email": email}
    ACTORS[account["id"]] = actor
    return actor


def act_as(client: TestClient, actor: dict) -> None:
    client.headers.update(actor["headers"])


def create_accounts(client: TestClient):
    """A customer and a merchant; the client is left logged in as the customer."""
    merchant = signup(client, "MERCHANT")
    customer = signup(client, "CUSTOMER")
    act_as(client, customer)
    return customer, merchant


def payment_entries(db, payment_id) -> list[LedgerEntry]:
    return db.scalars(select(LedgerEntry).where(LedgerEntry.payment_id == uuid.UUID(str(payment_id)))).all()


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


def test_registering_creates_an_empty_account_for_the_role(client):
    customer = signup(client, "CUSTOMER")
    merchant = signup(client, "MERCHANT")
    assert (customer["account_type"], customer["balance"]) == ("CUSTOMER", "0.00")
    assert merchant["account_type"] == "MERCHANT"


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
        entries = payment_entries(db, response.json()["payment_id"])
        assert {entry.entry_type for entry in entries} == {EntryType.DEBIT, EntryType.CREDIT}
        assert all(entry.amount == Decimal("250.00") for entry in entries)


def test_api_keeps_working_when_redis_is_down(client, monkeypatch):
    import redis

    monkeypatch.setattr(redis_client, "client", redis.Redis(host="localhost", port=1))  # nothing listens
    response, _, _ = create_payment(client)  # rate limiting fails open
    assert response.status_code == 202
    assert client.get(f"/payments/{response.json()['payment_id']}").json()["status"] == "PENDING"


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
        "ledgerflow-ledger-mismatch", "ledgerflow-stuck-items",
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
        assert len(payment_entries(db, payment_id)) == 2
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
        assert len(payment_entries(db, payment_id)) == 2
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

    monkeypatch.setitem(worker.HANDLERS, "PAYMENT_CREATED", ("payment_id", broken))
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


# ---------- Ledger integrity: top-ups, refunds, reconciliation ----------

def settle(payment_id) -> None:
    with SessionLocal() as db:
        worker.process_payment(payment_id, db)


def refund(client: TestClient, payment_id: str, **body):
    """Requested by the merchant who was paid."""
    merchant_id = client.get(f"/payments/{payment_id}").json()["merchant_account_id"]
    response = client.post(f"/payments/{payment_id}/refunds", json=body, headers=ACTORS[merchant_id]["headers"])
    if response.status_code == 202:
        with SessionLocal() as db:
            worker.process_refund(response.json()["id"], db)
    return response


def test_top_up_is_recorded_in_the_ledger_against_the_system_account(client):
    customer, _ = create_accounts(client)
    client.post(f"/accounts/{customer['id']}/fund", json={"amount": "750.00"})
    statement = client.get(f"/accounts/{customer['id']}/ledger").json()
    assert [(e["entry_type"], e["amount"]) for e in statement] == [("CREDIT", "750.00")]
    assert statement[0]["top_up_id"] is not None
    # The system account is internal: it isn't listed.
    assert [a["account_type"] for a in client.get("/accounts").json()] == ["CUSTOMER"]
    with SessionLocal() as db:
        assert reconcile.run_checks(db)["ok"]


@pytest.mark.parametrize("body, field", [
    ({"amount": "0.001"}, "amount"),
    ({"amount": "-5"}, "amount"),
    ({"amount": "12345678901234"}, "amount"),
    ({"amount": "10", "currency": "XYZ"}, "currency"),
])
def test_invalid_payment_amounts_and_currencies_are_rejected(client, body, field):
    customer, merchant = create_accounts(client)
    response = client.post("/payments", json={
        "customer_account_id": customer["id"], "merchant_account_id": merchant["id"], **body,
    })
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][-1] == field


@pytest.mark.parametrize("role", ["SYSTEM", "ADMIN"])
def test_system_and_admin_users_cannot_sign_up(client, role):
    response = client.post("/auth/register", json={
        "email": "sneaky@example.com", "password": PASSWORD, "name": "Sneaky", "role": role,
    })
    assert response.status_code == 422


def test_database_rejects_a_negative_customer_balance(client):
    from sqlalchemy.exc import IntegrityError

    customer, _ = create_accounts(client)
    with SessionLocal() as db, pytest.raises(IntegrityError):
        db.get(Account, uuid.UUID(customer["id"])).balance = Decimal("-1.00")
        db.commit()


def test_full_refund_returns_the_money_and_is_recorded(client):
    payment_id = funded_payment(client)
    settle(payment_id)
    response = refund(client, payment_id, reason="Damaged item")
    assert response.status_code == 202
    assert response.json()["amount"] == "500.00"
    assert response.headers["Location"] == f"/refunds/{response.json()['id']}"
    assert client.get(f"/refunds/{response.json()['id']}").json()["status"] == "SUCCESS"
    payment = client.get(f"/payments/{payment_id}").json()
    assert payment["refunded_amount"] == "500.00"
    assert client.get(f"/accounts/{payment['customer_account_id']}").json()["balance"] == "1000.00"
    merchant = ACTORS[payment["merchant_account_id"]]
    assert client.get(f"/accounts/{merchant['id']}", headers=merchant["headers"]).json()["balance"] == "0.00"
    ledger = client.get(f"/payments/{payment_id}/ledger").json()
    assert len(ledger) == 4 and sum(1 for e in ledger if e["refund_id"]) == 2
    with SessionLocal() as db:
        assert reconcile.run_checks(db)["ok"]


def test_partial_refunds_cannot_exceed_the_payment(client):
    payment_id = funded_payment(client)
    settle(payment_id)
    assert refund(client, payment_id, amount="200.00").status_code == 202
    over = refund(client, payment_id, amount="300.01")
    assert over.status_code == 400 and "300.00" in over.json()["detail"]
    assert refund(client, payment_id).json()["amount"] == "300.00"  # the rest
    assert refund(client, payment_id).status_code == 409


def test_only_successful_payments_can_be_refunded(client):
    payment_id = create_payment(client)[0].json()["payment_id"]  # not funded
    assert refund(client, payment_id).status_code == 409  # still pending
    settle(payment_id)
    assert refund(client, payment_id).status_code == 409  # failed


def test_refund_fails_when_the_merchant_has_spent_the_money(client, monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_slack_message", sent.append)
    payment_id = funded_payment(client)
    settle(payment_id)
    merchant_id = client.get(f"/payments/{payment_id}").json()["merchant_account_id"]
    with SessionLocal() as db:
        db.get(Account, uuid.UUID(merchant_id)).balance = Decimal("100.00")
        db.commit()
    response = refund(client, payment_id)
    result = client.get(f"/refunds/{response.json()['id']}").json()
    assert (result["status"], result["failure_reason"]) == ("FAILED", "Merchant has insufficient balance")
    for message in aws_services.receive_notifications():
        notifier.handle_message(message)
    assert any("Refund FAILED" in text for text in sent)


def test_duplicate_refund_event_moves_money_once(client):
    payment_id = funded_payment(client)
    settle(payment_id)
    refund_id = refund(client, payment_id).json()["id"]
    with SessionLocal() as db:
        worker.process_refund(refund_id, db)
    assert client.get(f"/payments/{payment_id}").json()["refunded_amount"] == "500.00"
    assert len(client.get(f"/payments/{payment_id}/ledger").json()) == 4


def test_reconciliation_detects_a_tampered_balance_and_stuck_payments(client):
    from datetime import datetime, timedelta, timezone

    payment_id = funded_payment(client)
    with SessionLocal() as db:
        payment = db.get(Payment, uuid.UUID(payment_id))
        payment.created_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        db.scalars(select(OutboxEvent)).one().created_at = payment.created_at  # never published
        db.get(Account, payment.merchant_account_id).balance += Decimal("1.00")  # money from nowhere
        db.commit()
        report = reconcile.run_checks(db)
    assert not report["ok"]
    assert report["ledger_problems"] == 1
    assert report["account_mismatches"][0]["balance"] == "1.00"
    assert report["stuck_payments"] == 1
    assert report["stale_outbox_events"] == 1


def test_reconciliation_publishes_metrics(client):
    funded_payment(client)
    reconcile.run_once()
    metrics = aws_services.client("cloudwatch").list_metrics(Namespace="LedgerFlow")["Metrics"]
    assert {"LedgerMismatches", "StuckItems"} <= {metric["MetricName"] for metric in metrics}


def test_lists_are_paginated(client):
    customer, merchant = create_accounts(client)
    for _ in range(3):
        client.post("/payments", json={
            "customer_account_id": customer["id"], "merchant_account_id": merchant["id"], "amount": "1",
        })
    assert len(client.get("/payments?limit=2").json()) == 2
    assert len(client.get("/payments?limit=2&offset=2").json()) == 1
    assert client.get("/payments?limit=1000").status_code == 422


# ---------- Authentication and authorization ----------

def login_response(client: TestClient, email: str, password: str = PASSWORD):
    return client.post("/auth/login", data={"username": email, "password": password})


def test_endpoints_require_a_login(client):
    for method, path in [("get", "/payments"), ("get", "/accounts"), ("post", "/payments"),
                         ("get", "/notifications"), ("get", "/merchants"), ("get", "/auth/me")]:
        assert getattr(client, method)(path).status_code == 401, path


def test_passwords_are_stored_hashed(client):
    customer = signup(client)
    with SessionLocal() as db:
        from models import User
        stored = db.scalars(select(User)).one().password_hash
    assert PASSWORD not in stored and stored.startswith("$argon2")
    assert login_response(client, customer["email"]).status_code == 200


def test_wrong_password_and_unknown_email_get_the_same_answer(client):
    customer = signup(client)
    wrong = login_response(client, customer["email"], "wrong password!")
    unknown = login_response(client, "nobody@example.com")
    assert wrong.status_code == unknown.status_code == 401
    assert wrong.json() == unknown.json()


def test_duplicate_email_is_rejected(client):
    customer = signup(client)
    response = client.post("/auth/register", json={
        "email": customer["email"].upper(), "password": PASSWORD, "name": "Again", "role": "CUSTOMER",
    })
    assert response.status_code == 409


def test_login_is_rate_limited_per_email(client):
    customer = signup(client)
    for _ in range(4):  # the signup's own login was the first attempt
        assert login_response(client, customer["email"], "wrong password!").status_code == 401
    blocked = login_response(client, customer["email"])
    assert blocked.status_code == 429 and int(blocked.headers["Retry-After"]) > 0


@pytest.mark.parametrize("token", [
    "not-a-jwt",
    # Signed with a different secret.
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIiwidHlwZSI6ImFjY2VzcyIsImV4cCI6OTk5OTk5OTk5OX0.bm90LXRoZS1yaWdodC1zaWc",
])
def test_forged_tokens_are_rejected(client, token):
    assert client.get("/auth/me", headers={"Authorization": f"Bearer {token}"}).status_code == 401


def test_expired_and_unsigned_tokens_are_rejected(client):
    import jwt

    import auth

    customer = signup(client)
    user_id = client.get("/auth/me", headers=customer["headers"]).json()["id"]
    expired = jwt.encode({"sub": user_id, "type": "access", "exp": 1}, auth.JWT_SECRET, algorithm="HS256")
    unsigned = jwt.encode({"sub": user_id, "type": "access", "exp": 9999999999}, None, algorithm="none")
    for token in [expired, unsigned]:
        assert client.get("/auth/me", headers={"Authorization": f"Bearer {token}"}).status_code == 401


def test_refresh_token_rotates_and_reuse_revokes_every_session(client):
    import auth

    customer = signup(client)
    first = client.cookies.get(auth.REFRESH_COOKIE)
    refreshed = client.post("/auth/refresh")
    assert refreshed.status_code == 200 and refreshed.json()["access_token"]
    second = client.cookies.get(auth.REFRESH_COOKIE)
    assert second and second != first
    # Someone replays the old (already used) token: all sessions end, including the new one.
    client.cookies.set(auth.REFRESH_COOKIE, first, path="/auth")
    assert client.post("/auth/refresh").status_code == 401
    client.cookies.set(auth.REFRESH_COOKIE, second, path="/auth")
    assert client.post("/auth/refresh").status_code == 401
    assert login_response(client, customer["email"]).status_code == 200  # logging in again works


def test_logout_ends_the_session(client):
    import auth

    signup(client)
    token = client.cookies.get(auth.REFRESH_COOKIE)
    assert client.post("/auth/logout").status_code == 204
    client.cookies.set(auth.REFRESH_COOKIE, token, path="/auth")
    assert client.post("/auth/refresh").status_code == 401


def test_users_cannot_see_each_others_data(client):
    payment_id = funded_payment(client)
    settle(payment_id)
    stranger = signup(client, "CUSTOMER")
    other_merchant = signup(client, "MERCHANT")
    owner = client.get(f"/payments/{payment_id}").json()
    for actor in [stranger, other_merchant]:
        for path in [f"/payments/{payment_id}", f"/payments/{payment_id}/ledger",
                     f"/payments/{payment_id}/receipt", f"/payments/{payment_id}/refunds",
                     f"/accounts/{owner['customer_account_id']}",
                     f"/accounts/{owner['customer_account_id']}/ledger"]:
            assert client.get(path, headers=actor["headers"]).status_code == 404, path
        assert client.get("/payments", headers=actor["headers"]).json() == []
        assert client.get("/notifications", headers=actor["headers"]).json() == []
    # The merchant who was paid sees the payment.
    merchant = ACTORS[owner["merchant_account_id"]]
    assert [p["id"] for p in client.get("/payments", headers=merchant["headers"]).json()] == [payment_id]


def test_customers_can_only_pay_from_their_own_account(client):
    victim, merchant = create_accounts(client)
    thief = signup(client, "CUSTOMER")
    response = client.post("/payments", headers=thief["headers"], json={
        "customer_account_id": victim["id"], "merchant_account_id": merchant["id"], "amount": "10",
    })
    assert response.status_code == 404
    top_up = client.post(f"/accounts/{victim['id']}/fund", headers=thief["headers"], json={"amount": "10"})
    assert top_up.status_code == 404


def test_roles_limit_what_users_can_do(client):
    customer, merchant = create_accounts(client)
    as_merchant = {"headers": merchant["headers"]}
    pay = client.post("/payments", **as_merchant, json={
        "customer_account_id": customer["id"], "merchant_account_id": merchant["id"], "amount": "10",
    })
    assert pay.status_code == 403
    assert client.post(f"/accounts/{merchant['id']}/fund", **as_merchant, json={"amount": "10"}).status_code == 403
    assert client.get("/reconciliation").status_code == 403


def test_customers_cannot_refund_and_other_merchants_cannot_see_the_payment(client):
    payment_id = funded_payment(client)
    settle(payment_id)
    assert client.post(f"/payments/{payment_id}/refunds", json={}).status_code == 403
    other = signup(client, "MERCHANT")
    assert client.post(f"/payments/{payment_id}/refunds", json={}, headers=other["headers"]).status_code == 404


def test_customer_top_ups_are_limited(client):
    customer, _ = create_accounts(client)
    fund = lambda amount: client.post(f"/accounts/{customer['id']}/fund", json={"amount": amount})  # noqa: E731
    assert fund("10000.01").status_code == 400
    for _ in range(5):
        assert fund("10000.00").status_code == 200
    assert fund("0.01").status_code == 400  # 50,000 in 24 hours


def test_admins_see_everything_and_can_run_reconciliation(client, monkeypatch):
    import create_admin

    funded_payment(client)
    signup(client)
    monkeypatch.setenv("ADMIN_PASSWORD", "admin password 123")
    create_admin.main("admin@example.com")
    token = login_response(client, "admin@example.com", "admin password 123").json()["access_token"]
    admin = {"Authorization": f"Bearer {token}"}
    assert len(client.get("/payments", headers=admin).json()) == 1
    assert len(client.get("/accounts", headers=admin).json()) == 3
    assert client.get("/reconciliation", headers=admin).json()["ok"] is True


def test_same_idempotency_key_returns_the_first_payment(client):
    customer, merchant = create_accounts(client)
    body = {"customer_account_id": customer["id"], "merchant_account_id": merchant["id"], "amount": "25.00"}
    first = client.post("/payments", json=body, headers={"Idempotency-Key": "order-42"})
    retry = client.post("/payments", json=body, headers={"Idempotency-Key": "order-42"})
    assert first.status_code == retry.status_code == 202
    assert retry.json()["payment_id"] == first.json()["payment_id"]
    assert retry.headers["Idempotent-Replayed"] == "true"
    assert retry.headers["Location"] == first.headers["Location"]
    assert len(client.get("/payments").json()) == 1
    with SessionLocal() as db:
        assert len(db.scalars(select(OutboxEvent)).all()) == 1
    changed = client.post("/payments", json={**body, "amount": "26.00"}, headers={"Idempotency-Key": "order-42"})
    assert changed.status_code == 422
    # Keys belong to one user: another customer's "order-42" is a different payment.
    other = signup(client)
    other_body = {**body, "customer_account_id": other["id"]}
    assert client.post("/payments", json=other_body, headers={**other["headers"], "Idempotency-Key": "order-42"}).json()[
        "payment_id"] != first.json()["payment_id"]


def test_refunds_accept_idempotency_keys(client):
    payment_id = funded_payment(client)
    settle(payment_id)
    merchant = ACTORS[client.get(f"/payments/{payment_id}").json()["merchant_account_id"]]
    headers = {**merchant["headers"], "Idempotency-Key": "refund-1"}
    first = client.post(f"/payments/{payment_id}/refunds", json={"amount": "100"}, headers=headers)
    retry = client.post(f"/payments/{payment_id}/refunds", json={"amount": "100"}, headers=headers)
    assert first.json()["id"] == retry.json()["id"]
    assert len(client.get(f"/payments/{payment_id}/refunds").json()) == 1


def test_dashboard_is_served_with_security_headers(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "script-src 'self'" in response.headers["Content-Security-Policy"]
    assert response.headers["X-Frame-Options"] == "DENY"
    assert client.get("/static/app.js").status_code == 200
