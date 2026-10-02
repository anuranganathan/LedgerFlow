"""Merchant webhooks: LedgerFlow POSTs payment and refund events to the merchant's server.

This is how a shop's backend learns that a customer's payment succeeded without polling.

Delivery:
  - The worker creates the delivery row in the same transaction that settles the payment, then
    queues its ID on SQS. This process POSTs the event and records every attempt.
  - Failed attempts are retried with growing delays (10s ... 1h), 8 attempts in total. The
    merchant can see each attempt and retry a failed delivery from the API or dashboard.
  - Delivery is at-least-once, so each event has a stable ID the receiver can deduplicate on.

Security:
  - Each request is signed: LedgerFlow-Signature: t=<unix time>,v1=<HMAC-SHA256 of "t.body">
    with the endpoint's secret. Receivers check the signature and reject old timestamps, so
    events can't be forged or replayed (see examples/webhook_receiver.py).
  - SSRF protection: URLs must use https and resolve only to public IP addresses, checked when
    registered and again at every delivery. The request goes to the IP that was checked (with
    the real hostname for TLS), so DNS can't be switched to an internal address in between.
    Redirects are not followed.
"""
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import secrets
import socket
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

import aws_services
from database import SessionLocal
from models import DeliveryStatus, Payment, Refund, WebhookDelivery, WebhookEndpoint, utc_now

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
# Local development only: lets endpoints use http and private addresses (e.g. another container).
ALLOW_INSECURE_URLS = os.getenv("WEBHOOK_ALLOW_INSECURE_URLS", "false").lower() == "true"
RETRY_DELAYS = [10, 30, 60, 300, 900, 1800, 3600]  # seconds after attempts 1..7
MAX_ATTEMPTS = len(RETRY_DELAYS) + 1
TIMEOUT_SECONDS = 5
EVENT_NAMESPACE = uuid.UUID("0c3f8f5e-6f3e-4d59-8b0e-4f1f0b6f2a61")


class InvalidWebhookUrl(ValueError):
    pass


def new_secret() -> str:
    return "whsec_" + secrets.token_urlsafe(24)


def resolve(url: str) -> tuple[str, ipaddress.IPv4Address | ipaddress.IPv6Address]:
    """Checks the URL and returns its host and the public IP address to connect to."""
    parts = urlsplit(url)
    allowed = {"https", "http"} if ALLOW_INSECURE_URLS else {"https"}
    if parts.scheme not in allowed:
        raise InvalidWebhookUrl("Webhook URL must use https")
    if not parts.hostname or parts.username or parts.password:
        raise InvalidWebhookUrl("Webhook URL must have a host and no credentials")
    try:
        port = parts.port or (443 if parts.scheme == "https" else 80)
        infos = socket.getaddrinfo(parts.hostname, port, type=socket.SOCK_STREAM)
    except (socket.gaierror, ValueError):
        raise InvalidWebhookUrl("Webhook URL host can't be resolved")
    addresses = sorted({ipaddress.ip_address(info[4][0]) for info in infos}, key=str)
    if not ALLOW_INSECURE_URLS and not all(address.is_global for address in addresses):
        raise InvalidWebhookUrl("Webhook URL must not point to a private or internal address")
    return parts.hostname, addresses[0]


def sign(secret: str, timestamp: int, body: bytes) -> str:
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def post(url: str, secret: str, event_id: str, body: bytes) -> httpx.Response:
    host, address = resolve(url)
    parts = urlsplit(url)
    ip_host = f"[{address}]" if address.version == 6 else str(address)
    target = urlunsplit(parts._replace(netloc=f"{ip_host}:{parts.port}" if parts.port else ip_host))
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "LedgerFlow-Webhooks/1.0",
        "Host": parts.netloc,
        "LedgerFlow-Event-Id": event_id,
        "LedgerFlow-Signature": sign(secret, int(time.time()), body),
    }
    # sni_hostname makes TLS verify the certificate against the real hostname, not the IP.
    extensions = {"sni_hostname": host} if parts.scheme == "https" else {}
    return send(httpx.Request("POST", target, content=body, headers=headers, extensions=extensions))


def send(request: httpx.Request) -> httpx.Response:
    with httpx.Client(timeout=TIMEOUT_SECONDS) as client:
        return client.send(request, follow_redirects=False)


# ---------- Creating deliveries (called by the worker) ----------

def event_data(payment: Payment, refund: Refund | None) -> tuple[str, dict]:
    if refund is not None:
        return f"refund.{'succeeded' if refund.status.value == 'SUCCESS' else 'failed'}", {
            "refund_id": str(refund.id), "payment_id": str(payment.id), "amount": str(refund.amount),
            "currency": payment.currency, "status": refund.status.value,
            "failure_reason": refund.failure_reason,
        }
    return f"payment.{'succeeded' if payment.status.value == 'SUCCESS' else 'failed'}", {
        "payment_id": str(payment.id), "customer_account_id": str(payment.customer_account_id),
        "merchant_account_id": str(payment.merchant_account_id), "amount": str(payment.amount),
        "currency": payment.currency, "description": payment.description,
        "status": payment.status.value, "failure_reason": payment.failure_reason,
    }


def add_delivery(db: Session, payment: Payment, refund: Refund | None) -> None:
    """Adds a delivery if the merchant has a webhook endpoint. Saved by the caller's commit."""
    endpoint = db.scalar(select(WebhookEndpoint).where(
        WebhookEndpoint.account_id == payment.merchant_account_id, WebhookEndpoint.enabled.is_(True)
    ))
    if endpoint is None:
        return
    event_type, data = event_data(payment, refund)
    event_id = uuid.uuid5(EVENT_NAMESPACE, f"{event_type}:{(refund or payment).id}")
    db.add(WebhookDelivery(
        id=event_id, endpoint_id=endpoint.id, payment_id=payment.id,
        refund_id=refund.id if refund else None, event_type=event_type,
        payload={"id": str(event_id), "type": event_type,
                 "created_at": datetime.now(timezone.utc).isoformat(), "data": data},
    ))


def queue_deliveries(db: Session, payment_id: uuid.UUID, refund_id: uuid.UUID | None) -> None:
    """Puts not-yet-queued deliveries on SQS. Safe to repeat."""
    deliveries = db.scalars(select(WebhookDelivery).where(
        WebhookDelivery.payment_id == payment_id,
        WebhookDelivery.refund_id == refund_id if refund_id else WebhookDelivery.refund_id.is_(None),
        WebhookDelivery.enqueued_at.is_(None),
    )).all()
    for delivery in deliveries:
        queue(db, delivery)


def queue(db: Session, delivery: WebhookDelivery) -> None:
    aws_services.send_message(aws_services.WEBHOOK_QUEUE_NAME, {"delivery_id": str(delivery.id)})
    delivery.enqueued_at = utc_now()
    db.commit()


# ---------- Sending (this process) ----------

def handle_message(message: dict) -> None:
    receipt = message["receipt_handle"]
    queue_name = aws_services.WEBHOOK_QUEUE_NAME
    with SessionLocal() as db:
        delivery = db.get(WebhookDelivery, uuid.UUID(message["body"]["delivery_id"]))
        if delivery is None or delivery.status != DeliveryStatus.PENDING:
            aws_services.delete_message(queue_name, receipt)  # already delivered or given up
            return
        endpoint = db.get(WebhookEndpoint, delivery.endpoint_id)
        delivered, status_code, error = attempt(endpoint, delivery)
        delivery.attempts += 1
        delivery.last_status_code, delivery.last_error = status_code, error
        if delivered:
            delivery.status, delivery.delivered_at = DeliveryStatus.DELIVERED, utc_now()
        elif delivery.attempts >= MAX_ATTEMPTS or not endpoint.enabled:
            delivery.status = DeliveryStatus.FAILED
            aws_services.put_metric("WebhookDeliveriesFailed")
        db.commit()
        if delivery.status == DeliveryStatus.PENDING:
            aws_services.retry_later(queue_name, receipt, RETRY_DELAYS[delivery.attempts - 1])
        else:
            aws_services.delete_message(queue_name, receipt)
        logger.info("Webhook %s attempt %d: %s", delivery.id, delivery.attempts, error or status_code)


def attempt(endpoint: WebhookEndpoint, delivery: WebhookDelivery) -> tuple[bool, int | None, str | None]:
    if not endpoint.enabled:
        return False, None, "Webhook endpoint is disabled"
    body = json.dumps(delivery.payload, separators=(",", ":")).encode()
    try:
        response = post(endpoint.url, endpoint.secret, str(delivery.id), body)
    except InvalidWebhookUrl as exc:
        return False, None, str(exc)
    except httpx.HTTPError as exc:
        return False, None, f"{type(exc).__name__}: {exc}"[:500]
    if 200 <= response.status_code < 300:
        return True, response.status_code, None
    return False, response.status_code, f"HTTP {response.status_code}"


def run_sender() -> None:
    logger.info("Webhook sender started")
    while True:
        try:
            messages = aws_services.receive_messages(aws_services.WEBHOOK_QUEUE_NAME)
        except Exception as exc:
            logger.warning("SQS not reachable, retrying in 5 seconds: %s", exc)
            time.sleep(5)
            continue
        for message in messages:
            try:
                handle_message(message)
            except Exception:
                # Not deleted, so SQS delivers the message again after its visibility timeout.
                logger.exception("Could not process webhook message %s", message["body"])


if __name__ == "__main__":
    run_sender()
