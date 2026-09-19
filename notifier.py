"""Reads payment events from SQS and posts them to Slack."""
import logging
import time
import uuid

import aws_services
from database import SessionLocal
from models import Notification, NotificationStatus
from slack_client import format_payment_message, send_slack_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def handle_message(message: dict) -> None:
    event = message["body"]
    send_slack_message(format_payment_message(event))
    with SessionLocal() as db:
        notification = db.get(Notification, uuid.UUID(event["notification_id"]))
        if notification:
            notification.status = NotificationStatus.SENT
            db.commit()
    # Delete only after Slack accepted it. If Slack is down the message stays in SQS,
    # is retried, and after 3 failed attempts SQS moves it to the dead-letter queue.
    aws_services.delete_notification(message["receipt_handle"])


def run_notifier() -> None:
    logger.info("Notifier started")
    while True:
        try:
            messages = aws_services.receive_notifications()
        except Exception as exc:
            logger.warning("SQS not reachable, retrying in 5 seconds: %s", exc)
            time.sleep(5)
            continue
        for message in messages:
            try:
                handle_message(message)
            except Exception:
                logger.exception("Could not deliver notification %s", message["body"])
                aws_services.put_metric("NotificationsFailed")


if __name__ == "__main__":
    run_notifier()
