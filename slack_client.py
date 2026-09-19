import logging
import os

import httpx

logger = logging.getLogger(__name__)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def format_payment_message(event: dict[str, str]) -> str:
    icon = ":white_check_mark:" if event["status"] == "SUCCESS" else ":x:"
    lines = [
        f"{icon} *Payment {event['status']}*",
        f"Payment ID: `{event['payment_id']}`",
        f"Amount: {event['amount']} {event['currency']}",
    ]
    if event.get("reason"):
        lines.append(f"Reason: {event['reason']}")
    return "\n".join(lines)


def send_slack_message(text: str) -> None:
    """Posts to a Slack incoming webhook. Without a webhook URL the message is only logged."""
    if not SLACK_WEBHOOK_URL:
        logger.info("Slack disabled, message would be:\n%s", text)
        return
    response = httpx.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=5)
    response.raise_for_status()
