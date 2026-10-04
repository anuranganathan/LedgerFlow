import json
import logging
import os
from functools import cache
from typing import Any

import boto3

logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
# Locally this points at LocalStack (http://localstack:4566). On real AWS it is left empty.
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL") or None
S3_BUCKET = os.getenv("S3_BUCKET", "ledgerflow-receipts")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "ledgerflow-notifications")
WEBHOOK_QUEUE_NAME = os.getenv("WEBHOOK_QUEUE_NAME", "ledgerflow-webhooks")
METRICS_NAMESPACE = "LedgerFlow"


@cache
def client(service: str):
    """One client per service and process; boto3 clients are thread-safe and slow to create."""
    return boto3.client(service, region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL)


@cache
def queue_url(name: str = SQS_QUEUE_NAME) -> str:
    return client("sqs").get_queue_url(QueueName=name)["QueueUrl"]


# ---------- S3: payment receipts ----------

def store_receipt(payment_id: str, receipt: dict[str, Any]) -> str:
    key = f"receipts/{payment_id}.json"
    client("s3").put_object(
        Bucket=S3_BUCKET, Key=key, Body=json.dumps(receipt, indent=2), ContentType="application/json"
    )
    return key


def read_receipt(key: str) -> dict[str, Any]:
    response = client("s3").get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(response["Body"].read())


# ---------- SQS: notification and webhook queues ----------

def send_message(queue: str, message: dict[str, str]) -> None:
    client("sqs").send_message(QueueUrl=queue_url(queue), MessageBody=json.dumps(message))


def receive_messages(queue: str, max_messages: int = 5, wait_seconds: int = 10) -> list[dict[str, Any]]:
    response = client("sqs").receive_message(
        QueueUrl=queue_url(queue), MaxNumberOfMessages=max_messages, WaitTimeSeconds=wait_seconds
    )
    return [
        {"body": json.loads(item["Body"]), "receipt_handle": item["ReceiptHandle"]}
        for item in response.get("Messages", [])
    ]


def delete_message(queue: str, receipt_handle: str) -> None:
    client("sqs").delete_message(QueueUrl=queue_url(queue), ReceiptHandle=receipt_handle)


def retry_later(queue: str, receipt_handle: str, seconds: int) -> None:
    """Hides the message for a while; SQS delivers it again afterwards (used for backoff)."""
    client("sqs").change_message_visibility(
        QueueUrl=queue_url(queue), ReceiptHandle=receipt_handle, VisibilityTimeout=seconds
    )


def send_notification(message: dict[str, str]) -> None:
    send_message(SQS_QUEUE_NAME, message)


def receive_notifications(max_messages: int = 5) -> list[dict[str, Any]]:
    return receive_messages(SQS_QUEUE_NAME, max_messages)


def delete_notification(receipt_handle: str) -> None:
    delete_message(SQS_QUEUE_NAME, receipt_handle)


# ---------- CloudWatch: custom metrics ----------

def put_metric(name: str, value: float = 1, unit: str = "Count") -> None:
    """Metrics must never break payment processing, so errors are only logged."""
    try:
        client("cloudwatch").put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[{"MetricName": name, "Value": value, "Unit": unit}],
        )
    except Exception as exc:
        logger.warning("Could not publish metric %s: %s", name, exc)
