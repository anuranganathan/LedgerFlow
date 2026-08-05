import json
import os
from pathlib import Path
from typing import Any

import boto3

USE_AWS = os.getenv("USE_AWS", "false").lower() == "true"
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET = os.getenv("S3_BUCKET", "payflow-receipts")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")
RECEIPTS_DIR = Path(os.getenv("RECEIPTS_DIR", "receipts"))
LOCAL_QUEUE_FILE = Path(os.getenv("LOCAL_QUEUE_FILE", "local_notifications.json"))


def store_receipt(payment_id: str, receipt: dict[str, Any]) -> str:
    key = f"receipts/{payment_id}.json"
    body = json.dumps(receipt, indent=2)
    if USE_AWS:
        boto3.client("s3", region_name=AWS_REGION).put_object(
            Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json"
        )
    else:
        RECEIPTS_DIR.mkdir(parents=True, exist_ok=True)
        (RECEIPTS_DIR / f"{payment_id}.json").write_text(body)
    return key


def receipt_location(key: str) -> dict[str, str]:
    if USE_AWS:
        url = boto3.client("s3", region_name=AWS_REGION).generate_presigned_url(
            "get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=600
        )
        return {"receipt_s3_key": key, "download_url": url}
    return {"receipt_s3_key": key, "local_path": str((Path.cwd() / key).resolve())}


def send_notification(message: dict[str, str]) -> None:
    if USE_AWS:
        boto3.client("sqs", region_name=AWS_REGION).send_message(
            QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(message)
        )
        return
    queue = json.loads(LOCAL_QUEUE_FILE.read_text()) if LOCAL_QUEUE_FILE.exists() else []
    queue.append(message)
    LOCAL_QUEUE_FILE.write_text(json.dumps(queue, indent=2))


def receive_notification() -> dict[str, Any] | None:
    if USE_AWS:
        response = boto3.client("sqs", region_name=AWS_REGION).receive_message(
            QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=1
        )
        messages = response.get("Messages", [])
        if not messages:
            return None
        item = messages[0]
        return {"body": json.loads(item["Body"]), "receipt_handle": item["ReceiptHandle"]}
    queue = json.loads(LOCAL_QUEUE_FILE.read_text()) if LOCAL_QUEUE_FILE.exists() else []
    if not queue:
        return None
    body = queue.pop(0)
    LOCAL_QUEUE_FILE.write_text(json.dumps(queue, indent=2))
    return {"body": body}


def delete_notification(receipt_handle: str | None) -> None:
    if USE_AWS and receipt_handle:
        boto3.client("sqs", region_name=AWS_REGION).delete_message(
            QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
        )
