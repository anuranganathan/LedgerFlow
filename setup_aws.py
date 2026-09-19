"""Creates every AWS resource LedgerFlow needs.

Run it once before starting the app:  python setup_aws.py
It is safe to run again: resources that already exist are left as they are.
Locally it talks to LocalStack; with AWS_ENDPOINT_URL unset it talks to real AWS.
"""
import json
import os

from botocore.exceptions import ClientError

from aws_services import AWS_REGION, METRICS_NAMESPACE, S3_BUCKET, SQS_QUEUE_NAME, client

LOG_GROUP = "/ledgerflow/app"
ALERT_TOPIC = "ledgerflow-alerts"
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")


def create_bucket() -> None:
    s3 = client("s3")
    try:
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=S3_BUCKET)
        else:
            s3.create_bucket(
                Bucket=S3_BUCKET, CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
            )
        print(f"Created S3 bucket {S3_BUCKET}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
        print(f"S3 bucket {S3_BUCKET} already exists")
    # Receipts contain payment data, so the bucket must never be public.
    s3.put_public_access_block(
        Bucket=S3_BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
        },
    )


def create_queues() -> None:
    sqs = client("sqs")
    # Messages that fail 3 times move to the dead-letter queue instead of retrying forever.
    dlq_url = sqs.create_queue(QueueName=f"{SQS_QUEUE_NAME}-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"]["QueueArn"]
    sqs.create_queue(
        QueueName=SQS_QUEUE_NAME,
        Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}),
        },
    )
    print(f"SQS queue {SQS_QUEUE_NAME} ready (DLQ: {SQS_QUEUE_NAME}-dlq)")


def create_monitoring() -> None:
    logs = client("logs")
    try:
        logs.create_log_group(logGroupName=LOG_GROUP)
        logs.put_retention_policy(logGroupName=LOG_GROUP, retentionInDays=7)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    print(f"CloudWatch log group {LOG_GROUP} ready")

    sns = client("sns")
    topic_arn = sns.create_topic(Name=ALERT_TOPIC)["TopicArn"]
    if ALERT_EMAIL:
        sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=ALERT_EMAIL)

    # Alert when 3 or more payments fail within 5 minutes.
    client("cloudwatch").put_metric_alarm(
        AlarmName="ledgerflow-failed-payments",
        Namespace=METRICS_NAMESPACE,
        MetricName="PaymentsFailed",
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=3,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[topic_arn],
    )
    print("CloudWatch alarm ledgerflow-failed-payments ready")


if __name__ == "__main__":
    create_bucket()
    create_queues()
    create_monitoring()
    print("AWS setup complete")
