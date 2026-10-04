"""Creates every AWS resource LedgerFlow needs.

Run it once before starting the app:  python setup_aws.py
It is safe to run again: resources that already exist are left as they are.
Locally it talks to LocalStack; with AWS_ENDPOINT_URL unset it talks to real AWS.
"""
import json
import os

from botocore.exceptions import ClientError

from aws_services import AWS_REGION, METRICS_NAMESPACE, S3_BUCKET, SQS_QUEUE_NAME, WEBHOOK_QUEUE_NAME, client

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
    # Notifications: after 3 failed Slack attempts a message moves to the dead-letter queue.
    create_queue(SQS_QUEUE_NAME, max_receives=3)
    # Webhooks: webhooks.py retries with its own backoff and gives up after 8 attempts; the
    # dead-letter queue only catches messages the sender itself kept crashing on.
    create_queue(WEBHOOK_QUEUE_NAME, max_receives=12)


def create_queue(name: str, max_receives: int) -> None:
    sqs = client("sqs")
    dlq_url = sqs.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"]["QueueArn"]
    sqs.create_queue(
        QueueName=name,
        Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_receives)}),
        },
    )
    print(f"SQS queue {name} ready (DLQ: {name}-dlq)")


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

    alarm(topic_arn, "ledgerflow-failed-payments", "PaymentsFailed", threshold=3)  # 3+ in 5 minutes
    # Any event in the Kafka dead-letter topic needs a person to look at it.
    alarm(topic_arn, "ledgerflow-dead-lettered-events", "PaymentEventsDeadLettered", threshold=1)
    alarm(topic_arn, "ledgerflow-ledger-mismatch", "LedgerMismatches", threshold=1)
    # The reconciler reports StuckItems every 5 minutes. No data for 15 minutes means the
    # reconciler itself stopped, which is also worth an alert.
    alarm(topic_arn, "ledgerflow-stuck-items", "StuckItems", threshold=1, period=900,
          statistic="Maximum", missing_data="breaching")


def alarm(topic_arn: str, name: str, metric: str, threshold: float, period: int = 300,
          statistic: str = "Sum", missing_data: str = "notBreaching") -> None:
    client("cloudwatch").put_metric_alarm(
        AlarmName=name,
        Namespace=METRICS_NAMESPACE,
        MetricName=metric,
        Statistic=statistic,
        Period=period,
        EvaluationPeriods=1,
        Threshold=threshold,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        TreatMissingData=missing_data,
        AlarmActions=[topic_arn],
    )
    print(f"CloudWatch alarm {name} ready")

if __name__ == "__main__":
    create_bucket()
    create_queues()
    create_monitoring()
    print("AWS setup complete")
