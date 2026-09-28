# LedgerFlow — Event-Driven Payment Processing on AWS

LedgerFlow is an event-driven payment processing system built with a focus on reliability, asynchronous processing and cloud integration. A REST API accepts payments, a transactional outbox and Kafka hand them to a background worker, and the worker moves money with a double-entry ledger. Receipts go to Amazon S3, notifications flow through Amazon SQS to Slack, and metrics and logs go to Amazon CloudWatch. A Jenkins pipeline tests, builds and deploys it to AWS EC2.

**Live demo:** <http://52.66.120.13> · **API docs:** <http://52.66.120.13/docs> (hosted on AWS EC2, ap-south-1)

> Simulation only: no real money, banks or card data are involved.

## Architecture

```mermaid
flowchart LR
    Client[Dashboard / REST client] --> API[FastAPI]
    API -- payment + outbox event<br/>in one transaction --> DB[(PostgreSQL)]
    API --> Cache[(Redis)]
    DB --> Relay[Outbox relay]
    Relay -- PAYMENT_CREATED --> Kafka[[Kafka]]
    Kafka --> Worker[Payment worker]
    Worker -. after 3 failed attempts .-> KDLQ[[Kafka dead-letter topic]]
    Worker --> DB
    Worker -- receipt --> S3[(Amazon S3)]
    Worker -- event --> SQS[[Amazon SQS]]
    Worker -- metrics --> CW[Amazon CloudWatch]
    SQS --> Notifier[Notifier]
    Notifier --> Slack[Slack #payment-alerts]
    SQS -. after 3 failures .-> DLQ[[Dead-letter queue]]
    CW -- alarm --> SNS[Amazon SNS alert]
```

**What happens to one payment**

1. `POST /payments` validates the request and saves the payment as `PENDING` **and** a `PAYMENT_CREATED` event in the `outbox_events` table, in one database transaction. It returns `202 Accepted` with a `status_url` (also in the `Location` header) that the client polls for the result.
2. The **relay** publishes outbox events to Kafka and marks them as published. If Kafka is down, events wait in PostgreSQL and are sent when it's back.
3. The **worker** consumes the event, locks the payment and both account rows, checks the balance and writes two ledger entries (DEBIT the customer, CREDIT the merchant) in one database transaction. It commits the Kafka offset only after the event is fully handled.
4. The worker queues a notification on **SQS**, stores a JSON **receipt in S3** for a successful payment, and publishes `PaymentsSucceeded` / `PaymentsFailed` and end-to-end processing-time **metrics to CloudWatch**.
5. The **notifier** reads SQS and posts the result to **Slack**. It deletes the message only after Slack accepts it, so if Slack is down the message is retried. After 3 failed attempts it moves to a **dead-letter queue**.
6. A **CloudWatch alarm** fires when 3 or more payments fail within 5 minutes.

## Delivery guarantees

Every step can crash or find a dependency down, so each hand-off is designed to never lose a payment and never move money twice:

| Step | Guarantee | How |
|---|---|---|
| API → Kafka | No lost events | **Transactional outbox**: the payment and its event are committed together; `relay.py` publishes the event later. The API never returns an error because Kafka is down. |
| Kafka → worker | At-least-once | Auto-commit is off. The worker commits each offset only **after** handling the event, so a crash means redelivery, not loss. |
| Worker | Exactly-once money movement | The worker locks the payment row, re-reads its status, and skips settlement if it's already final. Account rows are locked in ID order (no deadlocks), so concurrent payments can't overdraw an account and a top-up can't overwrite a payment. |
| Worker side effects | Completed eventually, never duplicated | The SQS notification and S3 receipt are only created if missing. If one fails, the event is retried and only the missing step is redone. |
| Failing events | Never block the queue | 3 attempts with exponential backoff, then the event goes to the `payment-events-dlq` topic (CloudWatch alarm). Malformed messages go there straight away. After fixing the cause, `python replay_dlq.py --replay` sends them back safely. |
| SQS → Slack | At-least-once, duplicates skipped | The message is deleted only after Slack accepts it, and a notification already marked `SENT` isn't posted again. |

These are covered by tests, including concurrency tests on real PostgreSQL (`tests_postgres.py`).

## The ledger

Every change to a balance is a **double-entry transfer** written by `ledger.py`: one DEBIT and one CREDIT of the same amount, in the same transaction as the balance update.

| Movement | DEBIT | CREDIT |
|---|---|---|
| Top-up (`POST /accounts/{id}/fund`) | System account for the currency ("External funds") | The account |
| Payment | Customer | Merchant |
| Refund (`POST /payments/{id}/refunds`) | Merchant | Customer |

The **system account** stands for money entering from outside (a bank or card). Its balance is minus everything ever added, so across a currency all balances sum to zero and every rupee can be traced to where it came from.

Safeguards:
- **Database constraints**: customer and merchant balances can't go negative, amounts must be positive, a payment's refunded total can't exceed its amount, and every ledger entry belongs to exactly one payment or top-up.
- **Refunds** can be full or partial and go through the same outbox → Kafka → worker path as payments. The API locks the payment while checking the refundable amount (pending refunds count), so parallel requests can't refund more than was paid. A refund fails if the merchant no longer has the money.
- **Reconciliation** (`reconcile.py`, every 5 minutes, and `GET /reconciliation`) checks that every balance equals its ledger entries, debits equal credits per currency, every successful payment has exactly one debit and one credit, and refund totals match. It also flags payments, refunds or outbox events stuck for over 5 minutes. Problems raise CloudWatch alarms; the job never changes money itself.
- **Validation**: amounts must be positive with at most 2 decimal places and fit the database column; currencies must be supported (`SUPPORTED_CURRENCIES`, default `INR,USD,EUR,GBP`).

## Tech stack

| Area | Tools | Used for |
|---|---|---|
| Backend | Python, FastAPI, SQLAlchemy, Pydantic | REST API, validation, data models |
| Database / cache | PostgreSQL, Alembic, Redis | Source of truth; schema migrations; short-lived payment status cache |
| Messaging | Kafka, Amazon SQS | Kafka for payment events (outbox relay, dead-letter topic), SQS for notifications |
| AWS | boto3, S3, SQS, CloudWatch (metrics, logs, alarms), SNS, IAM, EC2 | Storage, queues, monitoring, hosting |
| DevOps | Docker, Docker Compose, Jenkins, LocalStack | Containers, CI/CD, local AWS |
| Integrations | Slack incoming webhooks | Payment alerts in a channel |
| Testing | pytest, moto | API, worker, AWS and Slack behaviour |

## DevOps and cloud

### CI/CD with Jenkins

The [`Jenkinsfile`](Jenkinsfile) defines the pipeline:

```
git push → Jenkins → install deps → pytest → docker build → deploy to EC2 (SSH + deploy.sh) → health check
```

- Test results are published to Jenkins as JUnit reports.
- The deploy stage runs [`deploy.sh`](deploy.sh) on the server. It pulls the code, rebuilds the containers and fails the build if `/health` doesn't respond.
- Jenkins itself runs in Docker: `docker compose -f jenkins/docker-compose.yml up -d --build`, then open <http://localhost:8080>.

### AWS automation with Python (boto3)

No AWS resources are created by hand; two scripts create them:

- [`setup_aws.py`](setup_aws.py) creates the S3 bucket (public access blocked), the SQS queue plus a dead-letter queue, a CloudWatch log group, an SNS alert topic and the CloudWatch alarm. It's idempotent, so running it again changes nothing. Locally it runs against LocalStack automatically on `docker compose up`.
- [`provision_ec2.py`](provision_ec2.py) builds the server: an IAM role, an SSH key pair, a security group, an EC2 instance and an Elastic IP. The server installs Docker on first boot and deploys itself. `python provision_ec2.py destroy` removes the server to stop charges.

### Monitoring (CloudWatch)

| Signal | Where |
|---|---|
| `PaymentsSucceeded`, `PaymentsFailed`, `NotificationsFailed` | CloudWatch metrics, namespace `LedgerFlow` |
| `PaymentEventsDeadLettered`, `OutboxPublishFailures`, `RefundsSucceeded`, `RefundsFailed` | CloudWatch metrics |
| `LedgerMismatches`, `StuckItems` | CloudWatch metrics from the reconciler |
| `PaymentProcessingTime` (ms, from request to settlement) | CloudWatch metric |
| API, worker and notifier logs | CloudWatch Logs group `/ledgerflow/app` (Docker `awslogs` driver) |
| 3+ failed payments in 5 minutes | CloudWatch alarm → SNS topic `ledgerflow-alerts` |
| Any event dead-lettered | CloudWatch alarm → SNS topic `ledgerflow-alerts` |
| Any ledger mismatch | CloudWatch alarm → SNS |
| Anything stuck for 5+ minutes, or the reconciler stopped reporting | CloudWatch alarm → SNS (missing data counts as a problem) |
| Is the app up? | `GET /health` (checks the database), used by `deploy.sh` |

### Security

- The EC2 server gets AWS access through an **IAM role**, so no access keys exist on the server or in the repo.
- The role follows **least privilege**: it only gets `PutObject`/`GetObject` on the one bucket, send/receive/delete on the one queue, metrics limited to the `LedgerFlow` namespace, and writes to the one log group.
- The security group opens HTTP to everyone but **SSH only to the admin's IP**. The server uses IMDSv2.
- The S3 bucket blocks all public access. Secrets such as the database password and Slack webhook live in a `.env` file that is never committed.

## Run locally

Requirements: Docker Desktop.

```bash
cp .env.example .env        # optional: add a Slack webhook URL
docker compose up --build
```

Open <http://localhost:8000> and click **Create demo accounts**, then send a payment. Try an amount larger than the balance to see a `FAILED` payment. Swagger UI is at <http://localhost:8000/docs>.

Everything runs locally, including AWS: LocalStack emulates S3, SQS, CloudWatch and SNS, and `setup_aws.py` provisions them on startup. The `migrate` service applies database migrations before the app starts. If port 8000 is taken, run `API_PORT=8001 docker compose up --build`. Without a Slack webhook, the notifier logs the Slack message instead (`docker compose logs notifier`).

### Database migrations

The schema is managed by **Alembic** (`migrations/`). The `migrate` service runs `alembic upgrade head` on every start and deploy. To change the schema, edit `models.py`, then:

```bash
docker compose run --rm migrate alembic revision --autogenerate -m "describe the change"
```

A test checks that the migrations produce exactly the schema in `models.py`.

### Tests

```bash
pip install -r requirements-dev.txt
pytest -q tests.py
```

The tests use SQLite and **moto**, an in-memory mock of AWS, so they need no Docker or AWS account. `tests_postgres.py` covers what SQLite can't (row locks under concurrency and migrating an existing database); it needs PostgreSQL:

```bash
docker run -d --rm --name ledgerflow-test-db -p 55432:5432 \
  -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test postgres:16-alpine
TEST_DATABASE_URL=postgresql+psycopg2://test:test@localhost:55432/test pytest -q tests_postgres.py
```

The tests cover:

- account and payment APIs
- successful and failed payments and the double-entry ledger
- the outbox and relay (including Kafka being down), offset commits, retries and the dead-letter topic
- duplicate events and crash recovery, without moving money twice
- concurrent payments, top-ups and refund requests on PostgreSQL
- top-ups and refunds in the ledger, validation, database constraints and reconciliation
- Redis fallback
- S3 receipts, SQS messages and CloudWatch metrics
- Slack delivery, including retries when Slack is down
- running the AWS setup script twice

### Slack

In Slack, create an app, enable **Incoming Webhooks**, add a webhook for a channel such as `#payment-alerts` and put the URL in `.env` as `SLACK_WEBHOOK_URL`. Messages look like:

```
❌ Payment FAILED
Payment ID: 118bf6e0-1fb2-46d7-8960-d4ffdd20eba4
Amount: 9000.00 INR
Reason: Insufficient balance
```

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/accounts` | Create a customer or merchant account |
| GET | `/accounts`, `/accounts/{id}` | List or get accounts |
| POST | `/accounts/{id}/fund` | Top up an account (recorded in the ledger) |
| GET | `/accounts/{id}/ledger` | Account statement: its ledger entries |
| POST | `/payments` | Create a payment (returns `202` + `PENDING`) |
| GET | `/payments`, `/payments/{id}` | List or get payments |
| GET | `/payments/{id}/ledger` | Double-entry ledger lines for a payment and its refunds |
| POST | `/payments/{id}/refunds` | Refund all or part of a payment (returns `202` + `PENDING`) |
| GET | `/payments/{id}/refunds`, `/refunds/{id}` | List or get refunds |
| GET | `/reconciliation` | Run the ledger checks now |
| GET | `/payments/{id}/receipt` | Receipt read back from S3 |
| GET | `/notifications` | Notification records and whether they were sent |

List endpoints take `limit` (1–100, default 50) and `offset`.
| GET | `/health` | Health check (includes a database query) |

## Failure handling

| Failure | Behaviour |
|---|---|
| Insufficient balance | Payment becomes `FAILED`, no balance changes, metric + Slack alert |
| Merchant can't cover a refund | Refund becomes `FAILED`, no balance changes, metric + Slack alert |
| A balance doesn't match the ledger | Reconciler reports it and the CloudWatch alarm fires |
| Kafka down when creating a payment | API still returns `202`; the event waits in the outbox and the relay publishes it when Kafka is back |
| Worker crashes mid-payment | Offset wasn't committed, so Kafka redelivers the event; the money transaction either fully happened or not at all |
| Same Kafka event delivered twice | Money moves once; missing side effects are completed, existing ones aren't repeated |
| Event keeps failing | 3 attempts with backoff, then the Kafka dead-letter topic + CloudWatch alarm; replay with `replay_dlq.py` |
| Malformed event | Sent straight to the dead-letter topic instead of crashing the worker |
| Redis down | Logged; reads fall back to PostgreSQL |
| S3 or SQS down | The committed payment is kept; the event is retried and then dead-lettered, and replaying it finishes only the missing receipt/notification |
| Slack down | Message stays in SQS and is retried, then moves to the dead-letter queue |
| CloudWatch down | Metric is skipped; payment processing continues |

## Project structure

```text
app.py               FastAPI app and endpoints
ledger.py            Double-entry transfers, top-ups and row locking
reconcile.py         Ledger checks and stuck-item detection (runs every 5 minutes)
outbox.py            Writes events to the outbox table
relay.py             Publishes outbox events to Kafka
worker.py            Kafka consumer that settles payments and refunds (retries, dead-letter topic)
replay_dlq.py        Lists or replays dead-lettered events
notifier.py          SQS consumer that sends Slack messages
models.py            SQLAlchemy tables: accounts, payments, refunds, top_ups, ledger_entries, notifications, outbox_events
migrations/          Alembic database migrations
schemas.py           Pydantic request/response models
database.py          Database engine and sessions
kafka_client.py      Kafka producer/consumer
redis_client.py      Payment status cache
aws_services.py      boto3 helpers for S3, SQS and CloudWatch
slack_client.py      Slack webhook client
setup_aws.py         Creates S3 / SQS / CloudWatch / SNS resources
provision_ec2.py     Creates IAM role, security group and EC2 server
deploy.sh            Deploy script run on the server
static/index.html    Dashboard
tests.py             pytest suite (SQLite + moto)
tests_postgres.py    Concurrency and migration tests on PostgreSQL
Jenkinsfile          CI/CD pipeline
jenkins/             Jenkins in Docker
docker-compose.yml       Local stack (with LocalStack)
docker-compose.prod.yml  Production stack on EC2 (real AWS)
```

## Limitations and next steps

- One EC2 instance runs everything. In production I'd use managed services (RDS for PostgreSQL, ElastiCache for Redis, MSK for Kafka) and run the app on ECS.
- Jenkins builds the image, but the server also rebuilds it. Pushing a versioned image to Amazon ECR would make deploys faster and rollbacks easy.
- The demo is served over plain HTTP, with no domain or TLS.
- There is no authentication, and no idempotency keys on `POST /payments`.
- Infrastructure is scripted with boto3. Terraform or CloudFormation would add drift detection and plan/apply reviews.
