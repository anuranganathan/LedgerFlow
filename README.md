# LedgerFlow — Event-Driven Payment Processing on AWS

LedgerFlow is an event-driven payment processing system built with a focus on reliability, asynchronous processing and cloud integration. A REST API accepts payments, a transactional outbox and Kafka hand them to a background worker, and the worker moves money with a double-entry ledger. Receipts go to Amazon S3, notifications flow through Amazon SQS to Slack, and metrics and logs go to Amazon CloudWatch. A Jenkins pipeline tests, builds and deploys it to AWS EC2.

## Live demo

| | |
|---|---|
| **App** | <https://52-66-120-13.sslip.io> |
| **API docs (Swagger)** | <https://52-66-120-13.sslip.io/docs> |
| **Health check** | <https://52-66-120-13.sslip.io/health> |

Hosted on AWS EC2 (ap-south-1) over HTTPS with a Let's Encrypt certificate, deployed by the Jenkins pipeline below.

**Try it in a minute:**

1. Open the app and click **Try the demo**. It creates a customer with ₹5000 and a merchant, and logs you in as the customer.
2. Pay the demo store. The result appears in about a second through a live update (watch the **● Live** badge), with no page refresh.
3. Pay more than your balance to see a `FAILED` payment with its reason.
4. Click a payment to see its double-entry ledger lines and the receipt stored in S3.
5. Click **Switch to merchant view** and refund part of the payment.

> Simulation only: no real money, banks or card data are involved. Demo accounts are throwaway.

## Highlights

- **Never loses a payment:** transactional outbox, Kafka with manual offset commits, retries and a dead-letter topic.
- **Never moves money twice:** idempotent workers, row locking in a fixed order, `Idempotency-Key` on payments and refunds.
- **Auditable money:** double-entry ledger, refunds, database constraints, and a reconciliation job with CloudWatch alarms.
- **Secure:** argon2 passwords, short-lived JWTs with rotating refresh tokens, per-user data access, rate limiting, HTTPS, least-privilege IAM with no keys on the server.
- **Real-time:** Server-Sent Events to the dashboard and HMAC-signed webhooks to merchants.
- **Shipped by CI/CD:** Jenkins runs 81 tests (including PostgreSQL concurrency tests), builds one image per commit, pushes it to ECR (scanned on push), and deploys that exact commit to EC2 with health checks and automatic rollback.

## Architecture

```mermaid
flowchart LR
    Client[Dashboard / REST client] --> API[FastAPI]
    API -- payment + outbox event<br/>in one transaction --> DB[(PostgreSQL)]
    API -- rate limits --> Cache[(Redis)]
    DB --> Relay[Outbox relay]
    Relay -- PAYMENT_CREATED --> Kafka[[Kafka]]
    Kafka --> Worker[Payment worker]
    Worker -. after 3 failed attempts .-> KDLQ[[Kafka dead-letter topic]]
    Worker --> DB
    Worker -- receipt --> S3[(Amazon S3)]
    Worker -- event --> SQS[[Amazon SQS]]
    Worker -- metrics --> CW[Amazon CloudWatch]
    Worker -- live update --> Cache
    Cache -- Server-Sent Events --> API
    Worker -- webhook job --> WQ[[SQS webhooks]]
    WQ --> Sender[Webhook sender]
    Sender -- signed POST --> Shop[Merchant server]
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

## How customers and merchants find out the result

`POST /payments` answers `202 Accepted` straight away; the result follows in three ways:

| Who | How | Details |
|---|---|---|
| Customer and merchant in the dashboard | **Live update (Server-Sent Events)** | When the worker settles a payment or refund it publishes an event on Redis for both users; `GET /events` streams it to their open dashboards. The result appears in about 0.3 s, no polling. On reconnect the dashboard reloads, so nothing is missed. |
| The merchant's server | **Signed webhooks** | `PUT /accounts/{id}/webhook-endpoint` registers a URL. Events (`payment.succeeded`, `payment.failed`, `refund.succeeded`, `refund.failed`) are POSTed with an HMAC-SHA256 `LedgerFlow-Signature` header, retried with backoff (8 attempts over ~2 hours), and logged per attempt; failed deliveries can be retried. `examples/webhook_receiver.py` shows how to verify them. |
| Any API client | **Polling** | `GET /payments/{id}` (the `status_url` from the 202 response). |
| The operations team | **Slack** | Through SQS and the notifier, as before. |

Webhook safety: URLs must be https and resolve only to public IPs (checked when saved and again at every delivery; the request goes to the checked IP, so DNS rebinding can't redirect it to an internal address), redirects aren't followed, each event has a stable ID for deduplication, and the delivery row is created in the same transaction that settles the payment.

## Authentication and access control

| Piece | How it works |
|---|---|
| Users and roles | `POST /auth/register` creates a CUSTOMER or MERCHANT user and their first account. Admins can't sign up; they're created with `python create_admin.py <email>`. |
| Passwords | Hashed with **argon2**. Login answers the same way (and takes as long) for a wrong password and an unknown email, so it doesn't reveal who is registered. |
| Access tokens | **JWT**, HS256, valid 15 minutes, sent as `Authorization: Bearer ...`. The user and role are re-read from the database on every request. |
| Sessions | A random **refresh token** in an `HttpOnly`, `SameSite=Strict` cookie (JavaScript can't read it; `Secure` when served over HTTPS). Only its SHA-256 hash is stored. Each refresh token works once and is replaced (**rotation**); presenting a used one again revokes every session of that user. |
| Ownership | Customers pay only from their own account and see only their own payments; merchants see payments they received and can refund only those; admins see everything and run reconciliation. Other users' data returns `404`, so its existence isn't revealed. |
| Idempotency | `POST /payments` and refunds accept an `Idempotency-Key` header. The response is saved with the payment in one transaction; a retry gets the same response (`Idempotent-Replayed: true`), and reusing a key for a different request is rejected. The dashboard sends one with every payment. |
| Rate limits (Redis) | Login 5 per email per 5 minutes and 20 per IP per minute; sign-up 20 per IP per hour; payments and refunds 30 per user per minute; top-ups 10 per minute. If Redis is down, requests are allowed and the failure is logged. |
| Demo money | Customers can top up their own account (at most ₹10,000 at a time and ₹50,000 per 24 hours). A real system would take this money from a card or bank transfer. |
| Browser security | Content-Security-Policy (only the dashboard's own script can run), `X-Frame-Options: DENY`, `nosniff`, `no-referrer`. |

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
| Database / cache | PostgreSQL, Alembic, Redis | Source of truth; schema migrations; rate limiting and live-update pub/sub |
| Security | argon2, PyJWT | Password hashing, access tokens |
| Messaging | Kafka, Amazon SQS | Kafka for payment events (outbox relay, dead-letter topic), SQS for notifications |
| AWS | boto3, S3, SQS, CloudWatch (metrics, logs, alarms), SNS, IAM, EC2 | Storage, queues, monitoring, hosting |
| DevOps | Docker, Docker Compose, Jenkins, LocalStack | Containers, CI/CD, local AWS |
| Integrations | Slack incoming webhooks | Payment alerts in a channel |
| Testing | pytest, moto | API, worker, AWS and Slack behaviour |

## DevOps and cloud

### CI/CD with Jenkins

The [`Jenkinsfile`](Jenkinsfile) defines the pipeline:

```
git push → Jenkins → unit tests → PostgreSQL tests → image ledgerflow:<commit> → push to ECR
         → deploy.sh <commit> on EC2 → all containers healthy? → done (or automatic rollback)
```

- **Two test stages**, both published as JUnit reports: `tests.py`, and `tests_postgres.py` against a throwaway PostgreSQL container (row locks and migrations need the real database).
- **One image per commit.** Every app service runs the same image, built for arm64 (the server is Graviton) and pushed to **Amazon ECR** with immutable tags, scanned on push, the last 30 kept. The server only pulls, so production runs exactly the commit that passed the tests.
- **Deploy** ([`deploy.sh`](deploy.sh)) checks out that commit, starts it and waits until every container is healthy: the API answers `/health`, and each background process reports a heartbeat (a stuck worker counts as down). If that doesn't happen within 5 minutes, it **redeploys the last healthy commit** and fails the build. Only `main` is deployed, and builds never run in parallel.
- Without ECR configured, the server builds the image for that exact commit itself.
- Images apply Debian security updates at build time: ECR's scan went from 6 critical / 20 high findings to 0 critical / 3 high (the rest have no upstream fix yet).
- Jenkins itself runs in Docker: `docker compose -f jenkins/docker-compose.yml up -d --build`, then open <http://localhost:8080>. Set `ECR_REPOSITORY`, `DEPLOY_HOST`, `DEPLOY_USER` and optionally `SITE_URL`, plus the credentials `aws-ecr-push` and `deploy-ssh-key`.

### AWS automation with Python (boto3)

No AWS resources are created by hand; two scripts create them:

- [`setup_aws.py`](setup_aws.py) creates the S3 bucket (public access blocked), the SQS queue plus a dead-letter queue, a CloudWatch log group, an SNS alert topic and the CloudWatch alarm. It's idempotent, so running it again changes nothing. Locally it runs against LocalStack automatically on `docker compose up`.
- [`provision_ec2.py`](provision_ec2.py) builds the server: an IAM role, an ECR repository, an SSH key pair, a security group, an Elastic IP and an EC2 instance. The server installs Docker on first boot and deploys itself. `python provision_ec2.py destroy` removes the server to stop charges.

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
- **HTTPS**: Caddy gets and renews a Let's Encrypt certificate automatically (the server is reachable as `<ip-with-dashes>.sslip.io` without buying a domain), redirects HTTP to HTTPS and sends HSTS. Session cookies are `Secure`. The API container isn't exposed; only Caddy is.
- The security group opens HTTP/HTTPS to everyone but **SSH only to the admin's IP**. The server uses IMDSv2.
- The server can only **pull** images from ECR; pushing needs the separate CI credential.
- The S3 bucket blocks all public access. Secrets such as the database password and Slack webhook live in a `.env` file that is never committed.

## Run locally

Requirements: Docker Desktop.

```bash
cp .env.example .env        # optional: add a Slack webhook URL
docker compose up --build
```

Open <http://localhost:8000> and click **Try the demo**: it creates a customer with ₹5000 and a merchant, and logs you in as the customer. Pay the store, try an amount larger than the balance to see a `FAILED` payment, then **Switch to merchant view** to refund. Swagger UI is at <http://localhost:8000/docs> (register with `POST /auth/register`, then click **Authorize**). For an admin: `docker compose exec api python create_admin.py you@example.com`.

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
- login, token rotation and reuse detection, forged and expired tokens, rate limits
- that users can't see or touch each other's data, and role rules
- idempotent payments and refunds, including parallel retries on PostgreSQL
- the API keeping working when Redis is down
- live updates and the event stream; webhook signing, SSRF protection, retries and redelivery
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

All endpoints except `/`, `/health` and `/auth/*` need a logged-in user.

| Method | Path | Purpose |
|---|---|---|
| POST | `/auth/register` | Sign up as a customer or merchant |
| POST | `/auth/login` | Log in (form fields `username` = email, `password`); returns an access token and sets the session cookie |
| POST | `/auth/refresh` | New access token from the session cookie |
| POST | `/auth/logout`, `/auth/logout-everywhere` | End this session, or all of them |
| GET | `/auth/me` | The logged-in user and their accounts |
| POST | `/accounts` | Open another account (type follows your role) |
| GET | `/accounts`, `/accounts/{id}` | Your accounts (admins: all) |
| GET | `/merchants` | Merchants a customer can pay |
| POST | `/accounts/{id}/fund` | Top up your account, or any account as admin (recorded in the ledger) |
| GET | `/accounts/{id}/ledger` | Account statement: its ledger entries |
| POST | `/payments` | Pay a merchant (returns `202` + `PENDING`; supports `Idempotency-Key`) |
| GET | `/payments`, `/payments/{id}` | List or get payments |
| GET | `/payments/{id}/ledger` | Double-entry ledger lines for a payment and its refunds |
| POST | `/payments/{id}/refunds` | Refund all or part of a payment (returns `202` + `PENDING`) |
| GET | `/payments/{id}/refunds`, `/refunds/{id}` | List or get refunds |
| GET | `/reconciliation` | Run the ledger checks now (admins) |
| GET | `/payments/{id}/receipt` | Receipt read back from S3 |
| GET | `/notifications` | Notification records and whether they were sent |
| GET | `/events` | Live updates for your payments and refunds (Server-Sent Events) |
| GET, PUT, DELETE | `/accounts/{id}/webhook-endpoint` | Merchant webhook URL (the signing secret is returned when first set) |
| POST | `/accounts/{id}/webhook-endpoint/rotate-secret` | New signing secret |
| GET | `/accounts/{id}/webhook-deliveries` | Delivery log with attempts and results |
| POST | `/webhook-deliveries/{id}/retry` | Send a failed delivery again |

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
| Redis down | Logged; rate limiting is skipped, everything else works |
| Merchant's server is down | Webhook retried after 10 s, 30 s, 1 min, 5 min, 15 min, 30 min, 1 h, then marked `FAILED`; the merchant can retry it |
| Browser loses its live connection | Reconnects automatically and reloads; slow polling continues as a safety net |
| Client retries a payment after a timeout | Same `Idempotency-Key` returns the original payment; nothing is charged twice |
| Refresh token stolen and replayed | Detected on reuse; all of that user's sessions are revoked |
| S3 or SQS down | The committed payment is kept; the event is retried and then dead-lettered, and replaying it finishes only the missing receipt/notification |
| Slack down | Message stays in SQS and is retried, then moves to the dead-letter queue |
| CloudWatch down | Metric is skipped; payment processing continues |

## Project structure

```text
app.py               FastAPI app, endpoints and access control
auth.py              Password hashing, JWT access tokens, refresh-token sessions
idempotency.py       Idempotency-Key handling
create_admin.py      Creates an admin user
ledger.py            Double-entry transfers, top-ups and row locking
reconcile.py         Ledger checks and stuck-item detection (runs every 5 minutes)
outbox.py            Writes events to the outbox table
relay.py             Publishes outbox events to Kafka
worker.py            Kafka consumer that settles payments and refunds (retries, dead-letter topic)
events.py            Live updates: Redis pub/sub to Server-Sent Events
webhooks.py          Merchant webhooks: signing, SSRF checks, delivery with retries
examples/            Example webhook receiver that verifies signatures
replay_dlq.py        Lists or replays dead-lettered events
notifier.py          SQS consumer that sends Slack messages
models.py            SQLAlchemy tables: users, refresh_tokens, idempotency_keys, accounts, payments, refunds,
                     top_ups, ledger_entries, notifications, outbox_events
migrations/          Alembic database migrations
schemas.py           Pydantic request/response models
database.py          Database engine and sessions
kafka_client.py      Kafka producer/consumer
redis_client.py      Rate limiting
aws_services.py      boto3 helpers for S3, SQS and CloudWatch
slack_client.py      Slack webhook client
setup_aws.py         Creates S3 / SQS / CloudWatch / SNS resources
provision_ec2.py     Creates IAM role, security group and EC2 server
deploy.sh            Deploys one commit on the server, health-checks it, rolls back on failure
heartbeat.py         Health checks for the background processes
Caddyfile            HTTPS reverse proxy
ci/ecr.py            ECR login and tag check for Jenkins
static/              Dashboard (index.html, app.js, style.css)
tests.py             pytest suite (SQLite + moto)
tests_postgres.py    Concurrency and migration tests on PostgreSQL
Jenkinsfile          CI/CD pipeline
jenkins/             Jenkins in Docker
docker-compose.yml       Local stack (with LocalStack)
docker-compose.prod.yml  Production stack on EC2 (real AWS, HTTPS, health checks)
```

## Limitations and next steps

These are deliberate trade-offs for a single-server demo:

- **One EC2 instance runs everything**, including a single Kafka broker (no replicas) and PostgreSQL without automated backups. In production I'd use managed services: MSK with 3 replicas, RDS with point-in-time recovery, ElastiCache, and the app on ECS behind a load balancer.
- **Deploys briefly restart the containers** (seconds of downtime). Blue/green or rolling deploys need more than one server.
- **Rollback assumes backward-compatible migrations** (add columns/tables; drop them a release later). A destructive migration would need a restore.
- **Webhook signing secrets are stored in the database as-is** (they must be readable to sign); in production they'd be encrypted with AWS KMS.
- **Customers are notified in the app** (live updates) and merchants via webhooks; there's no email or SMS.
- **Infrastructure is scripted with boto3.** Terraform or CloudFormation would add drift detection and reviewed plans.
- **It's a simulation**: no real banks, cards, KYC or regulatory reporting.
