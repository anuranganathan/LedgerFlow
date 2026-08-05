# LedgerFlow Interview Guide

## 30-second explanation

LedgerFlow is an educational payment simulator. FastAPI validates a payment and stores it as `PENDING` in PostgreSQL, then publishes its ID to Kafka. A worker processes it later, atomically changes both balances and creates debit/credit ledger entries. Redis caches the status, S3 stores a JSON receipt, and SQS carries a notification task. Docker Compose runs the local stack. It does not process real money.

## 2-minute explanation

The client first creates customer and merchant accounts and adds a simulated balance. When it posts a payment, FastAPI checks the account roles, amount, and currency. It saves a `PENDING` row and publishes one `PAYMENT_CREATED` event containing only the payment ID. The API returns immediately, which demonstrates asynchronous processing.

The Kafka worker changes the status to `PROCESSING`. It checks the customer balance. If funds are sufficient, one database transaction debits the customer, credits the merchant, creates two ledger entries, and marks the payment `SUCCESS`. Otherwise it only marks it `FAILED` with a reason. PostgreSQL is the source of truth. The worker caches the final status in Redis, stores a successful receipt in S3, and sends a notification task to SQS. Local files replace S3 and SQS for an AWS-free demo.

## Complete flow

1. Create one `CUSTOMER` and one `MERCHANT` account.
2. Add simulated customer funds.
3. `POST /payments` validates input and stores `PENDING`.
4. FastAPI publishes `PAYMENT_CREATED` to `payment-events` and returns `202`.
5. The worker consumes the event and stores `PROCESSING`.
6. It checks the balance under a database row lock.
7. Success: both balances, two ledger rows, and `SUCCESS` commit together.
8. Failure: `FAILED` and its reason are committed; balances do not change.
9. Redis receives the final status for 10 minutes.
10. A successful payment gets a JSON receipt; every completed payment gets a notification.

## Technology choices

- **FastAPI:** concise REST routes, type hints, Pydantic validation, and automatic Swagger docs.
- **PostgreSQL:** durable relational data and transactions for consistent balances and ledger rows.
- **Kafka:** an event stream decouples accepting the request from processing it.
- **Redis:** a fast, temporary cache for one frequently read value: payment status.
- **S3:** durable object storage for receipt files, separate from transactional rows.
- **SQS:** a work queue for notifications; one consumer can receive and remove a task.
- **Docker:** the same local commands start all required runtime components.

Kafka keeps an ordered event log for consumer groups; SQS is a task queue where a received message is deleted after work. PostgreSQL is durable relational truth with transactions; Redis is temporary key-value acceleration. Asynchronous means the caller does not wait for the worker, so the API truthfully returns `PENDING`.

The debit and credit entries give an auditable record of value leaving one account and entering another. If funds are insufficient, neither balance changes. If Redis fails, reads use PostgreSQL. If S3 fails, the successful database payment remains valid but has no receipt key and the error is logged. If Kafka publish fails, the API returns `503`; the saved row remains `PENDING` and would need manual recovery in this simple version.

## Likely interview questions

1. **Is this a real payment gateway?** No. It is a simulator and has no bank, card, or UPI integration.
2. **Why return `PENDING`?** Processing is delegated to a Kafka worker, so the HTTP request finishes quickly.
3. **Why Kafka?** It demonstrates producer-consumer decoupling and asynchronous event processing.
4. **Why not use SQS for payments too?** The project gives Kafka the event-stream role and SQS the notification-task role so their difference is visible.
5. **What is the Kafka key?** The payment ID, which keeps events for the same payment on the same partition.
6. **What is the consumer group?** `payflow-payment-worker`; Kafka assigns each event to one member of that group.
7. **Why PostgreSQL?** Accounts, balances, and ledgers are related and require transactions.
8. **What commits atomically?** The final status, both balance updates, and both ledger entries.
9. **Why use `Decimal`?** Binary floats can introduce rounding errors; `Decimal` matches PostgreSQL `NUMERIC`.
10. **Why two ledger entries?** One records the customer debit and one records the merchant credit.
11. **What if funds are low?** The payment becomes `FAILED`; neither balance nor ledger changes.
12. **Why Redis?** It demonstrates a cache on the common status lookup without replacing PostgreSQL.
13. **What if Redis is down?** The error is logged and PostgreSQL serves the status.
14. **Why does the cache expire?** A 10-minute TTL limits stale data and memory use.
15. **Why S3 instead of PostgreSQL for receipts?** Receipts are objects; S3 is designed for object storage while the database stores only their key.
16. **Why a presigned URL?** It gives temporary access to a private receipt without exposing AWS credentials.
17. **Why SQS?** It represents a notification work queue whose message is deleted after processing.
18. **What does local mode do?** It writes receipts and queued messages to local files, requiring no AWS account.
19. **What if Kafka is unavailable?** Payment creation reports `503`, while its database row remains visibly `PENDING`.
20. **Does this guarantee exactly-once processing?** No. It intentionally has no deduplication or Kafka transaction.
21. **Can a duplicate event cause a problem?** Completed payments are ignored, but simultaneous or interrupted duplicate processing is not fully protected.
22. **How would you improve reliability?** Add an outbox, idempotency/deduplication, retries, a DLQ, and reconciliation.
23. **How would it scale?** Add API replicas and worker instances in the same consumer group, then partition Kafka; first add idempotency and concurrency tests.
24. **Why no microservices?** One API and one worker demonstrate the required concepts with much less operational complexity.
25. **Why no Alembic?** The fixed educational schema uses `create_all`; production evolution would need migrations.
26. **What security is missing?** Authentication, authorization, rate limits, secrets management, encryption policy, and payment-industry controls.
27. **Is it production ready or PCI compliant?** No, and the project makes neither claim.

## Honest limitations and future improvements

There is no authentication, idempotency, processed-event table, outbox, retry topic, DLQ, schema registry, monitoring, reconciliation, or real notification provider. Local file queues are single-process demo tools. Future work would add those features only after defining production requirements and failure guarantees.
