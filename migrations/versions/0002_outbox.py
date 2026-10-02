"""Transactional outbox, and re-queue payments stuck by the old direct Kafka publish.

Before this change a payment stayed PENDING (or PROCESSING, if the worker crashed) forever when
its Kafka event was lost. This migration adds an outbox event for every such payment, so the
worker settles them once the new code is running.

Revision ID: 0002
"""
import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("topic", sa.String(100), nullable=False),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_events_unpublished", "outbox_events", ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"), sqlite_where=sa.text("published_at IS NULL"),
    )
    with op.batch_alter_table("notifications") as batch:
        batch.add_column(sa.Column("enqueued_at", sa.DateTime(timezone=True), nullable=True))

    bind = op.get_bind()
    now = datetime.now(timezone.utc)
    # Notifications that existed before this change were already handed to SQS.
    bind.execute(sa.text("UPDATE notifications SET enqueued_at = created_at"))
    # PROCESSING is no longer used: the worker now settles a payment in one transaction.
    bind.execute(sa.text("UPDATE payments SET status = 'PENDING' WHERE status = 'PROCESSING'"))
    outbox = sa.table(
        "outbox_events",
        sa.column("id", sa.Uuid()), sa.column("topic", sa.String()), sa.column("key", sa.String()),
        sa.column("payload", sa.JSON()), sa.column("attempts", sa.Integer()),
        sa.column("created_at", sa.DateTime(timezone=True)),
    )
    stuck = bind.execute(sa.text("SELECT id FROM payments WHERE status = 'PENDING'")).scalars().all()
    if stuck:
        op.bulk_insert(outbox, [
            {
                "id": uuid.uuid4(), "topic": "payment-events", "key": str(payment_id),
                "payload": {
                    "event_id": str(uuid.uuid4()), "event_type": "PAYMENT_CREATED",
                    "payment_id": str(payment_id), "occurred_at": now.isoformat(),
                },
                "attempts": 0, "created_at": now,
            }
            for payment_id in (uuid.UUID(str(value)) for value in stuck)
        ])


def downgrade() -> None:
    with op.batch_alter_table("notifications") as batch:
        batch.drop_column("enqueued_at")
    op.drop_index("ix_outbox_events_unpublished", table_name="outbox_events")
    op.drop_table("outbox_events")
