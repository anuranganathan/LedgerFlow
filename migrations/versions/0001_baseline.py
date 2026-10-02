"""Baseline: the tables the app used to create with Base.metadata.create_all().

Databases created before migrations existed already have these tables, so each table is only
created when it is missing. That lets the running server adopt migrations without data loss.

Revision ID: 0001
"""
import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def enum(*values: str, name: str) -> sa.Enum:
    return sa.Enum(*values, name=name, length=20)


def upgrade() -> None:
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    if "accounts" not in existing:
        op.create_table(
            "accounts",
            sa.Column("id", sa.Uuid(), primary_key=True),
            sa.Column("name", sa.String(100), nullable=False),
            sa.Column("account_type", enum("CUSTOMER", "MERCHANT", name="accounttype"), nullable=False),
            sa.Column("balance", sa.Numeric(14, 2), nullable=False),
            sa.Column("currency", sa.String(3), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        )
    if "payments" not in existing:
        op.create_table(
            "payments",
            sa.Column("id", sa.Uuid(), primary_key=True),
            sa.Column("customer_account_id", sa.Uuid(), sa.ForeignKey("accounts.id"), nullable=False),
            sa.Column("merchant_account_id", sa.Uuid(), sa.ForeignKey("accounts.id"), nullable=False),
            sa.Column("amount", sa.Numeric(14, 2), nullable=False),
            sa.Column("currency", sa.String(3), nullable=False),
            sa.Column("description", sa.String(200), nullable=True),
            sa.Column(
                "status",
                enum("PENDING", "PROCESSING", "SUCCESS", "FAILED", name="paymentstatus"),
                nullable=False,
            ),
            sa.Column("failure_reason", sa.String(200), nullable=True),
            sa.Column("receipt_s3_key", sa.String(300), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        )
    if "ledger_entries" not in existing:
        op.create_table(
            "ledger_entries",
            sa.Column("id", sa.Uuid(), primary_key=True),
            sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
            sa.Column("account_id", sa.Uuid(), sa.ForeignKey("accounts.id"), nullable=False),
            sa.Column("entry_type", enum("DEBIT", "CREDIT", name="entrytype"), nullable=False),
            sa.Column("amount", sa.Numeric(14, 2), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        )
    if "notifications" not in existing:
        op.create_table(
            "notifications",
            sa.Column("id", sa.Uuid(), primary_key=True),
            sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
            sa.Column("message", sa.String(300), nullable=False),
            sa.Column("status", enum("QUEUED", "SENT", name="notificationstatus"), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        )


def downgrade() -> None:
    for table in ["notifications", "ledger_entries", "payments", "accounts"]:
        op.drop_table(table)
