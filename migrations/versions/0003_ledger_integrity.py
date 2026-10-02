"""Ledger integrity: top-ups and refunds in the ledger, constraints and indexes.

Before this change, funding an account raised its balance without any ledger entry. For every
account whose balance is higher than its ledger says, this migration records the difference as
an opening-balance top-up from the currency's system account, so every balance matches the ledger.

Revision ID: 0003
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

# Same values as ledger.py; copied so this migration never changes when the app code does.
SYSTEM_ACCOUNT_NAMESPACE = uuid.UUID("5b0f4a52-1c0e-4c5e-9d64-2f4f3e9e7a10")


def upgrade() -> None:
    bind = op.get_bind()
    postgres = bind.dialect.name == "postgresql"
    if postgres:
        # A new enum value must be committed before it can be used.
        with op.get_context().autocommit_block():
            op.execute("ALTER TYPE accounttype ADD VALUE IF NOT EXISTS 'SYSTEM'")

    op.create_table(
        "top_ups",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("account_id", sa.Uuid(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("description", sa.String(200), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("amount > 0", name="ck_top_ups_amount_positive"),
    )
    op.create_index("ix_top_ups_account_id", "top_ups", ["account_id"])
    op.create_table(
        "refunds",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("reason", sa.String(200), nullable=True),
        sa.Column("status", sa.Enum("PENDING", "SUCCESS", "FAILED", name="refundstatus", length=20),
                  nullable=False),
        sa.Column("failure_reason", sa.String(200), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("amount > 0", name="ck_refunds_amount_positive"),
    )
    op.create_index("ix_refunds_payment_id", "refunds", ["payment_id"])

    with op.batch_alter_table("accounts") as batch:
        batch.create_check_constraint(
            "ck_accounts_balance_non_negative", "account_type = 'SYSTEM' OR balance >= 0"
        )
    with op.batch_alter_table("payments") as batch:
        batch.add_column(sa.Column("refunded_amount", sa.Numeric(14, 2), nullable=False, server_default="0"))
        batch.create_check_constraint("ck_payments_amount_positive", "amount > 0")
        batch.create_check_constraint(
            "ck_payments_refunded_amount_valid", "refunded_amount >= 0 AND refunded_amount <= amount"
        )
        batch.create_index("ix_payments_customer_created", ["customer_account_id", "created_at"])
        batch.create_index("ix_payments_merchant_created", ["merchant_account_id", "created_at"])
        batch.create_index("ix_payments_created_at", ["created_at"])
    with op.batch_alter_table("ledger_entries") as batch:
        batch.alter_column("payment_id", existing_type=sa.Uuid(), nullable=True)
        batch.add_column(sa.Column("refund_id", sa.Uuid(), nullable=True))
        batch.add_column(sa.Column("top_up_id", sa.Uuid(), nullable=True))
        batch.create_foreign_key("fk_ledger_entries_refund_id", "refunds", ["refund_id"], ["id"])
        batch.create_foreign_key("fk_ledger_entries_top_up_id", "top_ups", ["top_up_id"], ["id"])
        batch.create_check_constraint(
            "ck_ledger_entries_one_source",
            "(payment_id IS NULL) <> (top_up_id IS NULL) AND (refund_id IS NULL OR payment_id IS NOT NULL)",
        )
        batch.create_check_constraint("ck_ledger_entries_amount_positive", "amount > 0")
        batch.create_index("ix_ledger_entries_payment_id", ["payment_id"])
        batch.create_index("ix_ledger_entries_account_created", ["account_id", "created_at"])
    with op.batch_alter_table("notifications") as batch:
        batch.add_column(sa.Column("refund_id", sa.Uuid(), nullable=True))
        batch.create_foreign_key("fk_notifications_refund_id", "refunds", ["refund_id"], ["id"])
        batch.create_index("ix_notifications_payment_id", ["payment_id"])

    record_opening_balances(bind)


def record_opening_balances(bind) -> None:
    now = datetime.now(timezone.utc)
    accounts = bind.execute(sa.text("""
        SELECT a.id, a.currency, a.balance,
               COALESCE(SUM(CASE WHEN l.entry_type = 'CREDIT' THEN l.amount ELSE -l.amount END), 0)
        FROM accounts a LEFT JOIN ledger_entries l ON l.account_id = a.id
        WHERE a.account_type <> 'SYSTEM'
        GROUP BY a.id, a.currency, a.balance
    """)).all()
    system_balances: dict[str, Decimal] = {}
    for account_id, currency, balance, ledger_total in accounts:
        missing = (Decimal(str(balance)) - Decimal(str(ledger_total))).quantize(Decimal("0.01"))
        if missing <= 0:
            continue
        system_id = uuid.uuid5(SYSTEM_ACCOUNT_NAMESPACE, f"external-funds:{currency}")
        if currency not in system_balances:
            exists = bind.execute(sa.text("SELECT 1 FROM accounts WHERE id = :id"), {"id": system_id}).first()
            if not exists:
                bind.execute(sa.text(
                    "INSERT INTO accounts (id, name, account_type, balance, currency, created_at) "
                    "VALUES (:id, :name, 'SYSTEM', 0, :currency, :now)"
                ), {"id": system_id, "name": f"External funds ({currency})", "currency": currency, "now": now})
            system_balances[currency] = Decimal("0")
        top_up_id = uuid.uuid4()
        bind.execute(sa.text(
            "INSERT INTO top_ups (id, account_id, amount, description, created_at) "
            "VALUES (:id, :account, :amount, 'Opening balance (funds added before top-ups were in the ledger)', :now)"
        ), {"id": top_up_id, "account": uuid.UUID(str(account_id)), "amount": missing, "now": now})
        for entry_account, entry_type in [(system_id, "DEBIT"), (uuid.UUID(str(account_id)), "CREDIT")]:
            bind.execute(sa.text(
                "INSERT INTO ledger_entries (id, top_up_id, account_id, entry_type, amount, created_at) "
                "VALUES (:id, :top_up, :account, :type, :amount, :now)"
            ), {"id": uuid.uuid4(), "top_up": top_up_id, "account": entry_account, "type": entry_type,
                "amount": missing, "now": now})
        system_balances[currency] -= missing
    for currency, change in system_balances.items():
        bind.execute(sa.text("UPDATE accounts SET balance = balance + :change WHERE id = :id"), {
            "change": change, "id": uuid.uuid5(SYSTEM_ACCOUNT_NAMESPACE, f"external-funds:{currency}"),
        })


def downgrade() -> None:
    raise NotImplementedError("Ledger history can't be un-recorded; restore from a backup instead.")
