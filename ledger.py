"""The only code that changes balances. Every movement writes a DEBIT and a CREDIT entry.

Locking rule, used everywhere to avoid deadlocks: lock the payment first, then the refund,
then accounts in ID order (lock() sorts them).
"""
import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from models import Account, AccountType, EntryType, LedgerEntry, TopUp

# Fixed namespace so each currency's system account always gets the same ID.
SYSTEM_ACCOUNT_NAMESPACE = uuid.UUID("5b0f4a52-1c0e-4c5e-9d64-2f4f3e9e7a10")


def lock(db: Session, model, ids: list[uuid.UUID]) -> list:
    """SELECT ... FOR UPDATE in ID order.

    populate_existing makes SQLAlchemy use the values read under the lock, not older copies
    already loaded in the session.
    """
    return db.scalars(
        select(model).where(model.id.in_(ids)).order_by(model.id)
        .with_for_update().execution_options(populate_existing=True)
    ).all()


def system_account_id(currency: str) -> uuid.UUID:
    return uuid.uuid5(SYSTEM_ACCOUNT_NAMESPACE, f"external-funds:{currency}")


def ensure_system_account(db: Session, currency: str) -> uuid.UUID:
    """Creates the currency's system account if needed. Safe when two requests race to do it."""
    account_id = system_account_id(currency)
    insert = postgresql_insert if db.get_bind().dialect.name == "postgresql" else sqlite_insert
    db.execute(insert(Account).values(
        id=account_id, name=f"External funds ({currency})", account_type=AccountType.SYSTEM,
        balance=Decimal("0.00"), currency=currency,
    ).on_conflict_do_nothing(index_elements=["id"]))
    return account_id


def transfer(db: Session, *, debit: Account, credit: Account, amount: Decimal, **source) -> None:
    """Moves amount from debit to credit. The caller holds the locks and checks the balance.

    source is payment_id (plus refund_id for refunds) or top_up_id.
    """
    debit.balance -= amount
    credit.balance += amount
    db.add_all([
        LedgerEntry(account_id=debit.id, entry_type=EntryType.DEBIT, amount=amount, **source),
        LedgerEntry(account_id=credit.id, entry_type=EntryType.CREDIT, amount=amount, **source),
    ])


def top_up(db: Session, account_id: uuid.UUID, amount: Decimal, description: str) -> Account | None:
    """Adds money from outside to an account, recorded in the ledger. The caller commits."""
    account = db.get(Account, account_id)
    if account is None or account.account_type == AccountType.SYSTEM:
        return None
    system_id = ensure_system_account(db, account.currency)
    accounts = {row.id: row for row in lock(db, Account, [account_id, system_id])}
    entry = TopUp(id=uuid.uuid4(), account_id=account_id, amount=amount, description=description)
    db.add(entry)
    db.flush()  # the ledger entries reference the top-up row
    transfer(db, debit=accounts[system_id], credit=accounts[account_id], amount=amount, top_up_id=entry.id)
    return accounts[account_id]
