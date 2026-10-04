"""Creates a demo customer (with 5000 INR) and a demo merchant, and prints their logins.

    docker compose exec api python seed.py

The passwords are random and only printed here. The dashboard's "Try the demo" button does the
same through the API, so this is mainly for trying the API from the command line.
"""
import secrets
from decimal import Decimal

import auth
import ledger
from database import SessionLocal
from models import Account, AccountType, User, UserRole


def create_user(db, role: UserRole, name: str) -> tuple[User, Account, str]:
    email = f"demo-{role.value.lower()}-{secrets.token_hex(3)}@example.com"
    password = secrets.token_urlsafe(12)
    user = User(email=email, name=name, role=role, password_hash=auth.hash_password(password))
    db.add(user)
    db.flush()
    account = Account(owner_id=user.id, name=name, account_type=AccountType(role.value), currency="INR")
    db.add(account)
    db.flush()
    return user, account, password


def seed() -> None:
    with SessionLocal() as db:
        customer, customer_account, customer_password = create_user(db, UserRole.CUSTOMER, "Demo Customer")
        merchant, merchant_account, merchant_password = create_user(db, UserRole.MERCHANT, "Demo Merchant")
        ledger.top_up(db, customer_account.id, Decimal("5000.00"), "Demo funds")
        db.commit()
    print(f"Customer: {customer.email} / {customer_password}  (account {customer_account.id})")
    print(f"Merchant: {merchant.email} / {merchant_password}  (account {merchant_account.id})")
    print("\nLog in and pay:")
    print(f"TOKEN=$(curl -s -X POST http://localhost:8000/auth/login "
          f"-d 'username={customer.email}&password={customer_password}' | python3 -c "
          "'import sys, json; print(json.load(sys.stdin)[\"access_token\"])')")
    print("curl -X POST http://localhost:8000/payments -H \"Authorization: Bearer $TOKEN\" "
          "-H 'Content-Type: application/json' -H \"Idempotency-Key: $(uuidgen)\" "
          f"-d '{{\"customer_account_id\":\"{customer_account.id}\","
          f"\"merchant_account_id\":\"{merchant_account.id}\",\"amount\":\"500.00\","
          "\"description\":\"Demo purchase\"}'")


if __name__ == "__main__":
    seed()
