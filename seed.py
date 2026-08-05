from decimal import Decimal

from database import Base, SessionLocal, engine
from models import Account, AccountType


def seed() -> None:
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        customer = Account(
            name="Demo Customer", account_type=AccountType.CUSTOMER, balance=Decimal("5000.00")
        )
        merchant = Account(name="Demo Merchant", account_type=AccountType.MERCHANT)
        db.add_all([customer, merchant])
        db.commit()
        print(f"Customer account ID: {customer.id}")
        print(f"Merchant account ID: {merchant.id}")
        print("\nCreate a payment:")
        print(
            "curl -X POST http://localhost:8000/payments "
            "-H 'Content-Type: application/json' "
            f"-d '{{\"customer_account_id\":\"{customer.id}\","
            f"\"merchant_account_id\":\"{merchant.id}\",\"amount\":\"500.00\","
            "\"currency\":\"INR\",\"description\":\"Demo purchase\"}}'"
        )


if __name__ == "__main__":
    seed()
