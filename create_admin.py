"""Creates an admin user, or resets an existing user's password and makes them an admin.

    docker compose exec api python create_admin.py admin@example.com

The password is asked for interactively (or read from ADMIN_PASSWORD), never passed as an
argument, so it doesn't end up in shell history.
"""
import getpass
import os
import sys

from sqlalchemy import select

import auth
from database import SessionLocal
from models import User, UserRole


def main(email: str) -> None:
    password = os.getenv("ADMIN_PASSWORD") or getpass.getpass("Password (at least 12 characters): ")
    if len(password) < 12:
        sys.exit("Password must be at least 12 characters")
    with SessionLocal() as db:
        user = db.scalar(select(User).where(User.email == email.lower()))
        if user is None:
            user = User(email=email.lower(), name="Admin", role=UserRole.ADMIN, password_hash="")
            db.add(user)
        user.role = UserRole.ADMIN
        user.password_hash = auth.hash_password(password)
        db.flush()
        auth.revoke_all_sessions(db, user.id)
        db.commit()
    print(f"Admin {email.lower()} is ready")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python create_admin.py <email>")
    main(sys.argv[1])
