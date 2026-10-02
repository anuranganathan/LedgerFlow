"""Passwords, access tokens and refresh tokens.

- Passwords are hashed with argon2 (slow on purpose, so stolen hashes are hard to crack).
- Access tokens are JWTs valid for 15 minutes, sent as "Authorization: Bearer <token>".
  The user and role are loaded from the database on every request, so a deleted user or a
  changed role takes effect immediately.
- Refresh tokens are random strings in an HttpOnly cookie (JavaScript can't read it). Each one
  works once: using it returns a new one. If a used token is presented again, it was probably
  stolen, so every session of that user is revoked.
"""
import hashlib
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import Depends, HTTPException, Response, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from database import get_db
from models import RefreshToken, User, UserRole

JWT_SECRET = os.getenv("JWT_SECRET", "")
if len(JWT_SECRET) < 32:
    raise RuntimeError("JWT_SECRET must be set to a random string of at least 32 characters")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_TTL = timedelta(minutes=15)
REFRESH_TOKEN_TTL = timedelta(days=7)
REFRESH_COOKIE = "ledgerflow_refresh"
# Secure cookies are only sent over HTTPS, so this is on in production and off for localhost.
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"

password_hasher = PasswordHasher()
# Checked when the email doesn't exist, so a wrong email takes as long as a wrong password
# and response times don't reveal which emails are registered.
DUMMY_HASH = password_hasher.hash("not-a-real-password")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def hash_password(password: str) -> str:
    return password_hasher.hash(password)


def authenticate(db: Session, email: str, password: str) -> User | None:
    user = db.scalar(select(User).where(User.email == email.lower()))
    try:
        password_hasher.verify(user.password_hash if user else DUMMY_HASH, password)
    except (VerificationError, InvalidHashError):
        return None
    return user


def create_access_token(user: User) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"sub": str(user.id), "type": "access", "iat": now, "exp": now + ACCESS_TOKEN_TTL},
        JWT_SECRET, algorithm=JWT_ALGORITHM,
    )


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def issue_refresh_token(db: Session, user: User, response: Response) -> None:
    """Creates a refresh token and sets it as a cookie. The caller commits."""
    token = secrets.token_urlsafe(32)
    db.add(RefreshToken(
        user_id=user.id, token_hash=hash_token(token),
        expires_at=datetime.now(timezone.utc) + REFRESH_TOKEN_TTL,
    ))
    response.set_cookie(
        REFRESH_COOKIE, token, max_age=int(REFRESH_TOKEN_TTL.total_seconds()), httponly=True,
        secure=COOKIE_SECURE, samesite="strict", path="/auth",
    )


def clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(REFRESH_COOKIE, path="/auth", secure=COOKIE_SECURE, httponly=True, samesite="strict")


def as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def use_refresh_token(db: Session, token: str) -> User:
    """Revokes the token and returns its user. The caller issues the replacement and commits."""
    stored = db.scalar(select(RefreshToken).where(RefreshToken.token_hash == hash_token(token)).with_for_update())
    if stored is None or as_utc(stored.expires_at) < datetime.now(timezone.utc):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Session expired; please log in again")
    if stored.revoked_at is not None:
        # A token that was already used is being reused: treat it as stolen.
        revoke_all_sessions(db, stored.user_id)
        db.commit()
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Session expired; please log in again")
    stored.revoked_at = datetime.now(timezone.utc)
    return db.get(User, stored.user_id)


def revoke_refresh_token(db: Session, token: str) -> None:
    db.execute(update(RefreshToken).where(
        RefreshToken.token_hash == hash_token(token), RefreshToken.revoked_at.is_(None)
    ).values(revoked_at=datetime.now(timezone.utc)))


def revoke_all_sessions(db: Session, user_id: uuid.UUID) -> None:
    db.execute(update(RefreshToken).where(
        RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None)
    ).values(revoked_at=datetime.now(timezone.utc)))


def user_for_token(db: Session, token: str) -> User:
    unauthorized = HTTPException(
        status.HTTP_401_UNAUTHORIZED, "Not logged in or session expired",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM], options={"require": ["exp", "sub"]})
        if claims.get("type") != "access":
            raise unauthorized
        user = db.get(User, uuid.UUID(claims["sub"]))
    except (jwt.PyJWTError, ValueError):
        raise unauthorized
    if user is None:
        raise unauthorized
    return user


def current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return user_for_token(db, token)


def require_role(*roles: UserRole):
    def check(user: User = Depends(current_user)) -> User:
        if user.role not in roles:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Your role can't do this")
        return user
    return check
