"""Idempotency keys: retrying a request returns the first response instead of repeating it.

A client sends a unique Idempotency-Key header with POST /payments or a refund. The response
is saved under that key in the same transaction as the payment, so either both are saved or
neither is. A retry with the same key and body gets the saved response back; the same key with
a different body is rejected. Keys are kept for 24 hours (the reconciler deletes older ones).
"""
import hashlib
import json
import uuid

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from models import IdempotencyKey


def fingerprint(path: str, body: BaseModel) -> str:
    canonical = json.dumps({"path": path, "body": body.model_dump(mode="json")}, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def find(db: Session, user_id: uuid.UUID, key: str) -> IdempotencyKey | None:
    return db.get(IdempotencyKey, (user_id, key))


def replay(record: IdempotencyKey, request_hash: str) -> JSONResponse:
    if record.request_hash != request_hash:
        raise HTTPException(422, "This Idempotency-Key was already used for a different request")
    headers = {"Idempotent-Replayed": "true"}
    if record.location:
        headers["Location"] = record.location
    return JSONResponse(record.response_body, status_code=record.status_code, headers=headers)


def save(db: Session, user_id: uuid.UUID, key: str, request_hash: str,
         response: JSONResponse, body: dict) -> None:
    """Adds the record to the session; the caller's commit saves it with the payment or refund."""
    db.add(IdempotencyKey(
        user_id=user_id, key=key, request_hash=request_hash, status_code=response.status_code,
        response_body=body, location=response.headers.get("Location"),
    ))
