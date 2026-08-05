import logging
import os
import uuid

import redis

logger = logging.getLogger(__name__)
client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
TTL_SECONDS = 600


def get_payment_status(payment_id: uuid.UUID) -> str | None:
    try:
        return client.get(f"payment-status:{payment_id}")
    except redis.RedisError as exc:
        logger.warning("Redis read failed; using PostgreSQL: %s", exc)
        return None


def set_payment_status(payment_id: uuid.UUID, status: str) -> None:
    try:
        client.setex(f"payment-status:{payment_id}", TTL_SECONDS, status)
    except redis.RedisError as exc:
        logger.warning("Redis write failed; continuing: %s", exc)
