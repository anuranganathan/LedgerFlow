"""Redis: request rate limiting.

(An earlier version cached payment statuses here. The API read PostgreSQL anyway, so the cache
saved no work and could serve a stale status, and it was removed. PostgreSQL is the only source
of truth for money and statuses.)
"""
import logging
import os
import time

import redis

logger = logging.getLogger(__name__)
client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)


def hit(name: str, identity: str, limit: int, window_seconds: int) -> int | None:
    """Counts a request in a fixed time window. Returns seconds to wait if over the limit.

    If Redis is down, requests are allowed (fail open): rate limiting protects against abuse,
    and it shouldn't take the whole API down with it. The failure is logged.
    """
    window = int(time.time() // window_seconds)
    key = f"ratelimit:{name}:{identity}:{window}"
    try:
        pipeline = client.pipeline()
        pipeline.incr(key)
        pipeline.expire(key, window_seconds)
        count, _ = pipeline.execute()
    except redis.RedisError as exc:
        logger.warning("Rate limiter unavailable, allowing request: %s", exc)
        return None
    if count > limit:
        return window_seconds - int(time.time()) % window_seconds
    return None
