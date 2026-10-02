"""Live updates for the dashboard: the worker publishes, the API streams to the browser.

When a payment or refund is settled, the worker publishes a small event on a Redis channel for
each user involved (the customer and the merchant). GET /events keeps an HTTP connection open
and forwards that user's events as Server-Sent Events, so the dashboard updates immediately
instead of polling.

Redis pub/sub doesn't store messages: an event is lost if the browser isn't connected at that
moment. That's acceptable because events only say "something changed". The dashboard reloads
the data from the API when it (re)connects, and PostgreSQL remains the source of truth.
"""
import asyncio
import json
import logging
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

import redis
import redis.asyncio

import redis_client

logger = logging.getLogger(__name__)
HEARTBEAT_SECONDS = 15
# Streams end after a while so the browser reconnects with a fresh access token.
MAX_STREAM_SECONDS = 600


def channel(user_id: uuid.UUID | str) -> str:
    return f"events:user:{user_id}"


def publish(user_ids: list[uuid.UUID | None], event: dict) -> None:
    """Best effort: a failure is logged and never fails the payment."""
    try:
        message = json.dumps(event)
        for user_id in {u for u in user_ids if u is not None}:
            redis_client.client.publish(channel(user_id), message)
    except redis.RedisError as exc:
        logger.warning("Could not publish live update: %s", exc)


def async_client() -> redis.asyncio.Redis:
    return redis.asyncio.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)


async def stream(user_id: uuid.UUID, is_disconnected: Callable[[], Awaitable[bool]],
                 client: redis.asyncio.Redis | None = None) -> AsyncIterator[str]:
    """Yields Server-Sent Events for one user until the browser disconnects."""
    client = client or async_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(channel(user_id))
    loop = asyncio.get_running_loop()
    started = last_sent = loop.time()
    try:
        yield "event: ready\ndata: {}\n\n"
        while loop.time() - started < MAX_STREAM_SECONDS and not await is_disconnected():
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is not None:
                yield f"event: update\ndata: {message['data']}\n\n"
                last_sent = loop.time()
            elif loop.time() - last_sent > HEARTBEAT_SECONDS:
                yield ": keep-alive\n\n"  # stops proxies from closing an idle connection
                last_sent = loop.time()
    finally:
        await pubsub.unsubscribe()
        await pubsub.aclose()
        await client.aclose()
