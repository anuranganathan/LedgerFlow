import json
import logging
import os
import time
from typing import Any

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

logger = logging.getLogger(__name__)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PAYMENT_EVENTS_TOPIC = "payment-events"
# Events the worker could not process after every retry. Replay them with replay_dlq.py.
PAYMENT_EVENTS_DLQ_TOPIC = "payment-events-dlq"
# Several partitions let several workers share the load (docker compose up --scale worker=3).
# Events are keyed by payment ID, so all events for one payment stay in order on one partition.
PAYMENT_EVENTS_PARTITIONS = int(os.getenv("KAFKA_PAYMENT_PARTITIONS", "3"))
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
CONSUMER_GROUP = "ledgerflow-payment-worker"

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    """One producer per process; opening a connection for every message is slow."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",  # wait until the broker has stored the message
            retries=5,
            value_serializer=lambda value: json.dumps(value).encode(),
            key_serializer=lambda key: key.encode(),
        )
    return _producer


def publish(topic: str, key: str, value: dict[str, Any], timeout: float = 10) -> None:
    """Sends one message and waits for Kafka to confirm it, raising if it can't."""
    try:
        get_producer().send(topic, key=key, value=value).get(timeout=timeout)
    except Exception:
        reset_producer()
        raise


def reset_producer() -> None:
    global _producer
    if _producer is not None:
        _producer.close(timeout=1)
    _producer = None


def ensure_topics() -> None:
    """Creates the topics with the right partition count (Kafka's auto-created topics get 1)."""
    admin = wait_for_kafka(lambda: KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS))
    try:
        for name, partitions in [
            (PAYMENT_EVENTS_TOPIC, PAYMENT_EVENTS_PARTITIONS), (PAYMENT_EVENTS_DLQ_TOPIC, 1),
        ]:
            try:
                admin.create_topics([NewTopic(name, partitions, REPLICATION_FACTOR)])
                logger.info("Created Kafka topic %s with %d partitions", name, partitions)
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def decode_event(raw: bytes) -> dict[str, Any]:
    # A message that isn't valid JSON must not crash the worker in a loop (a "poison pill"),
    # so it is passed on as an invalid event and sent to the dead-letter topic.
    try:
        value = json.loads(raw.decode())
        return value if isinstance(value, dict) else {"_invalid": raw.decode(errors="replace")}
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"_invalid": raw.decode(errors="replace")}


def create_consumer() -> KafkaConsumer:
    return wait_for_kafka(lambda: KafkaConsumer(
        PAYMENT_EVENTS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        # Offsets are committed by the worker only after an event is fully processed, so an
        # event is redelivered if the worker crashes halfway through it.
        enable_auto_commit=False,
        value_deserializer=decode_event,
    ))


def wait_for_kafka(connect):
    while True:
        try:
            return connect()
        except NoBrokersAvailable:
            logger.warning("Kafka is not ready; retrying in 5 seconds")
            time.sleep(5)
