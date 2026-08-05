import json
import logging
import os
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "payment-events"


def publish_payment_created(payment_id: uuid.UUID) -> None:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "PAYMENT_CREATED",
        "payment_id": str(payment_id),
    }
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode(),
        key_serializer=lambda key: key.encode(),
    )
    producer.send(TOPIC, key=str(payment_id), value=event).get(timeout=10)
    producer.close()


def create_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id="payflow-payment-worker",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode()),
            )
        except NoBrokersAvailable:
            logger.warning("Kafka is not ready; retrying in 5 seconds")
            time.sleep(5)
