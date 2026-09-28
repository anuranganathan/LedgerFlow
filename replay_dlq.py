"""Sends events from the dead-letter topic back to the payment-events topic.

Use it after fixing whatever made the events fail (for example S3 was down for an hour):

    docker compose exec worker python replay_dlq.py            # list dead-lettered events
    docker compose exec worker python replay_dlq.py --replay   # replay them

Replaying is safe because the worker handles duplicate events without moving money twice.
The replay tool reads with its own consumer group, so each event is replayed only once.
"""
import sys

from kafka import KafkaConsumer

import kafka_client


def main(replay: bool) -> None:
    consumer = KafkaConsumer(
        kafka_client.PAYMENT_EVENTS_DLQ_TOPIC,
        bootstrap_servers=kafka_client.BOOTSTRAP_SERVERS,
        # Listing uses no group, so it never marks events as replayed.
        group_id="ledgerflow-dlq-replay" if replay else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=kafka_client.decode_event,
    )
    count = 0
    for record in consumer:
        dead = record.value
        print(f"offset={record.offset} attempts={dead.get('attempts')} error={dead.get('error')}")
        print(f"  event={dead.get('event')}")
        if replay and isinstance(dead.get("event"), dict):
            event = dead["event"]
            kafka_client.publish(kafka_client.PAYMENT_EVENTS_TOPIC, str(event.get("payment_id", "")), event)
            consumer.commit()
        count += 1
    consumer.close()
    print(f"{'Replayed' if replay else 'Found'} {count} event(s)")


if __name__ == "__main__":
    main(replay="--replay" in sys.argv[1:])
