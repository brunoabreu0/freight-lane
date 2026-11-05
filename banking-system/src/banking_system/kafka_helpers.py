"""Shared Kafka producer/consumer helpers."""

from __future__ import annotations

import json
import logging
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from banking_system.config import ALL_TOPICS, BOOTSTRAP_SERVERS

logger = logging.getLogger("banking-system")


def setup_logging(component: str) -> None:
    """Configure structured logging for a component."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s | {component:<20s} | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Reduce noise from librdkafka
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)


def wait_for_kafka(timeout: int = 120) -> None:
    """Block until the Kafka broker is reachable."""
    logger.info("Waiting for Kafka at %s ...", BOOTSTRAP_SERVERS)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            metadata = admin.list_topics(timeout=5)
            if metadata.brokers:
                logger.info(
                    "Kafka is ready — %d broker(s) available", len(metadata.brokers)
                )
                return
        except KafkaException:
            pass
        time.sleep(2)
    raise RuntimeError(
        f"Kafka not reachable at {BOOTSTRAP_SERVERS} after {timeout}s"
    )


def ensure_topics() -> None:
    """Create workshop topics if they don't exist."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [
        NewTopic(topic, num_partitions=3, replication_factor=1)
        for topic in ALL_TOPICS
        if topic not in existing
    ]
    if not to_create:
        logger.info("All topics already exist")
        return

    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Created topic: %s", topic)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.info("Topic already exists: %s", topic)
            else:
                raise


def create_producer(component: str, transactional: bool = False) -> Producer:
    """Create a Kafka producer with idempotency enabled."""
    conf: dict = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": component,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }
    if transactional:
        conf["transactional.id"] = f"{component}-txn"
    return Producer(conf)


def create_consumer(component: str, group_id: str) -> Consumer:
    """Create a Kafka consumer."""
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "client.id": component,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
        }
    )


def serialize(data: dict) -> bytes:
    """Serialize a dict to JSON bytes."""
    return json.dumps(data).encode("utf-8")


def deserialize(data: bytes | None) -> dict | None:
    """Deserialize JSON bytes to a dict, ignoring non-JSON or non-dict payloads."""
    if data is None:
        return None
    try:
        parsed = json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None
