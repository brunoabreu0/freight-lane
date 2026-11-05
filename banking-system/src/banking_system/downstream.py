"""Downstream consumers — Ledger, Analytics, and Notifications.

These are simple consumers that read from the withdrawal-results topic and
log the events. In a real system each would persist to its own data store.
"""

from __future__ import annotations

import logging
import time

from banking_system.config import TOPIC_WITHDRAWAL_RESULTS
from banking_system.kafka_helpers import (
    create_consumer,
    deserialize,
    setup_logging,
    wait_for_kafka,
)

logger = logging.getLogger("banking-system")


def _run_consumer(component: str, group_id: str, emoji: str, action: str) -> None:
    """Generic downstream consumer loop."""
    setup_logging(component.upper())
    wait_for_kafka()

    consumer = create_consumer(f"{component}-consumer", group_id)
    consumer.subscribe([TOPIC_WITHDRAWAL_RESULTS])

    logger.info("%s %s started", emoji, component.replace("-", " ").title())

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
                continue

            result = deserialize(msg.value())
            if result is None:
                continue

            event_type = result.get("event_type", "Unknown")
            txn_id = result.get("transaction_id", "?")
            account_id = result.get("account_id", "?")
            amount = result.get("amount", 0)

            status_emoji = "✅" if event_type == "WithdrawalConfirmed" else "❌"

            logger.info(
                "%s [P%d] %s %s — %s €%.2f — %s",
                status_emoji,
                msg.partition(),
                txn_id,
                event_type,
                account_id,
                amount,
                action,
            )

            # Simulate processing time
            time.sleep(0.05)

    except KeyboardInterrupt:
        logger.info("%s shutting down", component)
    finally:
        consumer.close()


def run_ledger() -> None:
    """Transaction Ledger — records transactions permanently."""
    _run_consumer("ledger", "ledger-group", "📒", "Recorded in ledger")


def run_analytics() -> None:
    """Financial Analytics — updates dashboards and metrics."""
    _run_consumer("analytics", "analytics-group", "📊", "Updated metrics")


def run_notifications() -> None:
    """Push Notifications — alerts the customer."""
    _run_consumer("notifications", "notifications-group", "🔔", "Notification sent")
