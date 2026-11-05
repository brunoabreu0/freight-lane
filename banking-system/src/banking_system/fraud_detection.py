"""Fraud Detection component — consumes withdrawal requests, produces fraud decisions."""

from __future__ import annotations

import logging
import time

from banking_system.config import (
    FRAUD_RISK_THRESHOLD,
    SIMULATED_ACCOUNTS,
    TOPIC_WITHDRAWAL_DECISIONS,
    TOPIC_WITHDRAWAL_REQUESTS,
)
from banking_system.kafka_helpers import (
    create_consumer,
    create_producer,
    deserialize,
    serialize,
    setup_logging,
    wait_for_kafka,
)

logger = logging.getLogger("banking-system")

# Build a lookup of account risk profiles
RISK_PROFILES = {a["account_id"]: a["fraud_risk"] for a in SIMULATED_ACCOUNTS}


def run() -> None:
    """Run the Fraud Detection component."""
    setup_logging("FRAUD-DETECTION")
    wait_for_kafka()

    producer = create_producer("fraud-detection-producer")
    consumer = create_consumer("fraud-detection-consumer", "fraud-detection-group")
    consumer.subscribe([TOPIC_WITHDRAWAL_REQUESTS])

    logger.info("🔍 Fraud Detection System started (threshold: %.2f)", FRAUD_RISK_THRESHOLD)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
                continue

            request = deserialize(msg.value())
            if request is None or request.get("event_type") != "WithdrawalRequest":
                continue

            account_id = request["account_id"]
            txn_id = request["transaction_id"]
            risk_score = RISK_PROFILES.get(account_id, 0.50)

            # Simulate processing time
            time.sleep(0.1)

            if risk_score < FRAUD_RISK_THRESHOLD:
                decision = "APPROVED"
                reason = "Normal pattern"
            else:
                decision = "REJECTED"
                reason = f"High risk score ({risk_score:.2f})"

            event = {
                "event_type": "FraudDecision",
                "transaction_id": txn_id,
                "account_id": account_id,
                "decision": decision,
                "risk_score": risk_score,
                "reason": reason,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            producer.produce(
                TOPIC_WITHDRAWAL_DECISIONS,
                key=account_id.encode(),
                value=serialize(event),
            )
            producer.flush()

            emoji = "✅" if decision == "APPROVED" else "🚫"
            logger.info(
                "%s [P%d] %s → %s (risk: %.2f) — %s",
                emoji,
                msg.partition(),
                txn_id,
                decision,
                risk_score,
                reason,
            )

    except KeyboardInterrupt:
        logger.info("Fraud Detection shutting down")
    finally:
        consumer.close()
