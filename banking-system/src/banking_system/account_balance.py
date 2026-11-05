"""Account Balance component — consumes withdrawal requests, produces balance decisions."""

from __future__ import annotations

import logging
import time

from banking_system.config import (
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

# Simulated in-memory balances (reset on restart)
balances = {a["account_id"]: a["balance"] for a in SIMULATED_ACCOUNTS}


def run() -> None:
    """Run the Account Balance component."""
    setup_logging("ACCOUNT-BALANCE")
    wait_for_kafka()

    producer = create_producer("account-balance-producer")
    consumer = create_consumer("account-balance-consumer", "account-balance-group")
    consumer.subscribe([TOPIC_WITHDRAWAL_REQUESTS])

    logger.info("💰 Account Balance System started")

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
            amount = request["amount"]
            available = balances.get(account_id, 0.0)

            # Simulate processing time
            time.sleep(0.05)

            if available >= amount:
                decision = "APPROVED"
                hold_amount = amount
                reason = "Sufficient funds"
                # Place a hold (deduct from available balance)
                balances[account_id] = available - amount
            else:
                decision = "REJECTED"
                hold_amount = 0.0
                reason = f"Insufficient funds (available: €{available:.2f}, requested: €{amount:.2f})"

            event = {
                "event_type": "BalanceDecision",
                "transaction_id": txn_id,
                "account_id": account_id,
                "decision": decision,
                "available_balance": available,
                "hold_amount": hold_amount,
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
                "%s [P%d] %s → %s — Balance: €%.2f, Requested: €%.2f — %s",
                emoji,
                msg.partition(),
                txn_id,
                decision,
                available,
                amount,
                reason,
            )

    except KeyboardInterrupt:
        logger.info("Account Balance shutting down")
    finally:
        consumer.close()
