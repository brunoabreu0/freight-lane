"""ATM component — produces withdrawal requests on CLI command, consumes decisions, produces results."""

from __future__ import annotations

import itertools
import logging
import os
import random
import socket
import sys
import time
import uuid

from banking_system.config import (
    ATM_ID,
    SIMULATED_ACCOUNTS,
    TOPIC_WITHDRAWAL_DECISIONS,
    TOPIC_WITHDRAWAL_REQUESTS,
    TOPIC_WITHDRAWAL_RESULTS,
    WITHDRAWAL_AMOUNTS,
)
from banking_system.kafka_helpers import (
    create_consumer,
    create_producer,
    deserialize,
    ensure_topics,
    serialize,
    setup_logging,
    wait_for_kafka,
)

logger = logging.getLogger("banking-system")

TRIGGER_SOCKET_PATH = "/tmp/atm_trigger.sock"

# Track pending transactions: txn_id -> {request, decisions_received}
pending: dict[str, dict] = {}


def produce_request(producer, account: dict, amount: float) -> str:
    """Produce a WithdrawalRequest event and return the formatted log message."""
    txn_id = f"txn-{uuid.uuid4().hex[:8]}"
    event = {
        "event_type": "WithdrawalRequest",
        "transaction_id": txn_id,
        "account_id": account["account_id"],
        "amount": amount,
        "currency": "EUR",
        "atm_id": ATM_ID,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    delivered_partition: list[int] = []
    producer.produce(
        TOPIC_WITHDRAWAL_REQUESTS,
        key=account["account_id"].encode(),
        value=serialize(event),
        on_delivery=lambda err, msg: delivered_partition.append(msg.partition()) if err is None else None,
    )
    producer.flush()
    part_str = f"[P{delivered_partition[0]}] " if delivered_partition else ""
    log_msg = (
        f"📤 {part_str}Produced WithdrawalRequest {txn_id} for "
        f"{account['account_id']} ({account['name']}) — €{amount:.2f}"
    )
    logger.info(log_msg)
    pending[txn_id] = {"request": event, "decisions": {}}
    return log_msg


def process_decisions(consumer, producer) -> None:
    """Consume decision events and produce results when both decisions arrive."""
    msg = consumer.poll(timeout=0.2)
    if msg is None:
        return
    if msg.error():
        logger.warning("Consumer error: %s", msg.error())
        return

    decision = deserialize(msg.value())
    if decision is None:
        return

    txn_id = decision.get("transaction_id")
    event_type = decision.get("event_type")

    if txn_id not in pending:
        return  # Not our transaction or already processed

    pending[txn_id]["decisions"][event_type] = decision
    received = pending[txn_id]["decisions"]

    # Wait for both FraudDecision and BalanceDecision
    if "FraudDecision" not in received or "BalanceDecision" not in received:
        return

    # Both decisions received — determine outcome
    request = pending.pop(txn_id)["request"]
    fraud_ok = received["FraudDecision"]["decision"] == "APPROVED"
    balance_ok = received["BalanceDecision"]["decision"] == "APPROVED"

    if fraud_ok and balance_ok:
        result_type = "WithdrawalConfirmed"
        logger.info(
            "✅ Both decisions APPROVED for %s — producing WithdrawalConfirmed",
            txn_id,
        )
    else:
        result_type = "WithdrawalRejected"
        reasons = []
        if not fraud_ok:
            reasons.append(
                f"Fraud: {received['FraudDecision'].get('reason', 'REJECTED')}"
            )
        if not balance_ok:
            reasons.append(
                f"Balance: {received['BalanceDecision'].get('reason', 'REJECTED')}"
            )
        logger.info(
            "❌ %s REJECTED for %s — %s",
            txn_id,
            request["account_id"],
            ", ".join(reasons),
        )

    result = {
        "event_type": result_type,
        "transaction_id": txn_id,
        "account_id": request["account_id"],
        "amount": request["amount"],
        "currency": request["currency"],
        "atm_id": request["atm_id"],
        "fraud_decision": received["FraudDecision"]["decision"],
        "balance_decision": received["BalanceDecision"]["decision"],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    producer.produce(
        TOPIC_WITHDRAWAL_RESULTS,
        key=request["account_id"].encode(),
        value=serialize(result),
    )
    producer.flush()
    logger.info("📤 Produced %s %s", result_type, txn_id)


def _create_next_request(producer, account_cycle, amount_cycle) -> str:
    """Create a withdrawal request using the next account and amount in the cycle."""
    account = next(account_cycle)
    amount = next(amount_cycle)
    # Occasionally randomize the amount for variety
    if random.random() < 0.3:
        amount = round(random.uniform(50, 800), 2)
    return produce_request(producer, account, amount)


def trigger_request_cli() -> None:
    """CLI command (`create-atm-request`) to trigger one or more ATM withdrawal requests."""
    args = [a for a in sys.argv[1:] if a != "create-atm-request"]
    count = 1
    if args:
        try:
            count = max(1, int(args[0]))
        except ValueError:
            print("Usage: create-atm-request [count]")
            sys.exit(1)

    if not os.path.exists(TRIGGER_SOCKET_PATH):
        print("Error: ATM service is not running or not ready yet.")
        sys.exit(1)

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(TRIGGER_SOCKET_PATH)
        client.sendall(f"{count}\n".encode("utf-8"))
        response = client.recv(4096).decode("utf-8")
        print(response, end="")


def run() -> None:
    """Run the ATM component daemon."""
    setup_logging("ATM")
    wait_for_kafka()
    ensure_topics()

    producer = create_producer("atm-producer")
    consumer = create_consumer("atm-decision-consumer", "atm-decisions-group")
    consumer.subscribe([TOPIC_WITHDRAWAL_DECISIONS])

    account_cycle = itertools.cycle(SIMULATED_ACCOUNTS)
    amount_cycle = itertools.cycle(WITHDRAWAL_AMOUNTS)

    if os.path.exists(TRIGGER_SOCKET_PATH):
        os.remove(TRIGGER_SOCKET_PATH)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(TRIGGER_SOCKET_PATH)
    server.listen(5)
    server.setblocking(False)

    logger.info(
        "🏧 ATM %s started — waiting for 'create-atm-request' commands",
        ATM_ID,
    )

    try:
        while True:
            # Check for incoming CLI trigger commands
            try:
                conn, _ = server.accept()
                with conn:
                    data = conn.recv(128).decode("utf-8").strip()
                    count = int(data) if data.isdigit() else 1
                    lines = []
                    for _ in range(count):
                        lines.append(
                            _create_next_request(producer, account_cycle, amount_cycle)
                        )
                    conn.sendall(("\n".join(lines) + "\n").encode("utf-8"))
            except BlockingIOError:
                pass

            # Always poll for decisions
            process_decisions(consumer, producer)

    except KeyboardInterrupt:
        logger.info("ATM shutting down")
    finally:
        server.close()
        if os.path.exists(TRIGGER_SOCKET_PATH):
            os.remove(TRIGGER_SOCKET_PATH)
        consumer.close()
