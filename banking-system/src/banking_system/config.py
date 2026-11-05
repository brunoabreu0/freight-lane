"""Shared configuration for all banking-system components."""

import os

# Kafka connection
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker-1:29091")

# Topics
TOPIC_WITHDRAWAL_REQUESTS = "withdrawal-requests"
TOPIC_WITHDRAWAL_DECISIONS = "withdrawal-decisions"
TOPIC_WITHDRAWAL_RESULTS = "withdrawal-results"

ALL_TOPICS = [
    TOPIC_WITHDRAWAL_REQUESTS,
    TOPIC_WITHDRAWAL_DECISIONS,
    TOPIC_WITHDRAWAL_RESULTS,
]

# ATM settings
ATM_ID = os.environ.get("ATM_ID", "ATM-LISBON-042")

# Simulated accounts with varying profiles
SIMULATED_ACCOUNTS = [
    {"account_id": "ACC-7742", "balance": 1450.00, "fraud_risk": 0.12, "name": "Alice"},
    {"account_id": "ACC-1234", "balance": 320.00,  "fraud_risk": 0.05, "name": "Bob"},
    {"account_id": "ACC-8831", "balance": 50.00,   "fraud_risk": 0.08, "name": "Charlie"},
    {"account_id": "ACC-5519", "balance": 8900.00, "fraud_risk": 0.85, "name": "Diana"},
    {"account_id": "ACC-3307", "balance": 2100.00, "fraud_risk": 0.03, "name": "Erik"},
]

# Withdrawal amounts (cycled through for variety)
WITHDRAWAL_AMOUNTS = [200.00, 100.00, 500.00, 150.00, 300.00, 50.00, 1000.00]

# Fraud detection threshold
FRAUD_RISK_THRESHOLD = float(os.environ.get("FRAUD_RISK_THRESHOLD", "0.70"))
