# Architecture — ATM Withdrawal Pipeline

This document describes the system architecture of the banking-system application used in
the Freight Lane workshop. It is a simulated ATM withdrawal pipeline that demonstrates
Apache Kafka's core capabilities.

---

## Overview

The pipeline simulates what happens when a customer requests a cash withdrawal at an ATM.
Six independent system components coordinate through three Kafka topics to process the
transaction in real time.

---

## System Components

| Component | Container Name | Role | Kafka Behavior |
|-----------|---------------|------|----------------|
| **ATM** | `atm` | Initiates withdrawals, aggregates decisions, produces outcomes | Producer → `withdrawal-requests` Consumer ← `withdrawal-decisions` Producer → `withdrawal-results` |
| **Fraud Detection** | `fraud-detection` | Checks withdrawal requests for fraud signals | Consumer ← `withdrawal-requests` Producer → `withdrawal-decisions` |
| **Account Balance** | `account-balance` | Verifies sufficient funds and places holds | Consumer ← `withdrawal-requests` Producer → `withdrawal-decisions` |
| **Transaction Ledger** | `ledger` | Records confirmed/rejected transactions permanently | Consumer ← `withdrawal-results` |
| **Financial Analytics** | `analytics` | Updates dashboards and metrics | Consumer ← `withdrawal-results` |
| **Push Notifications** | `notifications` | Sends customer alerts | Consumer ← `withdrawal-results` |

**Key design point:** ATM, Fraud Detection, and Account Balance are simultaneously producers
*and* consumers. This demonstrates that Kafka components are not rigidly "one role" — they
participate in multi-stage event pipelines.

---

## Kafka Topics

| Topic | Partitions | Key | Producers | Consumers |
|-------|-----------|-----|-----------|-----------|
| `withdrawal-requests` | 3 | `account_id` | ATM | Fraud Detection, Account Balance |
| `withdrawal-decisions` | 3 | `account_id` | Fraud Detection, Account Balance | ATM |
| `withdrawal-results` | 3 | `account_id` | ATM | Ledger, Analytics, Notifications |

All topics use `account_id` as the message key. This ensures all events for the same bank
account always land in the same partition, guaranteeing ordering per account.

---

## Event Flow

```
┌─────────┐
│   ATM   │
└────┬────┘
     │
     │ ① WithdrawalRequest
     │    {txn_id, account_id, amount, atm_id}
     ▼
┌─────────────────────────────────┐
│   withdrawal-requests (topic)   │
└──────┬──────────────────┬───────┘
       │                  │
       ▼                  ▼
┌──────────────┐   ┌───────────────┐
│    Fraud     │   │   Account     │
│  Detection   │   │   Balance     │
└──────┬───────┘   └──────┬────────┘
       │                  │
       │ ② FraudDecision  │ ② BalanceDecision
       │ {decision, risk} │ {decision, balance}
       ▼                  ▼
┌─────────────────────────────────┐
│  withdrawal-decisions (topic)   │
└────────────────┬────────────────┘
                 │
                 ▼
            ┌─────────┐
            │   ATM   │  ← aggregates both decisions
            └────┬────┘
                 │
                 │ ③ WithdrawalConfirmed OR WithdrawalRejected
                 │    {txn_id, account_id, amount, fraud_decision, balance_decision}
                 ▼
┌─────────────────────────────────┐
│   withdrawal-results (topic)    │
└──────┬─────────┬────────┬───────┘
       │         │        │
       ▼         ▼        ▼
┌──────────┐ ┌─────────┐ ┌─────────────┐
│  Ledger  │ │Analytics│ │Notifications│
└──────────┘ └─────────┘ └─────────────┘
```

### Decision Logic

- **Fraud Detection:** Compares the account's risk score against a configurable threshold
  (default: 0.70). Scores above the threshold are rejected.
- **Account Balance:** Checks if the available balance covers the requested amount. If yes,
  places a hold (deducts from available balance). If not, rejects.
- **ATM:** Waits for both decisions. If both approve → `WithdrawalConfirmed`. If either
  rejects → `WithdrawalRejected`.

### Simulated Data

The application includes 5 simulated accounts with varying profiles:

| Account | Name | Balance | Fraud Risk | Expected Behavior |
|---------|------|---------|------------|-------------------|
| ACC-7742 | Alice | €1,450 | 0.12 (low) | Most withdrawals approved |
| ACC-1234 | Bob | €320 | 0.05 (low) | Approved until balance runs out |
| ACC-8831 | Charlie | €50 | 0.08 (low) | Most withdrawals rejected (insufficient funds) |
| ACC-5519 | Diana | €8,900 | 0.85 (high) | Always rejected by fraud detection |
| ACC-3307 | Erik | €2,100 | 0.03 (low) | Most withdrawals approved |

Withdrawal requests are triggered on demand via `docker compose exec atm create-atm-request` (or `create-atm-request <count>`). Withdrawal amounts cycle through: €200, €100, €500, €150, €300, €50, €1000 — with
occasional random amounts for variety.

---

## Consumer Groups

Each component runs in its own consumer group:

| Component | Consumer Group ID |
|-----------|------------------|
| Fraud Detection | `fraud-detection-group` |
| Account Balance | `account-balance-group` |
| ATM (decisions) | `atm-decisions-group` |
| Ledger | `ledger-group` |
| Analytics | `analytics-group` |
| Notifications | `notifications-group` |

This means:
- Fraud Detection and Account Balance each get **every** withdrawal request (separate groups
  on the same topic = independent consumption).
- Ledger, Analytics, and Notifications each get **every** result (same pattern).
- Scaling any component is straightforward: run more instances in the same group.

---

## Kafka Reliability Features Demonstrated

### Idempotent Producer

All producers are configured with `enable.idempotence=True`. Kafka assigns a Producer ID
(PID) and sequence number to each message. If a network error triggers a retry, the broker
detects the duplicate and discards it. No duplicate events in any topic.

### Exactly-Once Semantics (EOS)

The combination of idempotent producers and consumer offset management provides exactly-once
processing guarantees. Each withdrawal is processed exactly once — the account is debited
once, the notification is sent once, the ledger records once — even if components crash and
restart.

### Automatic Failover

If a broker crashes, Kafka clients automatically discover available brokers and reconnect.
The banking pipeline components handle this transparently — you'll see brief error messages
in the logs followed by automatic recovery.

---

## Monolith Architecture

The banking-system is a **single Python package** with a CLI entry point. Each component is
a separate module, but they all ship in the same Docker image:

```
banking-system <component>
```

This design choice simplifies the workshop:
- One image built locally and shared across all 6 application containers.
- Components share configuration and Kafka helper code.
- Easy to read and understand the full system.

In production, these would typically be separate microservices — but for a workshop, the
monolith approach eliminates unnecessary complexity.
