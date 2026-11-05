# Hands-on Lab Guide

> **Duration:** 45 minutes | **Prerequisites:** Docker, Git, Terminal
> See [PREREQUISITES.md](PREREQUISITES.md) for installation instructions.

This guide walks you through the 4 steps of the Freight Lane hands-on lab.

---

## Step 1: Start the Engine (8 min)

### 1.1 Clone the Repository

```bash
git clone https://github.com/brunoabreu0/freight-lane.git
cd freight-lane
```

### 1.2 Inspect the Configuration

```bash
cat docker-compose.yml
```

Notice:
- A single Kafka broker service (`broker-1`) in KRaft mode (no ZooKeeper).
- A pre-configured `broker-2` service behind a `scale` profile (for Step 4).
- Shared YAML anchors (`x-kafka-env`, `x-banking-app`, `x-banking-env`) to keep the configuration DRY.

### 1.3 Start the Broker

```bash
docker compose up -d broker-1
```

### 1.4 Verify

```bash
docker compose ps
```

**Expected:** `broker-1` container in `Up (healthy)` state.

> 💡 If you see `health: starting`, wait 15 seconds and check again.

✅ **Checkpoint:** Single-broker Kafka cluster is running.

---

## Step 2: Explore the Controls (10 min)

### 2.1 Create the Banking Topics

```bash
docker compose exec broker-1 \
  kafka-topics.sh --bootstrap-server broker-1:29091 \
  --create --topic withdrawal-requests --partitions 3 --replication-factor 1

docker compose exec broker-1 \
  kafka-topics.sh --bootstrap-server broker-1:29091 \
  --create --topic withdrawal-decisions --partitions 3 --replication-factor 1

docker compose exec broker-1 \
  kafka-topics.sh --bootstrap-server broker-1:29091 \
  --create --topic withdrawal-results --partitions 3 --replication-factor 1
```

### 2.2 List and Inspect Topics

```bash
# List all topics
docker compose exec broker-1 \
  kafka-topics.sh --bootstrap-server broker-1:29091 --list

# Describe a topic
docker compose exec broker-1 \
  kafka-topics.sh --bootstrap-server broker-1:29091 \
  --describe --topic withdrawal-requests
```

### 2.3 Produce and Consume from the CLI

**Terminal 1 — Producer:**
```bash
docker compose exec broker-1 \
  kafka-console-producer.sh --bootstrap-server broker-1:29091 \
  --topic withdrawal-requests
```
Type a message and press Enter.

**Terminal 2 — Consumer:**
```bash
docker compose exec broker-1 \
  kafka-console-consumer.sh --bootstrap-server broker-1:29091 \
  --topic withdrawal-requests --from-beginning
```

Your message appears instantly. Press `Ctrl+C` in both terminals when done.

✅ **Checkpoint:** Topics created, produce/consume verified.

---

## Step 3: Launch the Banking Pipeline (17 min)

### 3.1 Start All Components

```bash
docker compose up -d atm fraud-detection account-balance ledger analytics notifications
```

This builds and starts 6 containers using the `freight-lane-banking-system:latest` image:
- `atm` — produces withdrawal requests, consumes decisions, produces results
- `fraud-detection` — consumes requests, produces fraud decisions
- `account-balance` — consumes requests, produces balance decisions
- `ledger` — consumes results
- `analytics` — consumes results
- `notifications` — consumes results

### 3.2 Watch the Event Flow

In a second terminal, stream the logs of all components:

```bash
docker compose logs -f atm fraud-detection account-balance ledger analytics notifications
```

> 💡 Give it 5–10 seconds for all components to initialize.

### 3.3 Trigger ATM Withdrawal Requests

In your first terminal, trigger a withdrawal request whenever you want:

```bash
docker compose exec atm create-atm-request
```

Run it multiple times (or pass a count, e.g. `docker compose exec atm create-atm-request 5`) to cycle through the simulated accounts.

### 3.4 What You'll See

```
atm              | 📤 [P0] Produced WithdrawalRequest txn-abc123 for ACC-7742 (Alice) — €200.00
fraud-detection  | ✅ [P0] txn-abc123 → APPROVED (risk: 0.12) — Normal pattern
account-balance  | ✅ [P0] txn-abc123 → APPROVED — Balance: €1450.00, Requested: €200.00 — Sufficient funds
atm              | ✅ Both decisions APPROVED for txn-abc123 — producing WithdrawalConfirmed
atm              | 📤 Produced WithdrawalConfirmed txn-abc123
ledger           | ✅ [P0] txn-abc123 WithdrawalConfirmed — ACC-7742 €200.00 — Recorded in ledger
analytics        | ✅ [P0] txn-abc123 WithdrawalConfirmed — ACC-7742 €200.00 — Updated metrics
notifications    | ✅ [P0] txn-abc123 WithdrawalConfirmed — ACC-7742 €200.00 — Notification sent
```

Watch for **rejected transactions** too:
- **ACC-5519 (Diana):** Always lands in `[P2]` and is rejected by Fraud Detection (risk: 0.85).
- **ACC-8831 (Charlie):** Always lands in `[P0]` and is rejected by Account Balance when balance runs low (starts at €50).

### 3.5 Key Observations

| What You See | Kafka Concept |
|-------------|---------------|
| Same account → same partition (`[P0]`, `[P1]`, `[P2]`) | Message key partitioning (`account_id`) |
| Fraud + Balance process independently | Separate consumer groups on the same topic |
| ATM produces requests AND consumes decisions | Dual-role components |
| Ledger, Analytics, Notifications each get every result | Multiple consumer groups |
| Rejected transactions flow through the full pipeline | Event-driven error handling |

Leave the pipeline running for Step 4.

✅ **Checkpoint:** Full banking pipeline running with real-time event flow.

---

## Step 4: Test the Highway (10 min)

### 4.1 Add a Second Broker

```bash
docker compose up -d broker-2
```

Verify:
```bash
docker compose ps
```

You should see both `broker-1` and `broker-2` in `Up` state.

### 4.2 Crash the Original Broker

```bash
docker compose stop broker-1
```

### 4.3 Watch the Recovery

In the logs terminal (or by running `docker compose logs -f --tail=20 atm fraud-detection account-balance`), you'll see:
1. A brief disconnect notice as clients detect the lost `broker-1` connection.
2. Clients hold and retry automatically without crashing.

### 4.4 Bring the Broker Back

```bash
docker compose start broker-1
```

Wait 10 seconds — `broker-1` rejoins the cluster and transactions resume flowing automatically with **zero data loss**.

### 4.5 Clean Up

```bash
docker compose --profile scale down -v
```

✅ **Checkpoint:** Survived a broker crash with automatic failover and zero data loss.

---

## Congratulations! 🎉

You've completed all 4 steps:

- ✅ Built a Kafka cluster from scratch
- ✅ Created topics and used the Kafka CLI
- ✅ Ran a multi-component banking transaction pipeline
- ✅ Tested Kafka's resilience under failure

### Keep Exploring

- Stop a single component (`docker compose stop analytics`), trigger a few requests (`docker compose exec atm create-atm-request 3`), and
  restart it — watch it catch up from its last offset.
- Read the source code in `banking-system/src/` to understand the producer/consumer patterns.
- Modify `FRAUD_RISK_THRESHOLD` to change which transactions get approved.
- Build the image locally and add your own component.

### Resources

| Resource | Link |
|----------|------|
| This repository | [github.com/brunoabreu0/freight-lane](https://github.com/brunoabreu0/freight-lane) |
| Apache Kafka docs | [kafka.apache.org/documentation](https://kafka.apache.org/documentation) |
| Confluent Kafka courses | [developer.confluent.io/courses](https://developer.confluent.io/courses) |
