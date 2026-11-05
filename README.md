# 🚛 Freight Lane — Apache Kafka Workshop Repository

> **Workshop:** Freight Lane: Apache Kafka Foundation & Hands-on Workshop
> **Presenters:** Omkar Ranade & Bruno Abreu
> **Duration:** 1.5 hours | **Format:** In-person Workshop (Theory + Lab)

This repository contains everything you need for the hands-on lab portion of the Freight
Lane workshop: a local Apache Kafka cluster, a simulated ATM withdrawal pipeline, and
comprehensive step-by-step guides.

---

## What's Inside

```
freight-lane/
├── docker-compose.yml            # Kafka cluster + banking pipeline (single file)
├── banking-system/               # Python monolith source code + Dockerfile
│   ├── src/banking_system/       # Application source
│   │   ├── cli.py                # CLI entry point (subcommands per component)
│   │   ├── atm.py                # ATM — produces requests, consumes decisions
│   │   ├── fraud_detection.py    # Fraud Detection — consumes requests, produces decisions
│   │   ├── account_balance.py    # Account Balance — consumes requests, produces decisions
│   │   ├── downstream.py         # Ledger, Analytics, Notifications consumers
│   │   ├── kafka_helpers.py      # Shared Kafka producer/consumer helpers
│   │   └── config.py             # Configuration and simulated data
│   ├── Dockerfile                # Multi-arch Docker image (amd64 + arm64)
│   └── pyproject.toml            # Python project metadata
├── docs/                         # Detailed documentation
│   ├── PREREQUISITES.md          # Installation guide for Docker, Git, etc.
│   ├── LAB-GUIDE.md              # Step-by-step lab instructions (4 steps)
│   └── ARCHITECTURE.md           # Use case and system architecture
└── README.md                     # This file
```

---

## Quick Start (5 minutes)

### Prerequisites

- **Docker Desktop** with Docker Compose v2+ ([installation guide](docs/PREREQUISITES.md))
- **Git** ([installation guide](docs/PREREQUISITES.md))
- Works on **Intel/AMD (x86)** and **Apple Silicon (ARM64)** — no special configuration

### 1. Clone the repository

```bash
git clone https://github.com/brunoabreu0/freight-lane.git
cd freight-lane
```

### 2. Start the Kafka broker

```bash
docker compose up -d broker-1
```

Wait for the broker to be healthy (~15 seconds):

```bash
docker compose ps
# Should show: broker-1  Up (healthy)
```

### 3. Start the banking pipeline

```bash
docker compose up -d atm fraud-detection account-balance ledger analytics notifications
```

### 4. Watch the magic

In one terminal, follow the pipeline logs:

```bash
docker compose logs -f atm fraud-detection account-balance ledger analytics notifications
```

In another terminal, trigger withdrawal requests on demand:

```bash
docker compose exec atm create-atm-request
# Or trigger 5 requests at once (cycles through all 5 simulated accounts):
docker compose exec atm create-atm-request 5
```

You'll see ATM withdrawal transactions flowing through Kafka in real time — across 6
independent components communicating through 3 Kafka topics.

### 5. Clean up

```bash
docker compose down -v
```

---

## The Use Case: ATM Withdrawal Pipeline

A customer stands at an ATM and requests a withdrawal. Behind the scenes, 6 systems
coordinate through Kafka:

```
ATM → [withdrawal-requests] → Fraud Detection
                             → Account Balance

Fraud Detection → [withdrawal-decisions] → ATM
Account Balance → [withdrawal-decisions] → ATM

ATM → [withdrawal-results] → Transaction Ledger
                            → Financial Analytics
                            → Push Notifications
```

**Key Kafka concepts demonstrated:**

| What You'll See | Kafka Concept |
|----------------|---------------|
| Same account always in the same partition | Message keys & partitioning |
| Fraud and Balance process independently | Consumer groups |
| ATM produces AND consumes | Components as both producer and consumer |
| Ledger, Analytics, Notifications each get every event | Multiple consumer groups on one topic |
| No duplicates after broker crash | Exactly-Once Semantics & idempotent producer |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full system design.

---

## Workshop Lab Guide

The detailed, step-by-step lab instructions are in [docs/LAB-GUIDE.md](docs/LAB-GUIDE.md):

| Step | Title | What You Do |
|------|-------|------------|
| 1 | Start the Engine | Clone repo, start single-broker Kafka cluster |
| 2 | Explore the Controls | Create topics, produce/consume with Kafka CLI |
| 3 | Launch the Banking Pipeline | Run all 6 components, observe real-time event flow |
| 4 | Test the Highway | Add a broker, crash a broker, observe failover |

---

## Scaling the Kafka Cluster

The cluster starts with a single broker (`broker-1`). To add a second broker (`broker-2`):

```bash
docker compose up -d broker-2
```

To simulate a broker crash and recovery:

```bash
docker compose stop broker-1
docker compose start broker-1
```

The banking pipeline components automatically hold, reconnect, and resume processing — with zero data loss.

---

## Rebuilding the Application Image Locally

The `banking-system` image (`freight-lane-banking-system:latest`) is built locally by Docker Compose from `./banking-system`. If you modify the Python code and want to rebuild:

```bash
docker compose build
```

---

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Event streaming | Apache Kafka (KRaft mode) | 4.0.0 |
| Application | Python | 3.13 |
| Kafka client | confluent-kafka-python | 2.15.0 |
| Containerization | Docker + Docker Compose | Latest |
