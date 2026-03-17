# Data Space Grid

Federated Data Space research prototype for the electricity sector. Each participant retains local data ownership while sharing metadata, contracts, and control signals through a trust-mediated connector architecture.

## Architecture

The system implements a 5-layer stack:

| Layer | Purpose | Implementation |
|-------|---------|----------------|
| 1. Identity / Trust | Authentication & service-to-service trust | Keycloak OIDC + mTLS |
| 2. Semantic Model | Interoperable data definitions | CIM, IEC 61850, OpenADR (Pydantic) |
| 3. Catalog / Discovery | Data asset registration & search | Federated Catalog service |
| 4. Policy / Contract / Consent | Machine-enforceable usage agreements | Contract state machine + consent manager |
| 5. Access / Exchange | Actual data transfer | REST APIs + Kafka event bus |

## Participants

| Node | Port | Role |
|------|------|------|
| Federated Catalog | 8000 | Data asset discovery and contract negotiation |
| DSO | 8001 | Distribution System Operator — publishes feeder constraints and congestion signals |
| Aggregator | 8002 | DER Aggregator — publishes aggregate flexibility envelopes |
| Prosumer | 8003 | Campus prosumer — consent-gated anonymized demand profiles |

## Key Use Case: Distribution Congestion Management

1. DSO publishes feeder constraints to the catalog
2. Aggregator discovers the constraint asset
3. Aggregator negotiates a data usage contract with DSO
4. Aggregator reads constraint data (contract-gated)
5. Aggregator submits flexibility offer
6. DSO dispatches via Kafka
7. Aggregator reports actuals
8. Every step is recorded in an immutable audit trail

## Demos

### Full Test Suite

**Unit Tests (226 tests)** — Connector models, contract state machine, policy engine, audit, semantic models, anonymizer

![Unit Tests](docs/demos/01-unit-tests.gif)

**Integration Tests (96 tests)** — Catalog flow, contract negotiation, congestion management E2E, auth flow, audit trail

![Integration Tests](docs/demos/02-integration-tests.gif)

---

### Layer 1: Identity / Trust — OIDC + mTLS Authentication

Valid tokens grant access, expired/invalid tokens are rejected, wrong roles return 403, mTLS certificate validation.

![Auth Flow](docs/demos/05-auth-flow.gif)

---

### Layer 2: Semantic Models — CIM / IEC 61850 / OpenADR / Consumer

Strictly typed Pydantic models for grid topology, DER flexibility, DR events, and consumer data with sensitivity tiers.

![Semantic Models](docs/demos/09-semantic-models.gif)

---

### Layer 3: Federated Catalog — Asset Registration & Discovery

Participants register data assets with metadata and policy info. Others search and discover assets by provider, type, or sensitivity.

![Catalog Flow](docs/demos/07-catalog-flow.gif)

---

### Layer 4a: Contract Negotiation — OFFERED -> NEGOTIATING -> ACTIVE

Machine-enforceable contracts with purpose constraints, redistribution limits, retention limits, and emergency override.

![Contract Negotiation](docs/demos/03-contract-negotiation.gif)

---

### Layer 4b: Policy Engine — Purpose Constraints & Sensitivity Tiers

Purpose-based access control, sensitivity tier checks, emergency DSO override, redistribution/retention limits.

![Policy Engine](docs/demos/10-policy-engine.gif)

---

### Layer 4c: Privacy — Data Anonymization & Purpose-Based Disclosure

Consumer data is never shared raw. Disclosure level is determined by purpose: research -> aggregated, dr_dispatch -> controllability only, billing -> identified (with consent), forecasting -> k-anonymized.

![Anonymizer](docs/demos/08-anonymizer.gif)

---

### Layer 5: Congestion Management — End-to-End Flow

DSO publishes constraints -> Aggregator discovers -> negotiates contract -> reads constraints -> submits flexibility offer -> DSO dispatches via Kafka -> Aggregator reports actuals -> full audit trail.

![Congestion Management E2E](docs/demos/04-congestion-management.gif)

---

### Audit Trail — Immutable Hash-Verified Exchange Log

Every data exchange produces an audit entry with request/response hashes (SHA-256), purpose tag, timestamp, and requester identity.

![Audit Trail](docs/demos/06-audit-trail.gif)

---

## Tech Stack

- **Language**: Python 3.11+
- **Framework**: FastAPI + Uvicorn
- **Auth**: Keycloak 26.x (OIDC) + mTLS
- **Event Bus**: Apache Kafka (KRaft mode)
- **Database**: SQLite (dev) / PostgreSQL (prod)
- **Models**: Pydantic v2
- **Orchestration**: Docker Compose

## Project Structure

```
src/
├── connector/          # Reusable Data Space Connector library
│   ├── models.py       #   Core models (Participant, Contract, Policy, AuditEntry)
│   ├── contract.py     #   Contract negotiation state machine
│   ├── policy.py       #   Policy enforcement engine
│   ├── auth.py         #   OIDC token validation + mTLS
│   ├── audit.py        #   Immutable audit logger
│   ├── middleware.py    #   FastAPI middleware (auth + policy + audit)
│   ├── catalog_client.py  # Federated catalog client
│   └── events.py       #   Kafka producer/consumer wrapper
├── semantic/           # Industry-standard data models
│   ├── cim.py          #   CIM grid topology (Feeder, Constraint, HostingCapacity)
│   ├── iec61850.py     #   DER flexibility (FlexibilityEnvelope, PQRange)
│   ├── openadr.py      #   DR events (DREvent, Signal, Baseline)
│   └── consumer.py     #   Consumer data (DemandProfile, ConsentRecord)
├── catalog/            # Federated Catalog service
│   ├── main.py, routes.py, store.py, schemas.py
└── participants/
    ├── dso/            # DSO participant node
    ├── aggregator/     # Aggregator participant node
    └── prosumer/       # Prosumer participant node
infrastructure/
├── keycloak/           # Realm configuration
├── kafka/              # Topic initialization
└── certs/              # Dev certificate generation
tests/
├── unit/               # 6 unit test modules
└── integration/        # 5 integration test modules
```

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose

### Setup

```bash
# Create virtual environment and install dependencies
make setup
source .venv/bin/activate

# Generate dev certificates for mTLS
make certs

# Start infrastructure (Keycloak + Kafka)
docker compose up -d keycloak kafka

# Initialize Kafka topics
bash infrastructure/kafka/topics.sh
```

### Run Services

```bash
# Run individual services
make run-catalog      # http://localhost:8000
make run-dso          # https://localhost:8001
make run-aggregator   # https://localhost:8002
make run-prosumer     # https://localhost:8003

# Or start everything at once
make run-all

# Or use Docker Compose for the full stack
make docker-up
```

### Run Tests

```bash
make test              # Full test suite
make test-unit         # Unit tests only
make test-integration  # Integration tests only
```

### Code Quality

```bash
make lint              # Run ruff linter
make format            # Run ruff formatter
```

## Data Sensitivity Classification

| Data Type | Sensitivity | Access Policy |
|-----------|------------|---------------|
| Grid topology / protection settings | HIGH | Operators only |
| Feeder congestion signals | MEDIUM | Contract-gated |
| DER flexibility envelopes | MEDIUM | Contract-gated (aggregate only) |
| Smart meter / BEMS data | HIGH_PRIVACY | Consent-required, purpose-based anonymization |

Consumer data disclosure levels are determined by purpose:

| Purpose | Disclosure Level |
|---------|-----------------|
| `research` | Aggregated only |
| `dr_dispatch` | Controllability margin only |
| `billing` | Identified (with consent) |
| `forecasting` | Anonymized (k-anonymity) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KEYCLOAK_SERVER_URL` | `http://localhost:8080` | Keycloak base URL |
| `KEYCLOAK_REALM` | `dataspace` | OIDC realm |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `CATALOG_URL` | `http://localhost:8000` | Catalog service URL |
| `DATABASE_URL` | `sqlite:///data/{node}.db` | Local database |
| `PARTICIPANT_ID` | — | Unique node identifier |

## License

MIT
