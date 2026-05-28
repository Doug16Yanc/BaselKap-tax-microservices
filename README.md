# BaselKap Tax Engine

> Real-time fixed income tax calculation engine for Brazilian securities (CDB, LCI, LCA) — event-driven microservices with IOF/IR progressive tax rules, FIFO lot management, and BCB CDI integration.

---

## Overview

BaselKap is a **production-grade backend system** that models the full lifecycle of a fixed income investment in the Brazilian market: from the initial deposit through redemption, tax calculation (IOF + IR regressivo), and audit export to S3.

Built as a portfolio piece targeting **fintech and capital markets** engineering roles, it deliberately focuses on domain correctness over infrastructure complexity — two microservices, event-driven communication via Kafka, and real regulatory tax logic rather than mocked stubs.

---

## Architecture

```
┌──────────────────┐        investment.events        ┌─────────────────────┐
│ investment-ms    │ ──────────────────────────────► │ tax-engine-ms       │
│                  │                                  │                     │
│ • FIFO lot mgmt  │        tax.results               │ • IOF/IR regressivo │
│ • JWT auth       │ ◄────────────────────────────── │ • CDI via BCB API   │
│ • Position mgmt  │                                  │ • S3 audit export   │
└──────────────────┘                                  └─────────────────────┘
        │                                                       │
        ▼                                                       ▼
 PostgreSQL (investment-db)                          PostgreSQL (tax-db)
                          │           │
                          ▼           ▼
                       Redpanda    Floci (S3)
```

### Services

| Service | Port | Responsibility |
|---|---|---|
| `investment-ms` | `8080` | User registration, asset management, investment positions, FIFO tax lot tracking |
| `tax-engine-ms` | `8081` | CDI fetching from BCB, IOF/IR calculation, S3 audit export, result publishing |

### Infrastructure

| Component | Purpose |
|---|---|
| **Redpanda** | Kafka-compatible event streaming (drop-in, zero JVM overhead) |
| **PostgreSQL 16** | One database per service — no shared schema |
| **Floci** | Local AWS S3 emulator — MIT licensed, zero auth token, 24ms startup |

---

## Domain

### Tax Rules (legislação brasileira vigente)

**IOF Regressivo** — incide sobre o rendimento bruto nos primeiros 30 dias:

| Dias | Alíquota |
|------|----------|
| 1 | 96% |
| 2 | 93% |
| ... | ... |
| 29 | 3% |
| 30+ | 0% |

**IR Regressivo** — incide sobre rendimento após dedução do IOF:

| Prazo | Alíquota |
|-------|----------|
| Até 180 dias | 22,5% |
| 181 – 360 dias | 20,0% |
| 361 – 720 dias | 17,5% |
| Acima de 720 dias | 15,0% |

### FIFO Lot Management

Each investment creates a `TaxLot` with `purchaseDate` and `remainingAmount`. On redemption, lots are consumed oldest-first (`ORDER BY purchaseDate ASC, id ASC`) ensuring deterministic ordering even when multiple lots share the same purchase date.

### Gross Return Calculation

```
grossReturn = amount × (cdiRate / 100) × (holdingDays / 252)
```

Base 252 business days — Brazilian fixed income market standard.

---

## Tech Stack

```
Java 25 (Virtual Threads)       Spring Boot 3.x
Spring Security (JWT / Argon2)  Spring Data JPA
Spring Kafka                    Flyway
WebClient + Resilience4j        AWS SDK v2 (S3)
PostgreSQL 16                   Kafka/Redpanda
Floci (local AWS S3)            Docker Compose
```

### Key Design Decisions

**Why no API Gateway or Service Discovery?**
With two services and fixed Docker Compose hostnames (`investment-service:8080`, `tax-engine-service:8081`), both patterns would add infrastructure without solving a real problem. Documented as a deliberate MVP decision — Kong/Eureka would be justified at 5+ services.

**Why no Saga or Outbox pattern?**
The Investment Service publishes an event and the Tax Engine consumes it. Kafka retains the message on consumer failure — no distributed transaction requires compensation. Outbox would guarantee atomicity between PostgreSQL commit and Kafka publish; omitted intentionally in this scope and documented as a production concern.

**Why Redpanda over Kafka?**
Kafka-compatible wire protocol, no JVM overhead in the broker, Docker Compose startup in seconds. Production deployment could swap to MSK with zero application code changes.

**Why Floci over LocalStack?**
LocalStack community edition was sunset in March 2026 (auth token required, security updates frozen). Floci is MIT-licensed, no sign-up, 13MB idle memory vs 143MB, ~24ms startup. Drop-in replacement on port 4566.

**Why Argon2id for password hashing?**
OWASP recommendation for new systems. Memory-hard by design — resistant to GPU/ASIC brute-force attacks. Configured via `Argon2PasswordEncoder.defaultsForSpringSecurity_v5_8()`.

---

## Project Structure

```
BaselKap-Tax/
├── investment-ms/
│   └── src/main/java/tech/investmentms/
│       ├── api/
│       │   ├── controller/        # AuthController, InvestmentController, AssetController
│       │   ├── dto/request/       # RegisterRequest, LoginRequest, InvestRequest, RedeemRequest
│       │   ├── dto/response/      # AuthResponse, PositionResponse, RedemptionResponse
│       │   └── exception/         # GlobalExceptionHandler
│       ├── application/
│       │   └── service/           # UserService, InvestmentService
│       ├── domain/
│       │   └── entity/            # User, AssetReference, InvestmentPosition, TaxLot, Transaction
│       └── infrastructure/
│           ├── kafka/             # InvestmentEventPublisher, InvestmentEvent
│           ├── persistence/       # JPA Repositories
│           └── security/          # SecurityConfig, JwtService, JwtAuthenticationFilter
│
├── tax-engine-ms/
│   └── src/main/java/tech/taxenginems/
│       ├── application/
│       │   ├── scheduler/         # CdiScheduler
│       │   └── service/           # TaxCalculationService, CdiService, S3ExportService
│       ├── domain/
│       │   └── entity/            # CdiSnapshot, IofRule, IrRule, TaxCalculationRequest, TaxResult
│       └── infrastructure/
│           ├── bcb/               # BcbWebClientConfig, BcbCdiResponse
│           ├── kafka/             # InvestmentEventConsumer, TaxResultPublisher
│           ├── persistence/       # JPA Repositories
│           └── s3/                # S3Config, S3BucketInitializer
│
└── docker-compose.yml
```

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Java 21+
- Gradle 8+

### Environment

Create a `.env` file at the project root (never commit this):

```env
# Investment DB
INVESTMENT_DB_USER=investment_user
INVESTMENT_DB_PASSWORD=your_password

# Tax DB
TAX_DB_USER=tax_user
TAX_DB_PASSWORD=your_password

# JWT — minimum 32 characters
JWT_SECRET=your_long_secret_key_minimum_32_chars

# AWS S3 (real — only needed in production)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_S3_BUCKET=baselkap-tax-results
```

### Run locally

```bash
# start infrastructure only
docker compose up postgres-investment postgres-tax redpanda floci -d

# run services from IDE or:
./gradlew :investment-ms:bootRun
./gradlew :tax-engine-ms:bootRun
```

### Run full stack

```bash
docker compose up --build
```

Redpanda Console available at [http://localhost:8090](http://localhost:8090)

---

## API Reference

### Auth

```
POST /auth/register    — create account
POST /auth/login       — returns JWT
POST /auth/logout      — client-side invalidation (stateless)
```

### Investments

All endpoints require `Authorization: Bearer <token>`

```
POST   /investments                        — create position + FIFO lot
POST   /investments/{positionId}/redeem    — partial or full redemption (triggers tax calc)
GET    /investments                        — list active positions
GET    /investments/{positionId}           — position detail
```

### Assets

```
GET    /assets         — list available securities
GET    /assets/{id}    — asset detail
```

### Example: full CDB lifecycle (You can use Postman, Bruno or Insomnia)

```bash
# 1. register
curl -X POST http://localhost:8080/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name":"Doug","email":"doug@example.com","password":"strongpass123"}'

# 2. login
TOKEN=$(curl -s -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"doug@example.com","password":"strongpass123"}' | jq -r '.token')

# 3. invest R$10.000 in CDB
curl -X POST http://localhost:8080/investments \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"assetId":"<asset-id>","amount":10000.00,"date":"2026-05-01"}'

# 4. redeem after 20 days (triggers IOF 33% + IR 22.5%)
curl -X POST http://localhost:8080/investments/<position-id>/redeem \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"amount":10000.00}'

# 5. tax result lands in tax.results topic and S3:
# s3://baselkap-tax-results/tax-results/2026/05/20/<investmentId>.json
```

---

## Event Flow

```
investment-ms                    Redpanda                    tax-engine-ms
     │                               │                             │
     │── INVESTMENT_APPLIED ────────►│                             │
     │                               │                             │
     │── REDEMPTION_REQUESTED ──────►│── consume ─────────────────►│
     │                               │                             │── fetch CDI (BCB)
     │                               │                             │── calculate IOF
     │                               │                             │── calculate IR
     │                               │                             │── persist TaxResult
     │                               │                             │── export to S3
     │◄── tax.results ───────────────│◄── publish ─────────────────│
```

---

## CDI Scheduler

The Tax Engine fetches the CDI rate from the BCB public API on a schedule:

- **08:00 MON–FRI** — primary fetch (BCB publishes previous day's rate in the morning)
- **10:00 MON–FRI** — retry if 08:00 fetch failed or snapshot not found

On BCB unavailability, Resilience4j circuit breaker activates and the fallback uses the last persisted `CdiSnapshot`. The system degrades gracefully rather than failing the tax calculation.

---

## Known Limitations (by design)

- **JWT blacklist not implemented** — logout is client-side only. A Redis-backed blacklist would be the production approach.
- **Multi-lot tax calculation** — redemptions spanning lots with different purchase dates apply a single `holdingDays` value based on position start date. Per-lot tax calculation is the correct approach for positions with multiple deposit events.
- **No Outbox pattern** — atomicity between PostgreSQL commit and Kafka publish is not guaranteed. In production, Debezium CDC or a polling outbox table would be appropriate.
- **S3 export is non-blocking** — export failure is logged but does not fail the tax calculation pipeline. Audit completeness is eventually consistent.

---

## Roadmap

- [ ] Redis token blacklist
- [ ] Per-lot tax calculation on partial redemptions
- [ ] Outbox pattern with Debezium
- [ ] LCI/LCA tax exemption rules
- [ ] Integration tests with Testcontainers + `testcontainers-floci`
- [ ] SINACOR position reconciliation module

---


## License

MIT
