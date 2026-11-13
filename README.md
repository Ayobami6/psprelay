# PSP Relay

[![CI](https://github.com/Ayobami6/psprelay/actions/workflows/ci.yml/badge.svg)](https://github.com/Ayobami6/psprelay/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Ayobami6/psprelay/actions/workflows/codeql.yml/badge.svg)](https://github.com/Ayobami6/psprelay/actions/workflows/codeql.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Ayobami6/psprelay)](go.mod)
[![Coverage Gate](https://img.shields.io/badge/coverage-%E2%89%A590%25-brightgreen)](Makefile)

**PSP Relay** is a webhook gateway for Payment Service Providers (PSPs). It gives you **one webhook URL per provider** (Paystack, Flutterwave, Monnify, …) and fans each verified event out to the right product services.

Most PSP dashboards let you register only one webhook URL per account. If several products share that account, you need something to receive the webhooks, check them, work out which product owns the payment, and deliver them reliably. PSP Relay does that, with **signature verification, idempotent ingestion, a transactional outbox, Kafka buffering, retries with backoff, and a dead-letter queue**.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Webhook Lifecycle](#webhook-lifecycle)
- [Product Routing](#product-routing)
- [Supported Providers](#supported-providers)
- [API Reference](#api-reference)
- [Downstream Integration](#downstream-integration)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
- [Development](#development)
- [Data Model](#data-model)
- [Delivery Guarantees & Failure Handling](#delivery-guarantees--failure-handling)
- [Project Layout](#project-layout)
- [CI/CD & Quality Gates](#cicd--quality-gates)
- [Contributing](#contributing)

---

## Features

- 🔐 **Provider signature verification**: HMAC-SHA512 for Paystack and Monnify, secret-hash match for Flutterwave. All checks use constant-time comparison.
- ♻️ **Idempotent ingestion**: duplicate PSP deliveries are dropped using a `UNIQUE (provider, provider_event_id)` constraint.
- 📦 **Transactional outbox**: each event is saved before it is published. If Kafka is down, an outbox worker publishes `PENDING` events once it recovers.
- 🚀 **Kafka-buffered fan-out**: the ingress HTTP path and the slow outbound delivery path are separated by a Kafka topic, with messages keyed by payment reference so events for the same reference stay in order.
- 🎯 **Deterministic product routing**: a product is chosen by an HMAC-derived prefix token or a raw prefix in the payment reference. DVA / bank-transfer events with no matching prefix are broadcast to all products.
- 🔁 **Non-blocking retries**: failed deliveries are retried by a separate worker, up to 3 times, with exponential backoff and jitter. Retries never block the Kafka consumer.
- ☠️ **Dead-letter queue**: deliveries that run out of retries, and events that match no product, go to a DLQ topic for manual review or replay.
- ✍️ **Signed outbound requests**: every forwarded webhook can carry an `X-PSPRelay-Signature` (HMAC-SHA256) so downstream services can check it.
- 🧯 **RFC 7807 error responses** (`application/problem+json`) that don't expose internal details.
- 🛑 **Graceful shutdown**: SIGINT/SIGTERM drains the HTTP server and stops all background workers.
- 🧪 **≥ 90% test coverage gate**, race-detector tests, `golangci-lint`, `govulncheck`, and CodeQL in CI.

---

## Architecture

PSP Relay is a single Go binary laid out as ports and adapters. HTTP controllers are thin. Use cases handle the workflow. Persistence, messaging, and outbound HTTP sit behind interfaces, so each can be swapped or mocked.

```mermaid
flowchart LR
    subgraph PSPs["Payment Service Providers"]
        PS[Paystack]
        FW[Flutterwave]
        MN[Monnify]
    end

    subgraph Relay["PSP Relay"]
        direction TB
        API["HTTP API (Gin)<br/>POST /v1/webhook/:provider"]
        RCV["ReceiverUseCase<br/>verify + normalize"]
        RLY["RelayService<br/>persist + publish"]
        OBX["OutboxWorker"]
        CON["Kafka Consumer"]
        DSP["DispatcherUseCase<br/>route + fan-out"]
        RTY["RetryWorker"]
    end

    PG[("PostgreSQL<br/>webhook_events<br/>delivery_failures")]
    K[["Kafka<br/>psp.webhooks"]]
    DLQ[["Kafka<br/>psp.webhooks.dlq"]]

    subgraph Products["Product Services"]
        P1["Product A endpoints"]
        P2["Product B endpoints"]
    end

    PS & FW & MN --> API --> RCV --> RLY
    RLY --> PG
    RLY --> K
    OBX -- "re-publish PENDING" --> K
    OBX --- PG
    K --> CON --> DSP
    DSP --> P1 & P2
    DSP -- "failed attempt" --> PG
    RTY --- PG
    RTY --> P1 & P2
    RTY -- "exhausted" --> DLQ
    DSP -- "unmatched" --> DLQ
```

### Layers

| Layer | Responsibility | Key types |
|---|---|---|
| **Presentation** | Parse HTTP, enforce body limits, map errors to RFC 7807 | `PSPWebhookController`, `HealthController`, `ErrorHandlerMiddleware` |
| **Application** | Workflows, one use case per action | `ReceiverUseCase`, `RelayService`, `DispatcherUseCase`, `HealthCheckUseCase` |
| **Domain** | Provider verification and normalization, routing rules | `ProviderService` (Paystack/Flutterwave/Monnify), `GatewayConfig.MatchProductByReference` |
| **Ports** | Interfaces that the infrastructure implements | `WebhookStore`, `EventPublisher`, `UpstreamDispatcher` |
| **Infrastructure** | Concrete adapters | `PostgresWebhookStore`, `InMemoryWebhookStore`, `KafkaEventPublisher`, `KafkaConsumerRunner`, `defaultHTTPDispatcher` |
| **Workers** | Background reliability loops | `OutboxWorker`, `RetryWorker`, `KafkaConsumerRunner` |

---

## Webhook Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant PSP as PSP (e.g. Paystack)
    participant API as PSP Relay API
    participant DB as PostgreSQL
    participant K as Kafka (psp.webhooks)
    participant C as Consumer / Dispatcher
    participant P as Product Endpoint(s)
    participant R as RetryWorker
    participant DLQ as Kafka (DLQ)

    PSP->>API: POST /v1/webhook/paystack (signed)
    API->>API: Verify signature & normalize payload
    API->>DB: INSERT webhook_events (ON CONFLICT DO NOTHING)
    alt Duplicate & already processed
        API-->>PSP: 200 accepted (no-op)
    else New / still pending
        API->>K: Publish event (3s timeout)
        alt Publish OK
            API->>DB: status = PUBLISHED
        else Kafka unavailable
            Note over API,DB: Stays PENDING → OutboxWorker re-publishes
        end
        API-->>PSP: 200 accepted {event_id}
    end

    K->>C: Consume event
    C->>C: Resolve target product(s)
    par Fan-out to every endpoint
        C->>P: POST payload + X-PSPRelay-* headers
    end
    alt All 2xx
        C->>DB: status = DELIVERED
    else Any failure
        C->>DB: INSERT delivery_failures (RETRYING)
        loop Up to 3 retries, exponential backoff + jitter
            R->>P: Retry POST (X-PSPRelay-Retry-Attempt)
        end
        R->>DLQ: Publish exhausted delivery
        R->>DB: Reconcile status DELIVERED / FAILED
    end
```

### Event status state machine

```mermaid
stateDiagram-v2
    [*] --> PENDING: persisted
    PENDING --> PUBLISHED: Kafka publish OK (hot path or OutboxWorker)
    PUBLISHED --> DELIVERED: all endpoints 2xx (first attempt or retries)
    PUBLISHED --> FAILED: retries exhausted / no product matched
    DELIVERED --> [*]
    FAILED --> [*]
```

---

## Product Routing

Each product is defined in [`config.yaml`](config.yaml). When a webhook is consumed, the dispatcher reads the **payment reference** from the normalized event and picks the target product(s) in this order:

1. **HMAC prefix token (preferred).** The reference begins with a 12-char token followed by `_` or `-`, where:

   $$\text{token} = \text{hex}\big(\text{HMAC-SHA256}(\text{key} = \text{encodeKey},\ \text{msg} = \text{UPPER}(\text{prefix}))\big)[0{:}12]$$

   Example: `3f9a1c0be27d_TX-000123`. The token reveals neither the product name nor its prefix.
2. **Raw prefix (fallback).** The part of the reference before the first `_` or `-` equals the product's `prefix`, ignoring case (e.g. `MN_12345` → `mailnow`).
3. **DVA broadcast.** If nothing matches and the event is a dedicated virtual account / bank transfer, it is sent to **every** configured product, in alphabetical order. Those requests carry `X-PSPRelay-Routing-Mode: broadcast-dva`.
4. **Unmatched.** Anything else is recorded as a `FAILED` delivery (`422`), the event is marked `FAILED`, and it is published to the DLQ.

> **Reference extraction:** each provider service picks the best candidate reference. If the PSP's main reference has no `_`/`-` separator (for example a random NIP reference) but a metadata reference does (`metadata.reference`, `tx_ref`, `accountReference`, `product.reference`), the metadata reference is used.

### Generating a prefix token

```bash
# Token for prefix "MN" with encodeKey "$ENCODE_KEY"
printf 'MN' | openssl dgst -sha256 -hmac "$ENCODE_KEY" | awk '{print $NF}' | cut -c1-12
```

Or in Go:

```go
token := internal.ComputePrefixToken("MN", encodeKey) // e.g. "3f9a1c0be27d"
reference := token + "_" + transactionID
```

---

## Supported Providers

| Provider | Path segment | Signature header | Verification | DVA detection |
|---|---|---|---|---|
| **Paystack** | `paystack` | `X-Paystack-Signature` | HMAC-SHA512(body, secret key) | `channel == dedicated_nuban` or `dedicatedaccount.*` events |
| **Flutterwave** | `flutterwave` | `verif-hash` (fallback `X-Flutterwave-Signature`) | Constant-time compare with secret hash | `payment_type == bank_transfer` / `BANK_TRANSFER_TRANSACTION` |
| **Monnify** | `monnify` | `monnify-signature` | HMAC-SHA512(body, secret key) | `product.type` ∈ {`RESERVED_ACCOUNT`, `DYNAMIC_ACCOUNT`} |
| Payaza / Wema / Obiex | `payaza` / `wema` / `obiex` | — | 🚧 Recognized, not yet implemented (returns `400 unsupported payment service provider`) | — |

Each event gets a provider event ID of `<event_type>:<provider_id>`. If the payload has no ID, a SHA-256 hash of the body is used. This ID drives deduplication.

---

## API Reference

Base URL: `http://<host>:8083`

### `POST /v1/webhook/:provider`

Receives a raw PSP webhook. Register this URL in the PSP dashboard, e.g. `https://relay.example.com/v1/webhook/paystack`.

- Body: the raw JSON payload from the PSP (max **5 MB**)
- Headers: the provider's signature header (see [Supported Providers](#supported-providers))

**200 OK**

```json
{
  "status": "accepted",
  "event_id": "b6f1c1f2a0e94d0f9c5c1b7e2d8a4f31",
  "provider": "paystack"
}
```

**Error responses** (`Content-Type: application/problem+json`, [RFC 7807](https://datatracker.ietf.org/doc/html/rfc7807))

```json
{
  "type": "https://tools.ietf.org/html/rfc7807",
  "title": "Unauthorized",
  "status": 401,
  "detail": "invalid webhook signature"
}
```

| Status | Cause |
|---|---|
| `400` | Missing signature header, malformed / empty payload, provider recognized but not implemented |
| `401` | Signature verification failed |
| `404` | Unknown provider path segment |
| `500` | Provider secret not configured (shown as `service security configuration error`), or an unexpected internal error |

Example:

```bash
BODY='{"event":"charge.success","data":{"id":123,"reference":"MN_TX0001","channel":"card"}}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha512 -hmac "$PAYSTACK_SECRET_KEY" | awk '{print $NF}')

curl -X POST http://localhost:8083/v1/webhook/paystack \
  -H "Content-Type: application/json" \
  -H "X-Paystack-Signature: $SIG" \
  -d "$BODY"
```

### `GET /v1/healthz` and `GET /health`

Liveness and readiness check. Pings the backing store.

| Status | Body |
|---|---|
| `200` | `{"status":"healthy","message":"All systems operational"}` |
| `503` | `{"status":"degraded","error":"<reason>"}` |

---

## Downstream Integration

Product endpoints get a `POST` containing the **original, unmodified PSP payload**. The PSP's original headers are kept (minus hop-by-hop headers), and these are added:

| Header | Description |
|---|---|
| `X-PSP-Provider` | Source provider (`paystack`, `flutterwave`, `monnify`) |
| `X-PSPRelay-Event-ID` | Stable relay event ID. **Use it as your idempotency key.** |
| `X-PSPRelay-Routing-Mode` | `direct` or `broadcast-dva` |
| `X-PSPRelay-Signature` | Hex HMAC-SHA256 of the raw body using the product's `signatureKey` (sent only when one is configured) |
| `X-PSPRelay-Retry-Attempt` | Retry number (`1`–`3`), present on retries only |

A delivery counts as successful when the endpoint returns **any 2xx** within **5 seconds**.

> [!IMPORTANT]
> Delivery is **at-least-once**. The same event can arrive more than once, for example after a retry or a consumer rebalance. Deduplicate on `X-PSPRelay-Event-ID`.

### Verifying the relay signature

```go
func verifyRelaySignature(body []byte, header, signatureKey string) bool {
    mac := hmac.New(sha256.New, []byte(signatureKey))
    mac.Write(body)
    expected := hex.EncodeToString(mac.Sum(nil))
    return hmac.Equal([]byte(expected), []byte(strings.ToLower(header)))
}
```

```python
import hmac, hashlib

def verify_relay_signature(body: bytes, header: str, signature_key: str) -> bool:
    expected = hmac.new(signature_key.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header.lower())
```

---

## Configuration

### Environment variables

Configuration is read from the environment. A `.env` file in the working directory is loaded if present. Each variable also accepts a PascalCase alias (e.g. `DatabaseURL`).

| Variable | Default | Description |
|---|---|---|
| `ADDR` | `:8083` | HTTP listen address |
| `ENVIRONMENT` | `development` | `development` \| `staging` \| `production` (`production` turns on Gin release mode) |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` (structured JSON logs via `slog`) |
| `CONFIG_FILE_PATH` | `./config.yaml` | Path to the product routing config |
| `DATABASE_URL` | _(empty)_ | PostgreSQL DSN. **If empty, an in-memory store is used (local development only; no durability).** |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_TOPIC` | `psp.webhooks` | Main webhook topic |
| `KAFKA_DLQ_TOPIC` | `psp.webhooks.dlq` | Dead-letter topic |
| `KAFKA_CONSUMER_GROUP` | `psprelay-dispatcher` | Consumer group ID |
| `PAYSTACK_SECRET_KEY` | _(empty)_ | Paystack secret key used for HMAC verification |
| `FLUTTERWAVE_SECRET_HASH` | _(empty)_ | Flutterwave dashboard secret hash |
| `MONNIFY_SECRET_KEY` | _(empty)_ | Monnify client secret used for HMAC verification |
| `OUTBOX_POLL_INTERVAL` | `5s` | How often the OutboxWorker scans for `PENDING` events |
| `RETRY_POLL_INTERVAL` | `2s` | How often the RetryWorker scans for due retries |
| `RETRY_BASE_BACKOFF` | `5s` | Base delay for exponential retry backoff |

> [!WARNING]
> If a provider's secret is not set, every webhook from that provider is rejected with `500`. Configure secrets only for the providers you use.

### Product routing (`config.yaml`)

```yaml
version: v1
products:
  mailnow:
    prefix: MN                     # Raw prefix and input for the HMAC token
    secrets:
      type: Opaque
      encodeKey: <random-secret>   # Derives the 12-char reference prefix token
      signatureKey: <random-secret> # Signs outbound requests (X-PSPRelay-Signature); optional
    endpoints:                     # Every endpoint gets every matched event
      - name: mailnow
        url: https://mailnow.example.com/webhooks/psp
      - name: mailnow-legacy
        url: https://legacy.mailnow.example.com/webhooks/psp
  tibicloud:
    prefix: TC
    secrets:
      type: Opaque
      encodeKey: <random-secret>
    endpoints:
      - name: tibicloud
        url: https://tibicloud.example.com/webhooks/psp
```

If the file is missing or invalid, the service still starts with **no products**. Every event is then unmatched and goes to the DLQ, and a warning is logged.

> [!CAUTION]
> The bundled `config.yaml` contains placeholder secrets. Replace them before deploying, and mount the real file as a secret (for example a Kubernetes `Secret` or Docker secret) rather than baking it into the image.

---

## Getting Started

### Prerequisites

- [Go](https://go.dev/dl/) **1.27+**
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose (for PostgreSQL and Kafka)
- Optional: [`air`](https://github.com/air-verse/air) for hot reload, [`golangci-lint`](https://golangci-lint.run/) v2 for linting

### Option 1: Full stack with Docker Compose

```bash
git clone https://github.com/Ayobami6/psprelay.git
cd psprelay

# Add provider secrets to the psprelay service in docker-compose.yml (or an env file), then:
make docker-up        # postgres:16 + kafka 3.8 (KRaft) + psprelay on :8083

curl http://localhost:8083/v1/healthz
```

Stop everything with `make docker-down`.

### Option 2: Run locally against Compose infrastructure

```bash
docker compose up -d postgres kafka

cat > .env <<'EOF'
ADDR=:8083
LOG_LEVEL=debug
CONFIG_FILE_PATH=./config.yaml
DATABASE_URL=postgres://psprelay:psprelay_password@localhost:5432/psprelay?sslmode=disable
KAFKA_BROKERS=localhost:9092
PAYSTACK_SECRET_KEY=sk_test_xxx
FLUTTERWAVE_SECRET_HASH=my-flw-hash
MONNIFY_SECRET_KEY=monnify-secret
EOF

make run              # or `make dev` for hot reload with air
```

> [!NOTE]
> The Compose Kafka broker advertises `kafka:9092`. To connect from the host, add `127.0.0.1 kafka` to `/etc/hosts` or change `KAFKA_ADVERTISED_LISTENERS`.

The database schema (tables and indexes) is created automatically at startup with `CREATE TABLE IF NOT EXISTS`.

### Option 3: Docker image only

```bash
make docker-build
docker run --rm -p 8083:8083 --env-file .env \
  -v "$PWD/config.yaml:/app/config.yaml:ro" psprelay:latest
```

The image is a multi-stage build: a static `CGO_ENABLED=0` binary on `alpine:3.21`, running as a non-root user, with a built-in `HEALTHCHECK`.

---

## Development

| Command | Description |
|---|---|
| `make build` | Build a static binary into `bin/psprelay` (default target) |
| `make run` | Run with `go run` |
| `make dev` | Hot reload with `air` (see [`.air.toml`](.air.toml)) |
| `make test` | Run all tests with the race detector |
| `make test-cov` | Tests and coverage report. **Fails if coverage is below 90%** (`COVERAGE_THRESHOLD`) |
| `make test-cov-html` | Generate `coverage.html` |
| `make lint` | `golangci-lint run` (config in [`.golangci.yml`](.golangci.yml)) |
| `make security` | `govulncheck ./...` |
| `make ci` | `lint` + `test-cov` + `build`, the same checks CI runs |
| `make docker-build` / `docker-up` / `docker-down` | Container workflows |

### Testing approach

Tests live in [`internal/tests`](internal/tests) and [`cmd/main_test.go`](cmd/main_test.go). Every infrastructure dependency sits behind an interface built for testing (`PGPool`, `KafkaMessageWriter`, `KafkaMessageReader`, `UpstreamDispatcher`, `WebhookStore`), so the full suite runs **without PostgreSQL or Kafka**:

```bash
go test -race ./...
```

---

## Data Model

```mermaid
erDiagram
    webhook_events ||--o{ delivery_failures : "has"
    webhook_events {
        text id PK
        text provider "UNIQUE with provider_event_id"
        text provider_event_id
        text event_type
        text payment_reference
        boolean is_dva
        jsonb headers
        bytea payload
        text status "PENDING | PUBLISHED | DELIVERED | FAILED"
        timestamptz created_at
        timestamptz updated_at
    }
    delivery_failures {
        text id PK
        text event_id FK
        text provider
        text product
        text endpoint_name
        text target_url
        text routing_mode "direct | broadcast-dva | unmatched"
        jsonb headers
        bytea payload
        int status_code
        text error_message
        int retry_count
        int max_retries "default 3"
        text status "RETRYING | FAILED | RESOLVED"
        timestamptz next_retry_at
    }
```

Partial indexes keep the worker scans cheap:

- `idx_webhook_events_outbox` on `created_at WHERE status = 'PENDING'`
- `idx_delivery_failures_due_retries` on `next_retry_at WHERE status = 'RETRYING'`

---

## Delivery Guarantees & Failure Handling

| Failure | Behavior |
|---|---|
| PSP sends the same webhook twice | `ON CONFLICT DO NOTHING` on `(provider, provider_event_id)`. If the event was already published, the second request is acknowledged and ignored. |
| Kafka unavailable during ingestion | The event stays `PENDING` and the PSP still gets `200`. `OutboxWorker` claims events older than 5s in batches of 50 (`FOR UPDATE SKIP LOCKED`) and publishes them once Kafka recovers. |
| Product endpoint down / non-2xx / timeout | A `delivery_failures` row is added with status `RETRYING`. The Kafka offset is still committed, so one slow product can't block the others. |
| Retries | `RetryWorker` claims due rows (`FOR UPDATE SKIP LOCKED`, so multiple replicas are safe). It retries up to **3 times** with exponential backoff (`base × 2^(n-1)`, full jitter at 50–100%, capped at 2⁵ × base). |
| Retries exhausted | The row becomes `FAILED`, the message is published to `psp.webhooks.dlq`, and the parent event is reconciled to `FAILED`. |
| No product matched | Recorded as a `FAILED` delivery (`422`), published to the DLQ, and the event is marked `FAILED`. |
| Malformed Kafka message | Logged and its offset committed, so the bad message is skipped and doesn't block the partition. |
| SIGTERM | The HTTP server gets 10s to finish in-flight requests, and all workers stop through context cancellation. |

**DLQ message schema** (`psp.webhooks.dlq`, keyed by `event_id`):

```json
{
  "failure_id": "…",
  "event_id": "…",
  "provider": "paystack",
  "product": "mailnow",
  "endpoint_name": "mailnow",
  "target_url": "https://mailnow.example.com/webhooks/psp",
  "routing_mode": "direct",
  "headers": { "X-PSPRelay-Event-ID": "…" },
  "payload": "<base64 original body>",
  "status_code": 503,
  "error_message": "upstream returned status 503",
  "retry_count": 3,
  "failed_at": "2026-11-13T11:00:00Z"
}
```

---

## Project Layout

```text
.
├── cmd/
│   ├── main.go              # Composition root: wiring, workers, HTTP server, graceful shutdown
│   └── main_test.go
├── internal/
│   ├── config.go            # Env config loading, structured logger
│   ├── constants.go         # PSP provider identifiers
│   ├── controller.go        # HTTP controllers (presentation layer)
│   ├── middleware.go        # RFC 7807 error mapping, CORS
│   ├── errors.go            # Domain sentinel errors
│   ├── usecase.go           # Receiver / Relay / Dispatcher / Health use cases
│   ├── service.go           # Health domain service
│   ├── providers.go         # Per-PSP signature verification & normalization
│   ├── utils.go             # config.yaml model, prefix-token routing, HMAC helpers
│   ├── store.go             # WebhookStore port + PostgreSQL & in-memory adapters
│   ├── kafka.go             # Kafka publisher/consumer, OutboxWorker, RetryWorker
│   ├── dispatcher.go        # Outbound HTTP adapter (pooled, bounded timeouts)
│   └── tests/               # Unit & component tests
├── config.yaml              # Product routing configuration
├── docker-compose.yml       # Postgres + Kafka (KRaft) + PSP Relay
├── Dockerfile               # Multi-stage, non-root, static binary
├── Makefile                 # Build / test / lint / docker targets
├── .golangci.yml            # Linter configuration
└── .github/                 # CI, CodeQL, PR review workflows, Dependabot
```

---

## CI/CD & Quality Gates

| Workflow | What it does |
|---|---|
| [`ci.yml`](.github/workflows/ci.yml) | `go mod tidy` drift check, `golangci-lint` v2, race tests on Ubuntu and macOS with a **90% coverage gate**, cross-compiled binaries (`linux`/`darwin` amd64+arm64, `windows/amd64`), Docker image build with a health probe, `govulncheck` |
| [`codeql.yml`](.github/workflows/codeql.yml) | CodeQL static security analysis |
| [`code-review.yml`](.github/workflows/code-review.yml) | Conventional Commits PR title check, dependency review, reviewdog inline lint comments, AI-assisted review |
| [`dependabot.yml`](.github/dependabot.yml) | Weekly updates for Go modules, GitHub Actions, and Docker |

---

## Contributing

1. Create a branch from `main` named `<type>/<short-description>` (e.g. `feat/payaza-provider`).
2. Write commits that follow [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <description>`.
3. Run `make ci` locally. Lint, tests, the coverage gate, and the build must all pass.
4. Add an entry under `[Unreleased]` in [`CHANGELOG.md`](CHANGELOG.md) for any user-facing change.
5. Open a PR using the [PR template](.github/pull_request_template.md) and link the related issue (`Closes #123`).

### Adding a new PSP provider

1. Add the identifier to [`constants.go`](internal/constants.go), if it isn't there already.
2. Implement `ProviderService` (`Provider()` and `VerifyAndParse()`) in [`providers.go`](internal/providers.go). Verify the signature in constant time and fill in `PaymentReference`, `IsDVA`, and a stable `ProviderEventID`.
3. Add the secret to `Config` in [`config.go`](internal/config.go) and register the service in `NewReceiverUseCase` in [`cmd/main.go`](cmd/main.go).
4. Add tests in `internal/tests/providers_test.go` covering valid, missing, and invalid signatures plus DVA detection.

---

## License

No license has been chosen for this project yet. Until one is added, all rights are reserved by the author.
