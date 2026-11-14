# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Server-rendered HTMX Management UI (`/admin`) and JSON Admin REST API (`/v1/admin/*`) for viewing per-product webhook events, inspecting headers/payloads, viewing delivery failures, and executing immediate single-item retries, event replays, and asynchronous bulk retries.
- Optional HTTP Basic Authentication middleware (`AdminAuthMiddleware`) configured via `ADMIN_USERNAME` and `ADMIN_PASSWORD`.
- `product` attribution column and `(product, created_at DESC)` index on `webhook_events` resolved at ingestion and dispatch.
- Comprehensive `README.md` covering architecture, webhook lifecycle, product routing (HMAC prefix tokens), supported providers, API reference, downstream integration/signature verification, configuration, local setup, data model, delivery guarantees, and contribution guide.
- GitHub Actions CI workflow (`.github/workflows/ci.yml`) covering linting, cross-platform matrix testing (`ubuntu-latest`, `macos-latest`), cross-compilation for 5 target architectures (`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, `windows/amd64`), Docker buildx image verification, and `govulncheck`.
- Pull request quality workflow (`.github/workflows/code-review.yml`) with Conventional Commits validation, dependency license/CVE review, reviewdog inline comments, and automated Gemini AI architecture reviews.
- CodeQL security analysis workflow (`.github/workflows/codeql.yml`) for continuous vulnerability scanning.
- Strict linter configuration (`.golangci.yml`) with enabled linters: `bodyclose`, `errorlint`, `gosec`, `misspell`, `noctx`, `revive`, and `unconvert`.
- Dependabot configuration (`.github/dependabot.yml`) for automated weekly updates across `gomod`, `github-actions`, and `docker`.
- Pull request template (`.github/pull_request_template.md`) and code ownership definition (`.github/CODEOWNERS`).
- New Makefile targets: `test-cov`, `test-cov-html`, `security`, and `ci`.
- Comprehensive unit and component tests across services (`service.go`, `providers.go`, `RelayService`), use cases (`HealthCheckUseCase`, `ReceiverUseCase`, `DispatcherUseCase`), controllers, middleware, HTTP dispatcher, Kafka publisher/consumer/workers, PostgreSQL & in-memory stores, and `cmd/main.go` lifecycle.
- Automated 90.0% minimum statement coverage gate in `Makefile` (`test-cov`, `ci`) and GitHub Actions CI.

### Changed
- Standardized error wrapping in provider services to use `%w: %w`.
- Hardened config file path resolution with `filepath.Clean`.

### Fixed
- Upgraded `golangci/golangci-lint-action` to `v9` (`v2.15.0`) in `.github/workflows/ci.yml` to support `.golangci.yml` v2 and Go 1.27+.
- Updated `Dockerfile` builder image to `golang:1.27-alpine` with `GOTOOLCHAIN=auto` and corrected `HEALTHCHECK` endpoint to `/v1/healthz`; added `.dockerignore` to exclude build/coverage artifacts.
- Set `.DEFAULT_GOAL := build` in `Makefile` and replaced CodeQL autobuild with an explicit `go build -v ./...` step in `.github/workflows/codeql.yml` (`v4`) to prevent CodeQL from hanging on `make run`.
- Bumped Go version in `go.mod` to `1.27.4` and upgraded `github.com/klauspost/compress` (`v1.20.2`), `golang.org/x/net` (`v0.60.0`), and `github.com/quic-go/quic-go` (`v0.59.1`) to resolve all 7 `govulncheck` findings.
- Removed debug `println` statements from `internal/tests/utils_test.go`.
- Fixed filtered bulk retries in management UI ignoring HTMX form parameters (#10).
- Protected delivery failure retry results against race conditions overwriting resolved statuses (#10).
- Ensured successful event replays automatically resolve active failure records for the event (#10).
- Validated bulk retry JSON and form payloads against malformed parameters (#10).
- Emitted DLQ messages when manual failure retries reach maximum retry exhaustion (#10).
- Added schema backfill and startup reconciliation for unmatched historical webhook event products (#10).
- Added pagination controls to admin UI event and failure tables (#10).
- Enforced mandatory admin authentication credentials in production environments (#10).
- Added cross-site request validation (`Sec-Fetch-Site`) on admin state-mutating requests (#10).
