# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI workflow (`.github/workflows/ci.yml`) covering linting, cross-platform matrix testing (`ubuntu-latest`, `macos-latest`), cross-compilation for 5 target architectures (`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, `windows/amd64`), Docker buildx image verification, and `govulncheck`.
- Pull request quality workflow (`.github/workflows/code-review.yml`) with Conventional Commits validation, dependency license/CVE review, reviewdog inline comments, and automated Gemini AI architecture reviews.
- CodeQL security analysis workflow (`.github/workflows/codeql.yml`) for continuous vulnerability scanning.
- Strict linter configuration (`.golangci.yml`) with enabled linters: `bodyclose`, `errorlint`, `gosec`, `misspell`, `noctx`, `revive`, and `unconvert`.
- Dependabot configuration (`.github/dependabot.yml`) for automated weekly updates across `gomod`, `github-actions`, and `docker`.
- Pull request template (`.github/pull_request_template.md`) and code ownership definition (`.github/CODEOWNERS`).
- New Makefile targets: `test-cov`, `test-cov-html`, `security`, and `ci`.

### Changed
- Standardized error wrapping in provider services to use `%w: %w`.
- Hardened config file path resolution with `filepath.Clean`.
