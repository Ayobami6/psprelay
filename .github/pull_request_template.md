## Summary of Changes

<!-- Briefly describe what this PR changes and why. Link the issue it resolves, e.g. Closes #123 -->

Closes #

## Architecture Decisions & Trade-offs

<!-- Document any schema changes, concurrency considerations, cryptographic verification, or design trade-offs. Write "N/A" if none. -->

## Verification Evidence

<!-- Paste the test and linter commands run and their output. -->

- [ ] `make lint` (`golangci-lint run --timeout 5m ./...`) passes with 0 issues
- [ ] `make test` (`go test -v -race ./...`) passes
- [ ] `make test-cov` (`go test -race -coverpkg=./... -coverprofile=coverage.out ./...`) passes
- [ ] `make build` (`CGO_ENABLED=0 go build ...`) passes
