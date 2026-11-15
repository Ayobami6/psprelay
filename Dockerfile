# syntax=docker/dockerfile:1

# ------------------------------------------------------------------------------
# Stage 1: Build binary statically
# ------------------------------------------------------------------------------
FROM golang:1.27-alpine AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

# Cache module dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree
COPY . .

# Compile optimized static binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /app/bin/psprelay ./cmd/main.go

# ------------------------------------------------------------------------------
# Stage 2: Minimal, secure runtime with non-root user
# ------------------------------------------------------------------------------
FROM alpine:3.24 AS runner

RUN addgroup -S pspgroup && adduser -S pspuser -G pspgroup

WORKDIR /app

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/bin/psprelay /app/psprelay
COPY config.yaml /app/config.yaml

RUN chown -R pspuser:pspgroup /app

USER pspuser

EXPOSE 8083

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8083/v1/healthz || exit 1

ENTRYPOINT ["/app/psprelay"]
