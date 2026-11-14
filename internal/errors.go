package internal

import "errors"

// Product & Routing Errors
var (
	ErrProductNotFound     = errors.New("product configuration not found")
	ErrEndpointNotFound    = errors.New("product endpoint not found")
	ErrUnsupportedProvider = errors.New("unsupported payment service provider")
	ErrInvalidMethod       = errors.New("http method is invalid or empty")
	ErrProviderNotFound    = errors.New("provider not found")
)

// Idempotency & Concurrency Errors
var (
	ErrLockAcquisitionFailed = errors.New("concurrent request is currently processing")
	ErrKeyNotFound           = errors.New("idempotency key not found")
	ErrIdempotencyConflict   = errors.New("idempotency key conflict with mismatched payload")
)

// Webhook & Security Errors
var (
	ErrInvalidSignature = errors.New("invalid webhook signature")
	ErrMissingSignature = errors.New("missing signature header")
	ErrSecretNotFound   = errors.New("secret key not configured for provider")
	ErrInvalidPayload   = errors.New("invalid or malformed webhook payload")
	ErrEventNotFound    = errors.New("webhook event not found")
	ErrFailureNotFound  = errors.New("delivery failure not found")
)
