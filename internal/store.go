package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WebhookStatus represents the lifecycle state of an inbound webhook event.
type WebhookStatus string

const (
	WebhookStatusPending   WebhookStatus = "PENDING"
	WebhookStatusPublished WebhookStatus = "PUBLISHED"
	WebhookStatusDelivered WebhookStatus = "DELIVERED"
	WebhookStatusFailed    WebhookStatus = "FAILED"
)

// DeliveryFailureStatus represents the state of a failed endpoint delivery in the retry/failure table.
type DeliveryFailureStatus string

const (
	DeliveryStatusRetrying DeliveryFailureStatus = "RETRYING"
	DeliveryStatusFailed   DeliveryFailureStatus = "FAILED"
	DeliveryStatusResolved DeliveryFailureStatus = "RESOLVED"
)

// WebhookEvent represents a persisted inbound webhook and its outbox/delivery status.
type WebhookEvent struct {
	ID               string            `json:"id"`
	Provider         PSPProvider       `json:"provider"`
	ProviderEventID  string            `json:"provider_event_id"`
	EventType        string            `json:"event_type"`
	PaymentReference string            `json:"payment_reference"`
	IsDVA            bool              `json:"is_dva"`
	Headers          map[string]string `json:"headers"`
	Payload          []byte            `json:"payload"`
	Status           WebhookStatus     `json:"status"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

// DeliveryFailure represents a failed initial delivery scheduled for retry or permanently failed (DLQ).
type DeliveryFailure struct {
	ID           string                `json:"id"`
	EventID      string                `json:"event_id"`
	Provider     PSPProvider           `json:"provider"`
	Product      string                `json:"product"`
	EndpointName string                `json:"endpoint_name"`
	TargetURL    string                `json:"target_url"`
	RoutingMode  string                `json:"routing_mode"`
	Headers      map[string]string     `json:"headers"`
	Payload      []byte                `json:"payload"`
	StatusCode   int                   `json:"status_code"`
	ErrorMessage string                `json:"error_message"`
	RetryCount   int                   `json:"retry_count"`
	MaxRetries   int                   `json:"max_retries"`
	Status       DeliveryFailureStatus `json:"status"`
	NextRetryAt  time.Time             `json:"next_retry_at"`
	CreatedAt    time.Time             `json:"created_at"`
	UpdatedAt    time.Time             `json:"updated_at"`
}

// RetryAttemptResult represents the outcome of a single retry callback executed by RetryWorker.
type RetryAttemptResult struct {
	Resolved     bool
	StatusCode   int
	ErrorMessage string
	NextRetryAt  time.Time
}

// WebhookStore defines the secondary port for webhook persistence, transactional outbox, and retry/failure storage.
type WebhookStore interface {
	EnsureSchema(ctx context.Context) error
	SaveEvent(ctx context.Context, event *WebhookEvent) (saved *WebhookEvent, duplicate bool, err error)
	GetEvent(ctx context.Context, eventID string) (*WebhookEvent, error)
	UpdateStatus(ctx context.Context, eventID string, status WebhookStatus) error
	ClaimPendingOutboxEvents(ctx context.Context, olderThan time.Duration, limit int, fn func(ctx context.Context, events []WebhookEvent) ([]string, error)) error
	EnqueueDeliveryFailure(ctx context.Context, failure *DeliveryFailure) error
	ClaimDueRetries(ctx context.Context, now time.Time, limit int, fn func(ctx context.Context, item DeliveryFailure) RetryAttemptResult) ([]DeliveryFailure, error)
	ListDeliveryFailures(ctx context.Context, eventID string) ([]DeliveryFailure, error)
	HasPendingOrFailedDeliveries(ctx context.Context, eventID string) (hasRetrying bool, hasFailed bool, err error)
	Ping(ctx context.Context) error
	Close()
}

// PGPool abstracts *pgxpool.Pool for deterministic unit testing.
type PGPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// PostgresWebhookStore implements WebhookStore backed by PostgreSQL (pgxpool).
type PostgresWebhookStore struct {
	pool PGPool
}

// NewPostgresWebhookStore connects to PostgreSQL and returns a initialized store.
func NewPostgresWebhookStore(ctx context.Context, databaseURL string) (*PostgresWebhookStore, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing database url: %w", err)
	}
	cfg.MaxConns = 25
	cfg.MinConns = 2
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool: %w", err)
	}

	return NewPostgresWebhookStoreWithPool(pool), nil
}

// NewPostgresWebhookStoreWithPool constructs a PostgresWebhookStore backed by a custom PGPool.
func NewPostgresWebhookStoreWithPool(pool PGPool) *PostgresWebhookStore {
	return &PostgresWebhookStore{pool: pool}
}

func (s *PostgresWebhookStore) EnsureSchema(ctx context.Context) error {
	ddl := `
CREATE TABLE IF NOT EXISTS webhook_events (
	id TEXT PRIMARY KEY,
	provider TEXT NOT NULL,
	provider_event_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	payment_reference TEXT NOT NULL,
	is_dva BOOLEAN NOT NULL DEFAULT FALSE,
	headers JSONB NOT NULL DEFAULT '{}'::jsonb,
	payload BYTEA NOT NULL,
	status TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	UNIQUE (provider, provider_event_id)
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_outbox
	ON webhook_events (created_at ASC)
	WHERE status = 'PENDING';

CREATE TABLE IF NOT EXISTS delivery_failures (
	id TEXT PRIMARY KEY,
	event_id TEXT NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
	provider TEXT NOT NULL,
	product TEXT NOT NULL,
	endpoint_name TEXT NOT NULL,
	target_url TEXT NOT NULL,
	routing_mode TEXT NOT NULL DEFAULT 'direct',
	headers JSONB NOT NULL DEFAULT '{}'::jsonb,
	payload BYTEA NOT NULL,
	status_code INTEGER NOT NULL DEFAULT 0,
	error_message TEXT NOT NULL DEFAULT '',
	retry_count INTEGER NOT NULL DEFAULT 0,
	max_retries INTEGER NOT NULL DEFAULT 3,
	status TEXT NOT NULL,
	next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_delivery_failures_due_retries
	ON delivery_failures (next_retry_at ASC)
	WHERE status = 'RETRYING';

CREATE INDEX IF NOT EXISTS idx_delivery_failures_event_id
	ON delivery_failures (event_id);
`
	_, err := s.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("ensuring postgres schema: %w", err)
	}
	return nil
}

func (s *PostgresWebhookStore) SaveEvent(ctx context.Context, event *WebhookEvent) (*WebhookEvent, bool, error) {
	if event.ID == "" {
		event.ID = generateID()
	}
	now := time.Now().UTC()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	event.UpdatedAt = now
	if event.Status == "" {
		event.Status = WebhookStatusPending
	}

	headersJSON, err := json.Marshal(event.Headers)
	if err != nil {
		return nil, false, fmt.Errorf("marshaling headers: %w", err)
	}

	insertQuery := `
INSERT INTO webhook_events (
	id, provider, provider_event_id, event_type, payment_reference, is_dva, headers, payload, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (provider, provider_event_id) DO NOTHING
RETURNING id, status, created_at, updated_at
`
	var returnedID string
	var returnedStatus string
	err = s.pool.QueryRow(
		ctx,
		insertQuery,
		event.ID,
		string(event.Provider),
		event.ProviderEventID,
		event.EventType,
		event.PaymentReference,
		event.IsDVA,
		headersJSON,
		event.Payload,
		string(event.Status),
		event.CreatedAt,
		event.UpdatedAt,
	).Scan(&returnedID, &returnedStatus, &event.CreatedAt, &event.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		// Duplicate webhook: load existing record
		existing, getErr := s.getEventByProviderKey(ctx, event.Provider, event.ProviderEventID)
		if getErr != nil {
			return nil, true, getErr
		}
		return existing, true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inserting webhook event: %w", err)
	}

	event.ID = returnedID
	event.Status = WebhookStatus(returnedStatus)
	return event, false, nil
}

func (s *PostgresWebhookStore) getEventByProviderKey(ctx context.Context, provider PSPProvider, providerEventID string) (*WebhookEvent, error) {
	query := `
SELECT id, provider, provider_event_id, event_type, payment_reference, is_dva, headers, payload, status, created_at, updated_at
FROM webhook_events
WHERE provider = $1 AND provider_event_id = $2
`
	return s.scanEvent(s.pool.QueryRow(ctx, query, string(provider), providerEventID))
}

func (s *PostgresWebhookStore) GetEvent(ctx context.Context, eventID string) (*WebhookEvent, error) {
	query := `
SELECT id, provider, provider_event_id, event_type, payment_reference, is_dva, headers, payload, status, created_at, updated_at
FROM webhook_events
WHERE id = $1
`
	return s.scanEvent(s.pool.QueryRow(ctx, query, eventID))
}

func (s *PostgresWebhookStore) scanEvent(row pgx.Row) (*WebhookEvent, error) {
	var ev WebhookEvent
	var providerStr string
	var statusStr string
	var headersJSON []byte

	err := row.Scan(
		&ev.ID,
		&providerStr,
		&ev.ProviderEventID,
		&ev.EventType,
		&ev.PaymentReference,
		&ev.IsDVA,
		&headersJSON,
		&ev.Payload,
		&statusStr,
		&ev.CreatedAt,
		&ev.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	ev.Provider = PSPProvider(providerStr)
	ev.Status = WebhookStatus(statusStr)
	_ = json.Unmarshal(headersJSON, &ev.Headers)
	return &ev, nil
}

func (s *PostgresWebhookStore) UpdateStatus(ctx context.Context, eventID string, status WebhookStatus) error {
	query := `UPDATE webhook_events SET status = $1, updated_at = $2 WHERE id = $3`
	_, err := s.pool.Exec(ctx, query, string(status), time.Now().UTC(), eventID)
	return err
}

func (s *PostgresWebhookStore) ClaimPendingOutboxEvents(
	ctx context.Context,
	olderThan time.Duration,
	limit int,
	fn func(ctx context.Context, events []WebhookEvent) ([]string, error),
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning outbox transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	cutoff := time.Now().UTC().Add(-olderThan)
	query := `
SELECT id, provider, provider_event_id, event_type, payment_reference, is_dva, headers, payload, status, created_at, updated_at
FROM webhook_events
WHERE status = 'PENDING' AND created_at <= $1
ORDER BY created_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED
`
	rows, err := tx.Query(ctx, query, cutoff, limit)
	if err != nil {
		return fmt.Errorf("querying pending outbox events: %w", err)
	}

	var events []WebhookEvent
	for rows.Next() {
		var ev WebhookEvent
		var providerStr, statusStr string
		var headersJSON []byte
		if scanErr := rows.Scan(
			&ev.ID,
			&providerStr,
			&ev.ProviderEventID,
			&ev.EventType,
			&ev.PaymentReference,
			&ev.IsDVA,
			&headersJSON,
			&ev.Payload,
			&statusStr,
			&ev.CreatedAt,
			&ev.UpdatedAt,
		); scanErr != nil {
			rows.Close()
			return scanErr
		}
		ev.Provider = PSPProvider(providerStr)
		ev.Status = WebhookStatus(statusStr)
		_ = json.Unmarshal(headersJSON, &ev.Headers)
		events = append(events, ev)
	}
	rows.Close()

	if len(events) == 0 {
		return nil
	}

	publishedIDs, pubErr := fn(ctx, events)
	if len(publishedIDs) > 0 {
		updateQuery := `UPDATE webhook_events SET status = 'PUBLISHED', updated_at = $1 WHERE id = ANY($2)`
		if _, err := tx.Exec(ctx, updateQuery, time.Now().UTC(), publishedIDs); err != nil {
			return fmt.Errorf("updating published outbox events: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing outbox transaction: %w", err)
	}
	return pubErr
}

func (s *PostgresWebhookStore) EnqueueDeliveryFailure(ctx context.Context, failure *DeliveryFailure) error {
	if failure.ID == "" {
		failure.ID = generateID()
	}
	now := time.Now().UTC()
	if failure.CreatedAt.IsZero() {
		failure.CreatedAt = now
	}
	failure.UpdatedAt = now
	if failure.MaxRetries <= 0 {
		failure.MaxRetries = 3
	}

	headersJSON, err := json.Marshal(failure.Headers)
	if err != nil {
		return fmt.Errorf("marshaling failure headers: %w", err)
	}

	query := `
INSERT INTO delivery_failures (
	id, event_id, provider, product, endpoint_name, target_url, routing_mode,
	headers, payload, status_code, error_message, retry_count, max_retries, status, next_retry_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
`
	_, err = s.pool.Exec(
		ctx,
		query,
		failure.ID,
		failure.EventID,
		string(failure.Provider),
		failure.Product,
		failure.EndpointName,
		failure.TargetURL,
		failure.RoutingMode,
		headersJSON,
		failure.Payload,
		failure.StatusCode,
		failure.ErrorMessage,
		failure.RetryCount,
		failure.MaxRetries,
		string(failure.Status),
		failure.NextRetryAt,
		failure.CreatedAt,
		failure.UpdatedAt,
	)
	return err
}

func (s *PostgresWebhookStore) ClaimDueRetries(
	ctx context.Context,
	now time.Time,
	limit int,
	fn func(ctx context.Context, item DeliveryFailure) RetryAttemptResult,
) ([]DeliveryFailure, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning retry claim transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	query := `
SELECT id, event_id, provider, product, endpoint_name, target_url, routing_mode,
       headers, payload, status_code, error_message, retry_count, max_retries, status, next_retry_at, created_at, updated_at
FROM delivery_failures
WHERE status = 'RETRYING' AND next_retry_at <= $1
ORDER BY next_retry_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED
`
	rows, err := tx.Query(ctx, query, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("querying due retries: %w", err)
	}

	var items []DeliveryFailure
	for rows.Next() {
		var item DeliveryFailure
		var providerStr, statusStr string
		var headersJSON []byte
		if scanErr := rows.Scan(
			&item.ID,
			&item.EventID,
			&providerStr,
			&item.Product,
			&item.EndpointName,
			&item.TargetURL,
			&item.RoutingMode,
			&headersJSON,
			&item.Payload,
			&item.StatusCode,
			&item.ErrorMessage,
			&item.RetryCount,
			&item.MaxRetries,
			&statusStr,
			&item.NextRetryAt,
			&item.CreatedAt,
			&item.UpdatedAt,
		); scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		item.Provider = PSPProvider(providerStr)
		item.Status = DeliveryFailureStatus(statusStr)
		_ = json.Unmarshal(headersJSON, &item.Headers)
		items = append(items, item)
	}
	rows.Close()

	if len(items) == 0 {
		return nil, nil
	}

	var exhausted []DeliveryFailure
	for _, item := range items {
		res := fn(ctx, item)
		item.RetryCount++
		item.StatusCode = res.StatusCode
		item.ErrorMessage = res.ErrorMessage
		item.UpdatedAt = time.Now().UTC()

		if res.Resolved {
			item.Status = DeliveryStatusResolved
		} else if item.RetryCount >= item.MaxRetries {
			item.Status = DeliveryStatusFailed
			exhausted = append(exhausted, item)
		} else {
			item.Status = DeliveryStatusRetrying
			item.NextRetryAt = res.NextRetryAt
		}

		updateQuery := `
UPDATE delivery_failures
SET retry_count = $1, status_code = $2, error_message = $3, status = $4, next_retry_at = $5, updated_at = $6
WHERE id = $7
`
		if _, err := tx.Exec(
			ctx,
			updateQuery,
			item.RetryCount,
			item.StatusCode,
			item.ErrorMessage,
			string(item.Status),
			item.NextRetryAt,
			item.UpdatedAt,
			item.ID,
		); err != nil {
			return nil, fmt.Errorf("updating delivery failure %q: %w", item.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing retry transaction: %w", err)
	}

	return exhausted, nil
}

func (s *PostgresWebhookStore) ListDeliveryFailures(ctx context.Context, eventID string) ([]DeliveryFailure, error) {
	query := `
SELECT id, event_id, provider, product, endpoint_name, target_url, routing_mode,
       headers, payload, status_code, error_message, retry_count, max_retries, status, next_retry_at, created_at, updated_at
FROM delivery_failures
WHERE event_id = $1
ORDER BY created_at ASC
`
	rows, err := s.pool.Query(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DeliveryFailure
	for rows.Next() {
		var item DeliveryFailure
		var providerStr, statusStr string
		var headersJSON []byte
		if err := rows.Scan(
			&item.ID,
			&item.EventID,
			&providerStr,
			&item.Product,
			&item.EndpointName,
			&item.TargetURL,
			&item.RoutingMode,
			&headersJSON,
			&item.Payload,
			&item.StatusCode,
			&item.ErrorMessage,
			&item.RetryCount,
			&item.MaxRetries,
			&statusStr,
			&item.NextRetryAt,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.Provider = PSPProvider(providerStr)
		item.Status = DeliveryFailureStatus(statusStr)
		_ = json.Unmarshal(headersJSON, &item.Headers)
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *PostgresWebhookStore) HasPendingOrFailedDeliveries(ctx context.Context, eventID string) (bool, bool, error) {
	query := `
SELECT
	COALESCE(BOOL_OR(status = 'RETRYING'), FALSE) AS has_retrying,
	COALESCE(BOOL_OR(status = 'FAILED'), FALSE) AS has_failed
FROM delivery_failures
WHERE event_id = $1
`
	var hasRetrying, hasFailed bool
	err := s.pool.QueryRow(ctx, query, eventID).Scan(&hasRetrying, &hasFailed)
	return hasRetrying, hasFailed, err
}

func (s *PostgresWebhookStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresWebhookStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// InMemoryWebhookStore provides a thread-safe in-memory implementation of WebhookStore for testing and local dev.
type InMemoryWebhookStore struct {
	mu          sync.RWMutex
	eventsByID  map[string]*WebhookEvent
	providerIdx map[string]string
	failures    map[string]*DeliveryFailure
}

// NewInMemoryWebhookStore creates a new thread-safe in-memory WebhookStore.
func NewInMemoryWebhookStore() *InMemoryWebhookStore {
	return &InMemoryWebhookStore{
		eventsByID:  make(map[string]*WebhookEvent),
		providerIdx: make(map[string]string),
		failures:    make(map[string]*DeliveryFailure),
	}
}

func (s *InMemoryWebhookStore) EnsureSchema(_ context.Context) error {
	return nil
}

func (s *InMemoryWebhookStore) SaveEvent(_ context.Context, event *WebhookEvent) (*WebhookEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	compositeKey := string(event.Provider) + "|" + event.ProviderEventID
	if existingID, exists := s.providerIdx[compositeKey]; exists {
		existing := cloneWebhookEvent(s.eventsByID[existingID])
		return existing, true, nil
	}

	if event.ID == "" {
		event.ID = generateID()
	}
	now := time.Now().UTC()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	event.UpdatedAt = now
	if event.Status == "" {
		event.Status = WebhookStatusPending
	}

	stored := cloneWebhookEvent(event)
	s.eventsByID[stored.ID] = stored
	s.providerIdx[compositeKey] = stored.ID

	return cloneWebhookEvent(stored), false, nil
}

func (s *InMemoryWebhookStore) GetEvent(_ context.Context, eventID string) (*WebhookEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ev, ok := s.eventsByID[eventID]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return cloneWebhookEvent(ev), nil
}

func (s *InMemoryWebhookStore) UpdateStatus(_ context.Context, eventID string, status WebhookStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ev, ok := s.eventsByID[eventID]
	if !ok {
		return ErrKeyNotFound
	}
	ev.Status = status
	ev.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *InMemoryWebhookStore) ClaimPendingOutboxEvents(
	ctx context.Context,
	olderThan time.Duration,
	limit int,
	fn func(ctx context.Context, events []WebhookEvent) ([]string, error),
) error {
	s.mu.Lock()
	cutoff := time.Now().UTC().Add(-olderThan)
	var candidates []WebhookEvent
	for _, ev := range s.eventsByID {
		if ev.Status == WebhookStatusPending && !ev.CreatedAt.After(cutoff) {
			candidates = append(candidates, *cloneWebhookEvent(ev))
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}
	s.mu.Unlock()

	if len(candidates) == 0 {
		return nil
	}

	publishedIDs, pubErr := fn(ctx, candidates)
	if len(publishedIDs) > 0 {
		s.mu.Lock()
		now := time.Now().UTC()
		for _, id := range publishedIDs {
			if ev, ok := s.eventsByID[id]; ok {
				ev.Status = WebhookStatusPublished
				ev.UpdatedAt = now
			}
		}
		s.mu.Unlock()
	}
	return pubErr
}

func (s *InMemoryWebhookStore) EnqueueDeliveryFailure(_ context.Context, failure *DeliveryFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if failure.ID == "" {
		failure.ID = generateID()
	}
	now := time.Now().UTC()
	if failure.CreatedAt.IsZero() {
		failure.CreatedAt = now
	}
	failure.UpdatedAt = now
	if failure.MaxRetries <= 0 {
		failure.MaxRetries = 3
	}

	cloned := cloneDeliveryFailure(failure)
	s.failures[cloned.ID] = cloned
	return nil
}

func (s *InMemoryWebhookStore) ClaimDueRetries(
	ctx context.Context,
	now time.Time,
	limit int,
	fn func(ctx context.Context, item DeliveryFailure) RetryAttemptResult,
) ([]DeliveryFailure, error) {
	s.mu.Lock()
	var due []DeliveryFailure
	for _, item := range s.failures {
		if item.Status == DeliveryStatusRetrying && !item.NextRetryAt.After(now) {
			due = append(due, *cloneDeliveryFailure(item))
		}
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].NextRetryAt.Before(due[j].NextRetryAt)
	})
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	s.mu.Unlock()

	if len(due) == 0 {
		return nil, nil
	}

	var exhausted []DeliveryFailure
	for _, item := range due {
		res := fn(ctx, item)

		s.mu.Lock()
		stored, ok := s.failures[item.ID]
		if ok {
			stored.RetryCount++
			stored.StatusCode = res.StatusCode
			stored.ErrorMessage = res.ErrorMessage
			stored.UpdatedAt = time.Now().UTC()

			if res.Resolved {
				stored.Status = DeliveryStatusResolved
			} else if stored.RetryCount >= stored.MaxRetries {
				stored.Status = DeliveryStatusFailed
				exhausted = append(exhausted, *cloneDeliveryFailure(stored))
			} else {
				stored.Status = DeliveryStatusRetrying
				stored.NextRetryAt = res.NextRetryAt
			}
		}
		s.mu.Unlock()
	}

	return exhausted, nil
}

func (s *InMemoryWebhookStore) ListDeliveryFailures(_ context.Context, eventID string) ([]DeliveryFailure, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []DeliveryFailure
	for _, item := range s.failures {
		if item.EventID == eventID {
			result = append(result, *cloneDeliveryFailure(item))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (s *InMemoryWebhookStore) HasPendingOrFailedDeliveries(_ context.Context, eventID string) (bool, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var hasRetrying, hasFailed bool
	for _, item := range s.failures {
		if item.EventID == eventID {
			if item.Status == DeliveryStatusRetrying {
				hasRetrying = true
			}
			if item.Status == DeliveryStatusFailed {
				hasFailed = true
			}
		}
	}
	return hasRetrying, hasFailed, nil
}

func (s *InMemoryWebhookStore) Ping(_ context.Context) error {
	return nil
}

func (s *InMemoryWebhookStore) Close() {}

func cloneWebhookEvent(ev *WebhookEvent) *WebhookEvent {
	if ev == nil {
		return nil
	}
	cp := *ev
	if ev.Headers != nil {
		cp.Headers = make(map[string]string, len(ev.Headers))
		for k, v := range ev.Headers {
			cp.Headers[k] = v
		}
	}
	if ev.Payload != nil {
		cp.Payload = append([]byte(nil), ev.Payload...)
	}
	return &cp
}

func cloneDeliveryFailure(df *DeliveryFailure) *DeliveryFailure {
	if df == nil {
		return nil
	}
	cp := *df
	if df.Headers != nil {
		cp.Headers = make(map[string]string, len(df.Headers))
		for k, v := range df.Headers {
			cp.Headers[k] = v
		}
	}
	if df.Payload != nil {
		cp.Payload = append([]byte(nil), df.Payload...)
	}
	return &cp
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
