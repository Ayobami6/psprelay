package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	Product          string            `json:"product"`
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

// EventFilter specifies filtering and pagination parameters for listing webhook events.
type EventFilter struct {
	Product  string
	Provider PSPProvider
	Status   WebhookStatus
	Search   string
	Limit    int
	Offset   int
}

// FailureFilter specifies filtering and pagination parameters for listing delivery failures.
type FailureFilter struct {
	Product  string
	Provider PSPProvider
	Status   DeliveryFailureStatus
	EventID  string
	Limit    int
	Offset   int
}

// ProductMetrics holds aggregated event and delivery failure counts for a product.
type ProductMetrics struct {
	Product         string `json:"product"`
	Prefix          string `json:"prefix"`
	EndpointCount   int    `json:"endpoint_count"`
	TotalEvents     int    `json:"total_events"`
	DeliveredEvents int    `json:"delivered_events"`
	PendingEvents   int    `json:"pending_events"`
	FailedEvents    int    `json:"failed_events"`
	ActiveRetries   int    `json:"active_retries"`
	DeadLetterCount int    `json:"dead_letter_count"`
}

// WebhookStore defines the secondary port for webhook persistence, transactional outbox, and retry/failure storage.
type WebhookStore interface {
	EnsureSchema(ctx context.Context) error
	SaveEvent(ctx context.Context, event *WebhookEvent) (saved *WebhookEvent, duplicate bool, err error)
	GetEvent(ctx context.Context, eventID string) (*WebhookEvent, error)
	UpdateStatus(ctx context.Context, eventID string, status WebhookStatus) error
	UpdateEventProduct(ctx context.Context, eventID string, product string) error
	ClaimPendingOutboxEvents(ctx context.Context, olderThan time.Duration, limit int, fn func(ctx context.Context, events []WebhookEvent) ([]string, error)) error
	EnqueueDeliveryFailure(ctx context.Context, failure *DeliveryFailure) error
	ClaimDueRetries(ctx context.Context, now time.Time, limit int, fn func(ctx context.Context, item DeliveryFailure) RetryAttemptResult) ([]DeliveryFailure, error)
	ListDeliveryFailures(ctx context.Context, eventID string) ([]DeliveryFailure, error)
	HasPendingOrFailedDeliveries(ctx context.Context, eventID string) (hasRetrying bool, hasFailed bool, err error)
	ListEvents(ctx context.Context, filter EventFilter) ([]WebhookEvent, int, error)
	ListAllDeliveryFailures(ctx context.Context, filter FailureFilter) ([]DeliveryFailure, int, error)
	GetDeliveryFailure(ctx context.Context, failureID string) (*DeliveryFailure, error)
	UpdateDeliveryFailureAttempt(ctx context.Context, failure *DeliveryFailure) error
	ResetFailuresForRetry(ctx context.Context, product string, provider PSPProvider) (int, error)
	GetProductMetrics(ctx context.Context) (map[string]ProductMetrics, error)
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
	product TEXT NOT NULL DEFAULT 'unmatched',
	is_dva BOOLEAN NOT NULL DEFAULT FALSE,
	headers JSONB NOT NULL DEFAULT '{}'::jsonb,
	payload BYTEA NOT NULL,
	status TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	UNIQUE (provider, provider_event_id)
);

ALTER TABLE webhook_events ADD COLUMN IF NOT EXISTS product TEXT NOT NULL DEFAULT 'unmatched';

CREATE INDEX IF NOT EXISTS idx_webhook_events_outbox
	ON webhook_events (created_at ASC)
	WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_webhook_events_product_created
	ON webhook_events (product, created_at DESC);

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

CREATE INDEX IF NOT EXISTS idx_delivery_failures_product_status
	ON delivery_failures (product, status, updated_at DESC);
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
	if strings.TrimSpace(event.Product) == "" {
		event.Product = RoutingModeUnmatched
	}

	headersJSON, err := json.Marshal(event.Headers)
	if err != nil {
		return nil, false, fmt.Errorf("marshaling headers: %w", err)
	}

	insertQuery := `
INSERT INTO webhook_events (
	id, provider, provider_event_id, event_type, payment_reference, product, is_dva, headers, payload, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
		event.Product,
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
SELECT id, provider, provider_event_id, event_type, payment_reference, product, is_dva, headers, payload, status, created_at, updated_at
FROM webhook_events
WHERE provider = $1 AND provider_event_id = $2
`
	return s.scanEvent(s.pool.QueryRow(ctx, query, string(provider), providerEventID))
}

func (s *PostgresWebhookStore) GetEvent(ctx context.Context, eventID string) (*WebhookEvent, error) {
	query := `
SELECT id, provider, provider_event_id, event_type, payment_reference, product, is_dva, headers, payload, status, created_at, updated_at
FROM webhook_events
WHERE id = $1
`
	ev, err := s.scanEvent(s.pool.QueryRow(ctx, query, eventID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrEventNotFound
	}
	return ev, err
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
		&ev.Product,
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
	if ev.Product == "" {
		ev.Product = RoutingModeUnmatched
	}
	_ = json.Unmarshal(headersJSON, &ev.Headers)
	return &ev, nil
}

func (s *PostgresWebhookStore) UpdateStatus(ctx context.Context, eventID string, status WebhookStatus) error {
	query := `UPDATE webhook_events SET status = $1, updated_at = $2 WHERE id = $3`
	_, err := s.pool.Exec(ctx, query, string(status), time.Now().UTC(), eventID)
	return err
}

func (s *PostgresWebhookStore) UpdateEventProduct(ctx context.Context, eventID string, product string) error {
	if strings.TrimSpace(product) == "" {
		product = RoutingModeUnmatched
	}
	query := `UPDATE webhook_events SET product = $1, updated_at = $2 WHERE id = $3`
	_, err := s.pool.Exec(ctx, query, product, time.Now().UTC(), eventID)
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
SELECT id, provider, provider_event_id, event_type, payment_reference, product, is_dva, headers, payload, status, created_at, updated_at
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
			&ev.Product,
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
		if ev.Product == "" {
			ev.Product = RoutingModeUnmatched
		}
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

	return s.scanDeliveryFailureRows(rows)
}

func (s *PostgresWebhookStore) scanDeliveryFailureRows(rows pgx.Rows) ([]DeliveryFailure, error) {
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

func (s *PostgresWebhookStore) ListEvents(ctx context.Context, filter EventFilter) ([]WebhookEvent, int, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	var clauses []string
	var args []any
	argIdx := 1

	if filter.Product != "" {
		clauses = append(clauses, fmt.Sprintf("product = $%d", argIdx))
		args = append(args, filter.Product)
		argIdx++
	}
	if filter.Provider != "" {
		clauses = append(clauses, fmt.Sprintf("provider = $%d", argIdx))
		args = append(args, string(filter.Provider))
		argIdx++
	}
	if filter.Status != "" {
		clauses = append(clauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, string(filter.Status))
		argIdx++
	}
	if q := strings.TrimSpace(filter.Search); q != "" {
		clauses = append(clauses, fmt.Sprintf("(payment_reference ILIKE $%d OR provider_event_id ILIKE $%d OR id ILIKE $%d)", argIdx, argIdx, argIdx))
		args = append(args, "%"+q+"%")
		argIdx++
	}

	whereSQL := ""
	if len(clauses) > 0 {
		whereSQL = "WHERE " + strings.Join(clauses, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM webhook_events %s", whereSQL)
	var total int
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("counting webhook events: %w", err)
	}

	listArgs := append(append([]any(nil), args...), limit, offset)
	listQuery := fmt.Sprintf(`
SELECT id, provider, provider_event_id, event_type, payment_reference, product, is_dva, headers, payload, status, created_at, updated_at
FROM webhook_events
%s
ORDER BY created_at DESC
LIMIT $%d OFFSET $%d
`, whereSQL, argIdx, argIdx+1)

	rows, err := s.pool.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("querying webhook events: %w", err)
	}
	defer rows.Close()

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
			&ev.Product,
			&ev.IsDVA,
			&headersJSON,
			&ev.Payload,
			&statusStr,
			&ev.CreatedAt,
			&ev.UpdatedAt,
		); scanErr != nil {
			return nil, 0, scanErr
		}
		ev.Provider = PSPProvider(providerStr)
		ev.Status = WebhookStatus(statusStr)
		if ev.Product == "" {
			ev.Product = RoutingModeUnmatched
		}
		_ = json.Unmarshal(headersJSON, &ev.Headers)
		events = append(events, ev)
	}

	return events, total, rows.Err()
}

func (s *PostgresWebhookStore) ListAllDeliveryFailures(ctx context.Context, filter FailureFilter) ([]DeliveryFailure, int, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	var clauses []string
	var args []any
	argIdx := 1

	if filter.Product != "" {
		clauses = append(clauses, fmt.Sprintf("product = $%d", argIdx))
		args = append(args, filter.Product)
		argIdx++
	}
	if filter.Provider != "" {
		clauses = append(clauses, fmt.Sprintf("provider = $%d", argIdx))
		args = append(args, string(filter.Provider))
		argIdx++
	}
	if filter.Status != "" {
		clauses = append(clauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, string(filter.Status))
		argIdx++
	}
	if filter.EventID != "" {
		clauses = append(clauses, fmt.Sprintf("event_id = $%d", argIdx))
		args = append(args, filter.EventID)
		argIdx++
	}

	whereSQL := ""
	if len(clauses) > 0 {
		whereSQL = "WHERE " + strings.Join(clauses, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM delivery_failures %s", whereSQL)
	var total int
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("counting delivery failures: %w", err)
	}

	listArgs := append(append([]any(nil), args...), limit, offset)
	listQuery := fmt.Sprintf(`
SELECT id, event_id, provider, product, endpoint_name, target_url, routing_mode,
       headers, payload, status_code, error_message, retry_count, max_retries, status, next_retry_at, created_at, updated_at
FROM delivery_failures
%s
ORDER BY updated_at DESC
LIMIT $%d OFFSET $%d
`, whereSQL, argIdx, argIdx+1)

	rows, err := s.pool.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("querying delivery failures: %w", err)
	}
	defer rows.Close()

	failures, scanErr := s.scanDeliveryFailureRows(rows)
	if scanErr != nil {
		return nil, 0, scanErr
	}
	return failures, total, nil
}

func (s *PostgresWebhookStore) GetDeliveryFailure(ctx context.Context, failureID string) (*DeliveryFailure, error) {
	query := `
SELECT id, event_id, provider, product, endpoint_name, target_url, routing_mode,
       headers, payload, status_code, error_message, retry_count, max_retries, status, next_retry_at, created_at, updated_at
FROM delivery_failures
WHERE id = $1
`
	var item DeliveryFailure
	var providerStr, statusStr string
	var headersJSON []byte
	err := s.pool.QueryRow(ctx, query, failureID).Scan(
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
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrFailureNotFound
	}
	if err != nil {
		return nil, err
	}
	item.Provider = PSPProvider(providerStr)
	item.Status = DeliveryFailureStatus(statusStr)
	_ = json.Unmarshal(headersJSON, &item.Headers)
	return &item, nil
}

func (s *PostgresWebhookStore) UpdateDeliveryFailureAttempt(ctx context.Context, failure *DeliveryFailure) error {
	now := time.Now().UTC()
	failure.UpdatedAt = now
	query := `
UPDATE delivery_failures
SET retry_count = $1, max_retries = $2, status_code = $3, error_message = $4, status = $5, next_retry_at = $6, updated_at = $7
WHERE id = $8
`
	tag, err := s.pool.Exec(
		ctx,
		query,
		failure.RetryCount,
		failure.MaxRetries,
		failure.StatusCode,
		failure.ErrorMessage,
		string(failure.Status),
		failure.NextRetryAt,
		failure.UpdatedAt,
		failure.ID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrFailureNotFound
	}
	return nil
}

func (s *PostgresWebhookStore) ResetFailuresForRetry(ctx context.Context, product string, provider PSPProvider) (int, error) {
	now := time.Now().UTC()
	var clauses []string
	clauses = append(clauses, "status IN ('FAILED', 'RETRYING')", "target_url <> ''")

	var args []any
	args = append(args, now)
	argIdx := 2

	if product != "" {
		clauses = append(clauses, fmt.Sprintf("product = $%d", argIdx))
		args = append(args, product)
		argIdx++
	}
	if provider != "" {
		clauses = append(clauses, fmt.Sprintf("provider = $%d", argIdx))
		args = append(args, string(provider))
	}

	whereSQL := strings.Join(clauses, " AND ")
	query := fmt.Sprintf(`
UPDATE delivery_failures
SET status = 'RETRYING', retry_count = 0, max_retries = GREATEST(max_retries, 3), next_retry_at = $1, updated_at = $1
WHERE %s
`, whereSQL)

	tag, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("resetting failures for retry: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresWebhookStore) GetProductMetrics(ctx context.Context) (map[string]ProductMetrics, error) {
	metrics := make(map[string]ProductMetrics)

	eventQuery := `
SELECT
	COALESCE(NULLIF(product, ''), 'unmatched') AS prod,
	COUNT(*) AS total,
	COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered,
	COUNT(*) FILTER (WHERE status IN ('PENDING', 'PUBLISHED')) AS pending,
	COUNT(*) FILTER (WHERE status = 'FAILED') AS failed
FROM webhook_events
GROUP BY prod
`
	evRows, err := s.pool.Query(ctx, eventQuery)
	if err != nil {
		return nil, fmt.Errorf("querying event metrics: %w", err)
	}
	for evRows.Next() {
		var prod string
		var total, delivered, pending, failed int
		if scanErr := evRows.Scan(&prod, &total, &delivered, &pending, &failed); scanErr != nil {
			evRows.Close()
			return nil, scanErr
		}
		m := metrics[prod]
		m.Product = prod
		m.TotalEvents = total
		m.DeliveredEvents = delivered
		m.PendingEvents = pending
		m.FailedEvents = failed
		metrics[prod] = m
	}
	evRows.Close()
	if err := evRows.Err(); err != nil {
		return nil, err
	}

	failureQuery := `
SELECT
	COALESCE(NULLIF(product, ''), 'unmatched') AS prod,
	COUNT(*) FILTER (WHERE status = 'RETRYING') AS active_retries,
	COUNT(*) FILTER (WHERE status = 'FAILED') AS dead_letter
FROM delivery_failures
GROUP BY prod
`
	fRows, err := s.pool.Query(ctx, failureQuery)
	if err != nil {
		return nil, fmt.Errorf("querying failure metrics: %w", err)
	}
	for fRows.Next() {
		var prod string
		var activeRetries, deadLetter int
		if scanErr := fRows.Scan(&prod, &activeRetries, &deadLetter); scanErr != nil {
			fRows.Close()
			return nil, scanErr
		}
		m := metrics[prod]
		m.Product = prod
		m.ActiveRetries = activeRetries
		m.DeadLetterCount = deadLetter
		metrics[prod] = m
	}
	fRows.Close()
	if err := fRows.Err(); err != nil {
		return nil, err
	}

	return metrics, nil
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
	if strings.TrimSpace(event.Product) == "" {
		event.Product = RoutingModeUnmatched
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

func (s *InMemoryWebhookStore) UpdateEventProduct(_ context.Context, eventID string, product string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ev, ok := s.eventsByID[eventID]
	if !ok {
		return ErrKeyNotFound
	}
	if strings.TrimSpace(product) == "" {
		product = RoutingModeUnmatched
	}
	ev.Product = product
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

func (s *InMemoryWebhookStore) ListEvents(_ context.Context, filter EventFilter) ([]WebhookEvent, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	q := strings.ToLower(strings.TrimSpace(filter.Search))

	var matched []WebhookEvent
	for _, ev := range s.eventsByID {
		prod := ev.Product
		if prod == "" {
			prod = RoutingModeUnmatched
		}
		if filter.Product != "" && !strings.EqualFold(prod, filter.Product) {
			continue
		}
		if filter.Provider != "" && ev.Provider != filter.Provider {
			continue
		}
		if filter.Status != "" && ev.Status != filter.Status {
			continue
		}
		if q != "" {
			if !strings.Contains(strings.ToLower(ev.PaymentReference), q) &&
				!strings.Contains(strings.ToLower(ev.ProviderEventID), q) &&
				!strings.Contains(strings.ToLower(ev.ID), q) {
				continue
			}
		}
		cloned := cloneWebhookEvent(ev)
		if cloned.Product == "" {
			cloned.Product = RoutingModeUnmatched
		}
		matched = append(matched, *cloned)
	}

	sort.Slice(matched, func(i, j int) bool {
		if matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].ID > matched[j].ID
		}
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	total := len(matched)
	if offset >= total {
		return []WebhookEvent{}, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return matched[offset:end], total, nil
}

func (s *InMemoryWebhookStore) ListAllDeliveryFailures(_ context.Context, filter FailureFilter) ([]DeliveryFailure, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	var matched []DeliveryFailure
	for _, item := range s.failures {
		if filter.Product != "" && !strings.EqualFold(item.Product, filter.Product) {
			continue
		}
		if filter.Provider != "" && item.Provider != filter.Provider {
			continue
		}
		if filter.Status != "" && item.Status != filter.Status {
			continue
		}
		if filter.EventID != "" && item.EventID != filter.EventID {
			continue
		}
		matched = append(matched, *cloneDeliveryFailure(item))
	}

	sort.Slice(matched, func(i, j int) bool {
		if matched[i].UpdatedAt.Equal(matched[j].UpdatedAt) {
			return matched[i].ID > matched[j].ID
		}
		return matched[i].UpdatedAt.After(matched[j].UpdatedAt)
	})

	total := len(matched)
	if offset >= total {
		return []DeliveryFailure{}, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return matched[offset:end], total, nil
}

func (s *InMemoryWebhookStore) GetDeliveryFailure(_ context.Context, failureID string) (*DeliveryFailure, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.failures[failureID]
	if !ok {
		return nil, ErrFailureNotFound
	}
	return cloneDeliveryFailure(item), nil
}

func (s *InMemoryWebhookStore) UpdateDeliveryFailureAttempt(_ context.Context, failure *DeliveryFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.failures[failure.ID]
	if !ok {
		return ErrFailureNotFound
	}
	now := time.Now().UTC()
	stored.RetryCount = failure.RetryCount
	stored.MaxRetries = failure.MaxRetries
	stored.StatusCode = failure.StatusCode
	stored.ErrorMessage = failure.ErrorMessage
	stored.Status = failure.Status
	stored.NextRetryAt = failure.NextRetryAt
	stored.UpdatedAt = now
	failure.UpdatedAt = now
	return nil
}

func (s *InMemoryWebhookStore) ResetFailuresForRetry(_ context.Context, product string, provider PSPProvider) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	count := 0
	for _, item := range s.failures {
		if item.Status != DeliveryStatusFailed && item.Status != DeliveryStatusRetrying {
			continue
		}
		if item.TargetURL == "" {
			continue
		}
		if product != "" && !strings.EqualFold(item.Product, product) {
			continue
		}
		if provider != "" && item.Provider != provider {
			continue
		}
		item.Status = DeliveryStatusRetrying
		item.RetryCount = 0
		if item.MaxRetries < 3 {
			item.MaxRetries = 3
		}
		item.NextRetryAt = now
		item.UpdatedAt = now
		count++
	}
	return count, nil
}

func (s *InMemoryWebhookStore) GetProductMetrics(_ context.Context) (map[string]ProductMetrics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics := make(map[string]ProductMetrics)
	for _, ev := range s.eventsByID {
		prod := ev.Product
		if prod == "" {
			prod = RoutingModeUnmatched
		}
		m := metrics[prod]
		m.Product = prod
		m.TotalEvents++
		switch ev.Status {
		case WebhookStatusDelivered:
			m.DeliveredEvents++
		case WebhookStatusPending, WebhookStatusPublished:
			m.PendingEvents++
		case WebhookStatusFailed:
			m.FailedEvents++
		}
		metrics[prod] = m
	}

	for _, f := range s.failures {
		prod := f.Product
		if prod == "" {
			prod = RoutingModeUnmatched
		}
		m := metrics[prod]
		m.Product = prod
		switch f.Status {
		case DeliveryStatusRetrying:
			m.ActiveRetries++
		case DeliveryStatusFailed:
			m.DeadLetterCount++
		}
		metrics[prod] = m
	}

	return metrics, nil
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
