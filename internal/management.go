package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"
)

// DashboardOverview aggregates global and per-product webhook and delivery failure metrics.
type DashboardOverview struct {
	Products        []ProductMetrics `json:"products"`
	TotalEvents     int              `json:"total_events"`
	DeliveredEvents int              `json:"delivered_events"`
	PendingEvents   int              `json:"pending_events"`
	FailedEvents    int              `json:"failed_events"`
	ActiveRetries   int              `json:"active_retries"`
	DeadLetterCount int              `json:"dead_letter_count"`
}

// PaginatedEvents wraps a page of WebhookEvent records with total count metadata.
type PaginatedEvents struct {
	Items  []WebhookEvent `json:"items"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}

// PaginatedFailures wraps a page of DeliveryFailure records with total count metadata.
type PaginatedFailures struct {
	Items  []DeliveryFailure `json:"items"`
	Total  int               `json:"total"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
}

// EventDetail holds a single WebhookEvent, its formatted JSON payload, and all associated DeliveryFailure attempts.
type EventDetail struct {
	Event         WebhookEvent      `json:"event"`
	PrettyPayload string            `json:"pretty_payload"`
	Failures      []DeliveryFailure `json:"failures"`
}

// ManagementUseCase defines the application port for querying webhook telemetry and triggering manual retries.
type ManagementUseCase interface {
	GetDashboardOverview(ctx context.Context) (*DashboardOverview, error)
	ListEvents(ctx context.Context, filter EventFilter) (*PaginatedEvents, error)
	GetEventDetail(ctx context.Context, eventID string) (*EventDetail, error)
	ListFailures(ctx context.Context, filter FailureFilter) (*PaginatedFailures, error)
	RetryFailureNow(ctx context.Context, failureID string) (*DeliveryFailure, error)
	ReplayEventNow(ctx context.Context, eventID string) (*WebhookEvent, error)
	BulkRetryFailures(ctx context.Context, product string, provider PSPProvider) (int, error)
}

type managementUseCaseImpl struct {
	store            WebhookStore
	gatewayCfg       *GatewayConfig
	dispatcher       UpstreamDispatcher
	dispatcherUC     DispatcherUseCase
	retryBaseBackoff time.Duration
	logger           *slog.Logger
}

// NewManagementUseCase constructs a ManagementUseCase wired to the persistence store and outbound HTTP dispatcher.
func NewManagementUseCase(
	store WebhookStore,
	gatewayCfg *GatewayConfig,
	dispatcher UpstreamDispatcher,
	dispatcherUC DispatcherUseCase,
	retryBaseBackoff time.Duration,
	logger *slog.Logger,
) ManagementUseCase {
	if retryBaseBackoff <= 0 {
		retryBaseBackoff = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &managementUseCaseImpl{
		store:            store,
		gatewayCfg:       gatewayCfg,
		dispatcher:       dispatcher,
		dispatcherUC:     dispatcherUC,
		retryBaseBackoff: retryBaseBackoff,
		logger:           logger,
	}
}

func (u *managementUseCaseImpl) GetDashboardOverview(ctx context.Context) (*DashboardOverview, error) {
	storeMetrics, err := u.store.GetProductMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting product metrics: %w", err)
	}

	merged := make(map[string]ProductMetrics)
	for prod, m := range storeMetrics {
		merged[prod] = m
	}

	if u.gatewayCfg != nil {
		for prodName, cfg := range u.gatewayCfg.Products {
			m := merged[prodName]
			m.Product = prodName
			m.Prefix = cfg.Prefix
			m.EndpointCount = len(cfg.Endpoints)
			merged[prodName] = m
		}
	}

	// Ensure unmatched bucket is always present for visibility
	if _, exists := merged[RoutingModeUnmatched]; !exists {
		merged[RoutingModeUnmatched] = ProductMetrics{
			Product: RoutingModeUnmatched,
			Prefix:  "—",
		}
	} else {
		unmatched := merged[RoutingModeUnmatched]
		if unmatched.Prefix == "" {
			unmatched.Prefix = "—"
		}
		merged[RoutingModeUnmatched] = unmatched
	}

	overview := &DashboardOverview{
		Products: make([]ProductMetrics, 0, len(merged)),
	}

	for _, m := range merged {
		overview.Products = append(overview.Products, m)
		overview.TotalEvents += m.TotalEvents
		overview.DeliveredEvents += m.DeliveredEvents
		overview.PendingEvents += m.PendingEvents
		overview.FailedEvents += m.FailedEvents
		overview.ActiveRetries += m.ActiveRetries
		overview.DeadLetterCount += m.DeadLetterCount
	}

	sort.Slice(overview.Products, func(i, j int) bool {
		if overview.Products[i].Product == RoutingModeUnmatched {
			return false
		}
		if overview.Products[j].Product == RoutingModeUnmatched {
			return true
		}
		return overview.Products[i].Product < overview.Products[j].Product
	})

	return overview, nil
}

func (u *managementUseCaseImpl) ListEvents(ctx context.Context, filter EventFilter) (*PaginatedEvents, error) {
	if filter.Limit <= 0 || filter.Limit > 200 {
		filter.Limit = 25
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	items, total, err := u.store.ListEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("listing webhook events: %w", err)
	}
	if items == nil {
		items = []WebhookEvent{}
	}

	return &PaginatedEvents{
		Items:  items,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
}

func (u *managementUseCaseImpl) GetEventDetail(ctx context.Context, eventID string) (*EventDetail, error) {
	trimmedID := strings.TrimSpace(eventID)
	if trimmedID == "" {
		return nil, ErrEventNotFound
	}

	ev, err := u.store.GetEvent(ctx, trimmedID)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrEventNotFound) {
			return nil, ErrEventNotFound
		}
		return nil, fmt.Errorf("loading webhook event %q: %w", trimmedID, err)
	}

	failures, err := u.store.ListDeliveryFailures(ctx, trimmedID)
	if err != nil {
		return nil, fmt.Errorf("listing delivery failures for event %q: %w", trimmedID, err)
	}
	if failures == nil {
		failures = []DeliveryFailure{}
	}

	return &EventDetail{
		Event:         *ev,
		PrettyPayload: formatJSONPretty(ev.Payload),
		Failures:      failures,
	}, nil
}

func (u *managementUseCaseImpl) ListFailures(ctx context.Context, filter FailureFilter) (*PaginatedFailures, error) {
	if filter.Limit <= 0 || filter.Limit > 200 {
		filter.Limit = 25
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	items, total, err := u.store.ListAllDeliveryFailures(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("listing delivery failures: %w", err)
	}
	if items == nil {
		items = []DeliveryFailure{}
	}

	return &PaginatedFailures{
		Items:  items,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}, nil
}

func (u *managementUseCaseImpl) RetryFailureNow(ctx context.Context, failureID string) (*DeliveryFailure, error) {
	trimmedID := strings.TrimSpace(failureID)
	if trimmedID == "" {
		return nil, ErrFailureNotFound
	}

	failure, err := u.store.GetDeliveryFailure(ctx, trimmedID)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrFailureNotFound) {
			return nil, ErrFailureNotFound
		}
		return nil, fmt.Errorf("getting delivery failure %q: %w", trimmedID, err)
	}

	// If this failure was recorded as unmatched (no target URL), re-evaluate routing against current config.yaml
	if failure.RoutingMode == RoutingModeUnmatched || strings.TrimSpace(failure.TargetURL) == "" {
		ev, evErr := u.store.GetEvent(ctx, failure.EventID)
		if evErr == nil && u.gatewayCfg != nil {
			if matchedProd, _, matched := u.gatewayCfg.MatchProductByReference(ev.PaymentReference); matched && u.dispatcherUC != nil {
				ev.Product = matchedProd
				_ = u.store.UpdateEventProduct(ctx, ev.ID, matchedProd)
				if dispErr := u.dispatcherUC.Execute(ctx, *ev); dispErr == nil {
					failure.RetryCount++
					failure.Status = DeliveryStatusResolved
					failure.StatusCode = http.StatusOK
					failure.ErrorMessage = "resolved via product route re-evaluation (" + matchedProd + ")"
					_ = u.store.UpdateDeliveryFailureAttempt(ctx, failure)
					u.reconcileParentEventStatus(ctx, failure.EventID)
					return failure, nil
				}
			}
		}

		failure.RetryCount++
		failure.Status = DeliveryStatusFailed
		failure.NextRetryAt = time.Now().UTC()
		if updateErr := u.store.UpdateDeliveryFailureAttempt(ctx, failure); updateErr != nil {
			return nil, fmt.Errorf("updating unmatched delivery failure %q: %w", trimmedID, updateErr)
		}
		return failure, nil
	}

	reqHeaders := make(http.Header, len(failure.Headers)+1)
	for k, v := range failure.Headers {
		reqHeaders.Set(k, v)
	}
	reqHeaders.Set("X-PSPRelay-Retry-Attempt", fmt.Sprintf("%d", failure.RetryCount+1))

	statusCode, _, _, sendErr := u.dispatcher.Send(ctx, failure.TargetURL, http.MethodPost, reqHeaders, failure.Payload)
	failure.RetryCount++
	failure.StatusCode = statusCode

	if sendErr == nil && statusCode >= 200 && statusCode < 300 {
		failure.Status = DeliveryStatusResolved
		failure.ErrorMessage = ""
	} else {
		errMsg := fmt.Sprintf("upstream returned status %d", statusCode)
		if sendErr != nil {
			errMsg = sendErr.Error()
		}
		failure.ErrorMessage = errMsg
		if failure.MaxRetries <= 0 {
			failure.MaxRetries = 3
		}
		if failure.RetryCount >= failure.MaxRetries {
			failure.Status = DeliveryStatusFailed
		} else {
			failure.Status = DeliveryStatusRetrying
			failure.NextRetryAt = time.Now().UTC().Add(computeBackoffWithJitter(u.retryBaseBackoff, failure.RetryCount))
		}
	}

	if updateErr := u.store.UpdateDeliveryFailureAttempt(ctx, failure); updateErr != nil {
		return nil, fmt.Errorf("updating delivery failure attempt %q: %w", trimmedID, updateErr)
	}

	u.reconcileParentEventStatus(ctx, failure.EventID)
	return failure, nil
}

func (u *managementUseCaseImpl) ReplayEventNow(ctx context.Context, eventID string) (*WebhookEvent, error) {
	trimmedID := strings.TrimSpace(eventID)
	if trimmedID == "" {
		return nil, ErrEventNotFound
	}

	ev, err := u.store.GetEvent(ctx, trimmedID)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrEventNotFound) {
			return nil, ErrEventNotFound
		}
		return nil, fmt.Errorf("loading webhook event %q for replay: %w", trimmedID, err)
	}

	if u.dispatcherUC != nil {
		if dispErr := u.dispatcherUC.Execute(ctx, *ev); dispErr != nil {
			return nil, fmt.Errorf("replaying webhook event %q: %w", trimmedID, dispErr)
		}
	}

	updated, getErr := u.store.GetEvent(ctx, trimmedID)
	if getErr != nil {
		return ev, nil
	}
	return updated, nil
}

func (u *managementUseCaseImpl) BulkRetryFailures(ctx context.Context, product string, provider PSPProvider) (int, error) {
	trimmedProd := strings.TrimSpace(product)
	count, err := u.store.ResetFailuresForRetry(ctx, trimmedProd, provider)
	if err != nil {
		return 0, fmt.Errorf("bulk resetting failures for retry: %w", err)
	}
	return count, nil
}

func (u *managementUseCaseImpl) reconcileParentEventStatus(ctx context.Context, eventID string) {
	hasRetrying, hasFailed, err := u.store.HasPendingOrFailedDeliveries(ctx, eventID)
	if err != nil {
		u.logger.Warn("Failed checking delivery statuses during manual retry reconciliation", "event_id", eventID, "error", err)
		return
	}
	if hasRetrying {
		_ = u.store.UpdateStatus(ctx, eventID, WebhookStatusPublished)
		return
	}
	if hasFailed {
		_ = u.store.UpdateStatus(ctx, eventID, WebhookStatusFailed)
		return
	}
	_ = u.store.UpdateStatus(ctx, eventID, WebhookStatusDelivered)
}

func formatJSONPretty(raw []byte) string {
	if len(raw) == 0 {
		return "{}"
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}
	return out.String()
}
