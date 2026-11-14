package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	RoutingModeDirect    = "direct"
	RoutingModeUnmatched = "unmatched"
)

type healthCheckUseCaseImpl struct {
	service Service
}

// HealthCheckUseCase defines the health check application port.
type HealthCheckUseCase interface {
	Execute(ctx context.Context) (*HealthResponse, error)
}

// HealthResponse represents the health status payload.
type HealthResponse struct {
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

// NewHealthCheckUseCase creates a new HealthCheckUseCase.
func NewHealthCheckUseCase(service Service) HealthCheckUseCase {
	return &healthCheckUseCaseImpl{service: service}
}

func (u *healthCheckUseCaseImpl) Execute(ctx context.Context) (*HealthResponse, error) {
	err := u.service.CheckHealth(ctx)
	if err != nil {
		return &HealthResponse{
			Status: "degraded",
			Error:  err.Error(),
		}, err
	}
	return &HealthResponse{
		Status:  "healthy",
		Message: "All systems operational",
	}, nil
}

// RelayService orchestrates persisting normalized webhooks to the database outbox and publishing to Kafka.
type RelayService interface {
	Ingest(ctx context.Context, normalized *NormalizedWebhook) (*WebhookEvent, error)
}

type relayServiceImpl struct {
	store      WebhookStore
	publisher  EventPublisher
	gatewayCfg *GatewayConfig
	logger     *slog.Logger
}

// NewRelayService constructs the ingress RelayService.
func NewRelayService(store WebhookStore, publisher EventPublisher, logger *slog.Logger) RelayService {
	return NewRelayServiceWithConfig(store, publisher, nil, logger)
}

// NewRelayServiceWithConfig constructs the ingress RelayService with product prefix matching at ingestion time.
func NewRelayServiceWithConfig(store WebhookStore, publisher EventPublisher, gatewayCfg *GatewayConfig, logger *slog.Logger) RelayService {
	if logger == nil {
		logger = slog.Default()
	}
	return &relayServiceImpl{
		store:      store,
		publisher:  publisher,
		gatewayCfg: gatewayCfg,
		logger:     logger,
	}
}

func (s *relayServiceImpl) Ingest(ctx context.Context, normalized *NormalizedWebhook) (*WebhookEvent, error) {
	product := RoutingModeUnmatched
	if s.gatewayCfg != nil {
		if matchedProd, _, ok := s.gatewayCfg.MatchProductByReference(normalized.PaymentReference); ok {
			product = matchedProd
		}
	}

	event := &WebhookEvent{
		Provider:         normalized.Provider,
		ProviderEventID:  normalized.ProviderEventID,
		EventType:        normalized.EventType,
		PaymentReference: normalized.PaymentReference,
		Product:          product,
		IsDVA:            normalized.IsDVA,
		Headers:          normalized.RawHeaders,
		Payload:          normalized.RawPayload,
		Status:           WebhookStatusPending,
	}

	saved, duplicate, err := s.store.SaveEvent(ctx, event)
	if err != nil {
		return nil, fmt.Errorf("saving webhook event: %w", err)
	}

	// Idempotent fast return if duplicate webhook was already published or processed
	if duplicate && saved.Status != WebhookStatusPending {
		s.logger.Info("Duplicate webhook ignored (already processed)", "event_id", saved.ID, "provider", saved.Provider, "status", saved.Status)
		return saved, nil
	}

	// Synchronous Kafka publish on hot path; if it fails, leave status=PENDING for OutboxWorker
	if s.publisher != nil {
		pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		if pubErr := s.publisher.PublishWebhook(pubCtx, *saved); pubErr != nil {
			s.logger.Warn("Kafka publish failed on hot path; event retained in PENDING outbox", "event_id", saved.ID, "error", pubErr)
			return saved, nil
		}

		if updateErr := s.store.UpdateStatus(ctx, saved.ID, WebhookStatusPublished); updateErr != nil {
			s.logger.Warn("Failed to mark webhook as PUBLISHED after Kafka publish", "event_id", saved.ID, "error", updateErr)
		} else {
			saved.Status = WebhookStatusPublished
		}
	}

	return saved, nil
}

// ReceiverUseCase validates the PSP provider, verifies the webhook signature, and hands off to RelayService.
type ReceiverUseCase interface {
	Execute(ctx context.Context, provider string, headers http.Header, body []byte) (*WebhookEvent, error)
}

type receiverUseCaseImpl struct {
	providers map[PSPProvider]ProviderService
	relaySvc  RelayService
}

// NewReceiverUseCase creates a new ReceiverUseCase registered with the given provider services.
func NewReceiverUseCase(relaySvc RelayService, providerServices ...ProviderService) ReceiverUseCase {
	reg := make(map[PSPProvider]ProviderService, len(providerServices))
	for _, svc := range providerServices {
		if svc != nil {
			reg[svc.Provider()] = svc
		}
	}
	return &receiverUseCaseImpl{
		providers: reg,
		relaySvc:  relaySvc,
	}
}

func (u *receiverUseCaseImpl) Execute(ctx context.Context, provider string, headers http.Header, body []byte) (*WebhookEvent, error) {
	normalizedProvider := PSPProvider(strings.ToLower(strings.TrimSpace(provider)))
	if !IsValidProvider(string(normalizedProvider)) {
		return nil, ErrProviderNotFound
	}

	svc, ok := u.providers[normalizedProvider]
	if !ok {
		return nil, ErrUnsupportedProvider
	}

	normalized, err := svc.VerifyAndParse(ctx, headers, body)
	if err != nil {
		return nil, err
	}

	return u.relaySvc.Ingest(ctx, normalized)
}

// DispatcherUseCase resolves target product endpoints from a WebhookEvent and performs the non-blocking 1st HTTP delivery attempt.
type DispatcherUseCase interface {
	Execute(ctx context.Context, event WebhookEvent) error
}

type dispatcherUseCaseImpl struct {
	gatewayCfg       *GatewayConfig
	dispatcher       UpstreamDispatcher
	store            WebhookStore
	publisher        EventPublisher
	retryBaseBackoff time.Duration
	logger           *slog.Logger
}

// NewDispatcherUseCase constructs a DispatcherUseCase for consuming and fanning out webhooks.
func NewDispatcherUseCase(
	gatewayCfg *GatewayConfig,
	dispatcher UpstreamDispatcher,
	store WebhookStore,
	publisher EventPublisher,
	retryBaseBackoff time.Duration,
	logger *slog.Logger,
) DispatcherUseCase {
	if retryBaseBackoff <= 0 {
		retryBaseBackoff = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &dispatcherUseCaseImpl{
		gatewayCfg:       gatewayCfg,
		dispatcher:       dispatcher,
		store:            store,
		publisher:        publisher,
		retryBaseBackoff: retryBaseBackoff,
		logger:           logger,
	}
}

type targetProductRoute struct {
	ProductName string
	Config      ProductConfig
	RoutingMode string
}

func (u *dispatcherUseCaseImpl) Execute(ctx context.Context, event WebhookEvent) error {
	routes := u.resolveTargetProducts(event)
	if len(routes) == 0 {
		return u.handleUnmatchedWebhook(ctx, event)
	}

	primaryProduct := routes[0].ProductName
	if event.Product != primaryProduct {
		_ = u.store.UpdateEventProduct(ctx, event.ID, primaryProduct)
	}

	type endpointTask struct {
		productName string
		productCfg  ProductConfig
		endpoint    ProductEndpoint
		routingMode string
	}

	var tasks []endpointTask
	for _, r := range routes {
		for _, ep := range r.Config.Endpoints {
			tasks = append(tasks, endpointTask{
				productName: r.ProductName,
				productCfg:  r.Config,
				endpoint:    ep,
				routingMode: r.RoutingMode,
			})
		}
	}

	if len(tasks) == 0 {
		return u.handleUnmatchedWebhook(ctx, event)
	}

	var (
		wg            sync.WaitGroup
		mu            sync.Mutex
		failedInitial bool
	)

	for _, t := range tasks {
		wg.Add(1)
		go func(task endpointTask) {
			defer wg.Done()

			outHeaders := buildOutboundWebhookHeaders(event, task.productCfg, task.routingMode)
			statusCode, _, _, sendErr := u.dispatcher.Send(ctx, task.endpoint.URL, http.MethodPost, outHeaders, event.Payload)
			if sendErr == nil && statusCode >= 200 && statusCode < 300 {
				return
			}

			errMsg := fmt.Sprintf("upstream returned status %d", statusCode)
			if sendErr != nil {
				errMsg = sendErr.Error()
			}

			mu.Lock()
			failedInitial = true
			mu.Unlock()

			// Enqueue non-blocking retry for RetryWorker (up to 3 retries)
			failure := &DeliveryFailure{
				EventID:      event.ID,
				Provider:     event.Provider,
				Product:      task.productName,
				EndpointName: task.endpoint.Name,
				TargetURL:    task.endpoint.URL,
				RoutingMode:  task.routingMode,
				Headers:      flattenHeaders(outHeaders),
				Payload:      event.Payload,
				StatusCode:   statusCode,
				ErrorMessage: errMsg,
				RetryCount:   0,
				MaxRetries:   3,
				Status:       DeliveryStatusRetrying,
				NextRetryAt:  time.Now().UTC().Add(u.retryBaseBackoff),
			}

			if enqErr := u.store.EnqueueDeliveryFailure(ctx, failure); enqErr != nil {
				u.logger.Error("Failed to enqueue delivery retry", "event_id", event.ID, "product", task.productName, "endpoint", task.endpoint.Name, "error", enqErr)
			}
		}(t)
	}

	wg.Wait()

	if !failedInitial {
		if err := u.store.UpdateStatus(ctx, event.ID, WebhookStatusDelivered); err != nil {
			u.logger.Warn("Failed to update webhook status to DELIVERED", "event_id", event.ID, "error", err)
		}
	}

	return nil
}

func (u *dispatcherUseCaseImpl) resolveTargetProducts(event WebhookEvent) []targetProductRoute {
	if u.gatewayCfg == nil || len(u.gatewayCfg.Products) == 0 {
		return nil
	}

	// 1. Check for deterministic HMAC prefix token or explicit prefix match
	if productName, cfg, matched := u.gatewayCfg.MatchProductByReference(event.PaymentReference); matched {
		return []targetProductRoute{
			{
				ProductName: productName,
				Config:      cfg,
				RoutingMode: RoutingModeDirect,
			},
		}
	}

	// Unmatched events (including DVA / static bank transfers without a product prefix)
	// fail closed to handleUnmatchedWebhook -> DLQ rather than broadcasting to all products.
	return nil
}

func (u *dispatcherUseCaseImpl) handleUnmatchedWebhook(ctx context.Context, event WebhookEvent) error {
	errMsg := fmt.Sprintf("no product matched payment reference %q", event.PaymentReference)
	failure := &DeliveryFailure{
		EventID:      event.ID,
		Provider:     event.Provider,
		Product:      "unmatched",
		EndpointName: "unmatched",
		TargetURL:    "",
		RoutingMode:  RoutingModeUnmatched,
		Headers:      event.Headers,
		Payload:      event.Payload,
		StatusCode:   http.StatusUnprocessableEntity,
		ErrorMessage: errMsg,
		RetryCount:   0,
		MaxRetries:   0,
		Status:       DeliveryStatusFailed,
		NextRetryAt:  time.Now().UTC(),
	}

	_ = u.store.EnqueueDeliveryFailure(ctx, failure)
	_ = u.store.UpdateStatus(ctx, event.ID, WebhookStatusFailed)

	if u.publisher != nil {
		_ = u.publisher.PublishDLQ(ctx, DLQMessage{
			FailureID:    failure.ID,
			EventID:      event.ID,
			Provider:     event.Provider,
			Product:      "unmatched",
			EndpointName: "unmatched",
			RoutingMode:  RoutingModeUnmatched,
			Headers:      event.Headers,
			Payload:      event.Payload,
			StatusCode:   http.StatusUnprocessableEntity,
			ErrorMessage: errMsg,
			RetryCount:   0,
			FailedAt:     time.Now().UTC(),
		})
	}

	return nil
}

func buildOutboundWebhookHeaders(event WebhookEvent, product ProductConfig, routingMode string) http.Header {
	out := make(http.Header)
	for k, v := range event.Headers {
		out.Set(k, v)
	}

	// Strip hop-by-hop headers
	out.Del("Host")
	out.Del("Connection")
	out.Del("Keep-Alive")
	out.Del("Transfer-Encoding")
	out.Del("Content-Length")

	out.Set("Content-Type", "application/json")
	out.Set("X-PSP-Provider", string(event.Provider))
	out.Set("X-PSPRelay-Event-ID", event.ID)
	out.Set("X-PSPRelay-Routing-Mode", routingMode)

	if product.Secrets.SignatureKey != "" {
		sig := ComputeHMACSHA256Hex(event.Payload, product.Secrets.SignatureKey)
		out.Set("X-PSPRelay-Signature", sig)
	}

	return out
}
