package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

// DLQMessage represents a permanently failed webhook delivery published to the Kafka Dead-Letter Queue.
type DLQMessage struct {
	FailureID    string            `json:"failure_id"`
	EventID      string            `json:"event_id"`
	Provider     PSPProvider       `json:"provider"`
	Product      string            `json:"product"`
	EndpointName string            `json:"endpoint_name"`
	TargetURL    string            `json:"target_url"`
	RoutingMode  string            `json:"routing_mode"`
	Headers      map[string]string `json:"headers"`
	Payload      []byte            `json:"payload"`
	StatusCode   int               `json:"status_code"`
	ErrorMessage string            `json:"error_message"`
	RetryCount   int               `json:"retry_count"`
	FailedAt     time.Time         `json:"failed_at"`
}

// EventPublisher defines the secondary port for publishing webhook events and DLQ messages to Kafka.
type EventPublisher interface {
	PublishWebhook(ctx context.Context, event WebhookEvent) error
	PublishDLQ(ctx context.Context, msg DLQMessage) error
	Close() error
}

// KafkaMessageWriter abstracts kafka.Writer for deterministic unit testing.
type KafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaMessageReader abstracts kafka.Reader for deterministic unit testing.
type KafkaMessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaEventPublisher implements EventPublisher using segmentio/kafka-go writers.
type KafkaEventPublisher struct {
	webhookWriter KafkaMessageWriter
	dlqWriter     KafkaMessageWriter
}

// NewKafkaEventPublisher initializes Kafka writers for the main webhook topic and DLQ topic.
func NewKafkaEventPublisher(brokers []string, webhookTopic, dlqTopic string) *KafkaEventPublisher {
	webhookWriter := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  webhookTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		WriteTimeout:           3 * time.Second,
		ReadTimeout:            3 * time.Second,
		AllowAutoTopicCreation: true,
	}

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  dlqTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		WriteTimeout:           3 * time.Second,
		ReadTimeout:            3 * time.Second,
		AllowAutoTopicCreation: true,
	}

	return NewKafkaEventPublisherWithWriters(webhookWriter, dlqWriter)
}

// NewKafkaEventPublisherWithWriters constructs a KafkaEventPublisher with injected writers.
func NewKafkaEventPublisherWithWriters(webhookWriter, dlqWriter KafkaMessageWriter) *KafkaEventPublisher {
	return &KafkaEventPublisher{
		webhookWriter: webhookWriter,
		dlqWriter:     dlqWriter,
	}
}

func (p *KafkaEventPublisher) PublishWebhook(ctx context.Context, event WebhookEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling webhook event: %w", err)
	}

	key := event.PaymentReference
	if key == "" {
		key = event.ID
	}

	return p.webhookWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

func (p *KafkaEventPublisher) PublishDLQ(ctx context.Context, msg DLQMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling dlq message: %w", err)
	}

	return p.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.EventID),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

func (p *KafkaEventPublisher) Close() error {
	var firstErr error
	if p.webhookWriter != nil {
		if err := p.webhookWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if p.dlqWriter != nil {
		if err := p.dlqWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// KafkaConsumerRunner consumes webhook events from Kafka and delegates them to DispatcherUseCase.
type KafkaConsumerRunner struct {
	reader     KafkaMessageReader
	dispatcher DispatcherUseCase
	logger     *slog.Logger
}

// NewKafkaConsumerRunner creates a Kafka consumer group runner for the webhook topic.
func NewKafkaConsumerRunner(brokers []string, topic, groupID string, dispatcher DispatcherUseCase, logger *slog.Logger) *KafkaConsumerRunner {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Explicit synchronous commits after 1st attempt / retry handoff
		MaxWait:        1 * time.Second,
	})

	return NewKafkaConsumerRunnerWithReader(reader, dispatcher, logger)
}

// NewKafkaConsumerRunnerWithReader constructs a KafkaConsumerRunner with an injected reader.
func NewKafkaConsumerRunnerWithReader(reader KafkaMessageReader, dispatcher DispatcherUseCase, logger *slog.Logger) *KafkaConsumerRunner {
	if logger == nil {
		logger = slog.Default()
	}
	return &KafkaConsumerRunner{
		reader:     reader,
		dispatcher: dispatcher,
		logger:     logger,
	}
}

// Start runs the consumer loop until ctx is canceled.
func (c *KafkaConsumerRunner) Start(ctx context.Context) {
	c.logger.Info("Starting Kafka webhook consumer")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				c.logger.Info("Kafka consumer stopping on context cancellation")
				return
			}
			c.logger.Warn("Kafka fetch message error", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				continue
			}
		}

		var event WebhookEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Error("Failed to unmarshal Kafka webhook message; committing offset to skip poison message", "error", err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := c.dispatcher.Execute(ctx, event); err != nil {
			c.logger.Error("Dispatcher execution error", "event_id", event.ID, "error", err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			c.logger.Warn("Failed to commit Kafka offset", "event_id", event.ID, "error", err)
		}
	}
}

// Close closes the underlying Kafka reader.
func (c *KafkaConsumerRunner) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

// OutboxWorker sweeps PENDING webhook events from the database that failed initial synchronous Kafka publishing.
type OutboxWorker struct {
	store        WebhookStore
	publisher    EventPublisher
	pollInterval time.Duration
	minAge       time.Duration
	batchSize    int
	logger       *slog.Logger
}

// NewOutboxWorker creates a background worker to publish pending webhook events.
func NewOutboxWorker(
	store WebhookStore,
	publisher EventPublisher,
	pollInterval time.Duration,
	minAge time.Duration,
	logger *slog.Logger,
) *OutboxWorker {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	if minAge < 0 {
		minAge = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OutboxWorker{
		store:        store,
		publisher:    publisher,
		pollInterval: pollInterval,
		minAge:       minAge,
		batchSize:    50,
		logger:       logger,
	}
}

// Start runs the periodic outbox sweep until ctx is canceled.
func (w *OutboxWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	w.logger.Info("Starting OutboxWorker", "poll_interval", w.pollInterval, "min_age", w.minAge)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("OutboxWorker stopping")
			return
		case <-ticker.C:
			if err := w.ProcessBatch(ctx); err != nil && ctx.Err() == nil {
				w.logger.Warn("OutboxWorker batch error", "error", err)
			}
		}
	}
}

// ProcessBatch claims a batch of PENDING webhook events and publishes them to Kafka.
func (w *OutboxWorker) ProcessBatch(ctx context.Context) error {
	return w.store.ClaimPendingOutboxEvents(ctx, w.minAge, w.batchSize, func(ctx context.Context, events []WebhookEvent) ([]string, error) {
		published := make([]string, 0, len(events))
		var lastErr error
		for _, ev := range events {
			if err := w.publisher.PublishWebhook(ctx, ev); err != nil {
				lastErr = err
				w.logger.Warn("OutboxWorker failed to publish event", "event_id", ev.ID, "error", err)
				continue
			}
			published = append(published, ev.ID)
		}
		return published, lastErr
	})
}

// RetryWorker processes due endpoint retry rows outside the Kafka consumer and escalates exhausted deliveries to DLQ.
type RetryWorker struct {
	store        WebhookStore
	dispatcher   UpstreamDispatcher
	publisher    EventPublisher
	pollInterval time.Duration
	baseBackoff  time.Duration
	batchSize    int
	logger       *slog.Logger
}

// NewRetryWorker creates a background worker for non-blocking HTTP delivery retries.
func NewRetryWorker(
	store WebhookStore,
	dispatcher UpstreamDispatcher,
	publisher EventPublisher,
	pollInterval time.Duration,
	baseBackoff time.Duration,
	logger *slog.Logger,
) *RetryWorker {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	if baseBackoff <= 0 {
		baseBackoff = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RetryWorker{
		store:        store,
		dispatcher:   dispatcher,
		publisher:    publisher,
		pollInterval: pollInterval,
		baseBackoff:  baseBackoff,
		batchSize:    50,
		logger:       logger,
	}
}

// Start runs the periodic retry sweep until ctx is canceled.
func (w *RetryWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	// ticker cleanup afer context cancellation to avoid goroutine leak
	stopCleanup := context.AfterFunc(ctx, func() {
		ticker.Stop()
	})
	defer stopCleanup()

	w.logger.Info("Starting RetryWorker", "poll_interval", w.pollInterval, "base_backoff", w.baseBackoff)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("RetryWorker stopping")
			return
		case <-ticker.C:
			if err := w.ProcessDueRetries(ctx, time.Now().UTC()); err != nil && ctx.Err() == nil {
				w.logger.Warn("RetryWorker batch error", "error", err)
			}
		}
	}
}

// ProcessDueRetries claims due RETRYING rows, executes the HTTP attempt, updates status, and sends exhausted items to DLQ.
func (w *RetryWorker) ProcessDueRetries(ctx context.Context, now time.Time) error {
	touchedEvents := make(map[string]struct{})

	exhausted, err := w.store.ClaimDueRetries(ctx, now, w.batchSize, func(ctx context.Context, item DeliveryFailure) RetryAttemptResult {
		touchedEvents[item.EventID] = struct{}{}

		reqHeaders := make(http.Header, len(item.Headers))
		for k, v := range item.Headers {
			reqHeaders.Set(k, v)
		}
		reqHeaders.Set("X-PSPRelay-Retry-Attempt", fmt.Sprintf("%d", item.RetryCount+1))

		statusCode, _, _, sendErr := w.dispatcher.Send(ctx, item.TargetURL, http.MethodPost, reqHeaders, item.Payload)
		if sendErr == nil && statusCode >= 200 && statusCode < 300 {
			return RetryAttemptResult{
				Resolved:   true,
				StatusCode: statusCode,
			}
		}

		errMsg := fmt.Sprintf("upstream returned status %d", statusCode)
		if sendErr != nil {
			errMsg = sendErr.Error()
		}

		nextAttempt := item.RetryCount + 1
		backoff := computeBackoffWithJitter(w.baseBackoff, nextAttempt)

		return RetryAttemptResult{
			Resolved:     false,
			StatusCode:   statusCode,
			ErrorMessage: errMsg,
			NextRetryAt:  now.Add(backoff),
		}
	})
	if err != nil {
		return err
	}

	// Publish exhausted retries (after 3 retries) to DLQ
	for _, item := range exhausted {
		dlqMsg := DLQMessage{
			FailureID:    item.ID,
			EventID:      item.EventID,
			Provider:     item.Provider,
			Product:      item.Product,
			EndpointName: item.EndpointName,
			TargetURL:    item.TargetURL,
			RoutingMode:  item.RoutingMode,
			Headers:      item.Headers,
			Payload:      item.Payload,
			StatusCode:   item.StatusCode,
			ErrorMessage: item.ErrorMessage,
			RetryCount:   item.RetryCount,
			FailedAt:     time.Now().UTC(),
		}
		if pubErr := w.publisher.PublishDLQ(ctx, dlqMsg); pubErr != nil {
			w.logger.Error("Failed to publish exhausted delivery to DLQ", "failure_id", item.ID, "event_id", item.EventID, "error", pubErr)
		}
	}

	// Reconcile parent webhook_events status for all touched events
	for eventID := range touchedEvents {
		hasRetrying, hasFailed, checkErr := w.store.HasPendingOrFailedDeliveries(ctx, eventID)
		if checkErr != nil {
			w.logger.Warn("Failed checking delivery statuses for event", "event_id", eventID, "error", checkErr)
			continue
		}
		if hasRetrying {
			continue
		}
		if hasFailed {
			_ = w.store.UpdateStatus(ctx, eventID, WebhookStatusFailed)
		} else {
			_ = w.store.UpdateStatus(ctx, eventID, WebhookStatusDelivered)
		}
	}

	return nil
}

func computeBackoffWithJitter(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		attempt = 6
	}
	multiplier := 1 << (attempt - 1)
	maxDelay := base * time.Duration(multiplier)
	if maxDelay <= 0 {
		return base
	}
	// Full jitter between 50% and 100% of maxDelay
	half := maxDelay / 2
	jitter := time.Duration(rand.Int64N(int64(half) + 1))
	return half + jitter
}
