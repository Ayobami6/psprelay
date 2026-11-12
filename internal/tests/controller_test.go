package tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"psprelay/internal"

	"github.com/gin-gonic/gin"
)

type mockService struct {
	healthErr error
}

func (m *mockService) CheckHealth(_ context.Context) error {
	return m.healthErr
}

type mockEventPublisher struct {
	mu        sync.Mutex
	webhooks  []internal.WebhookEvent
	dlqMsgs   []internal.DLQMessage
	pubErr    error
	dlqPubErr error
}

func (p *mockEventPublisher) PublishWebhook(_ context.Context, event internal.WebhookEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pubErr != nil {
		return p.pubErr
	}
	p.webhooks = append(p.webhooks, event)
	return nil
}

func (p *mockEventPublisher) PublishDLQ(_ context.Context, msg internal.DLQMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dlqPubErr != nil {
		return p.dlqPubErr
	}
	p.dlqMsgs = append(p.dlqMsgs, msg)
	return nil
}

func (p *mockEventPublisher) Close() error {
	return nil
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func init() {
	gin.SetMode(gin.TestMode)
}

func TestController_Health(t *testing.T) {
	t.Run("returns 200 healthy when service reports no errors", func(t *testing.T) {
		svc := &mockService{healthErr: nil}
		healthUseCase := internal.NewHealthCheckUseCase(svc)
		ctrl := internal.NewHealthController(healthUseCase)

		router := gin.New()
		ctrl.RegisterRoutes(&router.RouterGroup)

		req, err := http.NewRequest(http.MethodGet, "/healthz", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status code %d, got %d", http.StatusOK, w.Code)
		}
	})

	t.Run("returns 503 degraded when service reports an error", func(t *testing.T) {
		svc := &mockService{healthErr: errors.New("database connection refused")}
		healthUseCase := internal.NewHealthCheckUseCase(svc)
		ctrl := internal.NewHealthController(healthUseCase)

		router := gin.New()
		ctrl.RegisterRoutes(&router.RouterGroup)

		req, err := http.NewRequest(http.MethodGet, "/healthz", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected status code %d, got %d", http.StatusServiceUnavailable, w.Code)
		}
	})
}

func TestPSPWebhookController(t *testing.T) {
	paystackSecret := "sk_test_webhook_123"
	paystackSvc := internal.NewPaystackService(paystackSecret)

	t.Run("accepts valid Paystack webhook, persists to store, publishes to Kafka, and marks PUBLISHED", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{}
		relaySvc := internal.NewRelayService(store, pub, silentLogger())
		receiverUC := internal.NewReceiverUseCase(relaySvc, paystackSvc)
		ctrl := internal.NewPSPWebhookController(receiverUC)

		router := gin.New()
		router.Use(internal.ErrorHandlerMiddleware())
		v1 := router.Group("/v1")
		ctrl.RegisterRoutes(v1)

		payload := []byte(`{"event":"charge.success","data":{"id":101,"reference":"MN_tx_101"}}`)
		sig := internal.ComputeHMACSHA512Hex(payload, paystackSecret)

		req := httptest.NewRequest(http.MethodPost, "/v1/webhook/paystack", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Paystack-Signature", sig)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}

		if len(pub.webhooks) != 1 {
			t.Fatalf("expected 1 published Kafka event, got %d", len(pub.webhooks))
		}

		saved, err := store.GetEvent(context.Background(), pub.webhooks[0].ID)
		if err != nil {
			t.Fatalf("failed to load saved event: %v", err)
		}
		if saved.Status != internal.WebhookStatusPublished {
			t.Fatalf("expected saved status PUBLISHED, got %q", saved.Status)
		}

		// Send duplicate webhook -> should return 200 OK without re-publishing
		reqDup := httptest.NewRequest(http.MethodPost, "/v1/webhook/paystack", bytes.NewReader(payload))
		reqDup.Header.Set("X-Paystack-Signature", sig)
		wDup := httptest.NewRecorder()
		router.ServeHTTP(wDup, reqDup)

		if wDup.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on duplicate webhook, got %d", wDup.Code)
		}
		if len(pub.webhooks) != 1 {
			t.Fatalf("expected Kafka publish count to remain 1 on duplicate, got %d", len(pub.webhooks))
		}
	})

	t.Run("retains PENDING status when Kafka publish fails and OutboxWorker recovers it", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{pubErr: errors.New("kafka broker unreachable")}
		relaySvc := internal.NewRelayService(store, pub, silentLogger())
		receiverUC := internal.NewReceiverUseCase(relaySvc, paystackSvc)
		ctrl := internal.NewPSPWebhookController(receiverUC)

		router := gin.New()
		router.Use(internal.ErrorHandlerMiddleware())
		v1 := router.Group("/v1")
		ctrl.RegisterRoutes(v1)

		payload := []byte(`{"event":"charge.success","data":{"id":202,"reference":"MN_tx_202"}}`)
		sig := internal.ComputeHMACSHA512Hex(payload, paystackSecret)

		req := httptest.NewRequest(http.MethodPost, "/v1/webhook/paystack", bytes.NewReader(payload))
		req.Header.Set("X-Paystack-Signature", sig)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK even when Kafka fails (saved to outbox), got %d", w.Code)
		}
		if len(pub.webhooks) != 0 {
			t.Fatalf("expected 0 published webhooks while Kafka is down, got %d", len(pub.webhooks))
		}

		// Recover Kafka and run OutboxWorker with minAge=0
		pub.pubErr = nil
		outboxWorker := internal.NewOutboxWorker(store, pub, 0, 0, silentLogger())
		if err := outboxWorker.ProcessBatch(context.Background()); err != nil {
			t.Fatalf("unexpected error in OutboxWorker.ProcessBatch: %v", err)
		}

		if len(pub.webhooks) != 1 {
			t.Fatalf("expected OutboxWorker to publish 1 pending event, got %d", len(pub.webhooks))
		}

		saved, err := store.GetEvent(context.Background(), pub.webhooks[0].ID)
		if err != nil {
			t.Fatalf("failed to load event: %v", err)
		}
		if saved.Status != internal.WebhookStatusPublished {
			t.Fatalf("expected status PUBLISHED after OutboxWorker sweep, got %q", saved.Status)
		}
	})

	t.Run("returns 401 ProblemDetails on invalid signature and 404 on unknown provider", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{}
		relaySvc := internal.NewRelayService(store, pub, silentLogger())
		receiverUC := internal.NewReceiverUseCase(relaySvc, paystackSvc)
		ctrl := internal.NewPSPWebhookController(receiverUC)

		router := gin.New()
		router.Use(internal.ErrorHandlerMiddleware())
		v1 := router.Group("/v1")
		ctrl.RegisterRoutes(v1)

		// Invalid signature
		reqInvalid := httptest.NewRequest(http.MethodPost, "/v1/webhook/paystack", bytes.NewReader([]byte(`{}`)))
		reqInvalid.Header.Set("X-Paystack-Signature", "wrong_sig")
		wInvalid := httptest.NewRecorder()
		router.ServeHTTP(wInvalid, reqInvalid)
		if wInvalid.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized, got %d", wInvalid.Code)
		}

		// Unknown provider
		reqUnknown := httptest.NewRequest(http.MethodPost, "/v1/webhook/stripe", bytes.NewReader([]byte(`{}`)))
		wUnknown := httptest.NewRecorder()
		router.ServeHTTP(wUnknown, reqUnknown)
		if wUnknown.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got %d", wUnknown.Code)
		}
	})
}

func TestLoadConfig(t *testing.T) {
	t.Run("loads defaults cleanly without error", func(t *testing.T) {
		cfg, err := internal.LoadConfig()
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected non-nil config")
		}
		if cfg.Addr == "" {
			t.Fatal("expected non-empty Addr")
		}
	})
}

func TestLoadConfigYaml(t *testing.T) {
	t.Run("successfully parses valid YAML schema and matches encrypted prefix token", func(t *testing.T) {
		yamlContent := `
version: v1
products:
  mailnow:
    prefix: MN
    secrets:
      type: Opaque
      encodeKey: key123
      signatureKey: sig123
    endpoints:
      - name: mailnow
        url: https://mailnow.com/api
`
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "config.yaml")
		if err := os.WriteFile(filePath, []byte(yamlContent), 0600); err != nil {
			t.Fatalf("failed to write temp file: %v", err)
		}

		cfg, err := internal.LoadConfigYaml(context.Background(), filePath)
		if err != nil {
			t.Fatalf("unexpected error parsing yaml: %v", err)
		}

		token := internal.ComputePrefixToken("MN", "key123")
		prodName, prodCfg, matched := cfg.MatchProductByReference(token + "_order99")
		if !matched || prodName != "mailnow" || prodCfg.Prefix != "MN" {
			t.Fatalf("expected MatchProductByReference to match mailnow for token %q, got matched=%v name=%q", token, matched, prodName)
		}
	})

	t.Run("returns descriptive error on missing file", func(t *testing.T) {
		_, err := internal.LoadConfigYaml(context.Background(), "/nonexistent/config.yaml")
		if err == nil {
			t.Fatal("expected error for non-existent file, got nil")
		}
	})
}
