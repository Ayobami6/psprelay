package tests

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"psprelay/internal"
)

type recordedDispatchCall struct {
	targetURL string
	headers   http.Header
	body      []byte
}

type stubUpstreamDispatcher struct {
	mu        sync.Mutex
	calls     []recordedDispatchCall
	responses map[string]int
	defaultSC int
	sendErr   error
}

func (d *stubUpstreamDispatcher) Send(_ context.Context, targetURL string, _ string, headers http.Header, body []byte) (int, http.Header, []byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.calls = append(d.calls, recordedDispatchCall{
		targetURL: targetURL,
		headers:   headers.Clone(),
		body:      append([]byte(nil), body...),
	})

	if d.sendErr != nil {
		return 0, nil, nil, d.sendErr
	}

	sc := d.defaultSC
	if sc == 0 {
		sc = http.StatusOK
	}
	if custom, ok := d.responses[targetURL]; ok {
		sc = custom
	}

	respHeaders := make(http.Header)
	respHeaders.Set("Content-Type", "application/json")
	return sc, respHeaders, []byte(`{"ok":true}`), nil
}

func sampleGatewayConfig() *internal.GatewayConfig {
	return &internal.GatewayConfig{
		Version: "v1",
		Products: map[string]internal.ProductConfig{
			"mailnow": {
				Prefix: "MN",
				Secrets: internal.ProductSecrets{
					Type:         "Opaque",
					EncodeKey:    "mn_encode_secret",
					SignatureKey: "mn_sig_secret",
				},
				Endpoints: []internal.ProductEndpoint{
					{Name: "mailnow", URL: "https://mailnow.com/api/webhook"},
					{Name: "mailnow-legacy", URL: "https://legacy.mailnow.com/api/webhook"},
				},
			},
			"tibicloud": {
				Prefix: "TC",
				Secrets: internal.ProductSecrets{
					Type:         "Opaque",
					EncodeKey:    "tc_encode_secret",
					SignatureKey: "tc_sig_secret",
				},
				Endpoints: []internal.ProductEndpoint{
					{Name: "tibicloud", URL: "https://tibicloud.com/api/webhook"},
				},
			},
		},
	}
}

func TestDispatcherUseCase_FanOutAndDVABroadcast(t *testing.T) {
	gatewayCfg := sampleGatewayConfig()

	t.Run("matches deterministic HMAC prefix token and fans out to all endpoints of owning product", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{}
		httpDisp := &stubUpstreamDispatcher{defaultSC: http.StatusOK}
		dispatcherUC := internal.NewDispatcherUseCase(gatewayCfg, httpDisp, store, pub, 10*time.Millisecond, silentLogger())

		mnToken := internal.ComputePrefixToken("MN", "mn_encode_secret")
		rawPayload := []byte(`{"event":"charge.success","data":{"reference":"` + mnToken + `_tx900"}}`)

		saved, _, err := store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Paystack,
			ProviderEventID:  "evt_900",
			EventType:        "charge.success",
			PaymentReference: mnToken + "_tx900",
			IsDVA:            false,
			Headers:          map[string]string{"X-Paystack-Signature": "orig_sig"},
			Payload:          rawPayload,
			Status:           internal.WebhookStatusPublished,
		})
		if err != nil {
			t.Fatalf("SaveEvent failed: %v", err)
		}

		if err := dispatcherUC.Execute(context.Background(), *saved); err != nil {
			t.Fatalf("DispatcherUseCase.Execute failed: %v", err)
		}

		// mailnow has 2 endpoints (mailnow and mailnow-legacy)
		if len(httpDisp.calls) != 2 {
			t.Fatalf("expected 2 fan-out HTTP calls for mailnow, got %d", len(httpDisp.calls))
		}

		expectedSig := internal.ComputeHMACSHA256Hex(rawPayload, "mn_sig_secret")
		for _, c := range httpDisp.calls {
			if c.headers.Get("X-PSPRelay-Signature") != expectedSig {
				t.Errorf("expected X-PSPRelay-Signature %q, got %q", expectedSig, c.headers.Get("X-PSPRelay-Signature"))
			}
			if c.headers.Get("X-PSPRelay-Routing-Mode") != internal.RoutingModeDirect {
				t.Errorf("expected routing mode direct, got %q", c.headers.Get("X-PSPRelay-Routing-Mode"))
			}
		}

		updated, _ := store.GetEvent(context.Background(), saved.ID)
		if updated.Status != internal.WebhookStatusDelivered {
			t.Fatalf("expected event status DELIVERED, got %q", updated.Status)
		}
	})

	t.Run("broadcasts unmatched DVA webhook to all products with broadcast-dva routing mode", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{}
		httpDisp := &stubUpstreamDispatcher{defaultSC: http.StatusOK}
		dispatcherUC := internal.NewDispatcherUseCase(gatewayCfg, httpDisp, store, pub, 10*time.Millisecond, silentLogger())

		saved, _, _ := store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Paystack,
			ProviderEventID:  "evt_dva_1",
			EventType:        "charge.success",
			PaymentReference: "NIP_RANDOM_REF_889900", // No product prefix
			IsDVA:            true,
			Payload:          []byte(`{"event":"charge.success","data":{"channel":"dedicated_nuban"}}`),
			Status:           internal.WebhookStatusPublished,
		})

		if err := dispatcherUC.Execute(context.Background(), *saved); err != nil {
			t.Fatalf("DispatcherUseCase.Execute failed: %v", err)
		}

		// mailnow (2 endpoints) + tibicloud (1 endpoint) = 3 calls
		if len(httpDisp.calls) != 3 {
			t.Fatalf("expected 3 broadcast HTTP calls across all products, got %d", len(httpDisp.calls))
		}

		for _, c := range httpDisp.calls {
			if c.headers.Get("X-PSPRelay-Routing-Mode") != internal.RoutingModeBroadcastDVA {
				t.Errorf("expected X-PSPRelay-Routing-Mode broadcast-dva, got %q", c.headers.Get("X-PSPRelay-Routing-Mode"))
			}
		}
	})
}

func TestDispatcherUseCase_NonBlockingRetryAndDLQ(t *testing.T) {
	gatewayCfg := sampleGatewayConfig()

	t.Run("enqueues failing endpoint on 1st attempt without blocking consumer, then exhausts 3x retries in RetryWorker and publishes to DLQ", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{}
		// tibicloud has 1 endpoint; configure it to return 500 Internal Server Error
		httpDisp := &stubUpstreamDispatcher{
			defaultSC: http.StatusInternalServerError,
		}

		dispatcherUC := internal.NewDispatcherUseCase(gatewayCfg, httpDisp, store, pub, 1*time.Millisecond, silentLogger())
		retryWorker := internal.NewRetryWorker(store, httpDisp, pub, 1*time.Millisecond, 1*time.Millisecond, silentLogger())

		tcToken := internal.ComputePrefixToken("TC", "tc_encode_secret")
		rawPayload := []byte(`{"event":"charge.success","data":{"reference":"` + tcToken + `_fail_1"}}`)

		saved, _, _ := store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Monnify,
			ProviderEventID:  "evt_fail_1",
			EventType:        "SUCCESSFUL_TRANSACTION",
			PaymentReference: tcToken + "_fail_1",
			Payload:          rawPayload,
			Status:           internal.WebhookStatusPublished,
		})

		// 1st attempt inside consumer
		if err := dispatcherUC.Execute(context.Background(), *saved); err != nil {
			t.Fatalf("DispatcherUseCase.Execute failed: %v", err)
		}

		if len(httpDisp.calls) != 1 {
			t.Fatalf("expected exactly 1 initial HTTP attempt in consumer, got %d", len(httpDisp.calls))
		}

		failures, _ := store.ListDeliveryFailures(context.Background(), saved.ID)
		if len(failures) != 1 || failures[0].Status != internal.DeliveryStatusRetrying || failures[0].RetryCount != 0 {
			t.Fatalf("expected 1 RETRYING record with RetryCount=0, got %+v", failures)
		}

		// Execute 3 async retries via RetryWorker
		futureTime := time.Now().UTC().Add(1 * time.Hour)
		for i := 1; i <= 3; i++ {
			if err := retryWorker.ProcessDueRetries(context.Background(), futureTime); err != nil {
				t.Fatalf("ProcessDueRetries iteration %d failed: %v", i, err)
			}
			futureTime = futureTime.Add(1 * time.Hour)
		}

		// Total HTTP calls = 1 initial + 3 retries = 4 calls
		if len(httpDisp.calls) != 4 {
			t.Fatalf("expected 4 total HTTP calls (1 initial + 3 retries), got %d", len(httpDisp.calls))
		}

		// Verify delivery_failures row is marked FAILED and retains original payload
		failures, _ = store.ListDeliveryFailures(context.Background(), saved.ID)
		if failures[0].Status != internal.DeliveryStatusFailed {
			t.Fatalf("expected delivery_failures status FAILED after 3 retries, got %q", failures[0].Status)
		}
		if string(failures[0].Payload) != string(rawPayload) {
			t.Fatalf("expected original payload saved in delivery_failures")
		}

		// Verify parent webhook_events status is updated to FAILED
		updatedEvent, _ := store.GetEvent(context.Background(), saved.ID)
		if updatedEvent.Status != internal.WebhookStatusFailed {
			t.Fatalf("expected webhook_events status FAILED, got %q", updatedEvent.Status)
		}

		// Verify DLQ message was published
		if len(pub.dlqMsgs) != 1 {
			t.Fatalf("expected 1 message published to Kafka DLQ, got %d", len(pub.dlqMsgs))
		}
		if pub.dlqMsgs[0].EventID != saved.ID || pub.dlqMsgs[0].RetryCount != 3 {
			t.Fatalf("unexpected DLQ message: %+v", pub.dlqMsgs[0])
		}
	})

	t.Run("recovers on retry #2 and transitions webhook_events to DELIVERED", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{}
		httpDisp := &stubUpstreamDispatcher{
			defaultSC: http.StatusBadGateway,
		}

		dispatcherUC := internal.NewDispatcherUseCase(gatewayCfg, httpDisp, store, pub, 1*time.Millisecond, silentLogger())
		retryWorker := internal.NewRetryWorker(store, httpDisp, pub, 1*time.Millisecond, 1*time.Millisecond, silentLogger())

		tcToken := internal.ComputePrefixToken("TC", "tc_encode_secret")
		saved, _, _ := store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Flutterwave,
			ProviderEventID:  "evt_recover_1",
			EventType:        "charge.completed",
			PaymentReference: tcToken + "_rec_1",
			Payload:          []byte(`{"event":"charge.completed"}`),
			Status:           internal.WebhookStatusPublished,
		})

		// 1st attempt fails (502)
		_ = dispatcherUC.Execute(context.Background(), *saved)

		// Retry #1 fails (502)
		futureTime := time.Now().UTC().Add(1 * time.Hour)
		_ = retryWorker.ProcessDueRetries(context.Background(), futureTime)

		// Upstream recovers (200 OK) before Retry #2
		httpDisp.mu.Lock()
		httpDisp.defaultSC = http.StatusOK
		httpDisp.mu.Unlock()

		futureTime = futureTime.Add(1 * time.Hour)
		_ = retryWorker.ProcessDueRetries(context.Background(), futureTime)

		failures, _ := store.ListDeliveryFailures(context.Background(), saved.ID)
		if failures[0].Status != internal.DeliveryStatusResolved {
			t.Fatalf("expected delivery failure status RESOLVED, got %q", failures[0].Status)
		}

		updatedEvent, _ := store.GetEvent(context.Background(), saved.ID)
		if updatedEvent.Status != internal.WebhookStatusDelivered {
			t.Fatalf("expected webhook_events status DELIVERED after retry recovery, got %q", updatedEvent.Status)
		}
		if len(pub.dlqMsgs) != 0 {
			t.Fatalf("expected 0 DLQ messages on successful retry recovery, got %d", len(pub.dlqMsgs))
		}
	})
}

type failingWebhookStore struct {
	internal.WebhookStore
	saveErr          error
	updateStatusErr  error
	enqueueFailErr   error
	claimOutboxErr   error
	claimRetriesErr  error
	hasDeliveriesErr error
}

func (f *failingWebhookStore) SaveEvent(ctx context.Context, event *internal.WebhookEvent) (*internal.WebhookEvent, bool, error) {
	if f.saveErr != nil {
		return nil, false, f.saveErr
	}
	return f.WebhookStore.SaveEvent(ctx, event)
}

func (f *failingWebhookStore) UpdateStatus(ctx context.Context, eventID string, status internal.WebhookStatus) error {
	if f.updateStatusErr != nil {
		return f.updateStatusErr
	}
	return f.WebhookStore.UpdateStatus(ctx, eventID, status)
}

func (f *failingWebhookStore) EnqueueDeliveryFailure(ctx context.Context, failure *internal.DeliveryFailure) error {
	if f.enqueueFailErr != nil {
		return f.enqueueFailErr
	}
	return f.WebhookStore.EnqueueDeliveryFailure(ctx, failure)
}

func (f *failingWebhookStore) ClaimPendingOutboxEvents(ctx context.Context, olderThan time.Duration, limit int, fn func(ctx context.Context, events []internal.WebhookEvent) ([]string, error)) error {
	if f.claimOutboxErr != nil {
		return f.claimOutboxErr
	}
	return f.WebhookStore.ClaimPendingOutboxEvents(ctx, olderThan, limit, fn)
}

func (f *failingWebhookStore) ClaimDueRetries(ctx context.Context, now time.Time, limit int, fn func(ctx context.Context, item internal.DeliveryFailure) internal.RetryAttemptResult) ([]internal.DeliveryFailure, error) {
	if f.claimRetriesErr != nil {
		return nil, f.claimRetriesErr
	}
	return f.WebhookStore.ClaimDueRetries(ctx, now, limit, fn)
}

func (f *failingWebhookStore) HasPendingOrFailedDeliveries(ctx context.Context, eventID string) (bool, bool, error) {
	if f.hasDeliveriesErr != nil {
		return false, false, f.hasDeliveriesErr
	}
	return f.WebhookStore.HasPendingOrFailedDeliveries(ctx, eventID)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type errReaderCloser struct{}

func (errReaderCloser) Read(_ []byte) (int, error) { return 0, errors.New("read body error") }
func (errReaderCloser) Close() error               { return nil }

func TestDefaultHTTPDispatcher_Send(t *testing.T) {
	t.Run("constructs default dispatcher and sends POST body and headers", func(t *testing.T) {
		// Verify default and custom timeout constructors
		if d := internal.NewDefaultHTTPDispatcher(0); d == nil {
			t.Fatal("expected non-nil default dispatcher")
		}
		if d := internal.NewDefaultHTTPDispatcher(2 * time.Second); d == nil {
			t.Fatal("expected non-nil custom dispatcher")
		}

		var gotMethod string
		var gotBody string
		var gotCustomHeader string

		disp := internal.NewHTTPDispatcherWithClient(&http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotMethod = r.Method
				gotCustomHeader = r.Header.Get("X-Test-Header")
				raw, _ := io.ReadAll(r.Body)
				gotBody = string(raw)
				return &http.Response{
					StatusCode: http.StatusAccepted,
					Header:     http.Header{"X-Resp-Header": []string{"ok"}},
					Body:       io.NopCloser(strings.NewReader(`{"accepted":true}`)),
				}, nil
			}),
		})

		h := make(http.Header)
		h.Add("X-Test-Header", "value-1")

		sc, respH, respBody, err := disp.Send(context.Background(), "https://example.test/hook", http.MethodPost, h, []byte(`{"ping":1}`))
		if err != nil {
			t.Fatalf("unexpected Send error: %v", err)
		}
		if sc != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", sc)
		}
		if gotMethod != http.MethodPost || gotBody != `{"ping":1}` || gotCustomHeader != "value-1" {
			t.Fatalf("unexpected request received: method=%s body=%s header=%s", gotMethod, gotBody, gotCustomHeader)
		}
		if respH.Get("X-Resp-Header") != "ok" || string(respBody) != `{"accepted":true}` {
			t.Fatalf("unexpected response: headers=%v body=%s", respH, string(respBody))
		}
	})

	t.Run("handles nil body, invalid method, transport error, and response read error", func(t *testing.T) {
		disp := internal.NewHTTPDispatcherWithClient(&http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			}),
		})

		sc, _, _, err := disp.Send(context.Background(), "https://example.test/hook", http.MethodGet, nil, nil)
		if err != nil || sc != http.StatusOK {
			t.Fatalf("expected 200 OK for GET with nil body, got sc=%d err=%v", sc, err)
		}

		// Invalid HTTP method triggers request creation error
		if _, _, _, err := disp.Send(context.Background(), "https://example.test/hook", "INVALID METHOD\n", nil, nil); err == nil {
			t.Fatal("expected error for invalid HTTP method, got nil")
		}

		// Transport failure
		dispErr := internal.NewHTTPDispatcherWithClient(&http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return nil, errors.New("connection refused")
			}),
		})
		if _, _, _, err := dispErr.Send(context.Background(), "https://example.test/hook", http.MethodPost, nil, []byte("x")); err == nil {
			t.Fatal("expected transport error, got nil")
		}

		// Response body read error
		dispReadErr := internal.NewHTTPDispatcherWithClient(&http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       errReaderCloser{},
				}, nil
			}),
		})
		if sc, _, _, err := dispReadErr.Send(context.Background(), "https://example.test/hook", http.MethodPost, nil, []byte("x")); err == nil || sc != http.StatusOK {
			t.Fatalf("expected body read error with status 200, got sc=%d err=%v", sc, err)
		}
	})
}

func TestRelayService_EdgeCases(t *testing.T) {
	t.Run("handles SaveEvent error, nil publisher, UpdateStatus error, and pending duplicate re-publish", func(t *testing.T) {
		base := internal.NewInMemoryWebhookStore()
		failStore := &failingWebhookStore{WebhookStore: base, saveErr: errors.New("db write error")}
		relay := internal.NewRelayService(failStore, nil, nil)

		norm := &internal.NormalizedWebhook{
			Provider:         internal.Paystack,
			ProviderEventID:  "evt_edge_1",
			EventType:        "charge.success",
			PaymentReference: "MN_edge_1",
			RawHeaders:       map[string]string{"X-Test": "1"},
			RawPayload:       []byte(`{}`),
		}

		if _, err := relay.Ingest(context.Background(), norm); err == nil {
			t.Fatal("expected SaveEvent error, got nil")
		}

		// nil publisher leaves status PENDING without error
		relayNoPub := internal.NewRelayService(base, nil, nil)
		ev, err := relayNoPub.Ingest(context.Background(), norm)
		if err != nil {
			t.Fatalf("unexpected error with nil publisher: %v", err)
		}
		if ev.Status != internal.WebhookStatusPending {
			t.Fatalf("expected PENDING status, got %q", ev.Status)
		}

		// Duplicate PENDING event with active publisher and failing UpdateStatus
		pub := &mockEventPublisher{}
		updFailStore := &failingWebhookStore{WebhookStore: base, updateStatusErr: errors.New("update failed")}
		relayUpdFail := internal.NewRelayService(updFailStore, pub, silentLogger())
		ev2, err := relayUpdFail.Ingest(context.Background(), norm)
		if err != nil {
			t.Fatalf("unexpected error when UpdateStatus fails: %v", err)
		}
		if len(pub.webhooks) != 1 {
			t.Fatalf("expected duplicate PENDING event to publish to Kafka, got %d", len(pub.webhooks))
		}
		if ev2.Status != internal.WebhookStatusPending {
			t.Fatalf("expected status to remain PENDING when UpdateStatus fails, got %q", ev2.Status)
		}
	})
}

func TestDispatcherUseCase_UnmatchedAndErrorBranches(t *testing.T) {
	t.Run("handles unmatched non-DVA webhook, nil gatewayCfg, and zero-endpoint product via DLQ", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		pub := &mockEventPublisher{}
		httpDisp := &stubUpstreamDispatcher{defaultSC: http.StatusOK}
		gatewayCfg := sampleGatewayConfig()

		// Non-DVA unmatched payment reference
		uc := internal.NewDispatcherUseCase(gatewayCfg, httpDisp, store, pub, 0, nil)
		saved, _, _ := store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Paystack,
			ProviderEventID:  "evt_unmatched_1",
			PaymentReference: "UNMATCHED_REF",
			IsDVA:            false,
			Headers:          map[string]string{"Host": "evil.local"},
			Payload:          []byte(`{"event":"charge.success"}`),
			Status:           internal.WebhookStatusPublished,
		})
		if err := uc.Execute(context.Background(), *saved); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ev, _ := store.GetEvent(context.Background(), saved.ID)
		if ev.Status != internal.WebhookStatusFailed {
			t.Fatalf("expected FAILED status for unmatched webhook, got %q", ev.Status)
		}
		if len(pub.dlqMsgs) != 1 || pub.dlqMsgs[0].RoutingMode != internal.RoutingModeUnmatched {
			t.Fatalf("expected 1 unmatched DLQ message, got %+v", pub.dlqMsgs)
		}

		// nil gatewayCfg also routes to unmatched
		ucNilCfg := internal.NewDispatcherUseCase(nil, httpDisp, store, pub, 0, silentLogger())
		saved2, _, _ := store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Paystack,
			ProviderEventID:  "evt_unmatched_2",
			PaymentReference: "MN_123",
			IsDVA:            true,
			Payload:          []byte(`{}`),
			Status:           internal.WebhookStatusPublished,
		})
		if err := ucNilCfg.Execute(context.Background(), *saved2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pub.dlqMsgs) != 2 {
			t.Fatalf("expected 2 DLQ messages, got %d", len(pub.dlqMsgs))
		}

		// Matched product with 0 endpoints routes to unmatched
		noEpCfg := &internal.GatewayConfig{
			Products: map[string]internal.ProductConfig{
				"emptyprod": {Prefix: "EP", Endpoints: nil},
			},
		}
		ucNoEp := internal.NewDispatcherUseCase(noEpCfg, httpDisp, store, pub, 0, silentLogger())
		saved3, _, _ := store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Paystack,
			ProviderEventID:  "evt_unmatched_3",
			PaymentReference: "EP_123",
			Payload:          []byte(`{}`),
			Status:           internal.WebhookStatusPublished,
		})
		if err := ucNoEp.Execute(context.Background(), *saved3); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pub.dlqMsgs) != 3 {
			t.Fatalf("expected 3 DLQ messages, got %d", len(pub.dlqMsgs))
		}
	})

	t.Run("strips hop-by-hop headers, omits signature when key empty, and logs transport/store errors", func(t *testing.T) {
		base := internal.NewInMemoryWebhookStore()
		failStore := &failingWebhookStore{
			WebhookStore:    base,
			enqueueFailErr:  errors.New("enqueue failed"),
			updateStatusErr: errors.New("update failed"),
		}
		cfg := &internal.GatewayConfig{
			Products: map[string]internal.ProductConfig{
				"nosig": {
					Prefix:    "NS",
					Endpoints: []internal.ProductEndpoint{{Name: "ep1", URL: "https://example.com/hook"}},
				},
			},
		}

		// Transport error + failing enqueue
		httpDispErr := &stubUpstreamDispatcher{sendErr: errors.New("dial tcp timeout")}
		ucErr := internal.NewDispatcherUseCase(cfg, httpDispErr, failStore, nil, time.Millisecond, silentLogger())
		saved, _, _ := base.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:         internal.Paystack,
			ProviderEventID:  "evt_disp_err",
			PaymentReference: "NS_1",
			Headers: map[string]string{
				"Host":              "orig.host",
				"Connection":        "keep-alive",
				"Keep-Alive":        "timeout=5",
				"Transfer-Encoding": "chunked",
				"Content-Length":    "99",
				"X-Custom":          "keep",
			},
			Payload: []byte(`{"ok":1}`),
			Status:  internal.WebhookStatusPublished,
		})
		if err := ucErr.Execute(context.Background(), *saved); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(httpDispErr.calls) != 1 {
			t.Fatalf("expected 1 call, got %d", len(httpDispErr.calls))
		}
		sentHeaders := httpDispErr.calls[0].headers
		for _, stripped := range []string{"Host", "Connection", "Keep-Alive", "Transfer-Encoding", "Content-Length", "X-PSPRelay-Signature"} {
			if sentHeaders.Get(stripped) != "" {
				t.Errorf("expected header %s to be stripped/omitted, got %q", stripped, sentHeaders.Get(stripped))
			}
		}
		if sentHeaders.Get("X-Custom") != "keep" {
			t.Errorf("expected X-Custom header to be preserved")
		}

		// Success delivery + UpdateStatus error
		httpDispOK := &stubUpstreamDispatcher{defaultSC: http.StatusOK}
		ucOK := internal.NewDispatcherUseCase(cfg, httpDispOK, failStore, nil, time.Millisecond, silentLogger())
		if err := ucOK.Execute(context.Background(), *saved); err != nil {
			t.Fatalf("unexpected error when UpdateStatus fails: %v", err)
		}
	})
}
