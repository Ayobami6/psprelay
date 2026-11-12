package tests

import (
	"context"
	"net/http"
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
}

func (d *stubUpstreamDispatcher) Send(_ context.Context, targetURL string, _ string, headers http.Header, body []byte) (int, http.Header, []byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.calls = append(d.calls, recordedDispatchCall{
		targetURL: targetURL,
		headers:   headers.Clone(),
		body:      append([]byte(nil), body...),
	})

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
