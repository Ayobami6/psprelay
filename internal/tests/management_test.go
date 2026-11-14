package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"psprelay/internal"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestAdminAuthMiddleware(t *testing.T) {
	t.Run("allows requests through in development when username and password are empty", func(t *testing.T) {
		router := gin.New()
		router.Use(internal.AdminAuthMiddleware("", "", internal.EnvironmentDevelopment))
		router.GET("/admin", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/admin", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK when auth is disabled in dev, got %d", w.Code)
		}
	})

	t.Run("blocks unauthenticated admin access in production when credentials are unset", func(t *testing.T) {
		router := gin.New()
		router.Use(internal.AdminAuthMiddleware("", "", internal.EnvironmentProduction))
		router.GET("/admin", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/admin", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized in production without credentials, got %d", w.Code)
		}
	})

	t.Run("rejects cross-site mutating requests", func(t *testing.T) {
		router := gin.New()
		router.Use(internal.AdminAuthMiddleware("admin", "secret123", internal.EnvironmentDevelopment))
		router.POST("/admin/failures/bulk-retry", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodPost, "/admin/failures/bulk-retry", nil)
		req.Header.Set("Sec-Fetch-Site", "cross-site")
		req.SetBasicAuth("admin", "secret123")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden for cross-site POST, got %d", w.Code)
		}
	})

	t.Run("enforces Basic Auth when configured", func(t *testing.T) {
		router := gin.New()
		router.Use(internal.AdminAuthMiddleware("admin", "secret123", internal.EnvironmentDevelopment))
		router.GET("/admin", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		// Missing credentials -> 401
		req1 := httptest.NewRequest(http.MethodGet, "/admin", nil)
		w1 := httptest.NewRecorder()
		router.ServeHTTP(w1, req1)
		if w1.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for missing credentials, got %d", w1.Code)
		}

		// Wrong credentials -> 401
		req2 := httptest.NewRequest(http.MethodGet, "/admin", nil)
		req2.SetBasicAuth("admin", "wrong")
		w2 := httptest.NewRecorder()
		router.ServeHTTP(w2, req2)
		if w2.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for wrong credentials, got %d", w2.Code)
		}

		// Valid credentials -> 200
		req3 := httptest.NewRequest(http.MethodGet, "/admin", nil)
		req3.SetBasicAuth("admin", "secret123")
		w3 := httptest.NewRecorder()
		router.ServeHTTP(w3, req3)
		if w3.Code != http.StatusOK {
			t.Fatalf("expected 200 for valid credentials, got %d", w3.Code)
		}
	})
}

func TestInMemoryStore_ManagementMethods(t *testing.T) {
	ctx := context.Background()
	store := internal.NewInMemoryWebhookStore()
	t0 := time.Now().UTC().Add(-5 * time.Minute)

	ev1, _, err := store.SaveEvent(ctx, &internal.WebhookEvent{
		ID:               "ev_mn_1",
		Provider:         internal.Paystack,
		ProviderEventID:  "ps_1",
		EventType:        "charge.success",
		PaymentReference: "MN_ref_100",
		Product:          "mailnow",
		Status:           internal.WebhookStatusDelivered,
		Payload:          []byte(`{"amount":5000}`),
		CreatedAt:        t0,
	})
	if err != nil {
		t.Fatalf("SaveEvent ev1 failed: %v", err)
	}

	ev2, _, err := store.SaveEvent(ctx, &internal.WebhookEvent{
		ID:               "ev_tc_1",
		Provider:         internal.Flutterwave,
		ProviderEventID:  "fw_1",
		EventType:        "charge.completed",
		PaymentReference: "TC_ref_200",
		Product:          "",
		Status:           internal.WebhookStatusFailed,
		Payload:          []byte(`not-json`),
		CreatedAt:        t0.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("SaveEvent ev2 failed: %v", err)
	}

	// UpdateEventProduct
	if err := store.UpdateEventProduct(ctx, ev2.ID, "tibicloud"); err != nil {
		t.Fatalf("UpdateEventProduct failed: %v", err)
	}
	if err := store.UpdateEventProduct(ctx, ev2.ID, ""); err != nil {
		t.Fatalf("UpdateEventProduct empty fallback failed: %v", err)
	}
	if err := store.UpdateEventProduct(ctx, "missing", "mailnow"); !errors.Is(err, internal.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
	_ = store.UpdateEventProduct(ctx, ev2.ID, "tibicloud")

	// ListEvents filtering & pagination
	events, total, err := store.ListEvents(ctx, internal.EventFilter{
		Product:  "mailnow",
		Provider: internal.Paystack,
		Status:   internal.WebhookStatusDelivered,
		Search:   "MN_ref",
		Limit:    10,
		Offset:   0,
	})
	if err != nil || total != 1 || len(events) != 1 || events[0].ID != ev1.ID {
		t.Fatalf("unexpected ListEvents result: total=%d len=%d err=%v", total, len(events), err)
	}

	// Out-of-range offset
	emptyPage, totalAll, err := store.ListEvents(ctx, internal.EventFilter{Limit: 0, Offset: 50})
	if err != nil || totalAll != 2 || len(emptyPage) != 0 {
		t.Fatalf("unexpected offset clamping result: total=%d len=%d err=%v", totalAll, len(emptyPage), err)
	}

	// Enqueue failures
	f1 := &internal.DeliveryFailure{
		ID:           "fail_1",
		EventID:      ev2.ID,
		Provider:     internal.Flutterwave,
		Product:      "tibicloud",
		EndpointName: "tibicloud",
		TargetURL:    "https://tibicloud.com/api/webhook",
		RoutingMode:  internal.RoutingModeDirect,
		StatusCode:   502,
		ErrorMessage: "bad gateway",
		RetryCount:   3,
		MaxRetries:   3,
		Status:       internal.DeliveryStatusFailed,
	}
	f2 := &internal.DeliveryFailure{
		ID:           "fail_unmatched",
		EventID:      ev2.ID,
		Provider:     internal.Flutterwave,
		Product:      "unmatched",
		EndpointName: "unmatched",
		TargetURL:    "",
		RoutingMode:  internal.RoutingModeUnmatched,
		StatusCode:   422,
		ErrorMessage: "unmatched",
		RetryCount:   0,
		MaxRetries:   0,
		Status:       internal.DeliveryStatusFailed,
	}
	_ = store.EnqueueDeliveryFailure(ctx, f1)
	_ = store.EnqueueDeliveryFailure(ctx, f2)

	// GetDeliveryFailure & UpdateDeliveryFailureAttempt
	loadedF1, err := store.GetDeliveryFailure(ctx, "fail_1")
	if err != nil || loadedF1.ID != "fail_1" {
		t.Fatalf("GetDeliveryFailure failed: %v", err)
	}
	if _, err := store.GetDeliveryFailure(ctx, "missing"); !errors.Is(err, internal.ErrFailureNotFound) {
		t.Fatalf("expected ErrFailureNotFound, got %v", err)
	}

	loadedF1.StatusCode = 503
	if err := store.UpdateDeliveryFailureAttempt(ctx, loadedF1); err != nil {
		t.Fatalf("UpdateDeliveryFailureAttempt failed: %v", err)
	}
	if err := store.UpdateDeliveryFailureAttempt(ctx, &internal.DeliveryFailure{ID: "missing"}); !errors.Is(err, internal.ErrFailureNotFound) {
		t.Fatalf("expected ErrFailureNotFound, got %v", err)
	}

	// ListAllDeliveryFailures
	fList, fTotal, err := store.ListAllDeliveryFailures(ctx, internal.FailureFilter{
		Product:  "tibicloud",
		Provider: internal.Flutterwave,
		Status:   internal.DeliveryStatusFailed,
		EventID:  ev2.ID,
		Limit:    10,
	})
	if err != nil || fTotal != 1 || len(fList) != 1 {
		t.Fatalf("unexpected ListAllDeliveryFailures: total=%d len=%d err=%v", fTotal, len(fList), err)
	}
	fEmpty, _, _ := store.ListAllDeliveryFailures(ctx, internal.FailureFilter{Offset: 99})
	if len(fEmpty) != 0 {
		t.Fatalf("expected empty slice for high offset, got %d", len(fEmpty))
	}

	// GetProductMetrics
	metrics, err := store.GetProductMetrics(ctx)
	if err != nil {
		t.Fatalf("GetProductMetrics failed: %v", err)
	}
	if metrics["mailnow"].DeliveredEvents != 1 || metrics["tibicloud"].DeadLetterCount != 1 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}

	// ResetFailuresForRetry (skips unmatched with empty TargetURL)
	resetCount, err := store.ResetFailuresForRetry(ctx, "tibicloud", internal.Flutterwave)
	if err != nil || resetCount != 1 {
		t.Fatalf("expected 1 failure reset, got count=%d err=%v", resetCount, err)
	}
	afterReset, _ := store.GetDeliveryFailure(ctx, "fail_1")
	if afterReset.Status != internal.DeliveryStatusRetrying || afterReset.RetryCount != 0 {
		t.Fatalf("expected RETRYING and RetryCount=0 after reset, got %+v", afterReset)
	}

	// ResolveDeliveryFailuresForEvent
	if err := store.ResolveDeliveryFailuresForEvent(ctx, ev2.ID, "resolved via replay"); err != nil {
		t.Fatalf("ResolveDeliveryFailuresForEvent failed: %v", err)
	}
	resF1, _ := store.GetDeliveryFailure(ctx, "fail_1")
	if resF1.Status != internal.DeliveryStatusResolved {
		t.Fatalf("expected fail_1 to be RESOLVED, got %s", resF1.Status)
	}

	// Concurrent update protection (RESOLVED status cannot be overwritten by failure attempt)
	attemptFail := &internal.DeliveryFailure{ID: "fail_1", Status: internal.DeliveryStatusFailed}
	if err := store.UpdateDeliveryFailureAttempt(ctx, attemptFail); err != nil {
		t.Fatalf("unexpected error updating already-resolved failure: %v", err)
	}
	if attemptFail.Status != internal.DeliveryStatusResolved {
		t.Fatalf("expected attemptFail to be preserved as RESOLVED, got %s", attemptFail.Status)
	}

	// BackfillEventProducts
	_, _, _ = store.SaveEvent(ctx, &internal.WebhookEvent{
		ID:               "ev_unm_bf",
		Provider:         internal.Paystack,
		ProviderEventID:  "ps_bf_1",
		PaymentReference: "MN_bf_100",
		Product:          internal.RoutingModeUnmatched,
	})
	bfCount, err := store.BackfillEventProducts(ctx, func(ref string) (string, bool) {
		if strings.HasPrefix(ref, "MN_") {
			return "mailnow", true
		}
		return "", false
	})
	if err != nil || bfCount != 1 {
		t.Fatalf("expected 1 event backfilled, got %d err=%v", bfCount, err)
	}
	bfEv, _ := store.GetEvent(ctx, "ev_unm_bf")
	if bfEv.Product != "mailnow" {
		t.Fatalf("expected ev_unm_bf product to be mailnow, got %s", bfEv.Product)
	}
}

func TestPostgresStore_ManagementMethods(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("UpdateEventProduct, GetDeliveryFailure, UpdateDeliveryFailureAttempt, and ResetFailuresForRetry", func(t *testing.T) {
		rowCall := 0
		pool := &mockPGPool{
			execFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
			queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
				rowCall++
				if rowCall == 1 {
					return &mockPGRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "df_1"
						*dest[1].(*string) = "ev_1"
						*dest[2].(*string) = "paystack"
						*dest[3].(*string) = "mailnow"
						*dest[4].(*string) = "mailnow"
						*dest[5].(*string) = "https://mailnow.com/api"
						*dest[6].(*string) = "direct"
						*dest[7].(*[]byte) = []byte(`{}`)
						*dest[8].(*[]byte) = []byte(`{}`)
						*dest[9].(*int) = 500
						*dest[10].(*string) = "err"
						*dest[11].(*int) = 1
						*dest[12].(*int) = 3
						*dest[13].(*string) = "FAILED"
						*dest[14].(*time.Time) = now
						*dest[15].(*time.Time) = now
						*dest[16].(*time.Time) = now
						return nil
					}}
				}
				return &mockPGRow{scanFn: func(_ ...any) error { return pgx.ErrNoRows }}
			},
		}
		store := internal.NewPostgresWebhookStoreWithPool(pool)

		if err := store.UpdateEventProduct(ctx, "ev_1", ""); err != nil {
			t.Fatalf("UpdateEventProduct failed: %v", err)
		}

		df, err := store.GetDeliveryFailure(ctx, "df_1")
		if err != nil || df.ID != "df_1" {
			t.Fatalf("GetDeliveryFailure failed: %v", err)
		}
		if _, err := store.GetDeliveryFailure(ctx, "missing"); !errors.Is(err, internal.ErrFailureNotFound) {
			t.Fatalf("expected ErrFailureNotFound, got %v", err)
		}

		if err := store.UpdateDeliveryFailureAttempt(ctx, df); err != nil {
			t.Fatalf("UpdateDeliveryFailureAttempt failed: %v", err)
		}

		count, err := store.ResetFailuresForRetry(ctx, "mailnow", internal.Paystack)
		if err != nil || count != 1 {
			t.Fatalf("ResetFailuresForRetry failed: count=%d err=%v", count, err)
		}

		// ResolveDeliveryFailuresForEvent
		pool.execFn = func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 2"), nil
		}
		if err := store.ResolveDeliveryFailuresForEvent(ctx, "ev_1", "resolved via replay"); err != nil {
			t.Fatalf("ResolveDeliveryFailuresForEvent failed: %v", err)
		}

		// BackfillEventProducts
		bfRows := &mockPGRows{
			data: [][]any{
				{"ev_unm", "MN_test_1"},
			},
		}
		pool.queryFn = func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return bfRows, nil
		}
		pool.execFn = func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}
		bfCount, err := store.BackfillEventProducts(ctx, func(ref string) (string, bool) {
			if strings.HasPrefix(ref, "MN_") {
				return "mailnow", true
			}
			return "", false
		})
		if err != nil || bfCount != 1 {
			t.Fatalf("BackfillEventProducts failed: count=%d err=%v", bfCount, err)
		}

		// Zero rows affected -> ErrFailureNotFound
		pool.execFn = func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		if err := store.UpdateDeliveryFailureAttempt(ctx, df); !errors.Is(err, internal.ErrFailureNotFound) {
			t.Fatalf("expected ErrFailureNotFound on 0 rows affected, got %v", err)
		}
	})

	t.Run("ListEvents, ListAllDeliveryFailures, and GetProductMetrics", func(t *testing.T) {
		queryCall := 0
		pool := &mockPGPool{
			queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
				return &mockPGRow{scanFn: func(dest ...any) error {
					*dest[0].(*int) = 1
					return nil
				}}
			},
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				queryCall++
				switch queryCall {
				case 1:
					return &mockPGRows{
						data: [][]any{
							{"ev_1", "paystack", "pe_1", "charge.success", "MN_1", "", false, []byte(`{}`), []byte(`{}`), "DELIVERED", now, now},
						},
					}, nil
				case 2:
					return &mockPGRows{
						data: [][]any{
							{"df_1", "ev_1", "paystack", "mailnow", "ep1", "https://a.test", "direct", []byte(`{}`), []byte(`{}`), 500, "err", 1, 3, "FAILED", now, now, now},
						},
					}, nil
				case 3:
					return &mockPGRows{
						data: [][]any{
							{"mailnow", 5, 4, 0, 1},
						},
					}, nil
				default:
					return &mockPGRows{
						data: [][]any{
							{"mailnow", 0, 1},
						},
					}, nil
				}
			},
		}
		store := internal.NewPostgresWebhookStoreWithPool(pool)

		evs, total, err := store.ListEvents(ctx, internal.EventFilter{
			Product:  "mailnow",
			Provider: internal.Paystack,
			Status:   internal.WebhookStatusDelivered,
			Search:   "MN_1",
			Limit:    10,
			Offset:   0,
		})
		if err != nil || total != 1 || len(evs) != 1 || evs[0].Product != "unmatched" {
			t.Fatalf("ListEvents failed: total=%d evs=%+v err=%v", total, evs, err)
		}

		fails, fTotal, err := store.ListAllDeliveryFailures(ctx, internal.FailureFilter{
			Product:  "mailnow",
			Provider: internal.Paystack,
			Status:   internal.DeliveryStatusFailed,
			EventID:  "ev_1",
			Limit:    10,
			Offset:   0,
		})
		if err != nil || fTotal != 1 || len(fails) != 1 {
			t.Fatalf("ListAllDeliveryFailures failed: total=%d fails=%+v err=%v", fTotal, fails, err)
		}

		metrics, err := store.GetProductMetrics(ctx)
		if err != nil || metrics["mailnow"].TotalEvents != 5 || metrics["mailnow"].DeadLetterCount != 1 {
			t.Fatalf("GetProductMetrics failed: metrics=%+v err=%v", metrics, err)
		}
	})
}

func TestManagementUseCase_And_AdminController(t *testing.T) {
	ctx := context.Background()
	gatewayCfg := sampleGatewayConfig()
	store := internal.NewInMemoryWebhookStore()
	pub := &mockEventPublisher{}
	httpDisp := &stubUpstreamDispatcher{defaultSC: http.StatusOK}

	relaySvc := internal.NewRelayServiceWithConfig(store, pub, gatewayCfg, silentLogger())
	dispUC := internal.NewDispatcherUseCase(gatewayCfg, httpDisp, store, pub, 10*time.Millisecond, silentLogger())
	mgmtUC := internal.NewManagementUseCase(store, gatewayCfg, httpDisp, dispUC, pub, 10*time.Millisecond, silentLogger())

	// 1. Ingest a matched webhook via RelayServiceWithConfig
	ev1, err := relaySvc.Ingest(ctx, &internal.NormalizedWebhook{
		Provider:         internal.Paystack,
		ProviderEventID:  "ps_evt_1",
		EventType:        "charge.success",
		PaymentReference: "MN_tx_100",
		RawHeaders:       map[string]string{"X-Test": "1"},
		RawPayload:       []byte(`{"event":"charge.success"}`),
	})
	if err != nil || ev1.Product != "mailnow" {
		t.Fatalf("expected ingested event product='mailnow', got %+v err=%v", ev1, err)
	}

	// 2. Enqueue a failed delivery for mailnow and an unmatched failure
	f1 := &internal.DeliveryFailure{
		ID:           "f_mailnow_1",
		EventID:      ev1.ID,
		Provider:     internal.Paystack,
		Product:      "mailnow",
		EndpointName: "mailnow",
		TargetURL:    "https://mailnow.com/api/webhook",
		RoutingMode:  internal.RoutingModeDirect,
		Headers:      map[string]string{"Content-Type": "application/json"},
		Payload:      ev1.Payload,
		StatusCode:   500,
		ErrorMessage: "internal error",
		RetryCount:   1,
		MaxRetries:   3,
		Status:       internal.DeliveryStatusFailed,
	}
	_ = store.EnqueueDeliveryFailure(ctx, f1)
	_ = store.UpdateStatus(ctx, ev1.ID, internal.WebhookStatusFailed)

	// Unmatched event that later matches when retried
	evUnmatched, _, _ := store.SaveEvent(ctx, &internal.WebhookEvent{
		ID:               "ev_unmatched_1",
		Provider:         internal.Paystack,
		ProviderEventID:  "ps_evt_2",
		EventType:        "charge.success",
		PaymentReference: "TC_tx_200",
		Product:          "unmatched",
		Payload:          []byte(`{"event":"charge.success"}`),
		Status:           internal.WebhookStatusFailed,
	})
	fUnmatched := &internal.DeliveryFailure{
		ID:           "f_unmatched_1",
		EventID:      evUnmatched.ID,
		Provider:     internal.Paystack,
		Product:      "unmatched",
		EndpointName: "unmatched",
		TargetURL:    "",
		RoutingMode:  internal.RoutingModeUnmatched,
		Payload:      evUnmatched.Payload,
		StatusCode:   422,
		ErrorMessage: "no product matched",
		Status:       internal.DeliveryStatusFailed,
	}
	_ = store.EnqueueDeliveryFailure(ctx, fUnmatched)

	// Setup router with AdminController
	adminCtrl := internal.NewAdminController(mgmtUC)
	router := gin.New()
	router.Use(internal.ErrorHandlerMiddleware())
	uiGroup := router.Group("/admin")
	adminCtrl.RegisterUIRoutes(uiGroup)
	apiGroup := router.Group("/v1/admin")
	adminCtrl.RegisterAPIRoutes(apiGroup)

	t.Run("UI Dashboard and Partials render HTML cleanly", func(t *testing.T) {
		for _, path := range []string{
			"/admin",
			"/admin?tab=failures&product=mailnow",
			"/admin/partials/events?product=mailnow",
			"/admin/partials/events/" + ev1.ID,
			"/admin/partials/failures?product=mailnow",
		} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("GET %s returned %d: %s", path, w.Code, w.Body.String())
			}
			if !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
				t.Fatalf("expected text/html content-type for %s, got %s", path, w.Header().Get("Content-Type"))
			}
		}
	})

	t.Run("UI Single Retry, Bulk Retry, and Event Replay work via HTMX POST", func(t *testing.T) {
		// Single Retry -> 200 OK resolves f_mailnow_1
		req1 := httptest.NewRequest(http.MethodPost, "/admin/failures/"+f1.ID+"/retry", nil)
		w1 := httptest.NewRecorder()
		router.ServeHTTP(w1, req1)
		if w1.Code != http.StatusOK || !strings.Contains(w1.Body.String(), "RESOLVED") {
			t.Fatalf("expected RESOLVED in single retry HTML, code=%d body=%s", w1.Code, w1.Body.String())
		}

		// Unmatched retry re-evaluates route (TC_tx_200 -> tibicloud) and resolves
		reqUnm := httptest.NewRequest(http.MethodPost, "/admin/failures/"+fUnmatched.ID+"/retry", nil)
		wUnm := httptest.NewRecorder()
		router.ServeHTTP(wUnm, reqUnm)
		if wUnm.Code != http.StatusOK || !strings.Contains(wUnm.Body.String(), "RESOLVED") {
			t.Fatalf("expected RESOLVED on unmatched route re-evaluation, code=%d body=%s", wUnm.Code, wUnm.Body.String())
		}

		// Bulk Retry via POST form body
		reqBulk := httptest.NewRequest(http.MethodPost, "/admin/failures/bulk-retry", strings.NewReader("product=mailnow&provider=paystack"))
		reqBulk.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		wBulk := httptest.NewRecorder()
		router.ServeHTTP(wBulk, reqBulk)
		if wBulk.Code != http.StatusOK || !strings.Contains(wBulk.Body.String(), "Queued") {
			t.Fatalf("expected bulk retry banner, code=%d body=%s", wBulk.Code, wBulk.Body.String())
		}

		// Replay Event (and verify failures resolved)
		reqReplay := httptest.NewRequest(http.MethodPost, "/admin/events/"+ev1.ID+"/replay", nil)
		wReplay := httptest.NewRecorder()
		router.ServeHTTP(wReplay, reqReplay)
		if wReplay.Code != http.StatusOK || !strings.Contains(wReplay.Body.String(), "replayed") {
			t.Fatalf("expected replay banner, code=%d body=%s", wReplay.Code, wReplay.Body.String())
		}
		fls, _ := store.ListDeliveryFailures(ctx, ev1.ID)
		for _, f := range fls {
			if f.Status != internal.DeliveryStatusResolved {
				t.Fatalf("expected failure for replayed event to be resolved, got %s", f.Status)
			}
		}
	})

	t.Run("JSON Admin API endpoints return expected payloads and 404 RFC7807 errors", func(t *testing.T) {
		// Overview
		reqOv := httptest.NewRequest(http.MethodGet, "/v1/admin/overview", nil)
		wOv := httptest.NewRecorder()
		router.ServeHTTP(wOv, reqOv)
		if wOv.Code != http.StatusOK {
			t.Fatalf("GET /v1/admin/overview returned %d", wOv.Code)
		}

		// ListEvents
		reqEv := httptest.NewRequest(http.MethodGet, "/v1/admin/events?product=mailnow", nil)
		wEv := httptest.NewRecorder()
		router.ServeHTTP(wEv, reqEv)
		if wEv.Code != http.StatusOK {
			t.Fatalf("GET /v1/admin/events returned %d", wEv.Code)
		}

		// GetEventDetail
		reqDet := httptest.NewRequest(http.MethodGet, "/v1/admin/events/"+ev1.ID, nil)
		wDet := httptest.NewRecorder()
		router.ServeHTTP(wDet, reqDet)
		if wDet.Code != http.StatusOK {
			t.Fatalf("GET /v1/admin/events/:id returned %d", wDet.Code)
		}

		// ReplayEvent API
		reqRep := httptest.NewRequest(http.MethodPost, "/v1/admin/events/"+ev1.ID+"/replay", nil)
		wRep := httptest.NewRecorder()
		router.ServeHTTP(wRep, reqRep)
		if wRep.Code != http.StatusOK {
			t.Fatalf("POST /v1/admin/events/:id/replay returned %d", wRep.Code)
		}

		// ListFailures API
		reqFl := httptest.NewRequest(http.MethodGet, "/v1/admin/failures", nil)
		wFl := httptest.NewRecorder()
		router.ServeHTTP(wFl, reqFl)
		if wFl.Code != http.StatusOK {
			t.Fatalf("GET /v1/admin/failures returned %d", wFl.Code)
		}

		// RetryFailure API when upstream fails (502)
		httpDisp.defaultSC = http.StatusBadGateway
		reqRet := httptest.NewRequest(http.MethodPost, "/v1/admin/failures/"+f1.ID+"/retry", nil)
		wRet := httptest.NewRecorder()
		router.ServeHTTP(wRet, reqRet)
		if wRet.Code != http.StatusOK {
			t.Fatalf("POST /v1/admin/failures/:id/retry returned %d", wRet.Code)
		}

		// BulkRetry API with JSON body
		bodyBytes, _ := json.Marshal(map[string]string{"product": "mailnow", "provider": "paystack"})
		reqBulk := httptest.NewRequest(http.MethodPost, "/v1/admin/failures/bulk-retry", bytes.NewReader(bodyBytes))
		reqBulk.Header.Set("Content-Type", "application/json")
		wBulk := httptest.NewRecorder()
		router.ServeHTTP(wBulk, reqBulk)
		if wBulk.Code != http.StatusOK {
			t.Fatalf("POST /v1/admin/failures/bulk-retry returned %d", wBulk.Code)
		}

		// Invalid JSON body on BulkRetry API -> 400 Bad Request
		badJSON := []byte(`{"product": 123}`)
		reqBad := httptest.NewRequest(http.MethodPost, "/v1/admin/failures/bulk-retry", bytes.NewReader(badJSON))
		reqBad.Header.Set("Content-Type", "application/json")
		wBad := httptest.NewRecorder()
		router.ServeHTTP(wBad, reqBad)
		if wBad.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for invalid JSON body on bulk-retry, got %d", wBad.Code)
		}

		// 404 Not Found branches
		for _, notFoundReq := range []*http.Request{
			httptest.NewRequest(http.MethodGet, "/v1/admin/events/nonexistent", nil),
			httptest.NewRequest(http.MethodPost, "/v1/admin/events/nonexistent/replay", nil),
			httptest.NewRequest(http.MethodPost, "/v1/admin/failures/nonexistent/retry", nil),
		} {
			wNF := httptest.NewRecorder()
			router.ServeHTTP(wNF, notFoundReq)
			if wNF.Code != http.StatusNotFound {
				t.Fatalf("expected 404 for %s %s, got %d", notFoundReq.Method, notFoundReq.URL.Path, wNF.Code)
			}
		}
	})

	t.Run("ManagementUseCase edge cases: defaults, still-unmatched retry, retry exhaustion, and formatJSONPretty", func(t *testing.T) {
		mgmtDefault := internal.NewManagementUseCase(store, gatewayCfg, httpDisp, dispUC, pub, 0, nil)

		// Empty ID checks
		if _, err := mgmtDefault.GetEventDetail(ctx, "  "); !errors.Is(err, internal.ErrEventNotFound) {
			t.Fatalf("expected ErrEventNotFound for blank ID, got %v", err)
		}
		if _, err := mgmtDefault.RetryFailureNow(ctx, "  "); !errors.Is(err, internal.ErrFailureNotFound) {
			t.Fatalf("expected ErrFailureNotFound for blank ID, got %v", err)
		}
		if _, err := mgmtDefault.ReplayEventNow(ctx, "  "); !errors.Is(err, internal.ErrEventNotFound) {
			t.Fatalf("expected ErrEventNotFound for blank ID, got %v", err)
		}

		// ListEvents and ListFailures negative offset & default limit
		if _, err := mgmtDefault.ListEvents(ctx, internal.EventFilter{Limit: -1, Offset: -5}); err != nil {
			t.Fatalf("ListEvents default clamping failed: %v", err)
		}
		if _, err := mgmtDefault.ListFailures(ctx, internal.FailureFilter{Limit: -1, Offset: -5}); err != nil {
			t.Fatalf("ListFailures default clamping failed: %v", err)
		}

		// Event with empty payload and event with non-JSON payload
		evEmpty, _, _ := store.SaveEvent(ctx, &internal.WebhookEvent{
			ID:               "ev_empty_payload",
			Provider:         internal.Paystack,
			ProviderEventID:  "ps_empty",
			PaymentReference: "UNKNOWN_999",
			Payload:          nil,
			Status:           internal.WebhookStatusPending,
		})
		detEmpty, err := mgmtDefault.GetEventDetail(ctx, evEmpty.ID)
		if err != nil || detEmpty.PrettyPayload != "{}" {
			t.Fatalf("expected '{}' pretty payload, got %q err=%v", detEmpty.PrettyPayload, err)
		}

		evRaw, _, _ := store.SaveEvent(ctx, &internal.WebhookEvent{
			ID:               "ev_raw_payload",
			Provider:         internal.Paystack,
			ProviderEventID:  "ps_raw",
			PaymentReference: "UNKNOWN_998",
			Payload:          []byte(`not-valid-json`),
			Status:           internal.WebhookStatusFailed,
		})
		detRaw, err := mgmtDefault.GetEventDetail(ctx, evRaw.ID)
		if err != nil || detRaw.PrettyPayload != "not-valid-json" {
			t.Fatalf("expected raw payload fallback, got %q err=%v", detRaw.PrettyPayload, err)
		}

		// Unmatched failure that remains unmatched on retry
		fStillUnmatched := &internal.DeliveryFailure{
			ID:           "f_still_unmatched",
			EventID:      evRaw.ID,
			Provider:     internal.Paystack,
			Product:      "unmatched",
			EndpointName: "unmatched",
			TargetURL:    "",
			RoutingMode:  internal.RoutingModeUnmatched,
			Status:       internal.DeliveryStatusFailed,
		}
		_ = store.EnqueueDeliveryFailure(ctx, fStillUnmatched)
		retUnm, err := mgmtDefault.RetryFailureNow(ctx, fStillUnmatched.ID)
		if err != nil || retUnm.Status != internal.DeliveryStatusFailed || retUnm.RetryCount != 1 {
			t.Fatalf("expected still-unmatched failure to remain FAILED with RetryCount=1, got %+v err=%v", retUnm, err)
		}

		// RetryFailureNow when upstream returns transport error and exhausts max_retries
		httpDisp.sendErr = errors.New("connection refused")
		fExhaust := &internal.DeliveryFailure{
			ID:           "f_exhaust",
			EventID:      ev1.ID,
			Provider:     internal.Paystack,
			Product:      "mailnow",
			EndpointName: "mailnow",
			TargetURL:    "https://mailnow.com/api/webhook",
			RoutingMode:  internal.RoutingModeDirect,
			RetryCount:   2,
			MaxRetries:   3,
			Status:       internal.DeliveryStatusRetrying,
		}
		_ = store.EnqueueDeliveryFailure(ctx, fExhaust)
		retExh, err := mgmtDefault.RetryFailureNow(ctx, fExhaust.ID)
		if err != nil || retExh.Status != internal.DeliveryStatusFailed {
			t.Fatalf("expected exhausted failure status=FAILED, got %+v err=%v", retExh, err)
		}
		if len(pub.dlqMsgs) == 0 || pub.dlqMsgs[len(pub.dlqMsgs)-1].FailureID != fExhaust.ID {
			t.Fatalf("expected DLQ message published for manually exhausted failure, got %d msgs", len(pub.dlqMsgs))
		}
		httpDisp.sendErr = nil
	})
}

type failingManagementUseCase struct {
	err           error
	listFailOn2nd bool
}

func (f *failingManagementUseCase) GetDashboardOverview(_ context.Context) (*internal.DashboardOverview, error) {
	if f.listFailOn2nd {
		return &internal.DashboardOverview{}, nil
	}
	return nil, f.err
}

func (f *failingManagementUseCase) ListEvents(_ context.Context, _ internal.EventFilter) (*internal.PaginatedEvents, error) {
	return nil, f.err
}

func (f *failingManagementUseCase) GetEventDetail(_ context.Context, _ string) (*internal.EventDetail, error) {
	return nil, f.err
}

func (f *failingManagementUseCase) ListFailures(_ context.Context, _ internal.FailureFilter) (*internal.PaginatedFailures, error) {
	return nil, f.err
}

func (f *failingManagementUseCase) RetryFailureNow(_ context.Context, _ string) (*internal.DeliveryFailure, error) {
	return nil, f.err
}

func (f *failingManagementUseCase) ReplayEventNow(_ context.Context, _ string) (*internal.WebhookEvent, error) {
	if f.listFailOn2nd {
		return &internal.WebhookEvent{ID: "ev_1"}, nil
	}
	return nil, f.err
}

func (f *failingManagementUseCase) BulkRetryFailures(_ context.Context, _ string, _ internal.PSPProvider) (int, error) {
	if f.listFailOn2nd {
		return 1, nil
	}
	return 0, f.err
}

func TestAdminController_ErrorBranches(t *testing.T) {
	failUC := &failingManagementUseCase{err: errors.New("storage failure")}
	ctrl := internal.NewAdminController(failUC)
	router := gin.New()
	router.Use(internal.ErrorHandlerMiddleware())
	ctrl.RegisterUIRoutes(router.Group("/admin"))
	ctrl.RegisterAPIRoutes(router.Group("/v1/admin"))

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/admin"},
		{http.MethodGet, "/admin/partials/events"},
		{http.MethodGet, "/admin/partials/events/ev_1"},
		{http.MethodGet, "/admin/partials/failures"},
		{http.MethodPost, "/admin/failures/f_1/retry"},
		{http.MethodPost, "/admin/failures/bulk-retry"},
		{http.MethodPost, "/admin/events/ev_1/replay"},
		{http.MethodGet, "/v1/admin/overview"},
		{http.MethodGet, "/v1/admin/events"},
		{http.MethodGet, "/v1/admin/failures"},
		{http.MethodPost, "/v1/admin/failures/bulk-retry"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500 for %s %s, got %d", tc.method, tc.path, w.Code)
		}
	}

	// Secondary error branches in RenderDashboard (tab=events vs tab=failures), HandleUIBulkRetry, and HandleUIReplayEvent
	failUC2 := &failingManagementUseCase{err: errors.New("secondary error"), listFailOn2nd: true}
	ctrl2 := internal.NewAdminController(failUC2)
	router2 := gin.New()
	router2.Use(internal.ErrorHandlerMiddleware())
	ctrl2.RegisterUIRoutes(router2.Group("/admin"))

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/admin?tab=events"},
		{http.MethodGet, "/admin?tab=failures"},
		{http.MethodPost, "/admin/failures/bulk-retry"},
		{http.MethodPost, "/admin/events/ev_1/replay"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		w := httptest.NewRecorder()
		router2.ServeHTTP(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500 for secondary error on %s %s, got %d", tc.method, tc.path, w.Code)
		}
	}
}
