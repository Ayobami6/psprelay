package tests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"psprelay/internal"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

// --- Kafka Publisher & Consumer Mocks ---

type mockKafkaWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
	writeErr error
	closeErr error
	closed   bool
}

func (m *mockKafkaWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return m.closeErr
}

type readerStep struct {
	msg kafka.Message
	err error
}

type mockKafkaReader struct {
	mu        sync.Mutex
	steps     []readerStep
	idx       int
	committed []kafka.Message
	commitErr error
	closeErr  error
	closed    bool
	cancelOn  int
	cancelFn  context.CancelFunc
}

func (m *mockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	m.mu.Lock()
	if m.cancelOn > 0 && m.idx >= m.cancelOn && m.cancelFn != nil {
		m.cancelFn()
	}
	if m.idx < len(m.steps) {
		step := m.steps[m.idx]
		m.idx++
		m.mu.Unlock()
		return step.msg, step.err
	}
	m.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (m *mockKafkaReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.commitErr != nil {
		return m.commitErr
	}
	m.committed = append(m.committed, msgs...)
	return nil
}

func (m *mockKafkaReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return m.closeErr
}

type mockDispatcherUseCase struct {
	mu     sync.Mutex
	events []internal.WebhookEvent
	err    error
}

func (m *mockDispatcherUseCase) Execute(_ context.Context, event internal.WebhookEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, event)
	return nil
}

func TestKafkaEventPublisher(t *testing.T) {
	t.Run("publishes webhook with payment_reference key and fallback to ID", func(t *testing.T) {
		whWriter := &mockKafkaWriter{}
		dlqWriter := &mockKafkaWriter{}
		pub := internal.NewKafkaEventPublisherWithWriters(whWriter, dlqWriter)

		ev1 := internal.WebhookEvent{ID: "id_1", PaymentReference: "MN_ref_1", Provider: internal.Paystack}
		if err := pub.PublishWebhook(context.Background(), ev1); err != nil {
			t.Fatalf("PublishWebhook failed: %v", err)
		}

		ev2 := internal.WebhookEvent{ID: "id_2", PaymentReference: "", Provider: internal.Monnify}
		if err := pub.PublishWebhook(context.Background(), ev2); err != nil {
			t.Fatalf("PublishWebhook fallback failed: %v", err)
		}

		if len(whWriter.messages) != 2 {
			t.Fatalf("expected 2 messages, got %d", len(whWriter.messages))
		}
		if string(whWriter.messages[0].Key) != "MN_ref_1" {
			t.Errorf("expected key MN_ref_1, got %q", string(whWriter.messages[0].Key))
		}
		if string(whWriter.messages[1].Key) != "id_2" {
			t.Errorf("expected fallback key id_2, got %q", string(whWriter.messages[1].Key))
		}
	})

	t.Run("publishes DLQ message and returns Close errors", func(t *testing.T) {
		closeErr := errors.New("close failed")
		whWriter := &mockKafkaWriter{closeErr: closeErr}
		dlqWriter := &mockKafkaWriter{closeErr: errors.New("dlq close failed")}
		pub := internal.NewKafkaEventPublisherWithWriters(whWriter, dlqWriter)

		if err := pub.PublishDLQ(context.Background(), internal.DLQMessage{
			FailureID: "fail_1",
			EventID:   "evt_1",
			Provider:  internal.Paystack,
		}); err != nil {
			t.Fatalf("PublishDLQ failed: %v", err)
		}
		if len(dlqWriter.messages) != 1 || string(dlqWriter.messages[0].Key) != "evt_1" {
			t.Fatalf("unexpected DLQ messages: %+v", dlqWriter.messages)
		}

		if err := pub.Close(); !errors.Is(err, closeErr) {
			t.Fatalf("expected first close error %v, got %v", closeErr, err)
		}

		// Also cover NewKafkaEventPublisher + Close cleanly without connecting to broker
		realPub := internal.NewKafkaEventPublisher([]string{"127.0.0.1:9092"}, "t1", "t1.dlq")
		if err := realPub.Close(); err != nil {
			t.Fatalf("expected realPub.Close to succeed, got: %v", err)
		}
	})
}

func TestKafkaConsumerRunner(t *testing.T) {
	t.Run("dispatches valid messages, skips poison JSON, handles dispatch & commit errors, and stops on cancel", func(t *testing.T) {
		validBytes, _ := json.Marshal(internal.WebhookEvent{ID: "evt_k1", Provider: internal.Paystack})
		ctx, cancel := context.WithCancel(context.Background())

		reader := &mockKafkaReader{
			cancelOn: 3,
			cancelFn: cancel,
			steps: []readerStep{
				{msg: kafka.Message{Value: []byte(`{bad-json`)}},
				{msg: kafka.Message{Value: validBytes}},
				{err: errors.New("temporary fetch error")},
			},
		}
		disp := &mockDispatcherUseCase{}
		runner := internal.NewKafkaConsumerRunnerWithReader(reader, disp, nil)
		runner.Start(ctx)

		if len(disp.events) != 1 || disp.events[0].ID != "evt_k1" {
			t.Fatalf("expected 1 dispatched event evt_k1, got %+v", disp.events)
		}
		// 1 poison commit + 1 valid commit = 2 committed messages
		if len(reader.committed) != 2 {
			t.Fatalf("expected 2 committed offsets, got %d", len(reader.committed))
		}
		if err := runner.Close(); err != nil {
			t.Fatalf("unexpected Close error: %v", err)
		}
	})

	t.Run("does not commit offset on dispatcher error and tolerates commit errors", func(t *testing.T) {
		validBytes, _ := json.Marshal(internal.WebhookEvent{ID: "evt_k2", Provider: internal.Paystack})

		// Dispatcher error branch
		ctx1, cancel1 := context.WithCancel(context.Background())
		reader1 := &mockKafkaReader{
			cancelOn: 1,
			cancelFn: cancel1,
			steps:    []readerStep{{msg: kafka.Message{Value: validBytes}}},
		}
		dispErr := &mockDispatcherUseCase{err: errors.New("dispatch failed")}
		runner1 := internal.NewKafkaConsumerRunnerWithReader(reader1, dispErr, silentLogger())
		runner1.Start(ctx1)
		if len(reader1.committed) != 0 {
			t.Fatalf("expected 0 commits when dispatcher fails, got %d", len(reader1.committed))
		}

		// Commit error branch
		ctx2, cancel2 := context.WithCancel(context.Background())
		reader2 := &mockKafkaReader{
			cancelOn:  1,
			cancelFn:  cancel2,
			commitErr: errors.New("commit failed"),
			steps:     []readerStep{{msg: kafka.Message{Value: validBytes}}},
		}
		runner2 := internal.NewKafkaConsumerRunnerWithReader(reader2, &mockDispatcherUseCase{}, silentLogger())
		runner2.Start(ctx2)

		// Real constructor + Close
		realRunner := internal.NewKafkaConsumerRunner([]string{"127.0.0.1:9092"}, "topic", "grp", &mockDispatcherUseCase{}, silentLogger())
		if err := realRunner.Close(); err != nil {
			t.Fatalf("unexpected realRunner.Close error: %v", err)
		}
	})
}

func TestOutboxAndRetryWorkers(t *testing.T) {
	t.Run("OutboxWorker Start loop and partial batch publish error", func(t *testing.T) {
		store := internal.NewInMemoryWebhookStore()
		_, _, _ = store.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:        internal.Paystack,
			ProviderEventID: "out_1",
			Status:          internal.WebhookStatusPending,
			CreatedAt:       time.Now().UTC().Add(-10 * time.Second),
		})

		pub := &mockEventPublisher{pubErr: errors.New("broker down")}
		worker := internal.NewOutboxWorker(store, pub, 10*time.Millisecond, -1, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Millisecond)
		defer cancel()
		worker.Start(ctx)

		// Error in ProcessBatch should propagate
		if err := worker.ProcessBatch(context.Background()); err == nil {
			t.Fatal("expected ProcessBatch error when publisher fails")
		}
	})

	t.Run("RetryWorker Start loop, transport errors, DLQ errors, and status reconciliation branches", func(t *testing.T) {
		base := internal.NewInMemoryWebhookStore()
		ev, _, _ := base.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:        internal.Paystack,
			ProviderEventID: "ret_1",
			Status:          internal.WebhookStatusPublished,
		})
		_ = base.EnqueueDeliveryFailure(context.Background(), &internal.DeliveryFailure{
			EventID:     ev.ID,
			Provider:    internal.Paystack,
			TargetURL:   "https://example.com/hook",
			Status:      internal.DeliveryStatusRetrying,
			RetryCount:  2,
			MaxRetries:  3,
			NextRetryAt: time.Now().UTC().Add(-time.Minute),
		})

		disp := &stubUpstreamDispatcher{sendErr: errors.New("connection reset")}
		pub := &mockEventPublisher{dlqPubErr: errors.New("dlq unavailable")}
		worker := internal.NewRetryWorker(base, disp, pub, 10*time.Millisecond, 0, nil)

		if err := worker.ProcessDueRetries(context.Background(), time.Now().UTC()); err != nil {
			t.Fatalf("expected ProcessDueRetries to tolerate DLQ publish error, got: %v", err)
		}

		// HasPendingOrFailedDeliveries error branch
		ev2, _, _ := base.SaveEvent(context.Background(), &internal.WebhookEvent{
			Provider:        internal.Paystack,
			ProviderEventID: "ret_2",
			Status:          internal.WebhookStatusPublished,
		})
		_ = base.EnqueueDeliveryFailure(context.Background(), &internal.DeliveryFailure{
			EventID:     ev2.ID,
			Provider:    internal.Paystack,
			TargetURL:   "https://example.com/hook",
			Status:      internal.DeliveryStatusRetrying,
			RetryCount:  0,
			MaxRetries:  3,
			NextRetryAt: time.Now().UTC().Add(-time.Minute),
		})
		failStore := &failingWebhookStore{WebhookStore: base, hasDeliveriesErr: errors.New("query failed")}
		workerFail := internal.NewRetryWorker(failStore, disp, pub, 10*time.Millisecond, time.Millisecond, silentLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		workerFail.Start(ctx)

		// ClaimDueRetries error branch
		failStore2 := &failingWebhookStore{WebhookStore: base, claimRetriesErr: errors.New("claim failed")}
		workerFail2 := internal.NewRetryWorker(failStore2, disp, pub, 10*time.Millisecond, time.Millisecond, silentLogger())
		if err := workerFail2.ProcessDueRetries(context.Background(), time.Now().UTC()); err == nil {
			t.Fatal("expected ClaimDueRetries error, got nil")
		}
	})
}

func TestInMemoryWebhookStore_EdgeCases(t *testing.T) {
	store := internal.NewInMemoryWebhookStore()
	ctx := context.Background()

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	store.Close()

	if _, err := store.GetEvent(ctx, "missing"); !errors.Is(err, internal.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
	if err := store.UpdateStatus(ctx, "missing", internal.WebhookStatusDelivered); !errors.Is(err, internal.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// Empty outbox and due retries
	if err := store.ClaimPendingOutboxEvents(ctx, 0, 10, func(_ context.Context, _ []internal.WebhookEvent) ([]string, error) {
		t.Fatal("callback should not be called when outbox is empty")
		return nil, nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	exhausted, err := store.ClaimDueRetries(ctx, time.Now().UTC(), 10, func(_ context.Context, _ internal.DeliveryFailure) internal.RetryAttemptResult {
		t.Fatal("callback should not be called when retries are empty")
		return internal.RetryAttemptResult{}
	})
	if err != nil || len(exhausted) != 0 {
		t.Fatalf("unexpected retries result: exhausted=%v err=%v", exhausted, err)
	}

	// Limit clamping with multiple pending events and multiple due retries
	t0 := time.Now().UTC().Add(-10 * time.Minute)
	ev1, _, _ := store.SaveEvent(ctx, &internal.WebhookEvent{Provider: internal.Paystack, ProviderEventID: "lim1", CreatedAt: t0})
	_, _, _ = store.SaveEvent(ctx, &internal.WebhookEvent{Provider: internal.Paystack, ProviderEventID: "lim2", CreatedAt: t0.Add(time.Minute)})
	var claimedCount int
	_ = store.ClaimPendingOutboxEvents(ctx, 0, 1, func(_ context.Context, events []internal.WebhookEvent) ([]string, error) {
		claimedCount = len(events)
		return []string{events[0].ID}, nil
	})
	if claimedCount != 1 {
		t.Fatalf("expected limit=1 outbox claim, got %d", claimedCount)
	}

	_ = store.EnqueueDeliveryFailure(ctx, &internal.DeliveryFailure{
		EventID:     ev1.ID,
		Provider:    internal.Paystack,
		Status:      internal.DeliveryStatusRetrying,
		NextRetryAt: t0,
	})
	_ = store.EnqueueDeliveryFailure(ctx, &internal.DeliveryFailure{
		EventID:     ev1.ID,
		Provider:    internal.Paystack,
		Status:      internal.DeliveryStatusRetrying,
		NextRetryAt: t0.Add(time.Minute),
	})
	var retriedCount int
	_, _ = store.ClaimDueRetries(ctx, time.Now().UTC(), 1, func(_ context.Context, _ internal.DeliveryFailure) internal.RetryAttemptResult {
		retriedCount++
		return internal.RetryAttemptResult{Resolved: true, StatusCode: 200}
	})
	if retriedCount != 1 {
		t.Fatalf("expected limit=1 retry claim, got %d", retriedCount)
	}
}

// --- PostgresWebhookStore Mock Pool ---

type mockPGRow struct {
	scanFn func(dest ...any) error
}

func (r *mockPGRow) Scan(dest ...any) error {
	return r.scanFn(dest...)
}

type mockPGRows struct {
	pgx.Rows
	data   [][]any
	idx    int
	closed bool
	err    error
}

func (r *mockPGRows) Next() bool {
	if r.idx >= len(r.data) {
		return false
	}
	r.idx++
	return true
}

func (r *mockPGRows) Scan(dest ...any) error {
	row := r.data[r.idx-1]
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = row[i].(string)
		case *bool:
			*d = row[i].(bool)
		case *int:
			*d = row[i].(int)
		case *[]byte:
			*d = row[i].([]byte)
		case *time.Time:
			*d = row[i].(time.Time)
		}
	}
	return nil
}

func (r *mockPGRows) Close() {
	r.closed = true
}

func (r *mockPGRows) Err() error {
	return r.err
}

type mockPGTx struct {
	pgx.Tx
	queryFn   func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	execFn    func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	commitErr error
	committed bool
	rolled    bool
}

func (t *mockPGTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.queryFn(ctx, sql, args...)
}

func (t *mockPGTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (t *mockPGTx) Commit(_ context.Context) error {
	t.committed = true
	return t.commitErr
}

func (t *mockPGTx) Rollback(_ context.Context) error {
	t.rolled = true
	return nil
}

type mockPGPool struct {
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	beginFn    func(ctx context.Context) (pgx.Tx, error)
	pingErr    error
	closed     bool
}

func (p *mockPGPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if p.execFn != nil {
		return p.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (p *mockPGPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.queryRowFn(ctx, sql, args...)
}

func (p *mockPGPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.queryFn(ctx, sql, args...)
}

func (p *mockPGPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.beginFn(ctx)
}

func (p *mockPGPool) Ping(_ context.Context) error {
	return p.pingErr
}

func (p *mockPGPool) Close() {
	p.closed = true
}

func TestPostgresWebhookStore(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("NewPostgresWebhookStore rejects invalid DSN", func(t *testing.T) {
		if _, err := internal.NewPostgresWebhookStore(ctx, "://bad-url"); err == nil {
			t.Fatal("expected error for invalid database URL")
		}
	})

	t.Run("EnsureSchema, Ping, Close, and UpdateStatus", func(t *testing.T) {
		pool := &mockPGPool{}
		store := internal.NewPostgresWebhookStoreWithPool(pool)

		if err := store.EnsureSchema(ctx); err != nil {
			t.Fatalf("EnsureSchema failed: %v", err)
		}
		if err := store.Ping(ctx); err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
		if err := store.UpdateStatus(ctx, "ev_1", internal.WebhookStatusDelivered); err != nil {
			t.Fatalf("UpdateStatus failed: %v", err)
		}
		store.Close()
		if !pool.closed {
			t.Fatal("expected pool.Close to be called")
		}

		poolErr := &mockPGPool{
			execFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, errors.New("ddl error")
			},
		}
		if err := internal.NewPostgresWebhookStoreWithPool(poolErr).EnsureSchema(ctx); err == nil {
			t.Fatal("expected EnsureSchema error")
		}
	})

	t.Run("SaveEvent handles insert, duplicate pgx.ErrNoRows, and errors, plus GetEvent", func(t *testing.T) {
		callCount := 0
		pool := &mockPGPool{
			queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
				callCount++
				switch callCount {
				case 1:
					return &mockPGRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "ev_new"
						*dest[1].(*string) = "PENDING"
						*dest[2].(*time.Time) = now
						*dest[3].(*time.Time) = now
						return nil
					}}
				case 2:
					// First QueryRow for duplicate returns pgx.ErrNoRows
					return &mockPGRow{scanFn: func(_ ...any) error {
						return pgx.ErrNoRows
					}}
				case 3:
					// Second QueryRow loads existing row via getEventByProviderKey
					return &mockPGRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "ev_existing"
						*dest[1].(*string) = "paystack"
						*dest[2].(*string) = "pe_1"
						*dest[3].(*string) = "charge.success"
						*dest[4].(*string) = "MN_1"
						*dest[5].(*string) = "mailnow"
						*dest[6].(*bool) = false
						*dest[7].(*[]byte) = []byte(`{"X-Test":"1"}`)
						*dest[8].(*[]byte) = []byte(`{}`)
						*dest[9].(*string) = "PUBLISHED"
						*dest[10].(*time.Time) = now
						*dest[11].(*time.Time) = now
						return nil
					}}
				default:
					return &mockPGRow{scanFn: func(_ ...any) error {
						return errors.New("db error")
					}}
				}
			},
		}
		store := internal.NewPostgresWebhookStoreWithPool(pool)

		saved, dup, err := store.SaveEvent(ctx, &internal.WebhookEvent{
			Provider:        internal.Paystack,
			ProviderEventID: "pe_1",
			Payload:         []byte(`{}`),
		})
		if err != nil || dup || saved.ID != "ev_new" {
			t.Fatalf("unexpected insert result: saved=%+v dup=%v err=%v", saved, dup, err)
		}

		dupEv, dup, err := store.SaveEvent(ctx, &internal.WebhookEvent{
			Provider:        internal.Paystack,
			ProviderEventID: "pe_1",
			Payload:         []byte(`{}`),
		})
		if err != nil || !dup || dupEv.ID != "ev_existing" {
			t.Fatalf("unexpected duplicate result: saved=%+v dup=%v err=%v", dupEv, dup, err)
		}

		if _, _, err := store.SaveEvent(ctx, &internal.WebhookEvent{Payload: []byte(`{}`)}); err == nil {
			t.Fatal("expected SaveEvent error")
		}
		if _, err := store.GetEvent(ctx, "ev_existing"); err == nil {
			t.Fatal("expected GetEvent scan error")
		}
	})

	t.Run("ClaimPendingOutboxEvents claims, updates PUBLISHED, and commits", func(t *testing.T) {
		rows := &mockPGRows{
			data: [][]any{
				{"ev_1", "paystack", "pe_1", "charge.success", "MN_1", "mailnow", false, []byte(`{}`), []byte(`{}`), "PENDING", now, now},
			},
		}
		var execCalled bool
		tx := &mockPGTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				return rows, nil
			},
			execFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
				execCalled = true
				return pgconn.CommandTag{}, nil
			},
		}
		pool := &mockPGPool{
			beginFn: func(_ context.Context) (pgx.Tx, error) {
				return tx, nil
			},
		}
		store := internal.NewPostgresWebhookStoreWithPool(pool)

		err := store.ClaimPendingOutboxEvents(ctx, time.Second, 10, func(_ context.Context, events []internal.WebhookEvent) ([]string, error) {
			if len(events) != 1 || events[0].ID != "ev_1" {
				t.Fatalf("unexpected events: %+v", events)
			}
			return []string{events[0].ID}, nil
		})
		if err != nil || !execCalled || !tx.committed {
			t.Fatalf("expected commit and update, err=%v execCalled=%v committed=%v", err, execCalled, tx.committed)
		}

		// Empty rows path
		txEmpty := &mockPGTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				return &mockPGRows{}, nil
			},
		}
		pool.beginFn = func(_ context.Context) (pgx.Tx, error) { return txEmpty, nil }
		if err := store.ClaimPendingOutboxEvents(ctx, time.Second, 10, nil); err != nil {
			t.Fatalf("unexpected empty outbox error: %v", err)
		}

		// Begin error path
		pool.beginFn = func(_ context.Context) (pgx.Tx, error) { return nil, errors.New("begin failed") }
		if err := store.ClaimPendingOutboxEvents(ctx, time.Second, 10, nil); err == nil {
			t.Fatal("expected Begin error")
		}
	})

	t.Run("EnqueueDeliveryFailure, ClaimDueRetries, ListDeliveryFailures, and HasPendingOrFailedDeliveries", func(t *testing.T) {
		rows := &mockPGRows{
			data: [][]any{
				{"df_1", "ev_1", "paystack", "mailnow", "ep1", "https://a.test", "direct", []byte(`{}`), []byte(`{}`), 500, "err", 0, 3, "RETRYING", now, now, now},
				{"df_2", "ev_1", "paystack", "mailnow", "ep2", "https://b.test", "direct", []byte(`{}`), []byte(`{}`), 500, "err", 2, 3, "RETRYING", now, now, now},
				{"df_3", "ev_1", "paystack", "mailnow", "ep3", "https://c.test", "direct", []byte(`{}`), []byte(`{}`), 500, "err", 0, 3, "RETRYING", now, now, now},
			},
		}
		tx := &mockPGTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				return rows, nil
			},
		}
		listRows := &mockPGRows{
			data: [][]any{
				{"df_1", "ev_1", "paystack", "mailnow", "ep1", "https://a.test", "direct", []byte(`{}`), []byte(`{}`), 200, "", 1, 3, "RESOLVED", now, now, now},
			},
		}
		pool := &mockPGPool{
			beginFn: func(_ context.Context) (pgx.Tx, error) { return tx, nil },
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return listRows, nil },
			queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
				return &mockPGRow{scanFn: func(dest ...any) error {
					*dest[0].(*bool) = false
					*dest[1].(*bool) = true
					return nil
				}}
			},
		}
		store := internal.NewPostgresWebhookStoreWithPool(pool)

		if err := store.EnqueueDeliveryFailure(ctx, &internal.DeliveryFailure{
			EventID:  "ev_1",
			Provider: internal.Paystack,
			Status:   internal.DeliveryStatusRetrying,
		}); err != nil {
			t.Fatalf("EnqueueDeliveryFailure failed: %v", err)
		}

		calls := 0
		exhausted, err := store.ClaimDueRetries(ctx, now, 10, func(_ context.Context, _ internal.DeliveryFailure) internal.RetryAttemptResult {
			calls++
			switch calls {
			case 1:
				return internal.RetryAttemptResult{Resolved: true, StatusCode: http.StatusOK}
			case 2:
				return internal.RetryAttemptResult{Resolved: false, StatusCode: 500, ErrorMessage: "still failing"}
			default:
				return internal.RetryAttemptResult{Resolved: false, StatusCode: 502, ErrorMessage: "retry later", NextRetryAt: now.Add(time.Minute)}
			}
		})
		if err != nil {
			t.Fatalf("ClaimDueRetries failed: %v", err)
		}
		if len(exhausted) != 1 || exhausted[0].ID != "df_2" || !tx.committed {
			t.Fatalf("unexpected ClaimDueRetries output: exhausted=%+v committed=%v", exhausted, tx.committed)
		}

		listed, err := store.ListDeliveryFailures(ctx, "ev_1")
		if err != nil || len(listed) != 1 || listed[0].Status != internal.DeliveryStatusResolved {
			t.Fatalf("unexpected ListDeliveryFailures output: listed=%+v err=%v", listed, err)
		}

		hasRetrying, hasFailed, err := store.HasPendingOrFailedDeliveries(ctx, "ev_1")
		if err != nil || hasRetrying || !hasFailed {
			t.Fatalf("unexpected HasPendingOrFailedDeliveries: retrying=%v failed=%v err=%v", hasRetrying, hasFailed, err)
		}
	})
}
