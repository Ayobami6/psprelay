package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"psprelay/internal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg, err := internal.LoadConfig()
	if err != nil {
		println("Error loading configuration:", err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		println("Error running PSP Relay:", err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg *internal.Config) error {
	logger := internal.NewLogger(cfg)
	rootCtx, rootCancel := context.WithCancel(ctx)
	defer rootCancel()

	// Load gateway product configurations
	gatewayCfg, err := internal.LoadConfigYaml(rootCtx, cfg.ConfigFilePath)
	if err != nil {
		logger.Warn("Failed to load gateway YAML config, running with empty product routes", "path", cfg.ConfigFilePath, "error", err)
		gatewayCfg = &internal.GatewayConfig{Products: make(map[string]internal.ProductConfig)}
	} else {
		logger.Info("Loaded gateway configuration", "version", gatewayCfg.Version, "product_count", len(gatewayCfg.Products))
	}

	// Initialize WebhookStore (PostgreSQL in production, in-memory fallback for local dev without DB)
	var store internal.WebhookStore
	if cfg.DatabaseURL != "" {
		pgStore, pgErr := internal.NewPostgresWebhookStore(rootCtx, cfg.DatabaseURL)
		if pgErr != nil {
			logger.Error("Failed to initialize PostgreSQL store", "error", pgErr)
			return fmt.Errorf("initializing postgres store: %w", pgErr)
		}
		if schemaErr := pgStore.EnsureSchema(rootCtx); schemaErr != nil {
			logger.Error("Failed to ensure PostgreSQL schema", "error", schemaErr)
			return fmt.Errorf("ensuring postgres schema: %w", schemaErr)
		}
		store = pgStore
		logger.Info("Connected to PostgreSQL webhook store")
	} else {
		logger.Warn("DATABASE_URL is empty; falling back to in-memory webhook store")
		store = internal.NewInMemoryWebhookStore()
	}
	defer store.Close()

	// Initialize Kafka Publisher & HTTP Dispatcher
	publisher := internal.NewKafkaEventPublisher(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaDLQTopic)
	defer func() {
		_ = publisher.Close()
	}()

	httpDispatcher := internal.NewDefaultHTTPDispatcher(5 * time.Second)

	// Initialize PSP Provider Services
	paystackSvc := internal.NewPaystackService(cfg.PaystackSecretKey)
	flutterwaveSvc := internal.NewFlutterwaveService(cfg.FlutterwaveSecretHash)
	monnifySvc := internal.NewMonnifyService(cfg.MonnifySecretKey)

	// Initialize Domain & Application Use Cases
	domainService := internal.NewService(store)
	healthCheckUseCase := internal.NewHealthCheckUseCase(domainService)
	relayService := internal.NewRelayService(store, publisher, logger)
	receiverUseCase := internal.NewReceiverUseCase(relayService, paystackSvc, flutterwaveSvc, monnifySvc)
	dispatcherUseCase := internal.NewDispatcherUseCase(gatewayCfg, httpDispatcher, store, publisher, cfg.RetryBaseBackoff, logger)

	// Initialize Background Workers (Outbox, Retry, Kafka Consumer)
	outboxWorker := internal.NewOutboxWorker(store, publisher, cfg.OutboxPollInterval, 5*time.Second, logger)
	retryWorker := internal.NewRetryWorker(store, httpDispatcher, publisher, cfg.RetryPollInterval, cfg.RetryBaseBackoff, logger)
	kafkaConsumer := internal.NewKafkaConsumerRunner(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaConsumerGroup, dispatcherUseCase, logger)
	defer func() {
		_ = kafkaConsumer.Close()
	}()

	var workerWG sync.WaitGroup
	workerWG.Add(3)
	go func() {
		defer workerWG.Done()
		outboxWorker.Start(rootCtx)
	}()
	go func() {
		defer workerWG.Done()
		retryWorker.Start(rootCtx)
	}()
	go func() {
		defer workerWG.Done()
		kafkaConsumer.Start(rootCtx)
	}()

	// Initialize HTTP Controllers
	healthCtrl := internal.NewHealthController(healthCheckUseCase)
	webhookCtrl := internal.NewPSPWebhookController(receiverUseCase)

	if cfg.Environment == internal.EnvironmentProduction {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())
	router.Use(internal.CorsMiddleware())
	router.Use(internal.ErrorHandlerMiddleware())

	router.GET("/health", func(c *gin.Context) {
		if healthResp, err := healthCheckUseCase.Execute(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, healthResp)
		} else {
			c.JSON(http.StatusOK, healthResp)
		}
	})

	v1 := router.Group("/v1")
	healthCtrl.RegisterRoutes(v1)
	webhookCtrl.RegisterRoutes(v1)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("Starting PSP Relay HTTP server", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Server listen failure", "error", err)
			errCh <- err
		}
	}()

	// Wait for context cancellation or unexpected server listen failure
	select {
	case <-ctx.Done():
	case listenErr := <-errCh:
		rootCancel()
		workerWG.Wait()
		return listenErr
	}

	logger.Info("Shutting down PSP Relay server and background workers...")
	rootCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server forced to shutdown", "error", err)
	}

	workerWG.Wait()
	logger.Info("PSP Relay exited cleanly.")
	return nil
}
