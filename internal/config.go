package internal

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

type Environment string

const (
	EnvironmentDevelopment Environment = "development"
	EnvironmentStaging     Environment = "staging"
	EnvironmentProduction  Environment = "production"
)

// Config holds the runtime configuration for the PSP Relay service.
type Config struct {
	Addr                  string
	ConfigFilePath        string
	DatabaseURL           string
	KafkaBrokers          []string
	KafkaTopic            string
	KafkaDLQTopic         string
	KafkaConsumerGroup    string
	PaystackSecretKey     string
	FlutterwaveSecretHash string
	MonnifySecretKey      string
	AdminUsername         string
	AdminPassword         string
	OutboxPollInterval    time.Duration
	RetryPollInterval     time.Duration
	RetryBaseBackoff      time.Duration
	LogLevel              slog.Level
	Environment           Environment
}

// LoadConfig loads environment variables (with .env fallback) into a strongly-typed Config struct.
func LoadConfig() (*Config, error) {
	// Best-effort loading of .env file (non-fatal if missing in containerized environments)
	_ = godotenv.Load()

	addr := getEnvFallback("ADDR", "Addr", ":8083")
	configFilePath := getEnvFallback("CONFIG_FILE_PATH", "ConfigFilePath", "./config.yaml")
	databaseURL := getEnvFallback("DATABASE_URL", "DatabaseURL", "")
	rawBrokers := getEnvFallback("KAFKA_BROKERS", "KafkaBrokers", "localhost:9092")
	kafkaTopic := getEnvFallback("KAFKA_TOPIC", "KafkaTopic", "psp.webhooks")
	kafkaDLQTopic := getEnvFallback("KAFKA_DLQ_TOPIC", "KafkaDLQTopic", "psp.webhooks.dlq")
	kafkaGroup := getEnvFallback("KAFKA_CONSUMER_GROUP", "KafkaConsumerGroup", "psprelay-dispatcher")

	paystackSecret := getEnvFallback("PAYSTACK_SECRET_KEY", "PaystackSecretKey", "")
	flutterwaveSecret := getEnvFallback("FLUTTERWAVE_SECRET_HASH", "FlutterwaveSecretHash", "")
	monnifySecret := getEnvFallback("MONNIFY_SECRET_KEY", "MonnifySecretKey", "")

	adminUsername := getEnvFallback("ADMIN_USERNAME", "AdminUsername", "")
	adminPassword := getEnvFallback("ADMIN_PASSWORD", "AdminPassword", "")

	outboxPoll := parseDurationFallback(getEnvFallback("OUTBOX_POLL_INTERVAL", "OutboxPollInterval", "5s"), 5*time.Second)
	retryPoll := parseDurationFallback(getEnvFallback("RETRY_POLL_INTERVAL", "RetryPollInterval", "2s"), 2*time.Second)
	retryBackoff := parseDurationFallback(getEnvFallback("RETRY_BASE_BACKOFF", "RetryBaseBackoff", "5s"), 5*time.Second)

	rawLogLevel := getEnvFallback("LOG_LEVEL", "LogLevel", "info")
	rawEnvironment := getEnvFallback("ENVIRONMENT", "Environment", string(EnvironmentDevelopment))

	return &Config{
		Addr:                  addr,
		ConfigFilePath:        configFilePath,
		DatabaseURL:           databaseURL,
		KafkaBrokers:          parseBrokers(rawBrokers),
		KafkaTopic:            kafkaTopic,
		KafkaDLQTopic:         kafkaDLQTopic,
		KafkaConsumerGroup:    kafkaGroup,
		PaystackSecretKey:     paystackSecret,
		FlutterwaveSecretHash: flutterwaveSecret,
		MonnifySecretKey:      monnifySecret,
		AdminUsername:         adminUsername,
		AdminPassword:         adminPassword,
		OutboxPollInterval:    outboxPoll,
		RetryPollInterval:     retryPoll,
		RetryBaseBackoff:      retryBackoff,
		LogLevel:              parseLogLevel(rawLogLevel),
		Environment:           Environment(rawEnvironment),
	}, nil
}

// NewLogger constructs a structured JSON logger configured at cfg.LogLevel.
func NewLogger(cfg *Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case string(LogLevelDebug):
		return slog.LevelDebug
	case string(LogLevelInfo):
		return slog.LevelInfo
	case string(LogLevelWarn):
		return slog.LevelWarn
	case string(LogLevelError):
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	if len(brokers) == 0 {
		return []string{"localhost:9092"}
	}
	return brokers
}

func parseDurationFallback(raw string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func getEnvFallback(primaryKey, secondaryKey, defaultValue string) string {
	if val, ok := os.LookupEnv(primaryKey); ok && val != "" {
		return val
	}
	if val, ok := os.LookupEnv(secondaryKey); ok && val != "" {
		return val
	}
	return defaultValue
}
