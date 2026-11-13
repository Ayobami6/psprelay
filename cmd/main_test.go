package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"psprelay/internal"
)

func TestRunLifecycle(t *testing.T) {
	t.Run("starts and shuts down cleanly on context cancellation with in-memory store", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfgPath := filepath.Join(tmpDir, "config.yaml")
		if err := os.WriteFile(cfgPath, []byte("version: v1\nproducts: {}\n"), 0600); err != nil {
			t.Fatalf("failed to write yaml config: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()

		cfg := &internal.Config{
			Addr:               "127.0.0.1:0",
			ConfigFilePath:     cfgPath,
			DatabaseURL:        "",
			KafkaBrokers:       []string{"127.0.0.1:9092"},
			KafkaTopic:         "psp.webhooks",
			KafkaDLQTopic:      "psp.webhooks.dlq",
			KafkaConsumerGroup: "psprelay-test",
			OutboxPollInterval: 10 * time.Millisecond,
			RetryPollInterval:  10 * time.Millisecond,
			RetryBaseBackoff:   10 * time.Millisecond,
			LogLevel:           slog.LevelError,
			Environment:        internal.EnvironmentProduction,
		}

		if err := run(ctx, cfg); err != nil {
			t.Fatalf("expected clean shutdown, got error: %v", err)
		}
	})

	t.Run("falls back to empty routes when config yaml is missing and cancels cleanly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		cfg := &internal.Config{
			Addr:               "127.0.0.1:0",
			ConfigFilePath:     "/nonexistent/config.yaml",
			DatabaseURL:        "",
			KafkaBrokers:       []string{"127.0.0.1:9092"},
			KafkaTopic:         "psp.webhooks",
			KafkaDLQTopic:      "psp.webhooks.dlq",
			KafkaConsumerGroup: "psprelay-test",
			OutboxPollInterval: 10 * time.Millisecond,
			RetryPollInterval:  10 * time.Millisecond,
			RetryBaseBackoff:   10 * time.Millisecond,
			LogLevel:           slog.LevelError,
			Environment:        internal.EnvironmentDevelopment,
		}

		if err := run(ctx, cfg); err != nil {
			t.Fatalf("expected clean shutdown with missing config yaml, got error: %v", err)
		}
	})

	t.Run("returns error when DatabaseURL is invalid", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		cfg := &internal.Config{
			Addr:               "127.0.0.1:0",
			ConfigFilePath:     "/nonexistent/config.yaml",
			DatabaseURL:        "://invalid-postgres-url",
			KafkaBrokers:       []string{"127.0.0.1:9092"},
			KafkaTopic:         "psp.webhooks",
			KafkaDLQTopic:      "psp.webhooks.dlq",
			KafkaConsumerGroup: "psprelay-test",
			OutboxPollInterval: 10 * time.Millisecond,
			RetryPollInterval:  10 * time.Millisecond,
			RetryBaseBackoff:   10 * time.Millisecond,
			LogLevel:           slog.LevelError,
			Environment:        internal.EnvironmentDevelopment,
		}

		if err := run(ctx, cfg); err == nil {
			t.Fatal("expected error for invalid DatabaseURL, got nil")
		}
	})
}
