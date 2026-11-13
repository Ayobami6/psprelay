package internal

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

// PrefixTokenLength defines the truncated hex length of the HMAC-SHA256 prefix token.
const PrefixTokenLength = 12

// ProductEndpoint represents a target webhook URL for a configured product.
type ProductEndpoint struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

// ProductSecrets holds the cryptographic keys for a product in config.yaml.
type ProductSecrets struct {
	Type         string `yaml:"type"`
	EncodeKey    string `yaml:"encodeKey"`
	SignatureKey string `yaml:"signatureKey"`
}

// ProductConfig represents the routing and security configuration for a product.
type ProductConfig struct {
	Prefix    string            `yaml:"prefix"`
	Secrets   ProductSecrets    `yaml:"secrets"`
	Endpoints []ProductEndpoint `yaml:"endpoints"`
}

// GatewayConfig represents the top-level config.yaml structure.
type GatewayConfig struct {
	Version  string                   `yaml:"version"`
	Products map[string]ProductConfig `yaml:"products"`
}

// LoadConfigYaml loads and parses the gateway YAML configuration file.
func LoadConfigYaml(_ context.Context, filePath string) (*GatewayConfig, error) {
	cleanPath := filepath.Clean(filePath)
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("reading yaml config at %q: %w", filePath, err)
	}

	var config GatewayConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unmarshaling yaml config from %q: %w", filePath, err)
	}

	return &config, nil
}

// ComputePrefixToken derives a deterministic 12-character hex token from a product's prefix and encodeKey
// using HMAC-SHA256(UPPER(prefix), encodeKey)[:12].
func ComputePrefixToken(prefix, encodeKey string) string {
	normalizedPrefix := strings.ToUpper(strings.TrimSpace(prefix))
	mac := hmac.New(sha256.New, []byte(encodeKey))
	_, _ = mac.Write([]byte(normalizedPrefix))
	sum := hex.EncodeToString(mac.Sum(nil))
	if len(sum) > PrefixTokenLength {
		return sum[:PrefixTokenLength]
	}
	return sum
}

// ComputeHMACSHA256Hex computes the lowercase hex-encoded HMAC-SHA256 signature of a payload.
func ComputeHMACSHA256Hex(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// MatchProductByReference inspects a payment reference for a matching product prefix token
// (<12-char-hmac-token>_<tx_id> or raw <PREFIX>_<tx_id>) and returns the owning product name and config.
func (g *GatewayConfig) MatchProductByReference(ref string) (string, ProductConfig, bool) {
	if g == nil || len(g.Products) == 0 {
		return "", ProductConfig{}, false
	}

	trimmedRef := strings.TrimSpace(ref)
	if trimmedRef == "" {
		return "", ProductConfig{}, false
	}

	// Extract candidate prefix before '_' or '-' if separator exists
	candidate := trimmedRef
	if idx := strings.IndexAny(trimmedRef, "_-"); idx > 0 {
		candidate = trimmedRef[:idx]
	}

	// 1. Primary check: match deterministic HMAC-SHA256 prefix token
	for productName, cfg := range g.Products {
		if cfg.Prefix == "" || cfg.Secrets.EncodeKey == "" {
			continue
		}
		expectedToken := ComputePrefixToken(cfg.Prefix, cfg.Secrets.EncodeKey)
		if strings.EqualFold(candidate, expectedToken) || strings.HasPrefix(strings.ToLower(trimmedRef), expectedToken+"_") {
			return productName, cfg, true
		}
	}

	// 2. Fallback check: match explicit raw prefix before '_' (e.g. "MN_12345" or "TC_12345")
	for productName, cfg := range g.Products {
		if cfg.Prefix == "" {
			continue
		}
		if strings.EqualFold(candidate, cfg.Prefix) {
			return productName, cfg, true
		}
	}

	return "", ProductConfig{}, false
}
