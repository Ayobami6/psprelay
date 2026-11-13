package tests

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"psprelay/internal"
)

func TestLoadConfigYaml(t *testing.T) {
	t.Run("successfully parses valid YAML schema with multiple products and endpoints", func(t *testing.T) {
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
      - name: mailnow-backup
        url: https://backup.mailnow.com/api
  tibicloud:
    prefix: TC
    secrets:
      type: Opaque
      encodeKey: tcKey
      signatureKey: tcSig
    endpoints:
      - name: tibicloud
        url: https://tibicloud.com/api
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
		if cfg.Version != "v1" {
			t.Errorf("expected version v1, got %q", cfg.Version)
		}
		if len(cfg.Products) != 2 {
			t.Fatalf("expected 2 products, got %d", len(cfg.Products))
		}
		mn := cfg.Products["mailnow"]
		if mn.Prefix != "MN" || mn.Secrets.EncodeKey != "key123" || mn.Secrets.SignatureKey != "sig123" {
			t.Errorf("unexpected mailnow config: %+v", mn)
		}
		if len(mn.Endpoints) != 2 || mn.Endpoints[1].URL != "https://backup.mailnow.com/api" {
			t.Errorf("unexpected mailnow endpoints: %+v", mn.Endpoints)
		}
	})

	t.Run("returns descriptive error on missing or malformed YAML file", func(t *testing.T) {
		if _, err := internal.LoadConfigYaml(context.Background(), "/nonexistent/config.yaml"); err == nil {
			t.Fatal("expected error for non-existent file, got nil")
		}

		tmpDir := t.TempDir()
		badPath := filepath.Join(tmpDir, "bad.yaml")
		if err := os.WriteFile(badPath, []byte(":::\n\t[invalid"), 0600); err != nil {
			t.Fatalf("failed to write bad yaml file: %v", err)
		}
		if _, err := internal.LoadConfigYaml(context.Background(), badPath); err == nil {
			t.Fatal("expected error for malformed YAML, got nil")
		}
	})
}

func TestComputePrefixToken(t *testing.T) {
	t.Run("produces deterministic 12-char lowercase hex token normalized for case and whitespace", func(t *testing.T) {
		tokUpper := internal.ComputePrefixToken("MN", "secret-key")
		tokLower := internal.ComputePrefixToken("mn", "secret-key")
		tokPadded := internal.ComputePrefixToken("  Mn  ", "secret-key")

		if len(tokUpper) != internal.PrefixTokenLength {
			t.Fatalf("expected token length %d, got %d (%q)", internal.PrefixTokenLength, len(tokUpper), tokUpper)
		}
		if tokUpper != tokLower || tokUpper != tokPadded {
			t.Fatalf("expected normalized tokens to match, got upper=%q lower=%q padded=%q", tokUpper, tokLower, tokPadded)
		}
		if tokUpper != strings.ToLower(tokUpper) {
			t.Errorf("expected lowercase hex token, got %q", tokUpper)
		}
	})

	t.Run("produces distinct tokens for different prefixes or encodeKeys", func(t *testing.T) {
		tokMN := internal.ComputePrefixToken("MN", "key1")
		tokTC := internal.ComputePrefixToken("TC", "key1")
		tokOtherKey := internal.ComputePrefixToken("MN", "key2")

		if tokMN == tokTC {
			t.Errorf("expected distinct tokens for MN vs TC, both %q", tokMN)
		}
		if tokMN == tokOtherKey {
			t.Errorf("expected distinct tokens for different keys, both %q", tokMN)
		}
	})
}

func TestComputeHMACSHA256Hex(t *testing.T) {
	t.Run("matches standard HMAC-SHA256 hex digest", func(t *testing.T) {
		payload := []byte(`{"event":"charge.success","amount":5000}`)
		secret := "whsec_test_123"

		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(payload)
		expected := hex.EncodeToString(mac.Sum(nil))

		got := internal.ComputeHMACSHA256Hex(payload, secret)
		if got != expected {
			t.Fatalf("expected %q, got %q", expected, got)
		}
	})

	t.Run("handles empty payload and is sensitive to payload and secret changes", func(t *testing.T) {
		emptyDigest := internal.ComputeHMACSHA256Hex(nil, "secret")
		if len(emptyDigest) != 64 {
			t.Fatalf("expected 64-char SHA-256 hex digest, got %d chars", len(emptyDigest))
		}
		if emptyDigest == internal.ComputeHMACSHA256Hex([]byte("x"), "secret") {
			t.Error("expected different digest for non-empty payload")
		}
		if emptyDigest == internal.ComputeHMACSHA256Hex(nil, "other-secret") {
			t.Error("expected different digest for different secret")
		}
	})
}

func TestMatchProductByReference(t *testing.T) {
	mnToken := internal.ComputePrefixToken("MN", "encode-mn")
	tcToken := internal.ComputePrefixToken("TC", "encode-tc")

	cfg := &internal.GatewayConfig{
		Version: "v1",
		Products: map[string]internal.ProductConfig{
			"emptyPrefix": {
				Prefix: "",
				Secrets: internal.ProductSecrets{
					EncodeKey: "encode-empty",
				},
			},
			"noEncodeKey": {
				Prefix: "NK",
			},
			"mailnow": {
				Prefix: "MN",
				Secrets: internal.ProductSecrets{
					EncodeKey: "encode-mn",
				},
			},
			"tibicloud": {
				Prefix: "TC",
				Secrets: internal.ProductSecrets{
					EncodeKey: "encode-tc",
				},
			},
		},
	}

	tests := []struct {
		name        string
		cfg         *internal.GatewayConfig
		ref         string
		wantMatched bool
		wantProduct string
	}{
		{
			name:        "matches HMAC prefix token with underscore separator",
			cfg:         cfg,
			ref:         mnToken + "_order_123",
			wantMatched: true,
			wantProduct: "mailnow",
		},
		{
			name:        "matches HMAC prefix token with hyphen separator",
			cfg:         cfg,
			ref:         tcToken + "-order-456",
			wantMatched: true,
			wantProduct: "tibicloud",
		},
		{
			name:        "matches uppercase HMAC prefix token with surrounding whitespace",
			cfg:         cfg,
			ref:         "  " + strings.ToUpper(mnToken) + "_tx789  ",
			wantMatched: true,
			wantProduct: "mailnow",
		},
		{
			name:        "matches exact HMAC prefix token without separator",
			cfg:         cfg,
			ref:         tcToken,
			wantMatched: true,
			wantProduct: "tibicloud",
		},
		{
			name:        "falls back to raw prefix with underscore separator",
			cfg:         cfg,
			ref:         "MN_order100",
			wantMatched: true,
			wantProduct: "mailnow",
		},
		{
			name:        "falls back to lowercase raw prefix with hyphen separator",
			cfg:         cfg,
			ref:         "tc-order101",
			wantMatched: true,
			wantProduct: "tibicloud",
		},
		{
			name:        "matches raw prefix for product without encodeKey",
			cfg:         cfg,
			ref:         "NK_555",
			wantMatched: true,
			wantProduct: "noEncodeKey",
		},
		{
			name:        "does not match unknown reference",
			cfg:         cfg,
			ref:         "UNKNOWN_999",
			wantMatched: false,
		},
		{
			name:        "does not match empty or whitespace reference",
			cfg:         cfg,
			ref:         "   ",
			wantMatched: false,
		},
		{
			name:        "does not match when GatewayConfig is nil",
			cfg:         nil,
			ref:         "MN_1",
			wantMatched: false,
		},
		{
			name:        "does not match when Products map is empty",
			cfg:         &internal.GatewayConfig{Products: map[string]internal.ProductConfig{}},
			ref:         "MN_1",
			wantMatched: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotProduct, gotCfg, gotMatched := tc.cfg.MatchProductByReference(tc.ref)
			if gotMatched != tc.wantMatched {
				t.Fatalf("MatchProductByReference(%q) matched=%v, want %v (product=%q)", tc.ref, gotMatched, tc.wantMatched, gotProduct)
			}
			if tc.wantMatched {
				if gotProduct != tc.wantProduct {
					t.Errorf("MatchProductByReference(%q) product=%q, want %q", tc.ref, gotProduct, tc.wantProduct)
				}
				if gotCfg.Prefix == "" {
					t.Errorf("expected non-empty ProductConfig.Prefix for %q", gotProduct)
				}
			}
		})
	}
}
