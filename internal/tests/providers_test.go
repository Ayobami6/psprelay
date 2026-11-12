package tests

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"psprelay/internal"
)

func TestPaystackService_VerifyAndParse(t *testing.T) {
	secret := "sk_test_paystack_secret"
	svc := internal.NewPaystackService(secret)

	t.Run("verifies valid HMAC-SHA512 signature and extracts checkout reference", func(t *testing.T) {
		body := []byte(`{"event":"charge.success","data":{"id":9001,"reference":"a1b2c3d4e5f6_tx100","channel":"card"}}`)
		headers := make(http.Header)
		headers.Set("X-Paystack-Signature", internal.ComputeHMACSHA512Hex(body, secret))

		norm, err := svc.VerifyAndParse(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if norm.Provider != internal.Paystack {
			t.Errorf("expected provider paystack, got %q", norm.Provider)
		}
		if norm.PaymentReference != "a1b2c3d4e5f6_tx100" {
			t.Errorf("expected reference a1b2c3d4e5f6_tx100, got %q", norm.PaymentReference)
		}
		if norm.IsDVA {
			t.Errorf("expected IsDVA=false for card channel")
		}
	})

	t.Run("detects dedicated_nuban DVA and prioritizes prefixed metadata reference over random NIP ref", func(t *testing.T) {
		body := []byte(`{
			"event":"charge.success",
			"data":{
				"id":9002,
				"reference":"T9988776655",
				"channel":"dedicated_nuban",
				"metadata":{"reference":"MN_dva_custom_1"}
			}
		}`)
		headers := make(http.Header)
		headers.Set("X-Paystack-Signature", internal.ComputeHMACSHA512Hex(body, secret))

		norm, err := svc.VerifyAndParse(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !norm.IsDVA {
			t.Errorf("expected IsDVA=true for dedicated_nuban")
		}
		if norm.PaymentReference != "MN_dva_custom_1" {
			t.Errorf("expected metadata reference MN_dva_custom_1, got %q", norm.PaymentReference)
		}
	})

	t.Run("rejects missing or invalid signature", func(t *testing.T) {
		body := []byte(`{"event":"charge.success"}`)
		_, err := svc.VerifyAndParse(context.Background(), make(http.Header), body)
		if !errors.Is(err, internal.ErrMissingSignature) {
			t.Fatalf("expected ErrMissingSignature, got %v", err)
		}

		headers := make(http.Header)
		headers.Set("X-Paystack-Signature", "deadbeef")
		_, err = svc.VerifyAndParse(context.Background(), headers, body)
		if !errors.Is(err, internal.ErrInvalidSignature) {
			t.Fatalf("expected ErrInvalidSignature, got %v", err)
		}
	})
}

func TestFlutterwaveService_VerifyAndParse(t *testing.T) {
	secretHash := "flw_verif_hash_123"
	svc := internal.NewFlutterwaveService(secretHash)

	t.Run("verifies verif-hash and detects bank_transfer DVA", func(t *testing.T) {
		body := []byte(`{
			"event":"charge.completed",
			"data":{
				"id":4455,
				"tx_ref":"FLW_NIP_9988",
				"payment_type":"bank_transfer"
			}
		}`)
		headers := make(http.Header)
		headers.Set("verif-hash", secretHash)

		norm, err := svc.VerifyAndParse(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if norm.Provider != internal.Flutterwave {
			t.Errorf("expected provider flutterwave, got %q", norm.Provider)
		}
		if !norm.IsDVA {
			t.Errorf("expected IsDVA=true for bank_transfer")
		}
		if norm.PaymentReference != "FLW_NIP_9988" {
			t.Errorf("expected reference FLW_NIP_9988, got %q", norm.PaymentReference)
		}
	})
}

func TestMonnifyService_VerifyAndParse(t *testing.T) {
	secret := "monnify_secret_key_xyz"
	svc := internal.NewMonnifyService(secret)

	t.Run("verifies monnify-signature and prioritizes product.reference for RESERVED_ACCOUNT", func(t *testing.T) {
		body := []byte(`{
			"eventType":"SUCCESSFUL_TRANSACTION",
			"eventData":{
				"transactionReference":"MNFY|2026|001",
				"paymentReference":"MNFY|NIP|9999",
				"product":{
					"type":"RESERVED_ACCOUNT",
					"reference":"TC_reserved_acct_01"
				}
			}
		}`)
		headers := make(http.Header)
		headers.Set("monnify-signature", internal.ComputeHMACSHA512Hex(body, secret))

		norm, err := svc.VerifyAndParse(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if norm.Provider != internal.Monnify {
			t.Errorf("expected provider monnify, got %q", norm.Provider)
		}
		if !norm.IsDVA {
			t.Errorf("expected IsDVA=true for RESERVED_ACCOUNT")
		}
		if norm.PaymentReference != "TC_reserved_acct_01" {
			t.Errorf("expected product.reference TC_reserved_acct_01, got %q", norm.PaymentReference)
		}
	})
}
