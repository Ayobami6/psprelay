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

	t.Run("returns ErrSecretNotFound when secret is empty", func(t *testing.T) {
		emptySvc := internal.NewPaystackService("   ")
		_, err := emptySvc.VerifyAndParse(context.Background(), make(http.Header), []byte(`{}`))
		if !errors.Is(err, internal.ErrSecretNotFound) {
			t.Fatalf("expected ErrSecretNotFound, got %v", err)
		}
	})

	t.Run("returns ErrInvalidPayload on malformed JSON", func(t *testing.T) {
		body := []byte(`{invalid-json`)
		headers := make(http.Header)
		headers.Set("X-Paystack-Signature", internal.ComputeHMACSHA512Hex(body, secret))

		_, err := svc.VerifyAndParse(context.Background(), headers, body)
		if !errors.Is(err, internal.ErrInvalidPayload) {
			t.Fatalf("expected ErrInvalidPayload, got %v", err)
		}
	})

	t.Run("extracts customer.metadata reference, detects authorization.channel and dedicatedaccount event, and resolves string/missing event IDs", func(t *testing.T) {
		body := []byte(`{
			"event":"dedicatedaccount.assign.success",
			"data":{
				"id":"evt_str_1",
				"reference":"plainref",
				"authorization":{"channel":"dedicated_nuban"},
				"customer":{"email":"user@example.com","metadata":{"payment_reference":"TC_cust_ref_1"}}
			}
		}`)
		headers := make(http.Header)
		headers.Set("X-Paystack-Signature", internal.ComputeHMACSHA512Hex(body, secret))

		norm, err := svc.VerifyAndParse(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !norm.IsDVA {
			t.Errorf("expected IsDVA=true")
		}
		if norm.PaymentReference != "TC_cust_ref_1" {
			t.Errorf("expected TC_cust_ref_1, got %q", norm.PaymentReference)
		}
		if norm.ProviderEventID != "dedicatedaccount.assign.success:evt_str_1" {
			t.Errorf("unexpected ProviderEventID: %q", norm.ProviderEventID)
		}

		// Missing data.id falls back to payload hash; boolean/other id is stringified
		bodyNoID := []byte(`{"event":"charge.success","data":{"reference":"MN_ref_no_id"}}`)
		h2 := make(http.Header)
		h2.Set("X-Paystack-Signature", internal.ComputeHMACSHA512Hex(bodyNoID, secret))
		norm2, err := svc.VerifyAndParse(context.Background(), h2, bodyNoID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(norm2.ProviderEventID) != 32 {
			t.Errorf("expected 32-char hex payload hash ID, got %q", norm2.ProviderEventID)
		}

		bodyBoolID := []byte(`{"event":"charge.success","data":{"id":true,"reference":"MN_ref_bool"}}`)
		h3 := make(http.Header)
		h3.Set("X-Paystack-Signature", internal.ComputeHMACSHA512Hex(bodyBoolID, secret))
		norm3, err := svc.VerifyAndParse(context.Background(), h3, bodyBoolID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if norm3.ProviderEventID != "charge.success:true" {
			t.Errorf("expected charge.success:true, got %q", norm3.ProviderEventID)
		}
	})
}

func TestFlutterwaveService_VerifyAndParse(t *testing.T) {
	secretHash := "flw_verif_hash_123"
	svc := internal.NewFlutterwaveService(secretHash)

	t.Run("Provider returns Flutterwave", func(t *testing.T) {
		if svc.Provider() != internal.Flutterwave {
			t.Fatalf("expected %q, got %q", internal.Flutterwave, svc.Provider())
		}
	})

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

	t.Run("rejects empty secret, missing signature, invalid signature, and malformed JSON", func(t *testing.T) {
		if _, err := internal.NewFlutterwaveService("").VerifyAndParse(context.Background(), make(http.Header), []byte(`{}`)); !errors.Is(err, internal.ErrSecretNotFound) {
			t.Fatalf("expected ErrSecretNotFound, got %v", err)
		}

		if _, err := svc.VerifyAndParse(context.Background(), make(http.Header), []byte(`{}`)); !errors.Is(err, internal.ErrMissingSignature) {
			t.Fatalf("expected ErrMissingSignature, got %v", err)
		}

		hInvalid := make(http.Header)
		hInvalid.Set("X-Flutterwave-Signature", "wrong")
		if _, err := svc.VerifyAndParse(context.Background(), hInvalid, []byte(`{}`)); !errors.Is(err, internal.ErrInvalidSignature) {
			t.Fatalf("expected ErrInvalidSignature, got %v", err)
		}

		hValid := make(http.Header)
		hValid.Set("X-Flutterwave-Signature", secretHash)
		if _, err := svc.VerifyAndParse(context.Background(), hValid, []byte(`{bad`)); !errors.Is(err, internal.ErrInvalidPayload) {
			t.Fatalf("expected ErrInvalidPayload, got %v", err)
		}
	})

	t.Run("supports X-Flutterwave-Signature, flw_ref fallback, BANK_TRANSFER_TRANSACTION DVA, and missing ID hash", func(t *testing.T) {
		body := []byte(`{
			"event.type":"BANK_TRANSFER_TRANSACTION",
			"data":{
				"flw_ref":"FLW_FALLBACK_99"
			}
		}`)
		headers := make(http.Header)
		headers.Set("X-Flutterwave-Signature", secretHash)

		norm, err := svc.VerifyAndParse(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !norm.IsDVA {
			t.Errorf("expected IsDVA=true for BANK_TRANSFER_TRANSACTION")
		}
		if norm.PaymentReference != "FLW_FALLBACK_99" {
			t.Errorf("expected FLW_FALLBACK_99, got %q", norm.PaymentReference)
		}
		if norm.EventType != "BANK_TRANSFER_TRANSACTION" {
			t.Errorf("expected BANK_TRANSFER_TRANSACTION, got %q", norm.EventType)
		}
		if len(norm.ProviderEventID) != 32 {
			t.Errorf("expected 32-char hash ID fallback, got %q", norm.ProviderEventID)
		}

		// txRef camelCase and meta_data override
		bodyMeta := []byte(`{
			"event":"charge.completed",
			"meta_data":{"tx_ref":"MN_meta_override"},
			"data":{"id":77,"txRef":"PLAINREF"}
		}`)
		normMeta, err := svc.VerifyAndParse(context.Background(), headers, bodyMeta)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if normMeta.PaymentReference != "MN_meta_override" {
			t.Errorf("expected MN_meta_override, got %q", normMeta.PaymentReference)
		}
	})
}

func TestMonnifyService_VerifyAndParse(t *testing.T) {
	secret := "monnify_secret_key_xyz"
	svc := internal.NewMonnifyService(secret)

	t.Run("Provider returns Monnify", func(t *testing.T) {
		if svc.Provider() != internal.Monnify {
			t.Fatalf("expected %q, got %q", internal.Monnify, svc.Provider())
		}
	})

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

	t.Run("rejects empty secret, missing signature, invalid signature, and malformed JSON", func(t *testing.T) {
		if _, err := internal.NewMonnifyService("").VerifyAndParse(context.Background(), make(http.Header), []byte(`{}`)); !errors.Is(err, internal.ErrSecretNotFound) {
			t.Fatalf("expected ErrSecretNotFound, got %v", err)
		}

		if _, err := svc.VerifyAndParse(context.Background(), make(http.Header), []byte(`{}`)); !errors.Is(err, internal.ErrMissingSignature) {
			t.Fatalf("expected ErrMissingSignature, got %v", err)
		}

		hInvalid := make(http.Header)
		hInvalid.Set("monnify-signature", "bad")
		if _, err := svc.VerifyAndParse(context.Background(), hInvalid, []byte(`{}`)); !errors.Is(err, internal.ErrInvalidSignature) {
			t.Fatalf("expected ErrInvalidSignature, got %v", err)
		}

		badJSON := []byte(`{invalid`)
		hValid := make(http.Header)
		hValid.Set("monnify-signature", internal.ComputeHMACSHA512Hex(badJSON, secret))
		if _, err := svc.VerifyAndParse(context.Background(), hValid, badJSON); !errors.Is(err, internal.ErrInvalidPayload) {
			t.Fatalf("expected ErrInvalidPayload, got %v", err)
		}
	})

	t.Run("detects DYNAMIC_ACCOUNT, extracts metaData.accountReference, and falls back to payload hash ID", func(t *testing.T) {
		body := []byte(`{
			"eventType":"SUCCESSFUL_TRANSACTION",
			"eventData":{
				"metaData":{"accountReference":"MN_dyn_01"},
				"product":{"type":"DYNAMIC_ACCOUNT"}
			}
		}`)
		headers := make(http.Header)
		headers.Set("monnify-signature", internal.ComputeHMACSHA512Hex(body, secret))

		norm, err := svc.VerifyAndParse(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !norm.IsDVA {
			t.Errorf("expected IsDVA=true for DYNAMIC_ACCOUNT")
		}
		if norm.PaymentReference != "MN_dyn_01" {
			t.Errorf("expected MN_dyn_01, got %q", norm.PaymentReference)
		}
		if len(norm.ProviderEventID) != 32 {
			t.Errorf("expected 32-char hash ID fallback, got %q", norm.ProviderEventID)
		}
	})
}
