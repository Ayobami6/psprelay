package internal

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// NormalizedWebhook holds the verified, canonicalized metadata extracted from an upstream PSP webhook.
type NormalizedWebhook struct {
	Provider         PSPProvider       `json:"provider"`
	ProviderEventID  string            `json:"provider_event_id"`
	EventType        string            `json:"event_type"`
	PaymentReference string            `json:"payment_reference"`
	IsDVA            bool              `json:"is_dva"`
	RawHeaders       map[string]string `json:"raw_headers"`
	RawPayload       []byte            `json:"raw_payload"`
}

// ProviderService defines the port for provider-specific webhook signature verification and payload normalization.
type ProviderService interface {
	Provider() PSPProvider
	VerifyAndParse(ctx context.Context, headers http.Header, body []byte) (*NormalizedWebhook, error)
}

// PaystackService verifies and normalizes Paystack webhooks.
type PaystackService struct {
	secretKey string
}

// NewPaystackService creates a new Paystack webhook verifier and parser.
func NewPaystackService(secretKey string) *PaystackService {
	return &PaystackService{secretKey: strings.TrimSpace(secretKey)}
}

func (s *PaystackService) Provider() PSPProvider {
	return Paystack
}

type paystackWebhookPayload struct {
	Event string `json:"event"`
	Data  struct {
		ID            any               `json:"id"`
		Reference     string            `json:"reference"`
		Channel       string            `json:"channel"`
		Metadata      map[string]any    `json:"metadata"`
		Authorization map[string]any    `json:"authorization"`
		Customer      paystackCustomer  `json:"customer"`
	} `json:"data"`
}

type paystackCustomer struct {
	Email    string         `json:"email"`
	Metadata map[string]any `json:"metadata"`
}

func (s *PaystackService) VerifyAndParse(_ context.Context, headers http.Header, body []byte) (*NormalizedWebhook, error) {
	if s.secretKey == "" {
		return nil, ErrSecretNotFound
	}

	signature := strings.TrimSpace(headers.Get("X-Paystack-Signature"))
	if signature == "" {
		return nil, ErrMissingSignature
	}

	if !verifyHMACSHA512(body, s.secretKey, signature) {
		return nil, ErrInvalidSignature
	}

	var parsed paystackWebhookPayload
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}

	metaRef := firstNonEmptyString(
		extractStringFromMap(parsed.Data.Metadata, "reference", "payment_reference", "tx_ref"),
		extractStringFromMap(parsed.Data.Customer.Metadata, "reference", "payment_reference", "tx_ref"),
	)
	paymentRef := selectBestReference(parsed.Data.Reference, metaRef)

	authChannel := extractStringFromMap(parsed.Data.Authorization, "channel")
	isDVA := strings.EqualFold(parsed.Data.Channel, "dedicated_nuban") ||
		strings.EqualFold(authChannel, "dedicated_nuban") ||
		strings.HasPrefix(strings.ToLower(parsed.Event), "dedicatedaccount")

	eventID := stringifyID(parsed.Data.ID)
	if eventID == "" {
		eventID = hashPayloadID(body)
	} else if parsed.Event != "" {
		eventID = parsed.Event + ":" + eventID
	}

	return &NormalizedWebhook{
		Provider:         Paystack,
		ProviderEventID:  eventID,
		EventType:        parsed.Event,
		PaymentReference: paymentRef,
		IsDVA:            isDVA,
		RawHeaders:       flattenHeaders(headers),
		RawPayload:       body,
	}, nil
}

// FlutterwaveService verifies and normalizes Flutterwave webhooks.
type FlutterwaveService struct {
	secretHash string
}

// NewFlutterwaveService creates a new Flutterwave webhook verifier and parser.
func NewFlutterwaveService(secretHash string) *FlutterwaveService {
	return &FlutterwaveService{secretHash: strings.TrimSpace(secretHash)}
}

func (s *FlutterwaveService) Provider() PSPProvider {
	return Flutterwave
}

type flutterwaveWebhookPayload struct {
	Event    string         `json:"event"`
	EventType string        `json:"event.type"`
	MetaData map[string]any `json:"meta_data"`
	Data     struct {
		ID          any            `json:"id"`
		TxRef       string         `json:"tx_ref"`
		TxRefCamel  string         `json:"txRef"`
		FlwRef      string         `json:"flw_ref"`
		PaymentType string         `json:"payment_type"`
		Meta        map[string]any `json:"meta"`
	} `json:"data"`
}

func (s *FlutterwaveService) VerifyAndParse(_ context.Context, headers http.Header, body []byte) (*NormalizedWebhook, error) {
	if s.secretHash == "" {
		return nil, ErrSecretNotFound
	}

	verifHash := strings.TrimSpace(headers.Get("verif-hash"))
	if verifHash == "" {
		verifHash = strings.TrimSpace(headers.Get("X-Flutterwave-Signature"))
	}
	if verifHash == "" {
		return nil, ErrMissingSignature
	}

	if subtle.ConstantTimeCompare([]byte(verifHash), []byte(s.secretHash)) != 1 {
		return nil, ErrInvalidSignature
	}

	var parsed flutterwaveWebhookPayload
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}

	primaryRef := firstNonEmptyString(parsed.Data.TxRef, parsed.Data.TxRefCamel)
	metaRef := firstNonEmptyString(
		extractStringFromMap(parsed.MetaData, "reference", "payment_reference", "tx_ref"),
		extractStringFromMap(parsed.Data.Meta, "reference", "payment_reference", "tx_ref"),
	)
	paymentRef := selectBestReference(primaryRef, metaRef)
	if paymentRef == "" {
		paymentRef = parsed.Data.FlwRef
	}

	eventType := firstNonEmptyString(parsed.Event, parsed.EventType)
	isDVA := strings.EqualFold(parsed.Data.PaymentType, "bank_transfer") ||
		strings.EqualFold(parsed.EventType, "BANK_TRANSFER_TRANSACTION")

	eventID := stringifyID(parsed.Data.ID)
	if eventID == "" {
		eventID = hashPayloadID(body)
	} else if eventType != "" {
		eventID = eventType + ":" + eventID
	}

	return &NormalizedWebhook{
		Provider:         Flutterwave,
		ProviderEventID:  eventID,
		EventType:        eventType,
		PaymentReference: paymentRef,
		IsDVA:            isDVA,
		RawHeaders:       flattenHeaders(headers),
		RawPayload:       body,
	}, nil
}

// MonnifyService verifies and normalizes Monnify webhooks.
type MonnifyService struct {
	secretKey string
}

// NewMonnifyService creates a new Monnify webhook verifier and parser.
func NewMonnifyService(secretKey string) *MonnifyService {
	return &MonnifyService{secretKey: strings.TrimSpace(secretKey)}
}

func (s *MonnifyService) Provider() PSPProvider {
	return Monnify
}

type monnifyWebhookPayload struct {
	EventType string `json:"eventType"`
	EventData struct {
		TransactionReference string         `json:"transactionReference"`
		PaymentReference     string         `json:"paymentReference"`
		MetaData             map[string]any `json:"metaData"`
		Product              struct {
			Type      string `json:"type"`
			Reference string `json:"reference"`
		} `json:"product"`
	} `json:"eventData"`
}

func (s *MonnifyService) VerifyAndParse(_ context.Context, headers http.Header, body []byte) (*NormalizedWebhook, error) {
	if s.secretKey == "" {
		return nil, ErrSecretNotFound
	}

	signature := strings.TrimSpace(headers.Get("monnify-signature"))
	if signature == "" {
		return nil, ErrMissingSignature
	}

	if !verifyHMACSHA512(body, s.secretKey, signature) {
		return nil, ErrInvalidSignature
	}

	var parsed monnifyWebhookPayload
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}

	metaRef := firstNonEmptyString(
		parsed.EventData.Product.Reference,
		extractStringFromMap(parsed.EventData.MetaData, "reference", "payment_reference", "accountReference"),
	)
	paymentRef := selectBestReference(parsed.EventData.PaymentReference, metaRef)

	isDVA := strings.EqualFold(parsed.EventData.Product.Type, "RESERVED_ACCOUNT") ||
		strings.EqualFold(parsed.EventData.Product.Type, "DYNAMIC_ACCOUNT")

	eventID := strings.TrimSpace(parsed.EventData.TransactionReference)
	if eventID == "" {
		eventID = hashPayloadID(body)
	} else if parsed.EventType != "" {
		eventID = parsed.EventType + ":" + eventID
	}

	return &NormalizedWebhook{
		Provider:         Monnify,
		ProviderEventID:  eventID,
		EventType:        parsed.EventType,
		PaymentReference: paymentRef,
		IsDVA:            isDVA,
		RawHeaders:       flattenHeaders(headers),
		RawPayload:       body,
	}, nil
}

// ComputeHMACSHA512Hex computes a lowercase hex-encoded HMAC-SHA512 digest.
func ComputeHMACSHA512Hex(payload []byte, secret string) string {
	mac := hmac.New(sha512.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func verifyHMACSHA512(payload []byte, secret, providedHexSig string) bool {
	expectedHex := ComputeHMACSHA512Hex(payload, secret)
	return hmac.Equal([]byte(strings.ToLower(providedHexSig)), []byte(expectedHex))
}

func selectBestReference(primaryRef, fallbackRef string) string {
	p := strings.TrimSpace(primaryRef)
	f := strings.TrimSpace(fallbackRef)

	// If fallbackRef contains a prefix separator ('_' or '-') and primaryRef does not,
	// prefer fallbackRef (e.g. DVA accountReference or metadata reference over random PSP NIP reference).
	if f != "" && strings.ContainsAny(f, "_-") && !strings.ContainsAny(p, "_-") {
		return f
	}
	if p != "" {
		return p
	}
	return f
}

func extractStringFromMap(m map[string]any, keys ...string) string {
	if len(m) == 0 {
		return ""
	}
	for _, k := range keys {
		if val, ok := m[k]; ok {
			if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func firstNonEmptyString(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func stringifyID(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return strings.TrimSpace(val)
	case float64:
		return fmt.Sprintf("%.0f", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func hashPayloadID(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:16])
}

func flattenHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vals := range h {
		if len(vals) > 0 {
			out[k] = vals[0]
		}
	}
	return out
}
