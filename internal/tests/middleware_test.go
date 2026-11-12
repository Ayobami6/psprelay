package tests

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"psprelay/internal"

	"github.com/gin-gonic/gin"
)

func TestErrorHandlerMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name               string
		triggerErr         error
		alreadyWritten     bool
		expectedStatus     int
		expectedTitle      string
		expectedDetail     string
		expectProblemResp  bool
	}{
		{
			name:              "ErrKeyNotFound -> 404 Not Found",
			triggerErr:        internal.ErrKeyNotFound,
			expectedStatus:    http.StatusNotFound,
			expectedTitle:     "Not Found",
			expectedDetail:    internal.ErrKeyNotFound.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrProductNotFound -> 404 Not Found",
			triggerErr:        internal.ErrProductNotFound,
			expectedStatus:    http.StatusNotFound,
			expectedTitle:     "Not Found",
			expectedDetail:    internal.ErrProductNotFound.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrEndpointNotFound -> 404 Not Found",
			triggerErr:        internal.ErrEndpointNotFound,
			expectedStatus:    http.StatusNotFound,
			expectedTitle:     "Not Found",
			expectedDetail:    internal.ErrEndpointNotFound.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrProviderNotFound -> 404 Not Found",
			triggerErr:        internal.ErrProviderNotFound,
			expectedStatus:    http.StatusNotFound,
			expectedTitle:     "Not Found",
			expectedDetail:    internal.ErrProviderNotFound.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrLockAcquisitionFailed -> 409 Conflict",
			triggerErr:        internal.ErrLockAcquisitionFailed,
			expectedStatus:    http.StatusConflict,
			expectedTitle:     "Conflict",
			expectedDetail:    internal.ErrLockAcquisitionFailed.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrIdempotencyConflict -> 409 Conflict",
			triggerErr:        internal.ErrIdempotencyConflict,
			expectedStatus:    http.StatusConflict,
			expectedTitle:     "Conflict",
			expectedDetail:    internal.ErrIdempotencyConflict.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrInvalidMethod -> 405 Method Not Allowed",
			triggerErr:        internal.ErrInvalidMethod,
			expectedStatus:    http.StatusMethodNotAllowed,
			expectedTitle:     "Method Not Allowed",
			expectedDetail:    internal.ErrInvalidMethod.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrUnsupportedProvider -> 400 Bad Request",
			triggerErr:        internal.ErrUnsupportedProvider,
			expectedStatus:    http.StatusBadRequest,
			expectedTitle:     "Bad Request",
			expectedDetail:    internal.ErrUnsupportedProvider.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrMissingSignature -> 400 Bad Request",
			triggerErr:        internal.ErrMissingSignature,
			expectedStatus:    http.StatusBadRequest,
			expectedTitle:     "Bad Request",
			expectedDetail:    internal.ErrMissingSignature.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrInvalidSignature -> 401 Unauthorized",
			triggerErr:        internal.ErrInvalidSignature,
			expectedStatus:    http.StatusUnauthorized,
			expectedTitle:     "Unauthorized",
			expectedDetail:    internal.ErrInvalidSignature.Error(),
			expectProblemResp: true,
		},
		{
			name:              "ErrSecretNotFound -> 500 Internal Server Error",
			triggerErr:        internal.ErrSecretNotFound,
			expectedStatus:    http.StatusInternalServerError,
			expectedTitle:     "Internal Server Error",
			expectedDetail:    "service security configuration error",
			expectProblemResp: true,
		},
		{
			name:              "Unknown error -> 500 Internal Server Error",
			triggerErr:        errors.New("unexpected database crash"),
			expectedStatus:    http.StatusInternalServerError,
			expectedTitle:     "Internal Server Error",
			expectedDetail:    "internal server error",
			expectProblemResp: true,
		},
		{
			name:              "No errors -> 200 OK without problem details",
			triggerErr:        nil,
			expectedStatus:    http.StatusOK,
			expectProblemResp: false,
		},
		{
			name:              "Already written response does not overwrite",
			triggerErr:        internal.ErrProductNotFound,
			alreadyWritten:    true,
			expectedStatus:    http.StatusOK,
			expectProblemResp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.Use(internal.ErrorHandlerMiddleware())

			router.GET("/test", func(c *gin.Context) {
				if tt.alreadyWritten {
					c.String(http.StatusOK, "already processed")
				}
				if tt.triggerErr != nil {
					_ = c.Error(tt.triggerErr)
				}
			})

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Fatalf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			if tt.expectProblemResp {
				contentType := w.Header().Get("Content-Type")
				if contentType != "application/problem+json" {
					t.Errorf("expected Content-Type application/problem+json, got %q", contentType)
				}

				var pd internal.ProblemDetails
				if err := json.Unmarshal(w.Body.Bytes(), &pd); err != nil {
					t.Fatalf("failed to unmarshal ProblemDetails response: %v", err)
				}

				if pd.Status != tt.expectedStatus {
					t.Errorf("expected ProblemDetails.Status %d, got %d", tt.expectedStatus, pd.Status)
				}
				if pd.Title != tt.expectedTitle {
					t.Errorf("expected ProblemDetails.Title %q, got %q", tt.expectedTitle, pd.Title)
				}
				if pd.Detail != tt.expectedDetail {
					t.Errorf("expected ProblemDetails.Detail %q, got %q", tt.expectedDetail, pd.Detail)
				}
				if pd.Type != internal.RFC7807ProblemType {
					t.Errorf("expected ProblemDetails.Type %q, got %q", internal.RFC7807ProblemType, pd.Type)
				}
			}
		})
	}
}

func TestConstructProblemDetails_SafeFromPanics(t *testing.T) {
	// Verify calling ConstructProblemDetails with all nil pointers no longer panics
	pd := internal.ConstructProblemDetails(nil, nil, nil, nil)
	if pd.Type != internal.RFC7807ProblemType {
		t.Errorf("expected default Type %q, got %q", internal.RFC7807ProblemType, pd.Type)
	}
	if pd.Status != 0 {
		t.Errorf("expected zero status, got %d", pd.Status)
	}
}
