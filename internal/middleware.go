package internal

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

const RFC7807ProblemType = "https://tools.ietf.org/html/rfc7807"

type ProblemDetails struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

// NewProblemDetails constructs an RFC 7807 compliant problem details payload.
func NewProblemDetails(status int, detail string) ProblemDetails {
	return ProblemDetails{
		Type:   RFC7807ProblemType,
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
	}
}

// ConstructProblemDetails constructs ProblemDetails with optional pointer values safely.
func ConstructProblemDetails(status *int, detail *string, title *string, _type *string) ProblemDetails {
	pd := ProblemDetails{
		Type: RFC7807ProblemType,
	}
	if status != nil {
		pd.Status = *status
		pd.Title = http.StatusText(*status)
	}
	if detail != nil {
		pd.Detail = *detail
	}
	if title != nil {
		pd.Title = *title
	}
	if _type != nil {
		pd.Type = *_type
	}
	return pd
}

func (p *ProblemDetails) Respond(c *gin.Context) {
	c.Header("Content-Type", "application/problem+json")
	c.AbortWithStatusJSON(p.Status, p)
}

func ErrorHandlerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// Do not write a response if headers were already committed or no error occurred
		if c.Writer.Written() || len(c.Errors) == 0 {
			return
		}

		err := c.Errors.Last().Err
		var statusCode int
		var detail string

		switch {
		// 404 Not Found
		case errors.Is(err, ErrProductNotFound),
			errors.Is(err, ErrEndpointNotFound),
			errors.Is(err, ErrProviderNotFound),
			errors.Is(err, ErrKeyNotFound):
			statusCode = http.StatusNotFound
			detail = err.Error()

		// 409 Conflict
		case errors.Is(err, ErrLockAcquisitionFailed),
			errors.Is(err, ErrIdempotencyConflict):
			statusCode = http.StatusConflict
			detail = err.Error()

		// 405 Method Not Allowed
		case errors.Is(err, ErrInvalidMethod):
			statusCode = http.StatusMethodNotAllowed
			detail = err.Error()

		// 400 Bad Request
		case errors.Is(err, ErrUnsupportedProvider),
			errors.Is(err, ErrMissingSignature),
			errors.Is(err, ErrInvalidPayload):
			statusCode = http.StatusBadRequest
			detail = err.Error()

		// 401 Unauthorized
		case errors.Is(err, ErrInvalidSignature):
			statusCode = http.StatusUnauthorized
			detail = err.Error()

		// 500 Server Configuration Error (Masked to prevent internal leakage)
		case errors.Is(err, ErrSecretNotFound):
			statusCode = http.StatusInternalServerError
			detail = "service security configuration error"

		// Default fallback for unhandled internal errors
		default:
			statusCode = http.StatusInternalServerError
			detail = "internal server error"
		}

		problemDetail := NewProblemDetails(statusCode, detail)
		problemDetail.Respond(c)
	}
}


func CorsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, X-Request-ID, Idempotency-Key")
		c.Header("Access-Control-Allow-Methods", "POST, HEAD, PATCH, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}