package internal

import (
	"crypto/subtle"
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
			errors.Is(err, ErrKeyNotFound),
			errors.Is(err, ErrEventNotFound),
			errors.Is(err, ErrFailureNotFound):
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

// AdminAuthMiddleware enforces HTTP Basic Authentication on admin UI and REST routes
// whenever username or password is configured. In production, credentials are required.
// It also protects against cross-site request forgery by rejecting cross-site mutating requests.
func AdminAuthMiddleware(username, password string, env Environment) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead && c.Request.Method != http.MethodOptions {
			if c.GetHeader("Sec-Fetch-Site") == "cross-site" {
				pd := NewProblemDetails(http.StatusForbidden, "cross-site requests not allowed")
				pd.Respond(c)
				return
			}
		}

		if username == "" && password == "" {
			if env == EnvironmentProduction {
				c.Header("WWW-Authenticate", `Basic realm="PSP Relay Admin"`)
				pd := NewProblemDetails(http.StatusUnauthorized, "admin access disabled: ADMIN_USERNAME and ADMIN_PASSWORD must be configured in production")
				pd.Respond(c)
				return
			}
			c.Next()
			return
		}

		reqUser, reqPass, ok := c.Request.BasicAuth()
		userMatch := subtle.ConstantTimeCompare([]byte(reqUser), []byte(username)) == 1
		passMatch := subtle.ConstantTimeCompare([]byte(reqPass), []byte(password)) == 1
		if !ok || !userMatch || !passMatch {
			c.Header("WWW-Authenticate", `Basic realm="PSP Relay Admin"`)
			pd := NewProblemDetails(http.StatusUnauthorized, "invalid or missing admin credentials")
			pd.Respond(c)
			return
		}

		c.Next()
	}
}
