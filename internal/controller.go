package internal

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

const maxWebhookBodyBytes = 5 * 1024 * 1024 // 5MB

// HealthController handles health verification endpoints.
type HealthController struct {
	useCase HealthCheckUseCase
}

// NewHealthController creates a new HealthController.
func NewHealthController(useCase HealthCheckUseCase) *HealthController {
	return &HealthController{
		useCase: useCase,
	}
}

func (c *HealthController) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/healthz", c.Health)
}

func (c *HealthController) Health(g *gin.Context) {
	resp, err := c.useCase.Execute(g.Request.Context())
	if err != nil {
		g.JSON(http.StatusServiceUnavailable, resp)
		return
	}

	g.JSON(http.StatusOK, resp)
}

// PSPWebhookController handles incoming webhook requests from upstream PSPs.
type PSPWebhookController struct {
	useCase ReceiverUseCase
}

// NewPSPWebhookController creates a new PSPWebhookController wired to ReceiverUseCase.
func NewPSPWebhookController(useCase ReceiverUseCase) *PSPWebhookController {
	return &PSPWebhookController{
		useCase: useCase,
	}
}

func (c *PSPWebhookController) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/webhook/:provider", c.HandleWebhook)
}

func (c *PSPWebhookController) HandleWebhook(g *gin.Context) {
	provider := g.Param("provider")
	if !IsValidProvider(provider) {
		_ = g.Error(ErrProviderNotFound)
		return
	}

	if g.Request.Body == nil {
		_ = g.Error(ErrInvalidPayload)
		return
	}

	body, err := io.ReadAll(io.LimitReader(g.Request.Body, maxWebhookBodyBytes))
	if err != nil || len(body) == 0 {
		_ = g.Error(fmt.Errorf("%w: empty or unreadable request body", ErrInvalidPayload))
		return
	}

	event, err := c.useCase.Execute(g.Request.Context(), provider, g.Request.Header, body)
	if err != nil {
		_ = g.Error(err)
		return
	}

	g.JSON(http.StatusOK, gin.H{
		"status":   "accepted",
		"event_id": event.ID,
		"provider": event.Provider,
	})
}
