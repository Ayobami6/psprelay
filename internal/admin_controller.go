package internal

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

//go:embed templates/*.html
var adminTemplatesFS embed.FS

// AdminController serves both the HTMX Management UI (/admin) and JSON Admin REST endpoints (/v1/admin).
type AdminController struct {
	useCase ManagementUseCase
	tmpl    *template.Template
}

// NewAdminController constructs an AdminController and parses the embedded HTML templates.
func NewAdminController(useCase ManagementUseCase) *AdminController {
	tmpl := template.Must(template.ParseFS(adminTemplatesFS, "templates/*.html"))
	return &AdminController{
		useCase: useCase,
		tmpl:    tmpl,
	}
}

// RegisterUIRoutes mounts the server-rendered HTMX management console routes onto the /admin router group.
func (c *AdminController) RegisterUIRoutes(rg *gin.RouterGroup) {
	rg.GET("", c.RenderDashboard)
	rg.GET("/", c.RenderDashboard)
	rg.GET("/partials/events", c.RenderEventsPartial)
	rg.GET("/partials/events/:id", c.RenderEventDetailPartial)
	rg.GET("/partials/failures", c.RenderFailuresPartial)
	rg.POST("/failures/:id/retry", c.HandleUISingleRetry)
	rg.POST("/failures/bulk-retry", c.HandleUIBulkRetry)
	rg.POST("/events/:id/replay", c.HandleUIReplayEvent)
}

// RegisterAPIRoutes mounts the JSON REST management endpoints onto the /v1/admin router group.
func (c *AdminController) RegisterAPIRoutes(rg *gin.RouterGroup) {
	rg.GET("/overview", c.GetOverviewAPI)
	rg.GET("/events", c.ListEventsAPI)
	rg.GET("/events/:id", c.GetEventDetailAPI)
	rg.POST("/events/:id/replay", c.ReplayEventAPI)
	rg.GET("/failures", c.ListFailuresAPI)
	rg.POST("/failures/:id/retry", c.RetryFailureAPI)
	rg.POST("/failures/bulk-retry", c.BulkRetryFailuresAPI)
}

type dashboardViewData struct {
	Overview      *DashboardOverview
	ActiveTab     string
	ActiveProduct string
	EventsPage    *PaginatedEvents
	FailuresPage  *failuresPartialData
}

type failuresPartialData struct {
	Items  []DeliveryFailure
	Total  int
	Limit  int
	Offset int
	Notice string
}

type eventDetailPartialData struct {
	Event         WebhookEvent
	PrettyPayload string
	Failures      []DeliveryFailure
	Notice        string
}

// RenderDashboard renders the main HTMX management UI console.
func (c *AdminController) RenderDashboard(g *gin.Context) {
	ctx := g.Request.Context()
	overview, err := c.useCase.GetDashboardOverview(ctx)
	if err != nil {
		_ = g.Error(err)
		return
	}

	tab := strings.ToLower(strings.TrimSpace(g.DefaultQuery("tab", "events")))
	if tab != "failures" {
		tab = "events"
	}
	product := strings.TrimSpace(g.Query("product"))

	data := dashboardViewData{
		Overview:      overview,
		ActiveTab:     tab,
		ActiveProduct: product,
	}

	if tab == "failures" {
		page, fErr := c.useCase.ListFailures(ctx, parseFailureFilter(g))
		if fErr != nil {
			_ = g.Error(fErr)
			return
		}
		data.FailuresPage = &failuresPartialData{
			Items:  page.Items,
			Total:  page.Total,
			Limit:  page.Limit,
			Offset: page.Offset,
		}
	} else {
		page, eErr := c.useCase.ListEvents(ctx, parseEventFilter(g))
		if eErr != nil {
			_ = g.Error(eErr)
			return
		}
		data.EventsPage = page
	}

	c.renderTemplate(g, http.StatusOK, "dashboard", data)
}

// RenderEventsPartial renders the HTMX table partial for filtered webhook events.
func (c *AdminController) RenderEventsPartial(g *gin.Context) {
	page, err := c.useCase.ListEvents(g.Request.Context(), parseEventFilter(g))
	if err != nil {
		_ = g.Error(err)
		return
	}
	c.renderTemplate(g, http.StatusOK, "events_table", page)
}

// RenderEventDetailPartial renders the slide-over inspector drawer for a specific webhook event.
func (c *AdminController) RenderEventDetailPartial(g *gin.Context) {
	detail, err := c.useCase.GetEventDetail(g.Request.Context(), g.Param("id"))
	if err != nil {
		_ = g.Error(err)
		return
	}
	c.renderTemplate(g, http.StatusOK, "event_detail", eventDetailPartialData{
		Event:         detail.Event,
		PrettyPayload: detail.PrettyPayload,
		Failures:      detail.Failures,
	})
}

// RenderFailuresPartial renders the HTMX table partial for filtered delivery failures.
func (c *AdminController) RenderFailuresPartial(g *gin.Context) {
	page, err := c.useCase.ListFailures(g.Request.Context(), parseFailureFilter(g))
	if err != nil {
		_ = g.Error(err)
		return
	}
	c.renderTemplate(g, http.StatusOK, "failures_table", failuresPartialData{
		Items:  page.Items,
		Total:  page.Total,
		Limit:  page.Limit,
		Offset: page.Offset,
	})
}

// HandleUISingleRetry executes an immediate synchronous HTTP retry for a single delivery failure and returns the updated row HTML.
func (c *AdminController) HandleUISingleRetry(g *gin.Context) {
	updated, err := c.useCase.RetryFailureNow(g.Request.Context(), g.Param("id"))
	if err != nil {
		_ = g.Error(err)
		return
	}
	c.renderTemplate(g, http.StatusOK, "failure_row", updated)
}

// HandleUIBulkRetry resets matching delivery failures to RETRYING and re-renders the failures table partial.
func (c *AdminController) HandleUIBulkRetry(g *gin.Context) {
	ctx := g.Request.Context()
	filter := parseFailureFilter(g)
	count, err := c.useCase.BulkRetryFailures(ctx, filter.Product, filter.Provider)
	if err != nil {
		_ = g.Error(err)
		return
	}

	page, listErr := c.useCase.ListFailures(ctx, filter)
	if listErr != nil {
		_ = g.Error(listErr)
		return
	}

	c.renderTemplate(g, http.StatusOK, "failures_table", failuresPartialData{
		Items:  page.Items,
		Total:  page.Total,
		Limit:  page.Limit,
		Offset: page.Offset,
		Notice: fmt.Sprintf("Queued %d delivery failure(s) for immediate background retry.", count),
	})
}

// HandleUIReplayEvent replays a webhook event synchronously and re-renders the event inspector drawer.
func (c *AdminController) HandleUIReplayEvent(g *gin.Context) {
	ctx := g.Request.Context()
	eventID := g.Param("id")
	if _, err := c.useCase.ReplayEventNow(ctx, eventID); err != nil {
		_ = g.Error(err)
		return
	}

	detail, err := c.useCase.GetEventDetail(ctx, eventID)
	if err != nil {
		_ = g.Error(err)
		return
	}

	c.renderTemplate(g, http.StatusOK, "event_detail", eventDetailPartialData{
		Event:         detail.Event,
		PrettyPayload: detail.PrettyPayload,
		Failures:      detail.Failures,
		Notice:        "Webhook event replayed across target product endpoints.",
	})
}

// GetOverviewAPI returns the JSON DashboardOverview.
func (c *AdminController) GetOverviewAPI(g *gin.Context) {
	overview, err := c.useCase.GetDashboardOverview(g.Request.Context())
	if err != nil {
		_ = g.Error(err)
		return
	}
	g.JSON(http.StatusOK, overview)
}

// ListEventsAPI returns a JSON paginated list of webhook events.
func (c *AdminController) ListEventsAPI(g *gin.Context) {
	page, err := c.useCase.ListEvents(g.Request.Context(), parseEventFilter(g))
	if err != nil {
		_ = g.Error(err)
		return
	}
	g.JSON(http.StatusOK, page)
}

// GetEventDetailAPI returns the JSON details and failure history for a single webhook event.
func (c *AdminController) GetEventDetailAPI(g *gin.Context) {
	detail, err := c.useCase.GetEventDetail(g.Request.Context(), g.Param("id"))
	if err != nil {
		_ = g.Error(err)
		return
	}
	g.JSON(http.StatusOK, detail)
}

// ReplayEventAPI triggers an immediate synchronous replay of a webhook event and returns the updated event JSON.
func (c *AdminController) ReplayEventAPI(g *gin.Context) {
	ev, err := c.useCase.ReplayEventNow(g.Request.Context(), g.Param("id"))
	if err != nil {
		_ = g.Error(err)
		return
	}
	g.JSON(http.StatusOK, gin.H{
		"status": "replayed",
		"event":  ev,
	})
}

// ListFailuresAPI returns a JSON paginated list of delivery failures.
func (c *AdminController) ListFailuresAPI(g *gin.Context) {
	page, err := c.useCase.ListFailures(g.Request.Context(), parseFailureFilter(g))
	if err != nil {
		_ = g.Error(err)
		return
	}
	g.JSON(http.StatusOK, page)
}

// RetryFailureAPI executes an immediate synchronous HTTP retry for a single delivery failure and returns JSON.
func (c *AdminController) RetryFailureAPI(g *gin.Context) {
	updated, err := c.useCase.RetryFailureNow(g.Request.Context(), g.Param("id"))
	if err != nil {
		_ = g.Error(err)
		return
	}
	g.JSON(http.StatusOK, gin.H{
		"status":  "retried",
		"failure": updated,
	})
}

type bulkRetryRequest struct {
	Product  string `json:"product" form:"product"`
	Provider string `json:"provider" form:"provider"`
}

// BulkRetryFailuresAPI resets matching delivery failures to RETRYING for asynchronous worker execution.
func (c *AdminController) BulkRetryFailuresAPI(g *gin.Context) {
	var req bulkRetryRequest
	_ = g.ShouldBind(&req)
	if req.Product == "" {
		req.Product = g.Query("product")
	}
	if req.Provider == "" {
		req.Provider = g.Query("provider")
	}

	count, err := c.useCase.BulkRetryFailures(
		g.Request.Context(),
		strings.TrimSpace(req.Product),
		PSPProvider(strings.ToLower(strings.TrimSpace(req.Provider))),
	)
	if err != nil {
		_ = g.Error(err)
		return
	}

	g.JSON(http.StatusOK, gin.H{
		"status":       "queued",
		"queued_count": count,
	})
}

func (c *AdminController) renderTemplate(g *gin.Context, status int, name string, data any) {
	var buf bytes.Buffer
	if err := c.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		_ = g.Error(err)
		return
	}
	g.Data(status, "text/html; charset=utf-8", buf.Bytes())
}

func parseEventFilter(g *gin.Context) EventFilter {
	limit, _ := strconv.Atoi(g.DefaultQuery("limit", "25"))
	offset, _ := strconv.Atoi(g.DefaultQuery("offset", "0"))
	status := strings.ToUpper(strings.TrimSpace(g.Query("status")))
	if status != string(WebhookStatusPending) &&
		status != string(WebhookStatusPublished) &&
		status != string(WebhookStatusDelivered) &&
		status != string(WebhookStatusFailed) {
		status = ""
	}
	return EventFilter{
		Product:  strings.TrimSpace(g.Query("product")),
		Provider: PSPProvider(strings.ToLower(strings.TrimSpace(g.Query("provider")))),
		Status:   WebhookStatus(status),
		Search:   strings.TrimSpace(g.Query("q")),
		Limit:    limit,
		Offset:   offset,
	}
}

func parseFailureFilter(g *gin.Context) FailureFilter {
	limit, _ := strconv.Atoi(g.DefaultQuery("limit", "25"))
	offset, _ := strconv.Atoi(g.DefaultQuery("offset", "0"))
	status := strings.ToUpper(strings.TrimSpace(g.Query("status")))
	if status != string(DeliveryStatusRetrying) &&
		status != string(DeliveryStatusFailed) &&
		status != string(DeliveryStatusResolved) {
		status = ""
	}
	return FailureFilter{
		Product:  strings.TrimSpace(g.Query("product")),
		Provider: PSPProvider(strings.ToLower(strings.TrimSpace(g.Query("provider")))),
		Status:   DeliveryFailureStatus(status),
		EventID:  strings.TrimSpace(g.Query("event_id")),
		Limit:    limit,
		Offset:   offset,
	}
}
