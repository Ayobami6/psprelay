package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// UpstreamDispatcher defines the secondary port for outbound HTTP communication with upstream PSPs.
type UpstreamDispatcher interface {
	Send(ctx context.Context, targetURL string, method string, headers http.Header, body []byte) (statusCode int, respHeaders http.Header, respBody []byte, err error)
}

type defaultHTTPDispatcher struct {
	client *http.Client
}

// NewDefaultHTTPDispatcher creates a production-grade outbound HTTP client configured with bounded timeouts and connection pooling.
func NewDefaultHTTPDispatcher(timeout time.Duration) UpstreamDispatcher {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression: false,
	}

	return &defaultHTTPDispatcher{
		client: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
	}
}

func (d *defaultHTTPDispatcher) Send(ctx context.Context, targetURL string, method string, headers http.Header, body []byte) (int, http.Header, []byte, error) {
	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, targetURL, bodyReader)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("creating upstream request: %w", err)
	}

	// Copy allowed headers
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("dispatching request to upstream %q: %w", targetURL, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, resp.Header, nil, fmt.Errorf("reading upstream response body: %w", err)
	}

	return resp.StatusCode, resp.Header, respBody, nil
}
