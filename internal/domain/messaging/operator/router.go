// Package operator defines the pluggable OperatorAdapter and prefix-based Router.
package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Adapter submits an SMS to a telecom operator.
type Adapter interface {
	Name() string
	Send(ctx context.Context, to, text, priority string) error
}

// HTTPAdapter calls a remote operator HTTP API (e.g. operator-mock).
type HTTPAdapter struct {
	name   string
	baseURL string
	client *http.Client
}

// NewHTTPAdapter creates an HTTP-backed adapter.
func NewHTTPAdapter(name, baseURL string) *HTTPAdapter {
	return &HTTPAdapter{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (a *HTTPAdapter) Name() string { return a.name }

func (a *HTTPAdapter) Send(ctx context.Context, to, text, priority string) error {
	body, _ := json.Marshal(map[string]string{
		"to": to, "text": text, "priority": priority,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/send", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("operator %s status %d", a.name, resp.StatusCode)
	}
	return nil
}

// RouteRule maps an E.164 prefix to an adapter name.
type RouteRule struct {
	Prefix string
	Adapter string
}

// Router picks an adapter by recipient number prefix.
type Router struct {
	rules   []RouteRule
	byName  map[string]Adapter
	fallback Adapter
}

// NewRouter builds a router. rules are evaluated longest-prefix-first.
func NewRouter(fallback Adapter, adapters []Adapter, rules []RouteRule) *Router {
	byName := make(map[string]Adapter, len(adapters))
	for _, a := range adapters {
		byName[a.Name()] = a
	}
	// Sort rules by prefix length descending for longest-match.
	sorted := append([]RouteRule(nil), rules...)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if len(sorted[j].Prefix) > len(sorted[i].Prefix) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	return &Router{rules: sorted, byName: byName, fallback: fallback}
}

// Resolve returns the adapter for a recipient number.
func (r *Router) Resolve(to string) Adapter {
	for _, rule := range r.rules {
		if strings.HasPrefix(to, rule.Prefix) {
			if a, ok := r.byName[rule.Adapter]; ok {
				return a
			}
		}
	}
	return r.fallback
}

// Send routes and submits.
func (r *Router) Send(ctx context.Context, to, text, priority string) (operatorName string, err error) {
	a := r.Resolve(to)
	return a.Name(), a.Send(ctx, to, text, priority)
}

// DefaultIranRules maps common Iranian MNO prefixes (E.164 +98…) to named adapters.
// In local demo all adapters may point at the same mock URL; production would use distinct endpoints.
func DefaultIranRules() []RouteRule {
	return []RouteRule{
		{Prefix: "+9891", Adapter: "mci"},      // Hamrah-e Avval (approx)
		{Prefix: "+9890", Adapter: "irancell"}, // Irancell (approx)
		{Prefix: "+9893", Adapter: "irancell"},
		{Prefix: "+9892", Adapter: "rightel"},
		{Prefix: "+98", Adapter: "default"},
	}
}
