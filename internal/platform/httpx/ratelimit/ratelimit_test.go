package ratelimit_test

import (
	"net/http"
	"testing"

	"github.com/amiri/sms-gateway/internal/platform/httpx/ratelimit"
)

func TestClientIPDefaultsToRemoteAddr(t *testing.T) {
	t.Setenv("TRUST_PROXY", "")
	r, _ := http.NewRequest(http.MethodPost, "/v1/accounts", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	if got := ratelimit.ClientIP(r); got != "10.0.0.1" {
		t.Fatalf("expected RemoteAddr host, got %q", got)
	}
}

func TestClientIPHonorsXFFWhenTrustProxy(t *testing.T) {
	t.Setenv("TRUST_PROXY", "1")
	r, _ := http.NewRequest(http.MethodPost, "/v1/accounts", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	if got := ratelimit.ClientIP(r); got != "203.0.113.9" {
		t.Fatalf("got %q", got)
	}
}
