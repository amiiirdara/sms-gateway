// Package ratelimit provides Redis token-bucket HTTP middleware.
package ratelimit

import (
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/amiri/sms-gateway/internal/platform/httpx"
	"github.com/amiri/sms-gateway/internal/platform/httpx/auth"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
)

// Config holds bucket capacity and steady-state refill rate.
type Config struct {
	Capacity     int64
	RefillPerSec float64
}

// ByIP limits unauthenticated endpoints (e.g. POST /v1/accounts) per client IP.
func ByIP(rdb *platredis.Client, cfg Config, scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := ClientIP(r)
			ok, err := rdb.TakeToken(r.Context(), platredis.SignupRateLimitKey(ip), cfg.Capacity, cfg.RefillPerSec, 1)
			if err != nil {
				log.Printf("ratelimit: %s: %v", scope, err)
				httpx.Error(w, http.StatusInternalServerError, "rate limiter unavailable")
				return
			}
			if !ok {
				metrics.RateLimited.WithLabelValues(scope).Inc()
				httpx.Error(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ByAccount limits authenticated endpoints per tenant account_id.
func ByAccount(rdb *platredis.Client, cfg Config, scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			acc, err := auth.FromContext(r.Context())
			if err != nil {
				httpx.Error(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			allowed, err := rdb.TakeToken(r.Context(), platredis.RateLimitKey(acc.ID.String()), cfg.Capacity, cfg.RefillPerSec, 1)
			if err != nil {
				log.Printf("ratelimit: %s: %v", scope, err)
				httpx.Error(w, http.StatusInternalServerError, "rate limiter unavailable")
				return
			}
			if !allowed {
				metrics.RateLimited.WithLabelValues(scope).Inc()
				httpx.Error(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ClientIP returns the client address for rate limiting.
//
// By default this is RemoteAddr only (safe when the gateway is directly exposed,
// as in local Compose). Set TRUST_PROXY=1 to honor the first X-Forwarded-For hop
// when a trusted reverse proxy sits in front and overwrites/appends XFF correctly.
func ClientIP(r *http.Request) string {
	if trustProxyEnabled() {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if len(parts) > 0 {
				ip := strings.TrimSpace(parts[0])
				if ip != "" {
					return ip
				}
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func trustProxyEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("TRUST_PROXY")))
	return v == "1" || v == "true" || v == "yes"
}
