// Package auth resolves tenant identity from API keys.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Context keys.
type ctxKey int

const accountKey ctxKey = 1

// Account is the authenticated tenant.
type Account struct {
	ID   uuid.UUID
	Name string
}

// ErrUnauthorized is returned when no valid API key is present.
var ErrUnauthorized = errors.New("unauthorized")

// HashAPIKey returns the hex-encoded SHA-256 of an API key.
// High-entropy 256-bit keys make unsalted SHA-256 appropriate; optional
// AUTH_KEY_PEPPER env adds defense-in-depth for shared environments.
func HashAPIKey(apiKey string) string {
	payload := apiKey
	if p := os.Getenv("AUTH_KEY_PEPPER"); p != "" {
		payload = p + ":" + apiKey
	}
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// GenerateAPIKey creates a random 32-byte hex API key.
func GenerateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Middleware authenticates Bearer API keys and injects Account into context.
func Middleware(q *sqlc.Queries) func(http.Handler) http.Handler {
	return MiddlewareWithCache(q, NewAccountCache(30*time.Second))
}

// MiddlewareWithCache is like Middleware but uses the provided account cache.
func MiddlewareWithCache(q *sqlc.Queries, cache *AccountCache) func(http.Handler) http.Handler {
	if cache == nil {
		cache = NewAccountCache(30 * time.Second)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := r.Header.Get("Authorization")
			if raw == "" {
				http.Error(w, `{"error":"missing Authorization header"}`, http.StatusUnauthorized)
				return
			}
			const prefix = "Bearer "
			if !strings.HasPrefix(raw, prefix) {
				http.Error(w, `{"error":"invalid Authorization header"}`, http.StatusUnauthorized)
				return
			}
			apiKey := strings.TrimSpace(strings.TrimPrefix(raw, prefix))
			if apiKey == "" {
				http.Error(w, `{"error":"empty API key"}`, http.StatusUnauthorized)
				return
			}
			keyHash := HashAPIKey(apiKey)
			if acc, ok := cache.Get(keyHash); ok {
				ctx := context.WithValue(r.Context(), accountKey, acc)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			row, err := q.GetAccountByAPIKeyHash(r.Context(), keyHash)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					http.Error(w, `{"error":"invalid API key"}`, http.StatusUnauthorized)
					return
				}
				http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
				return
			}
			acc := Account{ID: row.ID, Name: row.Name}
			cache.Set(keyHash, acc)
			ctx := context.WithValue(r.Context(), accountKey, acc)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// FromContext returns the authenticated account or ErrUnauthorized.
func FromContext(ctx context.Context) (Account, error) {
	acc, ok := ctx.Value(accountKey).(Account)
	if !ok {
		return Account{}, ErrUnauthorized
	}
	return acc, nil
}
