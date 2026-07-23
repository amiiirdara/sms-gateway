// Package config loads runtime configuration from the environment.
//
// Kept dependency-free (stdlib only). Defaults match the Compose demo stack
// (see ARCHITECTURE.md section 14 and docker-compose.yml).
package config

import (
	"os"
	"strconv"
)

// Config holds the environment-derived settings shared across services.
// Individual services read only the fields they need.
type Config struct {
	ServiceName    string
	HTTPAddr       string
	DatabaseURL    string
	RedisAddr      string
	KafkaBrokers   string
	ClickHouseAddr     string
	ClickHousePassword string

	// SignupRateLimit: open POST /v1/accounts per client IP (token bucket).
	SignupRateCapacity int64
	SignupRateRefill   float64 // tokens per second

	// IngestRateLimit: authenticated POST /v1/messages and /v1/campaigns per account.
	IngestRateCapacity int64
	IngestRateRefill   float64
}

// Load reads configuration from the environment, applying sane local-dev
// defaults so every cmd/ binary can run standalone against the
// docker-compose stack without extra setup.
func Load(serviceName string) Config {
	return Config{
		ServiceName:        serviceName,
		HTTPAddr:           getEnv("HTTP_ADDR", ":8080"),
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:       getEnv("KAFKA_BROKERS", "localhost:9092"),
		ClickHouseAddr:     getEnv("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", "sms"),
		SignupRateCapacity: getEnvInt64("SIGNUP_RATE_CAPACITY", 30),
		SignupRateRefill:   getEnvFloat("SIGNUP_RATE_REFILL_PER_SEC", 0.5), // ~30/min steady
		IngestRateCapacity: getEnvInt64("INGEST_RATE_CAPACITY", 500),
		IngestRateRefill:   getEnvFloat("INGEST_RATE_REFILL_PER_SEC", 200), // demo-friendly; tune in prod
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}
