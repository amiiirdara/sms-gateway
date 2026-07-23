// Package config loads runtime configuration from the environment.
//
// Kept dependency-free (stdlib only) at scaffold stage; extend as each
// service gains real logic. See ARCHITECTURE.md section 12 for the intended
// tech stack once real platform clients (pgx, go-redis, kafka-go,
// clickhouse-go) are wired in.
package config

import "os"

// Config holds the environment-derived settings shared across services.
// Individual services read only the fields they need.
type Config struct {
	ServiceName    string
	HTTPAddr       string
	DatabaseURL    string
	RedisAddr      string
	KafkaBrokers   string
	ClickHouseAddr string
}

// Load reads configuration from the environment, applying sane local-dev
// defaults so every cmd/ binary can run standalone against the
// docker-compose stack without extra setup.
func Load(serviceName string) Config {
	return Config{
		ServiceName:    serviceName,
		HTTPAddr:       getEnv("HTTP_ADDR", ":8080"),
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:   getEnv("KAFKA_BROKERS", "localhost:9092"),
		ClickHouseAddr: getEnv("CLICKHOUSE_ADDR", "localhost:9000"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
