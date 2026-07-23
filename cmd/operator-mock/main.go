// Command operator-mock simulates a telecom operator's SMS submission API,
// since this build has no real operator integration (see ARCHITECTURE.md
// section 15). It introduces artificial latency and a configurable random
// failure rate so the Dispatcher's retry/backoff and the Express deadline
// logic have something realistic to react to.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

type sendRequest struct {
	To       string `json:"to"`
	Text     string `json:"text"`
	Priority string `json:"priority"`
}

type sendResponse struct {
	Status string `json:"status"`
}

func main() {
	cfg := config.Load("operator-mock")
	failureRate := envFloat("OPERATOR_MOCK_FAILURE_RATE", 0.02)
	minLatency := envDuration("OPERATOR_MOCK_MIN_LATENCY", 50*time.Millisecond)
	maxLatency := envDuration("OPERATOR_MOCK_MAX_LATENCY", 400*time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req sendRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		jitter := minLatency + time.Duration(rand.Int63n(int64(maxLatency-minLatency)+1))
		time.Sleep(jitter)

		if rand.Float64() < failureRate {
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(sendResponse{Status: "failed"})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(sendResponse{Status: "sent"})
	})

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	go func() {
		log.Printf("operator-mock listening on %s (failureRate=%.2f)", cfg.HTTPAddr, failureRate)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("operator-mock: listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("operator-mock: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
