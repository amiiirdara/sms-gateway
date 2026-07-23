// Command api-gateway is the REST entry point described in ARCHITECTURE.md
// section 8.1. This is a scaffold stub: it wires config + a health endpoint
// so the service is deployable end-to-end from day one; the actual
// POST /v1/messages, /v1/campaigns, /v1/topups, /v1/balance handlers land in
// internal/domain/{messaging,campaigns,billing} in the implementation phase.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

func main() {
	cfg := config.Load("api-gateway")

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// TODO(implementation phase): mount POST /v1/accounts, /v1/topups, /v1/balance,
	// /v1/messages, /v1/campaigns, GET /v1/messages/{id} - see ARCHITECTURE.md
	// section 10 and openapi/openapi.yaml for the full contract.

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	go func() {
		log.Printf("api-gateway listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("api-gateway: listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("api-gateway: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("api-gateway: shutdown error: %v", err)
	}
}
