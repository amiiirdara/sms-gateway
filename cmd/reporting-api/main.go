// Command reporting-api serves the read side described in ARCHITECTURE.md
// section 8. Scaffold stub - see cmd/api-gateway for the pattern; real
// handlers (GET /v1/messages/{id}, /v1/reports, /v1/campaigns/*) land in
// internal/domain/reporting during the implementation phase.
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
	cfg := config.Load("reporting-api")

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// TODO(implementation phase): mount GET /v1/messages/{id}, /v1/reports,
	// /v1/campaigns, /v1/campaigns/{id}, /v1/campaigns/{id}/report.

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	go func() {
		log.Printf("reporting-api listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("reporting-api: listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("reporting-api: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("reporting-api: shutdown error: %v", err)
	}
}
