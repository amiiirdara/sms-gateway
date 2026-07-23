// Command billing-consumer writes durable ledger debit/refund entries.
// See ARCHITECTURE.md sections 5.1, 6, and 8.6. Scaffold stub - real consume
// loop lands in internal/domain/billing during the implementation phase.
package main

import (
	"context"
	"log"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

func main() {
	cfg := config.Load("billing-consumer")
	log.Printf("billing-consumer starting (kafka=%s db=%s)", cfg.KafkaBrokers, cfg.DatabaseURL)

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("billing-consumer: shutting down")
			return
		case <-ticker.C:
			// TODO(implementation phase): consume the accepted-event stream (via
			// Kafka) to write debit ledger entries, and sms.dispatch-results to
			// write refund entries on FAILED/expired_sla_missed. Inbox-dedup both.
			log.Println("billing-consumer: idle (not yet implemented)")
		}
	}
}
