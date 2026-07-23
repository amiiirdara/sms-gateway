// Command dispatcher consumes sms.outbound.{normal,express} and calls the
// Operator Adapter. See ARCHITECTURE.md sections 6 and 8.4. Scaffold stub -
// real consume loop, Inbox dedup, retry/backoff, and the Express hard-deadline
// check land in internal/domain/messaging during the implementation phase.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

func main() {
	mode := flag.String("mode", "normal", "dispatcher mode: normal|express")
	flag.Parse()

	if *mode != "normal" && *mode != "express" {
		log.Fatalf("dispatcher: invalid --mode %q (must be normal or express)", *mode)
	}

	cfg := config.Load("dispatcher-" + *mode)
	log.Printf("dispatcher starting (mode=%s kafka=%s)", *mode, cfg.KafkaBrokers)

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("dispatcher (mode=%s): shutting down", *mode)
			return
		case <-ticker.C:
			// TODO(implementation phase): consume sms.outbound.<mode>, Inbox-dedup,
			// call the Operator Adapter with retry/backoff. In express mode, check
			// the message's deadline_at before calling the operator and mark
			// expired_sla_missed (no call) if past deadline - see ARCHITECTURE.md
			// section 6.
			log.Printf("dispatcher (mode=%s): idle (not yet implemented)", *mode)
		}
	}
}
