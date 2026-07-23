// Command outbox-relay drains the Redis outbox streams (outbox:messages,
// outbox:campaigns) into Kafka reliably. See ARCHITECTURE.md section 5.1.
// Scaffold stub - real XREADGROUP/publish/XACK loop lands in
// internal/platform/{redis,kafka} during the implementation phase.
package main

import (
	"context"
	"log"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

func main() {
	cfg := config.Load("outbox-relay")
	log.Printf("outbox-relay starting (redis=%s kafka=%s)", cfg.RedisAddr, cfg.KafkaBrokers)

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("outbox-relay: shutting down")
			return
		case <-ticker.C:
			// TODO(implementation phase): XREADGROUP on outbox:messages/outbox:campaigns,
			// publish to Kafka with an idempotent producer, XACK on success,
			// XAUTOCLAIM stale pending entries. See ARCHITECTURE.md section 5.1.
			log.Println("outbox-relay: idle (not yet implemented)")
		}
	}
}
