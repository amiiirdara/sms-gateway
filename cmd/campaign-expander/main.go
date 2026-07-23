// Command campaign-expander fans an accepted campaign out into individual
// per-recipient messages. See ARCHITECTURE.md section 9.2. Scaffold stub -
// real Kafka consume loop + deterministic message_id generation + bulk
// Postgres insert lands in internal/domain/campaigns during the
// implementation phase.
package main

import (
	"context"
	"log"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

func main() {
	cfg := config.Load("campaign-expander")
	log.Printf("campaign-expander starting (redis=%s kafka=%s db=%s)", cfg.RedisAddr, cfg.KafkaBrokers, cfg.DatabaseURL)

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("campaign-expander: shutting down")
			return
		case <-ticker.C:
			// TODO(implementation phase): consume outbox:campaigns, generate
			// deterministic message_id = hash(campaign_id, recipient_index),
			// bulk-insert campaigns + messages (ON CONFLICT DO NOTHING),
			// publish one event per recipient to sms.outbound.normal only.
			log.Println("campaign-expander: idle (not yet implemented)")
		}
	}
}
