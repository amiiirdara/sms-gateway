// Command report-sink consumes sms.dispatch-results (and accepted events)
// and writes to Postgres (messages, message_status_events) and ClickHouse
// (message_events). See ARCHITECTURE.md sections 6 and 8.5. Scaffold stub -
// real consume loop lands in internal/domain/{messaging,reporting} during
// the implementation phase.
package main

import (
	"context"
	"log"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

func main() {
	cfg := config.Load("report-sink")
	log.Printf("report-sink starting (kafka=%s db=%s clickhouse=%s)", cfg.KafkaBrokers, cfg.DatabaseURL, cfg.ClickHouseAddr)

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("report-sink: shutting down")
			return
		case <-ticker.C:
			// TODO(implementation phase): consume sms.dispatch-results, Inbox-dedup,
			// update messages + insert message_status_events (Postgres), insert
			// message_events (ClickHouse, batched).
			log.Println("report-sink: idle (not yet implemented)")
		}
	}
}
