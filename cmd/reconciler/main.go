// Command reconciler is the safety-net drift detector between Redis balances
// and the Postgres ledger. See ARCHITECTURE.md section 5.3. Scaffold stub -
// real comparison/alerting/safe-direction-heal logic lands in
// internal/domain/billing during the implementation phase.
package main

import (
	"context"
	"log"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
)

func main() {
	cfg := config.Load("reconciler")
	log.Printf("reconciler starting (redis=%s db=%s)", cfg.RedisAddr, cfg.DatabaseURL)

	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("reconciler: shutting down")
			return
		case <-ticker.C:
			// TODO(implementation phase): for each account, compare
			// SUM(ledger_entries) to balance:{account_id} in Redis.
			// Redis higher -> auto-correct down + alert (dangerous direction).
			// Redis lower -> alert only, do not auto-heal (safe/expected-lag direction).
			log.Println("reconciler: tick (not yet implemented)")
		}
	}
}
