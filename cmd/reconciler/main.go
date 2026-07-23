// Command reconciler is a safety-net job that compares Redis balances to ledger sums.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
)

func main() {
	cfg := config.Load("reconciler")
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("reconciler: postgres: %v", err)
	}
	defer pool.Close()
	rdb, err := platredis.NewClient(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("reconciler: redis: %v", err)
	}
	defer rdb.Close()

	billingSvc := billing.New(pool, rdb)
	metrics.Serve(env("METRICS_ADDR", ":9090"))

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Println("reconciler: started")
	runOnce(ctx, billingSvc, rdb)
	for {
		select {
		case <-ctx.Done():
			log.Println("reconciler: shutting down")
			return
		case <-ticker.C:
			runOnce(ctx, billingSvc, rdb)
		}
	}
}

func runOnce(ctx context.Context, billingSvc *billing.Service, rdb *platredis.Client) {
	ids, err := billingSvc.ListAccountIDs(ctx)
	if err != nil {
		log.Printf("reconciler: list accounts: %v", err)
		return
	}
	for _, id := range ids {
		ledger, err := billingSvc.LedgerSum(ctx, id)
		if err != nil {
			log.Printf("reconciler: ledger sum %s: %v", id, err)
			continue
		}
		redisBal, err := rdb.GetBalance(ctx, id.String())
		if err != nil {
			log.Printf("reconciler: redis balance %s: %v", id, err)
			continue
		}
		switch {
		case redisBal > ledger:
			// Dangerous free-credit direction: auto-heal Redis down.
			metrics.ReconcilerDrift.WithLabelValues("redis_gt_ledger").Inc()
			log.Printf("reconciler: ALERT redis>%s ledger for %s (redis=%d ledger=%d); healing redis down",
				"ledger", id, redisBal, ledger)
			if err := rdb.SetBalance(ctx, id.String(), ledger); err != nil {
				log.Printf("reconciler: heal failed: %v", err)
			} else {
				metrics.ReconcilerHeals.Inc()
			}
		case redisBal < ledger:
			// Safe/expected lag direction: alert only.
			metrics.ReconcilerDrift.WithLabelValues("redis_lt_ledger").Inc()
			log.Printf("reconciler: WARN redis<ledger for %s (redis=%d ledger=%d); not auto-healing",
				id, redisBal, ledger)
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
