// Command reconciler is a safety-net job that compares Redis balances to ledger sums.
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
)

// Ignore tiny / fresh lag so async ledger writes do not flap alerts.
const (
	defaultDriftThreshold = int64(0) // any non-zero redis>ledger still heals
	pageSize              = int32(200)
	minLagBeforeWarn      = 0 // keep warn on redis<ledger; heal only redis>ledger
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

	threshold := defaultDriftThreshold
	if v := os.Getenv("RECONCILER_DRIFT_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			threshold = n
		}
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Println("reconciler: started")
	runOnce(ctx, billingSvc, rdb, threshold)
	for {
		select {
		case <-ctx.Done():
			log.Println("reconciler: shutting down")
			return
		case <-ticker.C:
			runOnce(ctx, billingSvc, rdb, threshold)
		}
	}
}

func runOnce(ctx context.Context, billingSvc *billing.Service, rdb *platredis.Client, threshold int64) {
	after := uuid.Nil
	for {
		ids, err := billingSvc.ListAccountIDsPage(ctx, after, pageSize)
		if err != nil {
			log.Printf("reconciler: list accounts: %v", err)
			return
		}
		if len(ids) == 0 {
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
			drift := redisBal - ledger
			switch {
			case drift > threshold:
				metrics.ReconcilerDrift.WithLabelValues("redis_gt_ledger").Inc()
				log.Printf("reconciler: ALERT redis>ledger for %s (redis=%d ledger=%d drift=%d); healing redis down",
					id, redisBal, ledger, drift)
				if err := rdb.SetBalance(ctx, id.String(), ledger); err != nil {
					log.Printf("reconciler: heal failed: %v", err)
				} else {
					metrics.ReconcilerHeals.Inc()
				}
			case drift < -minLagBeforeWarn:
				metrics.ReconcilerDrift.WithLabelValues("redis_lt_ledger").Inc()
				log.Printf("reconciler: WARN redis<ledger for %s (redis=%d ledger=%d); not auto-healing",
					id, redisBal, ledger)
			}
			after = id
		}
		if int32(len(ids)) < pageSize {
			return
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
