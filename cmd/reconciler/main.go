// Command reconciler is a safety-net job that heals live balance toward durable balance.
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
	"github.com/google/uuid"
)

const pageSize = int32(200)

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
	runOnce(ctx, billingSvc)
	for {
		select {
		case <-ctx.Done():
			log.Println("reconciler: shutting down")
			return
		case <-ticker.C:
			runOnce(ctx, billingSvc)
		}
	}
}

func runOnce(ctx context.Context, billingSvc *billing.Service) {
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
			if err := billingSvc.Heal(ctx, id); err != nil {
				log.Printf("reconciler: heal %s: %v", id, err)
				continue
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
