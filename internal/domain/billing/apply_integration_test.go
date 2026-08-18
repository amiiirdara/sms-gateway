package billing_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type billingStack struct {
	pool *pgxpool.Pool
	rdb  *platredis.Client
	log  *billing.MemoryCreditLog
	svc  *billing.Service
}

func startBillingStack(t *testing.T) (*billingStack, context.Context) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration")
	}
	ctx := context.Background()

	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
	upSQL, err := os.ReadFile(filepath.Join(root, "db", "migrations", "000001_init_schema.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	up2, err := os.ReadFile(filepath.Join(root, "db", "migrations", "000002_reliability_guards.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	up3, err := os.ReadFile(filepath.Join(root, "db", "migrations", "000003_durable_balance_drop_ledger.up.sql"))
	if err != nil {
		t.Fatal(err)
	}

	pgC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16.6-alpine",
			Env:          map[string]string{"POSTGRES_USER": "sms", "POSTGRES_PASSWORD": "sms", "POSTGRES_DB": "sms_gateway"},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("postgres: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	host, _ := pgC.Host(ctx)
	port, _ := pgC.MappedPort(ctx, "5432")
	dsn := "postgres://sms:sms@" + host + ":" + port.Port() + "/sms_gateway?sslmode=disable"
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	if _, err := pool.Exec(ctx, string(upSQL)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(up2)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(up3)); err != nil {
		t.Fatal(err)
	}

	rc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.4-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("redis: %v", err)
	}
	t.Cleanup(func() { _ = rc.Terminate(ctx) })
	rHost, _ := rc.Host(ctx)
	rPort, _ := rc.MappedPort(ctx, "6379")
	rdb, err := platredis.NewClient(ctx, rHost+":"+rPort.Port())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	log := &billing.MemoryCreditLog{}
	return &billingStack{pool: pool, rdb: rdb, log: log, svc: billing.NewWithCreditLog(pool, rdb, log)}, ctx
}

func TestApplyDebitInboxReplayDoesNotDoubleDecrement(t *testing.T) {
	st, ctx := startBillingStack(t)
	acc, err := st.svc.CreateAccount(ctx, "debit-replay")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.svc.TopUp(ctx, acc.AccountID, 10, "top-1"); err != nil {
		t.Fatal(err)
	}
	msgID := uuid.New()
	if err := st.svc.ApplyDebit(ctx, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.ApplyDebit(ctx, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	durable, err := st.svc.DurableBalance(ctx, acc.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	if durable != 9 {
		t.Fatalf("durable=%d want 9", durable)
	}
}

func TestApplyRefundInboxReplayAlignsLiveToDurable(t *testing.T) {
	st, ctx := startBillingStack(t)
	acc, err := st.svc.CreateAccount(ctx, "refund-replay")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.svc.TopUp(ctx, acc.AccountID, 5, ""); err != nil {
		t.Fatal(err)
	}
	msgID := uuid.New()
	if err := st.svc.ApplyDebit(ctx, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.rdb.SetBalance(ctx, acc.AccountID.String(), 4); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.ApplyRefund(ctx, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.rdb.SetBalance(ctx, acc.AccountID.String(), 0); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.ApplyRefund(ctx, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	live, err := st.svc.Balance(ctx, acc.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := st.svc.DurableBalance(ctx, acc.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	if live != durable || live != 5 {
		t.Fatalf("live=%d durable=%d want both 5", live, durable)
	}
}

func TestApplyRefundCreditsLiveAfterDurableCommit(t *testing.T) {
	st, ctx := startBillingStack(t)
	acc, err := st.svc.CreateAccount(ctx, "refund-order")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.svc.TopUp(ctx, acc.AccountID, 5, ""); err != nil {
		t.Fatal(err)
	}
	msgID := uuid.New()
	if err := st.svc.ApplyDebit(ctx, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.rdb.SetBalance(ctx, acc.AccountID.String(), 4); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.ApplyRefund(ctx, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	live, _ := st.rdb.GetBalance(ctx, acc.AccountID.String())
	if live != 5 {
		t.Fatalf("live after ApplyRefund want 5 got %d", live)
	}
	durable, err := st.svc.DurableBalance(ctx, acc.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	if durable != 5 {
		t.Fatalf("durable after ApplyRefund want 5 got %d", durable)
	}
}

func TestHealSetsLiveDownOnly(t *testing.T) {
	st, ctx := startBillingStack(t)
	acc, err := st.svc.CreateAccount(ctx, "heal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.svc.TopUp(ctx, acc.AccountID, 5, ""); err != nil {
		t.Fatal(err)
	}

	if err := st.rdb.SetBalance(ctx, acc.AccountID.String(), 9); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.Heal(ctx, acc.AccountID); err != nil {
		t.Fatal(err)
	}
	live, _ := st.svc.Balance(ctx, acc.AccountID)
	if live != 5 {
		t.Fatalf("heal down: live=%d want 5", live)
	}

	if err := st.rdb.SetBalance(ctx, acc.AccountID.String(), 2); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.Heal(ctx, acc.AccountID); err != nil {
		t.Fatal(err)
	}
	live, _ = st.svc.Balance(ctx, acc.AccountID)
	if live != 2 {
		t.Fatalf("heal must not raise live, got %d", live)
	}
}

func TestSeedFillsAbsentKeyOnly(t *testing.T) {
	st, ctx := startBillingStack(t)
	acc, err := st.svc.CreateAccount(ctx, "seed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.svc.TopUp(ctx, acc.AccountID, 7, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.rdb.Raw().Del(ctx, platredis.BalanceKey(acc.AccountID.String())).Err(); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.Seed(ctx, acc.AccountID); err != nil {
		t.Fatal(err)
	}
	live, _ := st.svc.Balance(ctx, acc.AccountID)
	if live != 7 {
		t.Fatalf("seed absent: live=%d want 7", live)
	}

	if err := st.rdb.SetBalance(ctx, acc.AccountID.String(), 1); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.Seed(ctx, acc.AccountID); err != nil {
		t.Fatal(err)
	}
	live, _ = st.svc.Balance(ctx, acc.AccountID)
	if live != 1 {
		t.Fatalf("seed must not overwrite present key, got %d", live)
	}
}

func TestTopUpCreditsDurableAndLive(t *testing.T) {
	st, ctx := startBillingStack(t)
	acc, err := st.svc.CreateAccount(ctx, "topup")
	if err != nil {
		t.Fatal(err)
	}
	live, err := st.svc.TopUp(ctx, acc.AccountID, 3, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if live != 3 {
		t.Fatalf("live=%d want 3", live)
	}
	durable, err := st.svc.DurableBalance(ctx, acc.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	if durable != 3 {
		t.Fatalf("durable=%d want 3", durable)
	}
}

func TestHealSeedsAbsentLiveKey(t *testing.T) {
	st, ctx := startBillingStack(t)
	acc, err := st.svc.CreateAccount(ctx, "heal-seed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.svc.TopUp(ctx, acc.AccountID, 6, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.rdb.Raw().Del(ctx, platredis.BalanceKey(acc.AccountID.String())).Err(); err != nil {
		t.Fatal(err)
	}
	if err := st.svc.Heal(ctx, acc.AccountID); err != nil {
		t.Fatal(err)
	}
	live, _ := st.svc.Balance(ctx, acc.AccountID)
	if live != 6 {
		t.Fatalf("heal seed: live=%d want 6", live)
	}
}
