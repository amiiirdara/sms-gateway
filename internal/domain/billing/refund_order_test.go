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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Proves refund Redis credit is separate from ledger write, and AlignRedisToLedger
// recovers a missing Redis credit after a committed refund.
func TestRefundLedgerThenRedisOrdering(t *testing.T) {
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

	pgReq := testcontainers.ContainerRequest{
		Image:        "postgres:16.6-alpine",
		Env:          map[string]string{"POSTGRES_USER": "sms", "POSTGRES_PASSWORD": "sms", "POSTGRES_DB": "sms_gateway"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}
	pgC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq, Started: true,
	})
	if err != nil {
		t.Skipf("postgres: %v", err)
	}
	defer func() { _ = pgC.Terminate(ctx) }()

	host, _ := pgC.Host(ctx)
	port, _ := pgC.MappedPort(ctx, "5432")
	dsn := "postgres://sms:sms@" + host + ":" + port.Port() + "/sms_gateway?sslmode=disable"
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, string(upSQL)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(up2)); err != nil {
		t.Fatal(err)
	}

	redisReq := testcontainers.ContainerRequest{
		Image:        "redis:7.4-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	rc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: redisReq, Started: true,
	})
	if err != nil {
		t.Skipf("redis: %v", err)
	}
	defer func() { _ = rc.Terminate(ctx) }()
	rHost, _ := rc.Host(ctx)
	rPort, _ := rc.MappedPort(ctx, "6379")
	rdb, err := platredis.NewClient(ctx, rHost+":"+rPort.Port())
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	svc := billing.New(pool, rdb)
	acc, err := svc.CreateAccount(ctx, "refund-order")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.TopUp(ctx, acc.AccountID, 5); err != nil {
		t.Fatal(err)
	}
	msgID := uuid.New()
	q := svc.Queries()
	if err := svc.RecordDebit(ctx, q, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SetBalance(ctx, acc.AccountID.String(), 4); err != nil {
		t.Fatal(err)
	}

	if err := svc.RecordRefundLedger(ctx, q, acc.AccountID, msgID, 1); err != nil {
		t.Fatal(err)
	}
	bal, _ := rdb.GetBalance(ctx, acc.AccountID.String())
	if bal != 4 {
		t.Fatalf("redis should still be 4 before CreditRedisAfterRefund, got %d", bal)
	}
	if err := svc.CreditRedisAfterRefund(ctx, acc.AccountID, 1); err != nil {
		t.Fatal(err)
	}
	bal, _ = rdb.GetBalance(ctx, acc.AccountID.String())
	if bal != 5 {
		t.Fatalf("redis after credit want 5 got %d", bal)
	}

	_ = rdb.SetBalance(ctx, acc.AccountID.String(), 0)
	sum, err := svc.AlignRedisToLedger(ctx, acc.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 5 {
		t.Fatalf("align want ledger 5 got %d", sum)
	}
}
