package inbox_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/amiri/sms-gateway/internal/platform/inbox"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestInboxDedup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		Env:          map[string]string{"POSTGRES_USER": "sms", "POSTGRES_PASSWORD": "sms", "POSTGRES_DB": "sms_gateway"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	defer func() { _ = c.Terminate(ctx) }()

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatal(err)
	}
	dsn := "postgres://sms:sms@" + host + ":" + port.Port() + "/sms_gateway?sslmode=disable"

	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "db", "migrations")
	mig, err := migrate.New("file://"+filepath.ToSlash(migrations), "pgx5://sms:sms@"+host+":"+port.Port()+"/sms_gateway?sslmode=disable")
	if err != nil {
		t.Fatalf("migrate new: %v", err)
	}
	if err := mig.Up(); err != nil && err != migrate.ErrNoChange {
		t.Fatalf("migrate up: %v", err)
	}
	_, _ = mig.Close()

	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	store := inbox.New(pool)

	tx1, _, err := store.TryBegin(ctx, "test-consumer", "evt-1")
	if err != nil {
		t.Fatalf("first TryBegin: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	_, _, err = store.TryBegin(ctx, "test-consumer", "evt-1")
	if !inbox.IsAlreadyProcessed(err) {
		t.Fatalf("expected AlreadyProcessed, got %v", err)
	}

	// Different consumer can process the same event id.
	tx2, _, err := store.TryBegin(ctx, "other-consumer", "evt-1")
	if err != nil {
		t.Fatalf("other consumer: %v", err)
	}
	_ = tx2.Rollback(ctx)
}
