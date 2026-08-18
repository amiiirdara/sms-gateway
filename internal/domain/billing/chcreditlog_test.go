package billing_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/amiri/sms-gateway/internal/domain/billing"
	platch "github.com/amiri/sms-gateway/internal/platform/clickhouse"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestNewClickHouseCreditLogImplementsCreditLog(t *testing.T) {
	var _ billing.CreditLog = billing.NewClickHouseCreditLog(nil)
}

func TestClickHouseCreditLogNilClient(t *testing.T) {
	log := billing.NewClickHouseCreditLog(nil)
	if err := log.Append(context.Background(), billing.CreditLogEntry{Type: "topup", Amount: 1}); err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestClickHouseCreditLogAppendDuplicate(t *testing.T) {
	client := startBillingClickHouse(t)
	defer client.Close()
	ctx := context.Background()
	log := billing.NewClickHouseCreditLog(client)

	accountID := uuid.New()
	messageID := uuid.New()
	entry := billing.CreditLogEntry{
		AccountID: accountID,
		Type:      "debit",
		Amount:    -1,
		MessageID: &messageID,
	}
	if err := log.Append(ctx, entry); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := log.Append(ctx, entry); err != nil {
		t.Fatalf("duplicate append: %v", err)
	}

	var n uint64
	if err := client.Conn().QueryRow(ctx, `
		SELECT count() FROM sms_gateway.credit_events
		WHERE account_id = ?
	`, accountID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("count before optimize=%d want 1", n)
	}
	if err := client.Conn().Exec(ctx, `OPTIMIZE TABLE sms_gateway.credit_events FINAL`); err != nil {
		t.Fatalf("optimize: %v", err)
	}
	if err := client.Conn().QueryRow(ctx, `
		SELECT count() FROM sms_gateway.credit_events FINAL
		WHERE account_id = ?
	`, accountID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("count after optimize=%d want 1", n)
	}
}

func TestClickHouseCreditLogBadAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := platch.NewWithPassword(ctx, "127.0.0.1:1", "sms")
	if err != nil {
		// Constructor ping failed: still an error, no panic.
		log := billing.NewClickHouseCreditLog(nil)
		if appendErr := log.Append(ctx, billing.CreditLogEntry{Type: "topup", Amount: 1}); appendErr == nil {
			t.Fatal("expected Append error for nil client after bad address")
		}
		return
	}
	defer client.Close()
	log := billing.NewClickHouseCreditLog(client)
	if err := log.Append(ctx, billing.CreditLogEntry{
		AccountID:      uuid.New(),
		Type:           "topup",
		Amount:         1,
		IdempotencyKey: "k",
	}); err == nil {
		t.Fatal("expected Append error for bad address")
	}
}

func startBillingClickHouse(t *testing.T) *platch.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image: "clickhouse/clickhouse-server:24.8-alpine",
		Env: map[string]string{
			"CLICKHOUSE_DB":                        "sms_gateway",
			"CLICKHOUSE_USER":                      "default",
			"CLICKHOUSE_PASSWORD":                  "sms",
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		WaitingFor: wait.ForHTTP("/ping").
			WithPort("8123/tcp").
			WithStartupTimeout(90 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "9000")
	if err != nil {
		t.Fatal(err)
	}
	addr := host + ":" + port.Port()

	var client *platch.Client
	var lastErr error
	for i := 0; i < 15; i++ {
		client, lastErr = platch.NewWithPassword(ctx, addr, "sms")
		if lastErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		t.Fatalf("clickhouse connect: %v", lastErr)
	}

	_, thisFile, _, _ := runtime.Caller(0)
	ddlPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "clickhouse", "init", "002_credit_events.sql")
	raw, err := os.ReadFile(ddlPath)
	if err != nil {
		t.Fatalf("read ddl: %v", err)
	}
	for _, stmt := range strings.Split(string(raw), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := client.Conn().Exec(ctx, stmt); err != nil {
			t.Fatalf("exec ddl %q: %v", stmt, err)
		}
	}
	return client
}
