package clickhouse

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestEnsureCreditEventDuplicate(t *testing.T) {
	client := startClickHouse(t)
	defer client.Close()
	ctx := context.Background()

	accountID := uuid.New()
	messageID := uuid.New()

	cases := []struct {
		name  string
		first CreditEvent
		dup   CreditEvent
	}{
		{
			name: "debit by message_id",
			first: CreditEvent{
				AccountID: accountID,
				Type:      "debit",
				Amount:    -1,
				MessageID: &messageID,
			},
			dup: CreditEvent{
				AccountID: accountID,
				Type:      "debit",
				Amount:    -1,
				MessageID: &messageID,
			},
		},
		{
			name: "topup by idempotency_key",
			first: CreditEvent{
				AccountID:      accountID,
				Type:           "topup",
				Amount:         10,
				IdempotencyKey: "topup-1",
			},
			dup: CreditEvent{
				AccountID:      accountID,
				Type:           "topup",
				Amount:         10,
				IdempotencyKey: "topup-1",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := client.EnsureCreditEvent(ctx, tc.first); err != nil {
				t.Fatalf("first insert: %v", err)
			}
			if err := client.EnsureCreditEvent(ctx, tc.dup); err != nil {
				t.Fatalf("duplicate insert: %v", err)
			}

			var n uint64
			if err := client.Conn().QueryRow(ctx, `
				SELECT count() FROM sms_gateway.credit_events
				WHERE account_id = ? AND type = ?
			`, accountID, tc.first.Type).Scan(&n); err != nil {
				t.Fatalf("count: %v", err)
			}
			if n != 1 {
				t.Fatalf("count before optimize=%d want 1 (ensure-insert)", n)
			}

			if err := client.Conn().Exec(ctx, `OPTIMIZE TABLE sms_gateway.credit_events FINAL`); err != nil {
				t.Fatalf("optimize: %v", err)
			}
			if err := client.Conn().QueryRow(ctx, `
				SELECT count() FROM sms_gateway.credit_events FINAL
				WHERE account_id = ? AND type = ?
			`, accountID, tc.first.Type).Scan(&n); err != nil {
				t.Fatalf("count final: %v", err)
			}
			if n != 1 {
				t.Fatalf("count after optimize=%d want 1", n)
			}
		})
	}
}

func TestEnsureCreditEventBadAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := ch.Open(&ch.Options{
		Addr: []string{"127.0.0.1:1"},
		Auth: ch.Auth{
			Database: "sms_gateway",
			Username: "default",
			Password: "sms",
		},
		DialTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		// Open is usually lazy; a connect-time error is still a failure without panic.
		return
	}
	defer conn.Close()

	client := &Client{conn: conn}
	err = client.EnsureCreditEvent(ctx, CreditEvent{
		AccountID:      uuid.New(),
		Type:           "topup",
		Amount:         1,
		IdempotencyKey: "k",
	})
	if err == nil {
		t.Fatal("expected error for unreachable ClickHouse")
	}
}

func TestEnsureCreditEventNilClient(t *testing.T) {
	var client *Client
	if err := client.EnsureCreditEvent(context.Background(), CreditEvent{Type: "topup"}); err == nil {
		t.Fatal("expected error for nil client")
	}
	if err := (&Client{}).EnsureCreditEvent(context.Background(), CreditEvent{Type: "debit"}); err == nil {
		t.Fatal("expected error for nil connection")
	}
}

func startClickHouse(t *testing.T) *Client {
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

	var client *Client
	var lastErr error
	for i := 0; i < 15; i++ {
		client, lastErr = NewWithPassword(ctx, addr, "sms")
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
