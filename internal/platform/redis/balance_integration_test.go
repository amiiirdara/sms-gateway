package redis_test

import (
	"context"
	"testing"
	"time"

	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestCheckAndDebitExactZeroAndRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
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
	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatal(err)
	}
	addr := host + ":" + port.Port()

	rdb, err := platredis.NewClient(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	accountID := "acct-test-1"
	if err := rdb.SetBalance(ctx, accountID, 1); err != nil {
		t.Fatal(err)
	}

	ok, err := rdb.CheckAndDebit(ctx, platredis.MessageOutboxFields{
		AccountID:  accountID,
		MessageID:  "msg-1",
		To:         "+989121234567",
		Text:       "hi",
		Priority:   "normal",
		Cost:       1,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok.OK || ok.NewBalance != 0 {
		t.Fatalf("expected exact-zero accept, got %+v", ok)
	}

	fail, err := rdb.CheckAndDebit(ctx, platredis.MessageOutboxFields{
		AccountID:  accountID,
		MessageID:  "msg-2",
		To:         "+989121234567",
		Text:       "hi",
		Priority:   "normal",
		Cost:       1,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	if fail.OK {
		t.Fatal("expected insufficient funds after zero balance")
	}

	// Concurrent race: set balance to 1, fire two concurrent debits, expect exactly one success.
	if err := rdb.SetBalance(ctx, accountID, 1); err != nil {
		t.Fatal(err)
	}
	type result struct {
		ok  bool
		err error
	}
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			r, err := rdb.CheckAndDebit(ctx, platredis.MessageOutboxFields{
				AccountID:  accountID,
				MessageID:  "race-" + string(rune('a'+i)),
				To:         "+989121234567",
				Text:       "race",
				Priority:   "normal",
				Cost:       1,
				AcceptedAt: time.Now().UTC().Format(time.RFC3339Nano),
			})
			ch <- result{ok: r.OK, err: err}
		}()
	}
	successes := 0
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.ok {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 success under race, got %d", successes)
	}
	bal, err := rdb.GetBalance(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if bal != 0 {
		t.Fatalf("balance after race=%d want 0", bal)
	}
}
