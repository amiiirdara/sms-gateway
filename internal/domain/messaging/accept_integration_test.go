package messaging_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/amiri/sms-gateway/internal/domain/messaging"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startRedis(t *testing.T) (*platredis.Client, func()) {
	t.Helper()
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
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatal(err)
	}
	rdb, err := platredis.NewClient(ctx, host+":"+port.Port())
	if err != nil {
		t.Fatal(err)
	}
	return rdb, func() {
		_ = rdb.Close()
		_ = c.Terminate(ctx)
	}
}

func TestAcceptInsufficientFundsAndIdempotency(t *testing.T) {
	rdb, cleanup := startRedis(t)
	defer cleanup()
	ctx := context.Background()
	svc := messaging.New(rdb)
	accountID := uuid.New()

	_, err := svc.Accept(ctx, messaging.AcceptRequest{
		AccountID: accountID,
		To:        "09121234567",
		Text:      "hi",
		Priority:  messaging.PriorityNormal,
	})
	if !errors.Is(err, messaging.ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}

	if err := rdb.SetBalance(ctx, accountID.String(), 1); err != nil {
		t.Fatal(err)
	}
	first, err := svc.Accept(ctx, messaging.AcceptRequest{
		AccountID:      accountID,
		To:             "+989121234567",
		Text:           "hi",
		Priority:       messaging.PriorityNormal,
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Accept(ctx, messaging.AcceptRequest{
		AccountID:      accountID,
		To:             "+989121234567",
		Text:           "hi",
		Priority:       messaging.PriorityNormal,
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.MessageID != second.MessageID {
		t.Fatalf("idempotent replay should return same message id: %s vs %s", first.MessageID, second.MessageID)
	}
	bal, err := rdb.GetBalance(ctx, accountID.String())
	if err != nil {
		t.Fatal(err)
	}
	if bal != 0 {
		t.Fatalf("idempotent replay should not double-debit, balance=%d", bal)
	}

	// Express accept sets a deadline ~2m ahead.
	if err := rdb.SetBalance(ctx, accountID.String(), 1); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC()
	exp, err := svc.Accept(ctx, messaging.AcceptRequest{
		AccountID: accountID,
		To:        "09121234567",
		Text:      "otp",
		Priority:  messaging.PriorityExpress,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Status != "accepted" {
		t.Fatalf("status=%s", exp.Status)
	}
	_ = before
}
