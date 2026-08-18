package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/google/uuid"
)

func TestMemoryCreditLogAppends(t *testing.T) {
	log := &billing.MemoryCreditLog{}
	id := uuid.New()
	if err := log.Append(context.Background(), billing.CreditLogEntry{
		AccountID: id,
		Type:      "topup",
		Amount:    4,
	}); err != nil {
		t.Fatal(err)
	}
	got := log.Entries()
	if len(got) != 1 || got[0].Amount != 4 || got[0].AccountID != id {
		t.Fatalf("entries=%+v", got)
	}
}

func TestCostMatchesCostPerMessage(t *testing.T) {
	svc := billing.New(nil, nil)
	if svc.Cost() != billing.CostPerMessage {
		t.Fatalf("Cost()=%d want %d", svc.Cost(), billing.CostPerMessage)
	}
}

func TestTopUpRequiresIdempotencyKey(t *testing.T) {
	svc := billing.New(nil, nil)
	_, err := svc.TopUp(context.Background(), uuid.New(), 1, "")
	if !errors.Is(err, billing.ErrMissingIdempotencyKey) {
		t.Fatalf("got %v", err)
	}
}
