package campaigns_test

import (
	"testing"

	"github.com/amiri/sms-gateway/internal/domain/campaigns"
	"github.com/google/uuid"
)

func TestDeterministicMessageIDStable(t *testing.T) {
	cid := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	a := campaigns.DeterministicMessageID(cid, 0)
	b := campaigns.DeterministicMessageID(cid, 0)
	c := campaigns.DeterministicMessageID(cid, 1)
	if a != b {
		t.Fatalf("expected stable id, got %s vs %s", a, b)
	}
	if a == c {
		t.Fatal("different indexes should produce different ids")
	}
}

func TestMaxRecipientsConstant(t *testing.T) {
	if campaigns.MaxRecipients != 10000 {
		t.Fatalf("MaxRecipients=%d", campaigns.MaxRecipients)
	}
}
