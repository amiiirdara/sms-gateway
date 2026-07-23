package messaging_test

import (
	"testing"
	"time"

	"github.com/amiri/sms-gateway/internal/domain/messaging"
)

func TestExpressDeadlineBoundary(t *testing.T) {
	accepted := time.Now().UTC()
	deadline := accepted.Add(messaging.ExpressSLA)

	if !deadline.After(accepted) {
		t.Fatal("deadline should be after accepted_at")
	}
	if messaging.ExpressSLA != 2*time.Minute {
		t.Fatalf("ExpressSLA=%v want 2m", messaging.ExpressSLA)
	}

	// Simulate dispatcher check: expired when now > deadline
	now := deadline.Add(time.Millisecond)
	if !now.After(deadline) {
		t.Fatal("expected expired")
	}
	stillValid := deadline.Add(-time.Millisecond)
	if stillValid.After(deadline) {
		t.Fatal("expected still valid")
	}
}
