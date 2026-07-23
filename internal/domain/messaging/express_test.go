package messaging_test

import (
	"testing"
	"time"

	"github.com/amiri/sms-gateway/internal/domain/messaging"
)

func TestExpressExpired(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Millisecond).Format(time.RFC3339Nano)
	future := now.Add(time.Minute).Format(time.RFC3339Nano)

	if !messaging.ExpressExpired(past, now) {
		t.Fatal("expected expired for past deadline")
	}
	if messaging.ExpressExpired(future, now) {
		t.Fatal("expected not expired for future deadline")
	}
	if messaging.ExpressExpired("", now) {
		t.Fatal("empty deadline should not expire")
	}
	if messaging.ExpressExpired("not-a-time", now) {
		t.Fatal("invalid deadline should not expire")
	}
}

func TestExpressDeadlineBoundary(t *testing.T) {
	accepted := time.Now().UTC()
	deadline := accepted.Add(messaging.ExpressSLA)
	if messaging.ExpressSLA != 2*time.Minute {
		t.Fatalf("ExpressSLA=%v want 2m", messaging.ExpressSLA)
	}
	if messaging.ExpressExpired(deadline.Format(time.RFC3339Nano), deadline) {
		t.Fatal("exactly at deadline should not be After, so not expired")
	}
	if !messaging.ExpressExpired(deadline.Format(time.RFC3339Nano), deadline.Add(time.Nanosecond)) {
		t.Fatal("one ns after deadline should be expired")
	}
}
