package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestAttemptTracker(t *testing.T) {
	tr := NewAttemptTracker()
	msg := kafkago.Message{Topic: "t", Partition: 1, Offset: 42}
	if n := tr.Inc(msg); n != 1 {
		t.Fatalf("inc1=%d", n)
	}
	if n := tr.Inc(msg); n != 2 {
		t.Fatalf("inc2=%d", n)
	}
	tr.Clear(msg)
	if n := tr.Inc(msg); n != 1 {
		t.Fatalf("after clear=%d", n)
	}
}

func TestBackoffCaps(t *testing.T) {
	if Backoff(1) <= 0 {
		t.Fatal("expected positive backoff")
	}
	if Backoff(100) > 5e9 { // 5s in ns
		t.Fatal("expected cap at 5s")
	}
}
