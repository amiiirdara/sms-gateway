package kafka

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestMemoryAttemptStorePersistsAcrossInc(t *testing.T) {
	store := NewMemoryAttemptStore()
	msg := kafkago.Message{Topic: "t", Partition: 0, Offset: 7}
	ctx := context.Background()
	n1, err := store.Inc(ctx, msg)
	if err != nil || n1 != 1 {
		t.Fatalf("n1=%d err=%v", n1, err)
	}
	n2, err := store.Inc(ctx, msg)
	if err != nil || n2 != 2 {
		t.Fatalf("n2=%d err=%v", n2, err)
	}
	if err := store.Clear(ctx, msg); err != nil {
		t.Fatal(err)
	}
	n3, err := store.Inc(ctx, msg)
	if err != nil || n3 != 1 {
		t.Fatalf("after clear n3=%d err=%v", n3, err)
	}
}
