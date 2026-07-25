package auth

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAccountCacheTTL(t *testing.T) {
	c := NewAccountCache(20 * time.Millisecond)
	id := uuid.New()
	c.Set("h1", Account{ID: id, Name: "a"})
	if acc, ok := c.Get("h1"); !ok || acc.ID != id {
		t.Fatal("expected cache hit")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := c.Get("h1"); ok {
		t.Fatal("expected expiry")
	}
}
