package auth_test

import (
	"testing"

	"github.com/amiri/sms-gateway/internal/platform/httpx/auth"
)

func TestHashAPIKeyDeterministic(t *testing.T) {
	a := auth.HashAPIKey("secret")
	b := auth.HashAPIKey("secret")
	c := auth.HashAPIKey("other")
	if a != b {
		t.Fatal("hash should be deterministic")
	}
	if a == c {
		t.Fatal("different keys should hash differently")
	}
	if len(a) != 64 {
		t.Fatalf("sha256 hex length=%d", len(a))
	}
}

func TestGenerateAPIKey(t *testing.T) {
	k1, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if k1 == k2 {
		t.Fatal("keys should be unique")
	}
	if len(k1) != 64 {
		t.Fatalf("key length=%d", len(k1))
	}
}
