package phone_test

import (
	"testing"

	"github.com/amiri/sms-gateway/internal/domain/messaging/phone"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		in, want string
		ok       bool
	}{
		{"+989121234567", "+989121234567", true},
		{"09121234567", "+989121234567", true},
		{" 0912 123 4567 ", "+989121234567", true},
		{"9121234567", "", false},
		{"+98912", "", false},
		{"", "", false},
	}
	for _, tc := range tests {
		got, err := phone.Normalize(tc.in)
		if tc.ok && err != nil {
			t.Fatalf("Normalize(%q): unexpected err %v", tc.in, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("Normalize(%q): expected error", tc.in)
		}
		if got != tc.want {
			t.Fatalf("Normalize(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}
