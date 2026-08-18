package billing

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// CreditLogEntry is one append-only topup, debit, or refund.
type CreditLogEntry struct {
	AccountID      uuid.UUID
	Type           string // topup | debit | refund
	Amount         int64
	MessageID      *uuid.UUID
	IdempotencyKey string
}

// CreditLog is the seam for async credit history. Production will use ClickHouse;
// tests use MemoryCreditLog. Billing never SUMs this log to produce a balance.
type CreditLog interface {
	Append(ctx context.Context, entry CreditLogEntry) error
}

// NopCreditLog discards entries. Used by cmd until the ClickHouse adapter is wired.
type NopCreditLog struct{}

// Append implements CreditLog.
func (NopCreditLog) Append(context.Context, CreditLogEntry) error { return nil }

// MemoryCreditLog is an in-memory CreditLog adapter for tests.
type MemoryCreditLog struct {
	mu      sync.Mutex
	entries []CreditLogEntry
}

// Append implements CreditLog.
func (m *MemoryCreditLog) Append(_ context.Context, entry CreditLogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
	return nil
}

// Entries returns a copy of appended rows.
func (m *MemoryCreditLog) Entries() []CreditLogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]CreditLogEntry, len(m.entries))
	copy(out, m.entries)
	return out
}
