package billing

import (
	"context"
	"fmt"

	platch "github.com/amiri/sms-gateway/internal/platform/clickhouse"
)

// ClickHouseCreditLog appends credit-history rows to ClickHouse with
// ensure-insert semantics (duplicate debit/refund by message_id and
// duplicate topup by idempotency_key are no-ops).
type ClickHouseCreditLog struct {
	ch *platch.Client
}

// NewClickHouseCreditLog wraps a ClickHouse client as a CreditLog.
func NewClickHouseCreditLog(ch *platch.Client) CreditLog {
	return ClickHouseCreditLog{ch: ch}
}

var _ CreditLog = ClickHouseCreditLog{}

// Append implements CreditLog.
func (l ClickHouseCreditLog) Append(ctx context.Context, e CreditLogEntry) error {
	if l.ch == nil {
		return fmt.Errorf("clickhouse credit log: nil client")
	}
	if err := l.ch.EnsureCreditEvent(ctx, platch.CreditEvent{
		AccountID:      e.AccountID,
		Type:           e.Type,
		Amount:         e.Amount,
		MessageID:      e.MessageID,
		IdempotencyKey: e.IdempotencyKey,
	}); err != nil {
		return fmt.Errorf("clickhouse credit log: %w", err)
	}
	return nil
}
