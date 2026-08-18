package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreditEvent is a row in sms_gateway.credit_events.
type CreditEvent struct {
	EventTime      time.Time
	AccountID      uuid.UUID
	Type           string // topup | debit | refund
	Amount         int64
	MessageID      *uuid.UUID
	IdempotencyKey string
}

// InsertCreditEvent appends one credit-history row.
func (c *Client) InsertCreditEvent(ctx context.Context, e CreditEvent) error {
	if c == nil || c.conn == nil {
		return fmt.Errorf("clickhouse: nil connection")
	}
	if e.EventTime.IsZero() {
		e.EventTime = time.Now().UTC()
	}
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO sms_gateway.credit_events
		(event_time, account_id, type, amount, message_id, idempotency_key)
	`)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare credit event: %w", err)
	}
	var idem *string
	if e.IdempotencyKey != "" {
		k := e.IdempotencyKey
		idem = &k
	}
	if err := batch.Append(
		e.EventTime,
		e.AccountID,
		e.Type,
		e.Amount,
		e.MessageID,
		idem,
	); err != nil {
		return fmt.Errorf("clickhouse: append credit event: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send credit event: %w", err)
	}
	return nil
}

// EnsureCreditEvent inserts unless a matching debit/refund (account_id, type, message_id)
// or topup (account_id, type, idempotency_key) already exists.
func (c *Client) EnsureCreditEvent(ctx context.Context, e CreditEvent) error {
	if c == nil || c.conn == nil {
		return fmt.Errorf("clickhouse: nil connection")
	}
	exists, err := c.creditEventExists(ctx, e)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return c.InsertCreditEvent(ctx, e)
}

func (c *Client) creditEventExists(ctx context.Context, e CreditEvent) (bool, error) {
	var n uint64
	var err error
	if e.Type == "topup" {
		err = c.conn.QueryRow(ctx, `
			SELECT count() FROM sms_gateway.credit_events
			WHERE account_id = ? AND type = ? AND ifNull(idempotency_key, '') = ?
		`, e.AccountID, e.Type, e.IdempotencyKey).Scan(&n)
	} else if e.MessageID == nil {
		err = c.conn.QueryRow(ctx, `
			SELECT count() FROM sms_gateway.credit_events
			WHERE account_id = ? AND type = ? AND message_id IS NULL
		`, e.AccountID, e.Type).Scan(&n)
	} else {
		err = c.conn.QueryRow(ctx, `
			SELECT count() FROM sms_gateway.credit_events
			WHERE account_id = ? AND type = ? AND message_id = ?
		`, e.AccountID, e.Type, *e.MessageID).Scan(&n)
	}
	if err != nil {
		return false, fmt.Errorf("clickhouse: credit event exists check: %w", err)
	}
	return n > 0, nil
}
