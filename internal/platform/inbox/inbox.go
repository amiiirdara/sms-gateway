// Package inbox implements the Inbox/dedup pattern for Kafka consumers
// (ARCHITECTURE.md section 5.2).
package inbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrAlreadyProcessed is returned when an event was already handled.
var ErrAlreadyProcessed = errors.New("inbox: event already processed")

// Store wraps processed_events inserts.
type Store struct {
	pool *pgxpool.Pool
	q    *sqlc.Queries
}

// New creates an Inbox store backed by Postgres.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, q: sqlc.New(pool)}
}

// TryBegin marks an event as processed inside an open transaction.
// If the event was already processed, returns ErrAlreadyProcessed.
// The caller must commit/rollback the returned transaction.
func (s *Store) TryBegin(ctx context.Context, consumerName, eventID string) (pgx.Tx, *sqlc.Queries, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("inbox: begin: %w", err)
	}
	qtx := s.q.WithTx(tx)
	row, err := qtx.MarkEventProcessed(ctx, sqlc.MarkEventProcessedParams{
		ConsumerName: consumerName,
		EventID:      eventID,
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrAlreadyProcessed
		}
		return nil, nil, fmt.Errorf("inbox: mark: %w", err)
	}
	_ = row
	return tx, qtx, nil
}

// AlreadyProcessed reports whether MarkEventProcessed returned no row
// (ON CONFLICT DO NOTHING) without starting a transaction. Prefer TryBegin
// for the real consumer path.
func IsAlreadyProcessed(err error) bool {
	return errors.Is(err, ErrAlreadyProcessed)
}
