// Package billing implements account creation, topups, and ledger writes.
package billing

import (
	"context"
	"errors"
	"fmt"

	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/amiri/sms-gateway/internal/platform/httpx/auth"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CostPerMessage is the flat per-message credit cost.
const CostPerMessage int64 = 1

// Service handles billing operations.
type Service struct {
	pool *pgxpool.Pool
	q    *sqlc.Queries
	rdb  *platredis.Client
}

// New creates a billing Service.
func New(pool *pgxpool.Pool, rdb *platredis.Client) *Service {
	return &Service{pool: pool, q: sqlc.New(pool), rdb: rdb}
}

// Queries exposes sqlc queries (used by auth middleware).
func (s *Service) Queries() *sqlc.Queries { return s.q }

// CreateAccountResult is returned by CreateAccount.
type CreateAccountResult struct {
	AccountID uuid.UUID
	APIKey    string
}

// CreateAccount provisions a new tenant and returns the plaintext API key once.
func (s *Service) CreateAccount(ctx context.Context, name string) (CreateAccountResult, error) {
	if name == "" {
		return CreateAccountResult{}, errors.New("name is required")
	}
	apiKey, err := auth.GenerateAPIKey()
	if err != nil {
		return CreateAccountResult{}, err
	}
	acc, err := s.q.CreateAccount(ctx, sqlc.CreateAccountParams{
		ApiKeyHash: auth.HashAPIKey(apiKey),
		Name:       name,
	})
	if err != nil {
		return CreateAccountResult{}, fmt.Errorf("create account: %w", err)
	}
	if err := s.rdb.SetBalance(ctx, acc.ID.String(), 0); err != nil {
		return CreateAccountResult{}, fmt.Errorf("init redis balance: %w", err)
	}
	return CreateAccountResult{AccountID: acc.ID, APIKey: apiKey}, nil
}

// TopUp adds credit to an account (ledger + Redis + cached Postgres balance).
func (s *Service) TopUp(ctx context.Context, accountID uuid.UUID, amount int64) (int64, error) {
	if amount <= 0 {
		return 0, errors.New("amount must be positive")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	qtx := s.q.WithTx(tx)
	_, err = qtx.InsertLedgerEntry(ctx, sqlc.InsertLedgerEntryParams{
		AccountID: accountID,
		Type:      "topup",
		Amount:    amount,
		MessageID: nil,
	})
	if err != nil {
		return 0, fmt.Errorf("ledger topup: %w", err)
	}

	sum, err := qtx.SumLedgerEntriesByAccount(ctx, accountID)
	if err != nil {
		return 0, fmt.Errorf("sum ledger: %w", err)
	}
	if _, err := qtx.UpdateAccountBalance(ctx, sqlc.UpdateAccountBalanceParams{
		ID:      accountID,
		Balance: sum,
	}); err != nil {
		return 0, fmt.Errorf("update account balance: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	newBal, err := s.rdb.IncrBalance(ctx, accountID.String(), amount)
	if err != nil {
		// Best-effort repair: set Redis from ledger sum.
		_ = s.rdb.SetBalance(ctx, accountID.String(), sum)
		return sum, nil
	}
	return newBal, nil
}

// Balance returns the hot-path Redis balance.
func (s *Service) Balance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	return s.rdb.GetBalance(ctx, accountID.String())
}

// RecordDebit writes a durable ledger debit and updates the cached Postgres balance.
func (s *Service) RecordDebit(ctx context.Context, q *sqlc.Queries, accountID, messageID uuid.UUID, amount int64) error {
	mid := messageID
	_, err := q.InsertLedgerEntry(ctx, sqlc.InsertLedgerEntryParams{
		AccountID: accountID,
		Type:      "debit",
		Amount:    -amount,
		MessageID: &mid,
	})
	if err != nil {
		return err
	}
	sum, err := q.SumLedgerEntriesByAccount(ctx, accountID)
	if err != nil {
		return err
	}
	_, err = q.UpdateAccountBalance(ctx, sqlc.UpdateAccountBalanceParams{ID: accountID, Balance: sum})
	return err
}

// RecordRefund writes a refund ledger entry, updates Postgres cache, and credits Redis.
func (s *Service) RecordRefund(ctx context.Context, q *sqlc.Queries, accountID, messageID uuid.UUID, amount int64) error {
	mid := messageID
	_, err := q.InsertLedgerEntry(ctx, sqlc.InsertLedgerEntryParams{
		AccountID: accountID,
		Type:      "refund",
		Amount:    amount,
		MessageID: &mid,
	})
	if err != nil {
		return err
	}
	sum, err := q.SumLedgerEntriesByAccount(ctx, accountID)
	if err != nil {
		return err
	}
	if _, err := q.UpdateAccountBalance(ctx, sqlc.UpdateAccountBalanceParams{ID: accountID, Balance: sum}); err != nil {
		return err
	}
	_, err = s.rdb.IncrBalance(ctx, accountID.String(), amount)
	return err
}

// LedgerSum returns SUM(ledger_entries) for reconciler.
func (s *Service) LedgerSum(ctx context.Context, accountID uuid.UUID) (int64, error) {
	return s.q.SumLedgerEntriesByAccount(ctx, accountID)
}

// ListAccountIDs returns all account IDs (for reconciler sweep).
func (s *Service) ListAccountIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM accounts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// AlignRedisToLedger sets Redis balance from the ledger sum (cold start / safe heal).
func (s *Service) AlignRedisToLedger(ctx context.Context, accountID uuid.UUID) (int64, error) {
	sum, err := s.LedgerSum(ctx, accountID)
	if err != nil {
		return 0, err
	}
	if err := s.rdb.SetBalance(ctx, accountID.String(), sum); err != nil {
		return 0, err
	}
	return sum, nil
}
