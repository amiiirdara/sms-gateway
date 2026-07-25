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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
// Unique (message_id) WHERE type=debit makes this safe under retries.
func (s *Service) RecordDebit(ctx context.Context, q *sqlc.Queries, accountID, messageID uuid.UUID, amount int64) error {
	mid := messageID
	_, err := q.InsertLedgerEntry(ctx, sqlc.InsertLedgerEntryParams{
		AccountID: accountID,
		Type:      "debit",
		Amount:    -amount,
		MessageID: &mid,
	})
	if isUniqueViolation(err) {
		return nil
	}
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

// RecordRefundLedger writes a refund ledger entry and updates the Postgres cache.
// Redis must be credited AFTER the surrounding Inbox transaction commits.
func (s *Service) RecordRefundLedger(ctx context.Context, q *sqlc.Queries, accountID, messageID uuid.UUID, amount int64) error {
	mid := messageID
	_, err := q.InsertLedgerEntry(ctx, sqlc.InsertLedgerEntryParams{
		AccountID: accountID,
		Type:      "refund",
		Amount:    amount,
		MessageID: &mid,
	})
	if isUniqueViolation(err) {
		return nil
	}
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

// CreditRedisAfterRefund increments Redis after a committed ledger refund.
// On failure, aligns Redis to the ledger sum (safe repair).
func (s *Service) CreditRedisAfterRefund(ctx context.Context, accountID uuid.UUID, amount int64) error {
	if _, err := s.rdb.IncrBalance(ctx, accountID.String(), amount); err != nil {
		_, alignErr := s.AlignRedisToLedger(ctx, accountID)
		if alignErr != nil {
			return fmt.Errorf("redis credit: %w (align: %v)", err, alignErr)
		}
	}
	return nil
}

// LedgerSum returns SUM(ledger_entries) for reconciler.
func (s *Service) LedgerSum(ctx context.Context, accountID uuid.UUID) (int64, error) {
	return s.q.SumLedgerEntriesByAccount(ctx, accountID)
}

// ListAccountIDsPage returns account IDs after afterID (keyset), up to limit.
func (s *Service) ListAccountIDsPage(ctx context.Context, afterID uuid.UUID, limit int32) ([]uuid.UUID, error) {
	return s.q.ListAccountIDs(ctx, sqlc.ListAccountIDsParams{ID: afterID, Limit: limit})
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

// ErrNoRows re-export for callers that still check pgx.ErrNoRows via billing.
var ErrNoRows = pgx.ErrNoRows
