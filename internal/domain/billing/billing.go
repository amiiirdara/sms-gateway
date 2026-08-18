// Package billing implements account creation, topups, and durable money apply.
package billing

import (
	"context"
	"errors"
	"fmt"

	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/amiri/sms-gateway/internal/platform/httpx/auth"
	"github.com/amiri/sms-gateway/internal/platform/inbox"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// CostPerMessage is the flat per-message credit cost.
	CostPerMessage int64 = 1

	inboxDebitConsumer  = "billing-debit"
	inboxRefundConsumer = "billing-refund"
)

// Service handles billing operations.
type Service struct {
	pool      *pgxpool.Pool
	q         *sqlc.Queries
	rdb       *platredis.Client
	inbox     *inbox.Store
	creditLog CreditLog
}

// New creates a billing Service with a no-op credit log.
func New(pool *pgxpool.Pool, rdb *platredis.Client) *Service {
	return NewWithCreditLog(pool, rdb, NopCreditLog{})
}

// NewWithCreditLog creates a billing Service with an injected credit log.
func NewWithCreditLog(pool *pgxpool.Pool, rdb *platredis.Client, log CreditLog) *Service {
	if log == nil {
		log = NopCreditLog{}
	}
	return &Service{pool: pool, q: sqlc.New(pool), rdb: rdb, inbox: inbox.New(pool), creditLog: log}
}

// Queries exposes sqlc queries (used by auth middleware).
func (s *Service) Queries() *sqlc.Queries { return s.q }

// Cost returns the per-message prepaid amount.
func (s *Service) Cost() int64 { return CostPerMessage }

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

// ErrMissingIdempotencyKey is returned when a topup has no Idempotency-Key.
var ErrMissingIdempotencyKey = errors.New("Idempotency-Key is required")

// TopUp adds credit to an account (durable balance + live balance + credit log).
func (s *Service) TopUp(ctx context.Context, accountID uuid.UUID, amount int64, idempotencyKey string) (int64, error) {
	if amount <= 0 {
		return 0, errors.New("amount must be positive")
	}
	if idempotencyKey == "" {
		return 0, ErrMissingIdempotencyKey
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	qtx := s.q.WithTx(tx)
	_, err = qtx.TryInsertTopupIdempotency(ctx, sqlc.TryInsertTopupIdempotencyParams{
		AccountID:      accountID,
		IdempotencyKey: idempotencyKey,
		Amount:         amount,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := qtx.GetTopupIdempotency(ctx, sqlc.GetTopupIdempotencyParams{
			AccountID:      accountID,
			IdempotencyKey: idempotencyKey,
		})
		if getErr != nil {
			return 0, fmt.Errorf("topup idempotency lookup: %w", getErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return existing.DurableBalance, nil
	}
	if err != nil {
		return 0, fmt.Errorf("topup idempotency: %w", err)
	}

	acc, err := qtx.AddAccountBalance(ctx, sqlc.AddAccountBalanceParams{
		ID:      accountID,
		Balance: amount,
	})
	if err != nil {
		return 0, fmt.Errorf("durable topup: %w", err)
	}
	if err := qtx.SetTopupIdempotencyBalance(ctx, sqlc.SetTopupIdempotencyBalanceParams{
		AccountID:      accountID,
		IdempotencyKey: idempotencyKey,
		DurableBalance: acc.Balance,
	}); err != nil {
		return 0, fmt.Errorf("topup idempotency store: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	if err := s.creditLog.Append(ctx, CreditLogEntry{
		AccountID:      accountID,
		Type:           "topup",
		Amount:         amount,
		IdempotencyKey: idempotencyKey,
	}); err != nil {
		return 0, fmt.Errorf("credit log topup: %w", err)
	}

	newBal, err := s.rdb.IncrBalance(ctx, accountID.String(), amount)
	if err != nil {
		_ = s.rdb.SetBalance(ctx, accountID.String(), acc.Balance)
		return acc.Balance, nil
	}
	return newBal, nil
}

// Balance returns the live (Redis) balance.
func (s *Service) Balance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	return s.rdb.GetBalance(ctx, accountID.String())
}

// DurableBalance returns the durable prepaid amount stored on the account.
func (s *Service) DurableBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	return s.q.GetAccountBalance(ctx, accountID)
}

// ApplyDebit records a durable debit inside an Inbox transaction, then appends the credit log.
func (s *Service) ApplyDebit(ctx context.Context, accountID, messageID uuid.UUID, cost int64) error {
	entry := CreditLogEntry{AccountID: accountID, Type: "debit", Amount: -cost, MessageID: &messageID}
	tx, qtx, err := s.inbox.TryBegin(ctx, inboxDebitConsumer, messageID.String()+":debit")
	if inbox.IsAlreadyProcessed(err) {
		return s.creditLog.Append(ctx, entry)
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.recordDebit(ctx, qtx, accountID, messageID, cost); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return s.creditLog.Append(ctx, entry)
}

// ApplyRefund records a durable refund inside Inbox, credits live balance after commit, then appends the credit log.
func (s *Service) ApplyRefund(ctx context.Context, accountID, messageID uuid.UUID, cost int64) error {
	entry := CreditLogEntry{AccountID: accountID, Type: "refund", Amount: cost, MessageID: &messageID}
	tx, qtx, err := s.inbox.TryBegin(ctx, inboxRefundConsumer, messageID.String()+":refund")
	if inbox.IsAlreadyProcessed(err) {
		if _, alignErr := s.alignLiveToDurable(ctx, accountID); alignErr != nil {
			return alignErr
		}
		return s.creditLog.Append(ctx, entry)
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.recordRefundLedger(ctx, qtx, accountID, messageID, cost); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if err := s.creditRedisAfterRefund(ctx, accountID, cost); err != nil {
		return err
	}
	return s.creditLog.Append(ctx, entry)
}

// Heal seeds a missing live-balance key, then sets live down to durable when live is higher.
func (s *Service) Heal(ctx context.Context, accountID uuid.UUID) error {
	if err := s.Seed(ctx, accountID); err != nil {
		return err
	}
	durable, err := s.DurableBalance(ctx, accountID)
	if err != nil {
		return err
	}
	live, err := s.rdb.GetBalance(ctx, accountID.String())
	if err != nil {
		return err
	}
	if live > durable {
		return s.rdb.SetBalance(ctx, accountID.String(), durable)
	}
	return nil
}

// Seed copies durable balance into live balance only when the live key is absent.
func (s *Service) Seed(ctx context.Context, accountID uuid.UUID) error {
	exists, err := s.rdb.HasBalance(ctx, accountID.String())
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	durable, err := s.DurableBalance(ctx, accountID)
	if err != nil {
		return err
	}
	return s.rdb.SetBalance(ctx, accountID.String(), durable)
}

func (s *Service) recordDebit(ctx context.Context, q *sqlc.Queries, accountID, messageID uuid.UUID, amount int64) error {
	_, err := q.AddAccountBalance(ctx, sqlc.AddAccountBalanceParams{
		ID:      accountID,
		Balance: -amount,
	})
	return err
}

func (s *Service) recordRefundLedger(ctx context.Context, q *sqlc.Queries, accountID, messageID uuid.UUID, amount int64) error {
	_, err := q.AddAccountBalance(ctx, sqlc.AddAccountBalanceParams{
		ID:      accountID,
		Balance: amount,
	})
	return err
}

func (s *Service) creditRedisAfterRefund(ctx context.Context, accountID uuid.UUID, amount int64) error {
	if _, err := s.rdb.IncrBalance(ctx, accountID.String(), amount); err != nil {
		_, alignErr := s.alignLiveToDurable(ctx, accountID)
		if alignErr != nil {
			return fmt.Errorf("redis credit: %w (align: %v)", err, alignErr)
		}
	}
	return nil
}

// ListAccountIDsPage returns account IDs after afterID (keyset), up to limit.
func (s *Service) ListAccountIDsPage(ctx context.Context, afterID uuid.UUID, limit int32) ([]uuid.UUID, error) {
	return s.q.ListAccountIDs(ctx, sqlc.ListAccountIDsParams{ID: afterID, Limit: limit})
}

func (s *Service) alignLiveToDurable(ctx context.Context, accountID uuid.UUID) (int64, error) {
	sum, err := s.DurableBalance(ctx, accountID)
	if err != nil {
		return 0, err
	}
	if err := s.rdb.SetBalance(ctx, accountID.String(), sum); err != nil {
		return 0, err
	}
	return sum, nil
}

// ErrNoRows re-export for callers that still check pgx.ErrNoRows via billing.
var ErrNoRows = pgx.ErrNoRows
