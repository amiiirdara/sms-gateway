package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/amiri/sms-gateway/internal/domain/messaging/phone"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
)

const (
	PriorityNormal  = "normal"
	PriorityExpress = "express"
	ExpressSLA      = 2 * time.Minute
	IdempotencyTTL  = 24 * time.Hour
)

// ErrInsufficientFunds is returned when balance cannot cover the cost.
var ErrInsufficientFunds = errors.New("insufficient funds")

// ErrInvalidPriority is returned for unknown priority values.
var ErrInvalidPriority = errors.New(`priority must be "normal" or "express"`)

// Service handles single-message acceptance.
type Service struct {
	rdb *platredis.Client
}

// New creates a messaging Service.
func New(rdb *platredis.Client) *Service {
	return &Service{rdb: rdb}
}

// AcceptRequest is the input for accepting a single SMS.
type AcceptRequest struct {
	AccountID      uuid.UUID
	To             string
	Text           string
	Priority       string
	Cost           int64
	IdempotencyKey string
}

// AcceptResponse is returned after a successful accept.
type AcceptResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Cost      int64  `json:"cost"`
}

// Accept validates, debits, and stages a message in the Redis outbox.
func (s *Service) Accept(ctx context.Context, req AcceptRequest) (AcceptResponse, error) {
	if req.Text == "" {
		return AcceptResponse{}, errors.New("text is required")
	}
	if req.Priority != PriorityNormal && req.Priority != PriorityExpress {
		return AcceptResponse{}, ErrInvalidPriority
	}
	to, err := phone.Normalize(req.To)
	if err != nil {
		return AcceptResponse{}, err
	}

	accountID := req.AccountID.String()
	msgID := uuid.New()
	acceptedAt := time.Now().UTC()
	deadline := ""
	if req.Priority == PriorityExpress {
		deadline = acceptedAt.Add(ExpressSLA).Format(time.RFC3339Nano)
	}

	resp := AcceptResponse{
		MessageID: msgID.String(),
		Status:    "accepted",
		Cost:      req.Cost,
	}
	idemKey := ""
	idemBody := ""
	if req.IdempotencyKey != "" {
		idemKey = platredis.IdempotencyKey(accountID, req.IdempotencyKey)
		b, _ := json.Marshal(resp)
		idemBody = string(b)
	}

	result, err := s.rdb.CheckAndDebit(ctx, platredis.MessageOutboxFields{
		AccountID:       accountID,
		MessageID:       msgID.String(),
		To:              to,
		Text:            req.Text,
		Priority:        req.Priority,
		Cost:            req.Cost,
		Deadline:        deadline,
		CampaignID:      "",
		AcceptedAt:      acceptedAt.Format(time.RFC3339Nano),
		IdempotencyKey:  idemKey,
		IdempotencyBody: idemBody,
		IdempotencyTTL:  IdempotencyTTL,
	})
	if err != nil {
		return AcceptResponse{}, fmt.Errorf("check and debit: %w", err)
	}
	if result.FromCache {
		var cached AcceptResponse
		if err := json.Unmarshal([]byte(result.CachedBody), &cached); err != nil {
			return AcceptResponse{}, err
		}
		return cached, nil
	}
	if !result.OK {
		return AcceptResponse{}, ErrInsufficientFunds
	}
	return resp, nil
}
