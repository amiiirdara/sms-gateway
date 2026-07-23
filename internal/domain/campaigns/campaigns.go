// Package campaigns implements batch SMS campaign acceptance and expansion helpers.
package campaigns

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/amiri/sms-gateway/internal/domain/messaging/phone"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
)

const MaxRecipients = 10000

// ErrInsufficientFunds is returned when balance cannot cover the full campaign.
var ErrInsufficientFunds = errors.New("insufficient funds")

// ErrTooManyRecipients is returned when recipients exceed MaxRecipients.
var ErrTooManyRecipients = errors.New("too many recipients")

// Service handles campaign acceptance.
type Service struct {
	rdb *platredis.Client
}

// New creates a campaigns Service.
func New(rdb *platredis.Client) *Service {
	return &Service{rdb: rdb}
}

// AcceptRequest is the input for accepting a campaign.
type AcceptRequest struct {
	AccountID      uuid.UUID
	Text           string
	Recipients     []string
	IdempotencyKey string
}

// AcceptResponse is returned after a successful campaign accept.
type AcceptResponse struct {
	CampaignID      string `json:"campaignId"`
	TotalRecipients int    `json:"totalRecipients"`
	Cost            int64  `json:"cost"`
}

// InsufficientFundsError carries shortfall details for 402 responses.
type InsufficientFundsError struct {
	Required  int64 `json:"required"`
	Available int64 `json:"available"`
}

func (e *InsufficientFundsError) Error() string { return ErrInsufficientFunds.Error() }
func (e *InsufficientFundsError) Unwrap() error { return ErrInsufficientFunds }

// Accept validates recipients, reserves total cost, and stages the campaign outbox.
func (s *Service) Accept(ctx context.Context, req AcceptRequest) (AcceptResponse, error) {
	if req.Text == "" {
		return AcceptResponse{}, errors.New("text is required")
	}
	if len(req.Recipients) == 0 {
		return AcceptResponse{}, errors.New("recipients required")
	}
	if len(req.Recipients) > MaxRecipients {
		return AcceptResponse{}, ErrTooManyRecipients
	}

	normalized := make([]string, 0, len(req.Recipients))
	for _, r := range req.Recipients {
		n, err := phone.Normalize(r)
		if err != nil {
			return AcceptResponse{}, fmt.Errorf("recipient %q: %w", r, err)
		}
		normalized = append(normalized, n)
	}

	accountID := req.AccountID.String()
	if req.IdempotencyKey != "" {
		if cached, ok, err := s.rdb.GetIdempotentResponse(ctx, accountID, "campaign:"+req.IdempotencyKey); err != nil {
			return AcceptResponse{}, err
		} else if ok {
			var resp AcceptResponse
			if err := json.Unmarshal([]byte(cached), &resp); err != nil {
				return AcceptResponse{}, err
			}
			return resp, nil
		}
	}

	totalCost := billing.CostPerMessage * int64(len(normalized))
	campaignID := uuid.New()
	acceptedAt := time.Now().UTC()
	recipientsJSON, err := json.Marshal(normalized)
	if err != nil {
		return AcceptResponse{}, err
	}

	result, err := s.rdb.CheckAndDebitCampaign(ctx, platredis.CampaignOutboxFields{
		AccountID:      accountID,
		CampaignID:     campaignID.String(),
		Text:           req.Text,
		TotalCost:      totalCost,
		CostPerMessage: billing.CostPerMessage,
		RecipientsJSON: string(recipientsJSON),
		AcceptedAt:     acceptedAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return AcceptResponse{}, err
	}
	if !result.OK {
		return AcceptResponse{}, &InsufficientFundsError{
			Required:  totalCost,
			Available: result.NewBalance,
		}
	}

	resp := AcceptResponse{
		CampaignID:      campaignID.String(),
		TotalRecipients: len(normalized),
		Cost:            totalCost,
	}
	if req.IdempotencyKey != "" {
		body, _ := json.Marshal(resp)
		_ = s.rdb.CacheIdempotentResponse(ctx, accountID, "campaign:"+req.IdempotencyKey, string(body), 24*time.Hour)
	}
	return resp, nil
}

// DeterministicMessageID derives a stable UUID from campaign ID + recipient index.
func DeterministicMessageID(campaignID uuid.UUID, index int) uuid.UUID {
	h := sha256.New()
	h.Write(campaignID[:])
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	h.Write(buf[:])
	sum := h.Sum(nil)
	var id uuid.UUID
	copy(id[:], sum[:16])
	// Set version 4 / variant bits for a valid UUID shape.
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}
