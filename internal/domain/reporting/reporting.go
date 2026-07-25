// Package reporting holds read-side helpers for message status and reports.
// HTTP wiring stays in cmd/reporting-api; this package keeps query shaping
// and response mapping out of main.
package reporting

import (
	"context"
	"time"

	"github.com/amiri/sms-gateway/internal/db/sqlc"
	platch "github.com/amiri/sms-gateway/internal/platform/clickhouse"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// Service is the reporting read model.
type Service struct {
	q  *sqlc.Queries
	ch *platch.Client
}

// New creates a reporting Service.
func New(q *sqlc.Queries, ch *platch.Client) *Service {
	return &Service{q: q, ch: ch}
}

// MessageView is the API-facing message status payload.
type MessageView struct {
	ID           string    `json:"id"`
	Recipient    string    `json:"recipient"`
	Priority     string    `json:"priority"`
	Cost         int64     `json:"cost"`
	Status       string    `json:"status"`
	Operator     string    `json:"operator"`
	CampaignID   any       `json:"campaignId"`
	CreatedAt    time.Time `json:"createdAt"`
	DispatchedAt any       `json:"dispatchedAt"`
}

// GetMessage returns a tenant-scoped message or pgx.ErrNoRows.
func (s *Service) GetMessage(ctx context.Context, accountID, messageID uuid.UUID) (MessageView, error) {
	msg, err := s.q.GetMessageByIDForAccount(ctx, sqlc.GetMessageByIDForAccountParams{
		ID:        messageID,
		AccountID: accountID,
	})
	if err != nil {
		return MessageView{}, err
	}
	return MessageView{
		ID:           msg.ID.String(),
		Recipient:    msg.Recipient,
		Priority:     msg.Priority,
		Cost:         msg.Cost,
		Status:       msg.Status,
		Operator:     textOrEmpty(msg.Operator),
		CampaignID:   uuidPtr(msg.CampaignID),
		CreatedAt:    msg.CreatedAt.Time,
		DispatchedAt: dispatchedAtOrNil(msg.DispatchedAt),
	}, nil
}

// ListReports proxies ClickHouse report queries.
func (s *Service) ListReports(ctx context.Context, f platch.ReportFilter) ([]platch.ReportRow, error) {
	return s.ch.QueryReports(ctx, f)
}

// CampaignAggregate proxies ClickHouse campaign aggregates.
func (s *Service) CampaignAggregate(ctx context.Context, accountID, campaignID uuid.UUID) (platch.CampaignAggregate, error) {
	return s.ch.AggregateCampaign(ctx, accountID, campaignID)
}

func uuidPtr(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return id.String()
}

func textOrEmpty(t pgtype.Text) string {
	if !t.Valid {
		return ""
	}
	return t.String
}

func dispatchedAtOrNil(t pgtype.Timestamptz) any {
	if !t.Valid {
		return nil
	}
	return t.Time
}
