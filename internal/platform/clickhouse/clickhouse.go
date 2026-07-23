// Package clickhouse wraps the ClickHouse client used by report-sink and
// reporting-api.
package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Client is a thin wrapper around clickhouse-go.
type Client struct {
	conn driver.Conn
}

// New connects to ClickHouse using the native protocol address (host:9000).
func New(ctx context.Context, addr string) (*Client, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: []string{addr},
		Auth: ch.Auth{
			Database: "sms_gateway",
			Username: "default",
			Password: "",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return &Client{conn: conn}, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// Conn exposes the raw driver connection.
func (c *Client) Conn() driver.Conn { return c.conn }

// MessageEvent is a row in message_events.
type MessageEvent struct {
	EventTime  time.Time
	MessageID  uuid.UUID
	AccountID  uuid.UUID
	CampaignID *uuid.UUID
	Recipient  string
	Priority   string
	Status     string
	Cost       int64
	Operator   string
}

// InsertMessageEvent appends one reporting event.
func (c *Client) InsertMessageEvent(ctx context.Context, e MessageEvent) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO sms_gateway.message_events
		(event_time, message_id, account_id, campaign_id, recipient, priority, status, cost, operator)
	`)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare: %w", err)
	}
	if err := batch.Append(
		e.EventTime,
		e.MessageID,
		e.AccountID,
		e.CampaignID,
		e.Recipient,
		e.Priority,
		e.Status,
		e.Cost,
		e.Operator,
	); err != nil {
		return fmt.Errorf("clickhouse: append: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send: %w", err)
	}
	return nil
}

// ReportFilter scopes a paginated report query.
type ReportFilter struct {
	AccountID  uuid.UUID
	CampaignID *uuid.UUID
	From       time.Time
	To         time.Time
	Status     string
	Limit      int
	Offset     int
}

// ReportRow is one message event returned by reports.
type ReportRow struct {
	EventTime  time.Time `ch:"event_time"`
	MessageID  uuid.UUID `ch:"message_id"`
	AccountID  uuid.UUID `ch:"account_id"`
	CampaignID *uuid.UUID `ch:"campaign_id"`
	Recipient  string    `ch:"recipient"`
	Priority   string    `ch:"priority"`
	Status     string    `ch:"status"`
	Cost       int64     `ch:"cost"`
	Operator   string    `ch:"operator"`
}

// QueryReports returns paginated message events for an account.
func (c *Client) QueryReports(ctx context.Context, f ReportFilter) ([]ReportRow, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	query := `
		SELECT event_time, message_id, account_id, campaign_id, recipient, priority, status, cost, operator
		FROM sms_gateway.message_events
		WHERE account_id = ?
		  AND event_time >= ?
		  AND event_time < ?
	`
	args := []any{f.AccountID, f.From, f.To}
	if f.Status != "" {
		query += ` AND status = ?`
		args = append(args, f.Status)
	}
	if f.CampaignID != nil {
		query += ` AND campaign_id = ?`
		args = append(args, *f.CampaignID)
	}
	query += ` ORDER BY event_time DESC LIMIT ? OFFSET ?`
	args = append(args, f.Limit, f.Offset)

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: query reports: %w", err)
	}
	defer rows.Close()

	var out []ReportRow
	for rows.Next() {
		var r ReportRow
		if err := rows.Scan(
			&r.EventTime, &r.MessageID, &r.AccountID, &r.CampaignID,
			&r.Recipient, &r.Priority, &r.Status, &r.Cost, &r.Operator,
		); err != nil {
			return nil, fmt.Errorf("clickhouse: scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CampaignAggregate is the aggregate report for one campaign.
type CampaignAggregate struct {
	TotalRecipients   uint64
	Sent              uint64
	Failed            uint64
	ExpiredSlaMissed  uint64
	Pending           uint64
	TotalCost         int64
	RefundedAmount    int64
}

// AggregateCampaign computes status counts for a campaign.
func (c *Client) AggregateCampaign(ctx context.Context, accountID, campaignID uuid.UUID) (CampaignAggregate, error) {
	row := c.conn.QueryRow(ctx, `
		SELECT
			count() AS total,
			countIf(status = 'sent') AS sent,
			countIf(status = 'failed') AS failed,
			countIf(status = 'expired_sla_missed') AS expired,
			countIf(status IN ('accepted','queued','dispatched')) AS pending,
			sum(cost) AS total_cost,
			sumIf(cost, status IN ('failed','expired_sla_missed')) AS refunded
		FROM sms_gateway.message_events
		WHERE account_id = ? AND campaign_id = ?
	`, accountID, campaignID)

	var a CampaignAggregate
	if err := row.Scan(
		&a.TotalRecipients, &a.Sent, &a.Failed, &a.ExpiredSlaMissed,
		&a.Pending, &a.TotalCost, &a.RefundedAmount,
	); err != nil {
		return CampaignAggregate{}, fmt.Errorf("clickhouse: aggregate campaign: %w", err)
	}
	return a, nil
}
