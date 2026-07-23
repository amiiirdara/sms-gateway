// Package messaging defines the JSON payloads exchanged on Kafka topics.
package messaging

import "time"

// AcceptedMessage is published to sms.outbound.{normal|express}.
type AcceptedMessage struct {
	MessageID  string    `json:"messageId"`
	AccountID  string    `json:"accountId"`
	CampaignID string    `json:"campaignId,omitempty"`
	To         string    `json:"to"`
	Text       string    `json:"text"`
	Priority   string    `json:"priority"`
	Cost       int64     `json:"cost"`
	Deadline   string    `json:"deadline,omitempty"`
	AcceptedAt time.Time `json:"acceptedAt"`
}

// DispatchResult is published to sms.dispatch-results.
type DispatchResult struct {
	MessageID    string    `json:"messageId"`
	AccountID    string    `json:"accountId"`
	CampaignID   string    `json:"campaignId,omitempty"`
	To           string    `json:"to"`
	Text         string    `json:"text"`
	Priority     string    `json:"priority"`
	Cost         int64     `json:"cost"`
	Status       string    `json:"status"` // sent | failed | expired_sla_missed
	Operator     string    `json:"operator,omitempty"`
	AcceptedAt   time.Time `json:"acceptedAt"`
	DispatchedAt time.Time `json:"dispatchedAt"`
	CreatedAt    time.Time `json:"createdAt"` // messages.created_at for partitioned updates
}
