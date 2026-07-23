// Package campaigns owns batch/campaign send logic. See ARCHITECTURE.md
// section 9.
//
// Planned contents:
//   - Accept handler for POST /v1/campaigns (cmd/api-gateway) - always
//     normal priority, all-or-nothing on insufficient balance.
//   - Campaign Expander handler: consumed by cmd/campaign-expander, generates
//     deterministic per-recipient message IDs and fans out to messaging's
//     dispatch topics.
//   - Campaign repository: wraps db/queries/campaigns.sql.
package campaigns
