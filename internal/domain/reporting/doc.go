// Package reporting serves read-side queries for cmd/reporting-api:
// GET /v1/messages/{id} (Postgres point lookup), GET /v1/reports and
// GET /v1/campaigns/{id}/report (ClickHouse aggregation). See
// ARCHITECTURE.md sections 9.4 and 11.
package reporting
