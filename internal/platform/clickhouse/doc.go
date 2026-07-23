// Package clickhouse will hold the ClickHouse client setup used by the
// Report Sink (writes) and Reporting API (reads).
//
// Planned surface (see ARCHITECTURE.md sections 9 and 11):
//   - NewClient(addr string) (driver.Conn, error)
//   - Batch-insert helper for message_events (Report Sink writes in batches,
//     not one row per HTTP-style round trip).
//
// Not implemented yet - this is a structural placeholder from the initial
// repo scaffold. Add the ClickHouse/clickhouse-go/v2 dependency when implementing.
package clickhouse
