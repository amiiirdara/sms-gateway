// Package postgres will hold the pgx connection pool setup used by every
// service that talks to Postgres.
//
// Planned surface (see ARCHITECTURE.md section 12 and
// .cursor/rules/db-and-query-performance.mdc):
//   - NewPool(ctx, dsn string) (*pgxpool.Pool, error)
//   - Thin transaction helper used by the Inbox pattern (internal/platform/inbox)
//     to run a business write + processed_events insert atomically.
//
// Not implemented yet - this is a structural placeholder from the initial
// repo scaffold. Add the jackc/pgx/v5 dependency when implementing.
package postgres
