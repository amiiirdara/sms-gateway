---
name: add-db-query
description: >-
  End-to-end recipe for adding a new Postgres migration and a new sqlc query in
  the SMS Gateway repo: write the migration, write the .sql query, regenerate
  sqlc code, and wire the repository method. Use when the user asks to add a
  new table/column, a new database query, or change the Postgres schema.
disable-model-invocation: true
---

# Add a DB Migration + Query

Follow `.cursor/rules/db-and-query-performance.mdc` throughout.

## Steps

1. **Write the migration**: create a new pair of files in `db/migrations/` using `golang-migrate`'s naming (`<next_number>_<description>.up.sql` / `.down.sql`). New tables always include `created_at`/`updated_at`. Add any index the new query needs in the same migration.
2. **Apply it locally**: `make migrate-up` (wraps `migrate -path db/migrations -database $DATABASE_URL up`).
3. **Write the query**: add or extend a `.sql` file in `db/queries/` (grouped by table/domain, e.g. `messages.sql`, `campaigns.sql`). Use `sqlc`'s annotation comments (`-- name: GetMessageByID :one`, etc.). Never `SELECT *` - list columns explicitly.
4. **Regenerate**: `make sqlc` (wraps `sqlc generate`). This writes into `internal/db/sqlc` - never hand-edit that directory.
5. **Wire the repository method**: add a method on the relevant repository type in `internal/domain/<domain>/` that calls the generated `sqlc` query. Handlers/consumers call the repository, never the generated `sqlc` code directly.
6. **Check for N+1 / missing indexes**: if the query is on a hot path (`messages`, `ledger_entries`), run `EXPLAIN ANALYZE` against it locally and confirm the new index is used.
7. **Test**: add/extend a `testcontainers-go` test exercising the new repository method against a real Postgres instance.

## Reference

See `ARCHITECTURE.md` section 11 (Data model) for the current schema, and `.cursor/rules/db-and-query-performance.mdc` for the non-negotiable conventions.
