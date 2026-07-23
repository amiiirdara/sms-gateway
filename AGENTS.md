# AGENTS.md

Orientation for any agent (or human) working on this repo cold.

## What this is

A multi-tenant SMS Gateway (send SMS, manage prepaid credit, batch "campaign" sends, delivery reports) built in Go. Full design rationale lives in [ARCHITECTURE.md](ARCHITECTURE.md) - read it before making non-trivial changes, especially before touching balance/billing logic, the Outbox/Inbox pipeline, or the Express SLA path.

## Repo layout

```
cmd/                One main package per deployable binary (api-gateway, outbox-relay,
                     campaign-expander, dispatcher, report-sink, billing-consumer,
                     reconciler, reporting-api, operator-mock)
internal/
  config/            Env/config loading
  platform/          Thin wrappers around infra: postgres, redis, kafka, clickhouse, inbox
  domain/            Business logic by domain: billing, messaging, campaigns, reporting
  db/sqlc/           Generated code from sqlc - do not hand-edit
db/
  migrations/        golang-migrate SQL migrations (source of truth for Postgres schema)
  queries/           Hand-written .sql files that sqlc compiles into internal/db/sqlc
clickhouse/init/     ClickHouse table DDL
openapi/openapi.yaml REST API contract
```

Dependencies point inward: `cmd` -> `domain` -> `platform`. `domain` packages must not import each other directly across bounded contexts (e.g. `messaging` must not reach into `billing`'s internals) - communicate through Kafka events or well-defined interfaces instead.

## Before you implement

1. Read [ARCHITECTURE.md](ARCHITECTURE.md) for the relevant section.
2. Check `.cursor/rules/` - they encode the non-negotiable conventions for this repo (clean code/SOLID, project layout, query performance, Kafka consumer conventions, tenant-isolation security, testing standards). They apply automatically; read them if you want the full rationale.
3. For repeatable multi-step tasks, use the skills in `.cursor/skills/`: `add-kafka-consumer` when adding a new Kafka consumer service, `add-db-query` when adding a new migration + query.

## Non-negotiables (see `.cursor/rules/` for full detail)

- Never trust a client-supplied account/tenant ID - always resolve it from the authenticated API key.
- Every Kafka consumer must be idempotent via the Inbox pattern before any side effect.
- Every Postgres table has `created_at`/`updated_at`.
- No GORM, no `SELECT *`, no N+1 queries - `sqlc`-generated queries only.
- Balance changes only ever happen through the atomic Redis Lua script described in ARCHITECTURE.md section 5 - never a plain `GET` then `SET`.
