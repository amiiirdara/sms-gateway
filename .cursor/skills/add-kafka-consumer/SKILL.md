---
name: add-kafka-consumer
description: >-
  Scaffolds a new Kafka consumer service (cmd/<name>) in the SMS Gateway repo,
  wired with Inbox dedup, manual offset commits, graceful shutdown, and metrics,
  following repo conventions. Use when the user asks to add a new Kafka
  consumer, a new event-driven service, or a new cmd/ binary that reads from Kafka.
disable-model-invocation: true
---

# Add a Kafka Consumer

Follow `.cursor/rules/kafka-consumer-conventions.mdc` and `.cursor/rules/go-project-layout.mdc` throughout.

## Steps

1. **Create the binary**: `cmd/<name>/main.go`. Wire config (`internal/config`), a Postgres pool (`internal/platform/postgres`), and a Kafka consumer group (`internal/platform/kafka`). Keep `main.go` to wiring only.
2. **Create the domain logic**: put the actual message-handling logic in `internal/domain/<relevant-domain>/`, exposing a small interface like `Handle(ctx context.Context, event Event) error`. `main.go` calls this from the consume loop - do not put business logic directly in `cmd/`.
3. **Wire the Inbox check**: before calling `Handle`, check `processed_events` for `(consumer_name, event_id)` via `internal/platform/inbox`. If already processed, skip `Handle` but still proceed to offset commit. Insert into `processed_events` in the **same transaction** as `Handle`'s business write.
4. **Manual offset commit**: only commit the Kafka offset after the transaction (business write + Inbox insert) succeeds. Never auto-commit.
5. **DLQ on exhausted retries**: after N retries (exponential backoff), publish the original message + failure reason to `sms.dlq` and commit the offset so the consumer isn't stuck.
6. **Graceful shutdown**: listen for `SIGINT`/`SIGTERM`, stop consuming, finish in-flight message processing, close the Kafka consumer group and DB pool cleanly.
7. **Metrics**: expose consumer lag, messages processed, DLQ count, and processing latency via Prometheus (reuse the shared metrics helper if one exists in `internal/platform`).
8. **Tests**: unit-test the domain handler against a fake dependency; add a `testcontainers-go` integration test that publishes the same message twice and asserts only one side effect occurs (see `.cursor/rules/testing-standards.mdc`).

## Reference

See `ARCHITECTURE.md` sections 5 (Outbox/Inbox) and 8 (Component breakdown) for the design this pattern is implementing, and the existing `report-sink` / `billing-consumer` / `dispatcher` services as the canonical examples to copy from.
