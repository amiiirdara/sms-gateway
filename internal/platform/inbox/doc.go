// Package inbox implements the generic Inbox dedup pattern used by every
// Kafka consumer in this repo (dispatcher, report-sink, billing-consumer,
// campaign-expander). See ARCHITECTURE.md section 5.2 and
// .cursor/rules/kafka-consumer-conventions.mdc.
//
// Planned surface:
//   - MarkProcessed(ctx, tx, consumerName, eventID) (alreadyProcessed bool, err error)
//     backed by the processed_events table (db/queries/processed_events.sql),
//     always called inside the same transaction as the business write it guards.
//
// Not implemented yet - this is a structural placeholder from the initial
// repo scaffold.
package inbox
