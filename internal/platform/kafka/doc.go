// Package kafka will hold the segmentio/kafka-go producer/consumer-group
// wrappers shared by every Kafka-facing service.
//
// Planned surface (see ARCHITECTURE.md sections 5 and 8):
//   - NewIdempotentProducer(brokers []string) *kafka.Writer
//   - NewConsumerGroup(brokers []string, topic, groupID string) *kafka.Reader
//   - A generic consume-loop helper that wires manual offset commits +
//     the Inbox check (internal/platform/inbox) around a handler func,
//     per .cursor/rules/kafka-consumer-conventions.mdc.
//
// Not implemented yet - this is a structural placeholder from the initial
// repo scaffold. Add the segmentio/kafka-go dependency when implementing.
package kafka
