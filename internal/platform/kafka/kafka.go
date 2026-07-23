// Package kafka provides thin helpers around segmentio/kafka-go for producers
// and consumers used by the SMS Gateway pipeline.
package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Topic names used across the pipeline.
const (
	TopicOutboundNormal  = "sms.outbound.normal"
	TopicOutboundExpress = "sms.outbound.express"
	TopicDispatchResults = "sms.dispatch-results"
	TopicDLQ             = "sms.dlq"
)

// NewWriter creates a Kafka writer for the given topic.
func NewWriter(brokers string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(splitBrokers(brokers)...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
	}
}

// NewReader creates a Kafka reader in the given consumer group.
func NewReader(brokers, topic, groupID string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        splitBrokers(brokers),
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commit after successful processing
		StartOffset:    kafkago.FirstOffset,
	})
}

// Publish writes a single message with the given key/value.
func Publish(ctx context.Context, w *kafkago.Writer, key, value []byte) error {
	err := w.WriteMessages(ctx, kafkago.Message{
		Key:   key,
		Value: value,
		Time:  time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("kafka: publish to %s: %w", w.Topic, err)
	}
	return nil
}

// EnsureTopics creates topics if they do not exist (local-dev convenience).
func EnsureTopics(ctx context.Context, brokers string, topics ...string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", splitBrokers(brokers)[0])
	if err != nil {
		return fmt.Errorf("kafka: dial: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka: controller: %w", err)
	}
	ctrlConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("kafka: dial controller: %w", err)
	}
	defer ctrlConn.Close()

	configs := make([]kafkago.TopicConfig, 0, len(topics))
	for _, t := range topics {
		configs = append(configs, kafkago.TopicConfig{
			Topic:             t,
			NumPartitions:     6,
			ReplicationFactor: 1,
		})
	}
	if err := ctrlConn.CreateTopics(configs...); err != nil {
		// Ignore "already exists" style errors by checking message.
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") &&
			!strings.Contains(strings.ToLower(err.Error()), "topic with this name already exists") {
			return fmt.Errorf("kafka: create topics: %w", err)
		}
	}
	return nil
}

func splitBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}
