// Package messaging owns single-message send/accept/dispatch/report logic.
// See ARCHITECTURE.md sections 4, 5, 7, and 8.
//
// Planned contents:
//   - Accept handler for POST /v1/messages (cmd/api-gateway).
//   - OperatorAdapter interface + Router (Strategy pattern - see
//     .cursor/rules/go-clean-code.mdc) consumed by cmd/dispatcher.
//   - Express deadline enforcement.
//   - Report Sink handler: consumed by cmd/report-sink.
//   - Message repository: wraps db/queries/messages.sql.
package messaging
