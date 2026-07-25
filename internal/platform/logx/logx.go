// Package logx provides a small slog-based logger with message/campaign context.
package logx

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey int

const (
	msgKey ctxKey = iota
	campKey
	acctKey
)

// New returns a JSON slog logger writing to stderr.
func New(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})).
		With("service", service)
}

// WithMessageID attaches message_id to ctx for later LogAttrs.
func WithMessageID(ctx context.Context, messageID string) context.Context {
	return context.WithValue(ctx, msgKey, messageID)
}

// WithCampaignID attaches campaign_id to ctx.
func WithCampaignID(ctx context.Context, campaignID string) context.Context {
	return context.WithValue(ctx, campKey, campaignID)
}

// WithAccountID attaches account_id to ctx.
func WithAccountID(ctx context.Context, accountID string) context.Context {
	return context.WithValue(ctx, acctKey, accountID)
}

// AttrsFromContext returns slog attributes from ctx correlation ids.
func AttrsFromContext(ctx context.Context) []any {
	var attrs []any
	if v, ok := ctx.Value(msgKey).(string); ok && v != "" {
		attrs = append(attrs, "message_id", v)
	}
	if v, ok := ctx.Value(campKey).(string); ok && v != "" {
		attrs = append(attrs, "campaign_id", v)
	}
	if v, ok := ctx.Value(acctKey).(string); ok && v != "" {
		attrs = append(attrs, "account_id", v)
	}
	return attrs
}

// Info logs at info with optional ctx correlation fields.
func Info(ctx context.Context, logger *slog.Logger, msg string, args ...any) {
	logger.InfoContext(ctx, msg, append(AttrsFromContext(ctx), args...)...)
}

// Error logs at error with optional ctx correlation fields.
func Error(ctx context.Context, logger *slog.Logger, msg string, args ...any) {
	logger.ErrorContext(ctx, msg, append(AttrsFromContext(ctx), args...)...)
}
