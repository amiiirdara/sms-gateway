// Package redis will hold the go-redis client setup plus the atomic Lua
// scripts that are the core of this system's correctness guarantees.
//
// Planned surface (see ARCHITECTURE.md section 5):
//   - NewClient(addr string) *redis.Client
//   - CheckAndDebit(ctx, accountID, cost, outboxPayload) - the single Lua
//     script that atomically checks balance, decrements it, and appends the
//     outbox entry. This is the only place a balance is ever mutated.
//   - Outbox stream helpers (XADD/XREADGROUP/XACK/XAUTOCLAIM wrappers) used
//     by cmd/outbox-relay and cmd/campaign-expander.
//
// Not implemented yet - this is a structural placeholder from the initial
// repo scaffold. Add the redis/go-redis/v9 dependency when implementing.
package redis
