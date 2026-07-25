package auth

import (
	"sync"
	"time"
)

type cacheEntry struct {
	account Account
	expires time.Time
}

// AccountCache is a short-TTL cache of API-key-hash → account.
type AccountCache struct {
	mu    sync.RWMutex
	ttl   time.Duration
	items map[string]cacheEntry
}

// NewAccountCache returns a cache with the given TTL (e.g. 30s).
func NewAccountCache(ttl time.Duration) *AccountCache {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &AccountCache{ttl: ttl, items: make(map[string]cacheEntry)}
}

// Get returns a cached account for keyHash if present and not expired.
func (c *AccountCache) Get(keyHash string) (Account, bool) {
	c.mu.RLock()
	e, ok := c.items[keyHash]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expires) {
		return Account{}, false
	}
	return e.account, true
}

// Set stores account for keyHash.
func (c *AccountCache) Set(keyHash string, acc Account) {
	c.mu.Lock()
	c.items[keyHash] = cacheEntry{account: acc, expires: time.Now().Add(c.ttl)}
	// Opportunistic prune when large.
	if len(c.items) > 10_000 {
		now := time.Now()
		for k, v := range c.items {
			if now.After(v.expires) {
				delete(c.items, k)
			}
		}
	}
	c.mu.Unlock()
}

// Invalidate removes one key (unused today; handy for rotation).
func (c *AccountCache) Invalidate(keyHash string) {
	c.mu.Lock()
	delete(c.items, keyHash)
	c.mu.Unlock()
}
