package messaging

import "time"

// ExpressExpired reports whether an Express message has passed its hard deadline.
// deadline is expected to be RFC3339Nano (as written by Accept into the outbox).
func ExpressExpired(deadline string, now time.Time) bool {
	if deadline == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339Nano, deadline)
	if err != nil {
		// Fall back to RFC3339 without nanos.
		t, err = time.Parse(time.RFC3339, deadline)
		if err != nil {
			return false
		}
	}
	return now.After(t)
}
