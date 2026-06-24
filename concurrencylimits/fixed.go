package concurrencylimits

import "time"

// FixedLimit is a non-adaptive Limit that always returns the same value
// (Netflix's FixedLimit). Useful as a baseline and for deterministic tests.
type FixedLimit int

// OnSample implements Limit (no-op — the limit never changes).
func (FixedLimit) OnSample(time.Duration, int, bool) {}

// Limit implements Limit.
func (f FixedLimit) Limit() int { return int(f) }
