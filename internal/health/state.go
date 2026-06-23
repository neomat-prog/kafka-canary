package health

import "sync/atomic"

// State hilds the minimal facts /ready and /status need: when the last
// probe was consumed, and its e2e latency. Lock-free.
type State struct {
	lastConsumedNanos atomic.Int64
	lastLatencyNanos  atomic.Int64
}
