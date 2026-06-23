package health

import (
	"sync/atomic"
	"time"
)

// State hilds the minimal facts /ready and /status need: when the last
// probe was consumed, and its e2e latency. Lock-free.
type State struct {
	lastConsumedNanos atomic.Int64
	lastLatencyNanos  atomic.Int64
}

func New() *State {
	return &State{}
}

func (s *State) RecordConsume(latency time.Duration) {
	s.lastConsumedNanos.Store(time.Now().UnixNano())
	s.lastLatencyNanos.Store(int64(latency))
}

type Status struct {
	MessagesFlowing bool   `json:"messagesFlowing"`
	LastLatencyMs   int64  `json:"lastLatencyMs"`
	LastConsumedAgo string `json:"lastConsumedAgo"`
}

func (s *State) Snapshot(staleAfter time.Duration) Status {
	last := s.lastConsumedNanos.Load()
	if last == 0 {
		return Status{MessagesFlowing: false, LastConsumedAgo: "never"}
	}
	ago := time.Since(time.Unix(0, last))
	return Status{
		MessagesFlowing: ago < staleAfter,
		LastLatencyMs:   s.lastLatencyNanos.Load() / int64(time.Millisecond),
		LastConsumedAgo: ago.Round(time.Millisecond).String(),
	}
}
