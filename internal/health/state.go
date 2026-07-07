package health

import (
	"sync"
	"time"
)

type State struct {
	mu    sync.Mutex
	parts map[int32]partStat
}

// State hilds the minimal facts /ready and /status need: when the last
// probe was consumed, and its e2e latency. Lock-free.
type partStat struct {
	lastConsumedNanos int64
	lastLatencyNanos  int64
}

func New() *State {
	return &State{parts: map[int32]partStat{}}
}

func (s *State) RecordConsume(part int32, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parts[part] = partStat{time.Now().UnixNano(), int64(latency)}
}

type Status struct {
	MessagesFlowing bool             `json:"messagesFlowing"`
	LastLatencyMs   int64            `json:"lastLatencyMs"`
	StalePartitions []int32          `json:"stalePartitions,omitempty"`
	Partitions      map[int32]string `json:"partitions"`
}

func (s *State) Snapshot(staleAfter time.Duration) Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Status{MessagesFlowing: len(s.parts) > 0, Partitions: map[int32]string{}}
	var maxLat int64
	for p, ps := range s.parts {
		ago := time.Since(time.Unix(0, ps.lastConsumedNanos))
		st.Partitions[p] = ago.Round(time.Millisecond).String()
		if ago >= staleAfter {
			st.MessagesFlowing = false
			st.StalePartitions = append(st.StalePartitions, p)
		}
		if ps.lastLatencyNanos > maxLat {
			maxLat = ps.lastLatencyNanos
		}
	}
	st.LastLatencyMs = maxLat / int64(time.Millisecond)
	return st
}
