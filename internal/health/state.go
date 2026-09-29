package health

import (
	"sync"
	"time"
)

type State struct {
	mu         sync.Mutex
	parts      map[int32]partStat
	staleAfter time.Duration
}

type partStat struct {
	lastConsumedNanos int64
	lastLatencyNanos  int64
}

func New(staleAfter time.Duration) *State {
	return &State{parts: map[int32]partStat{}, staleAfter: staleAfter}
}

func (s *State) RecordConsume(part int32, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parts[part] = partStat{time.Now().UnixNano(), int64(latency)}
}

func (s *State) SetAssigned(parts []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := make(map[int32]partStat, len(parts))
	for _, p := range parts {
		if ps, ok := s.parts[p]; ok {
			keep[p] = ps
		}
	}
	s.parts = keep
}

type Status struct {
	MessagesFlowing bool             `json:"messagesFlowing"`
	LastLatencyMs   int64            `json:"lastLatencyMs"`
	StalePartitions []int32          `json:"stalePartitions,omitempty"`
	Partitions      map[int32]string `json:"partitions"`
}

func (s *State) Snapshot() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Status{MessagesFlowing: len(s.parts) > 0, Partitions: map[int32]string{}}
	var maxLat int64
	for p, ps := range s.parts {
		ago := time.Since(time.Unix(0, ps.lastConsumedNanos))
		st.Partitions[p] = ago.Round(time.Millisecond).String()
		if ago >= s.staleAfter {
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
