package metrics

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type Stats struct {
	mu             sync.Mutex
	produced       uint64
	consumed       uint64
	errors         uint64
	lastLatency    time.Duration
	lastConsumedAt time.Time
}

func NewStats() *Stats { return &Stats{} }

func (s *Stats) RecordProduce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.produced++
}

func (s *Stats) RecordConsume(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumed++
	s.lastLatency = latency
	s.lastConsumedAt = time.Now()
}

func (s *Stats) RecordError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors++
}

type StatusResp struct {
	MessagesFlowing bool   `json:"messagesFlowing"`
	LastLatencyMs   int64  `json:"lastLatencyMs"`
	LastConsumedAgo string `json:"lastConsumedAgo"`
	Produced        uint64 `json:"produced"`
	Consumed        uint64 `json:"consumed"`
	Errors          uint64 `json:"errors"`
}

func (s *Stats) Snapshot(staleAfter time.Duration) StatusResp {
	s.mu.Lock()
	defer s.mu.Unlock()

	flowing := !s.lastConsumedAt.IsZero() && time.Since(s.lastConsumedAt) < staleAfter

	ago := "never"
	if !s.lastConsumedAt.IsZero() {
		ago = time.Since(s.lastConsumedAt).Round(time.Second).String()
	}

	return StatusResp{
		MessagesFlowing: flowing,
		LastLatencyMs:   s.lastLatency.Milliseconds(),
		LastConsumedAgo: ago,
		Produced:        s.produced,
		Consumed:        s.consumed,
		Errors:          s.errors,
	}
}

func (s *Stats) StatusHandler(staleAfter time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := s.Snapshot(staleAfter)
		w.Header().Set("Content-Type", "application/json")
		if !resp.MessagesFlowing {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(resp)
	}
}
