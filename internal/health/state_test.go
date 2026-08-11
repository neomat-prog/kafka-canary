package health

import (
	"testing"
	"time"
)

func TestSnapshot(t *testing.T) {
	const staleAfter = 100 * time.Millisecond

	t.Run("never consumed", func(t *testing.T) {
		got := New(staleAfter).Snapshot()
		if got.MessagesFlowing {
			t.Errorf("flowing = true, want false before any consume")
		}
		if got.LastLatencyMs != 0 {
			t.Errorf("ago = %d, want %q", got.LastLatencyMs, "never")
		}
	})

	t.Run("fresh consume flows", func(t *testing.T) {
		s := New(staleAfter)
		s.RecordConsume(0, time.Millisecond)
		got := s.Snapshot()
		if !got.MessagesFlowing {
			t.Errorf("flowing = false, want true right after consume")
		}
		if got.LastLatencyMs != 1 {
			t.Errorf("latencyMs = %d, want 1", got.LastLatencyMs)
		}
	})

	t.Run("stale consume stops flowing", func(t *testing.T) {
		s := New(staleAfter)
		s.RecordConsume(0, time.Millisecond)
		time.Sleep(2 * staleAfter)
		if s.Snapshot().MessagesFlowing {
			t.Errorf("flowing = true, want false after staleAfter elapsed")
		}
	})
}
