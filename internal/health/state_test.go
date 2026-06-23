package health

import (
	"testing"
	"time"
)

func TestSnapshot(t *testing.T) {
	const staleAfter = 100 * time.Millisecond

	t.Run("never consumed", func(t *testing.T) {
		got := New().Snapshot(staleAfter)
		if got.MessagesFlowing {
			t.Errorf("flowing = true, want false before any consume")
		}
		if got.LastConsumedAgo != "never" {
			t.Errorf("ago = %q, want %q", got.LastConsumedAgo, "never")
		}
	})

	t.Run("fresh consume flows", func(t *testing.T) {
		s := New()
		s.RecordConsume(7 * time.Millisecond)
		got := s.Snapshot(staleAfter)
		if !got.MessagesFlowing {
			t.Errorf("flowing = false, want true right after consume")
		}
		if got.LastLatencyMs != 7 {
			t.Errorf("latencyMs = %d, want 7", got.LastLatencyMs)
		}
	})

	t.Run("stale consume stops flowing", func(t *testing.T) {
		s := New()
		s.RecordConsume(time.Millisecond)
		time.Sleep(2 * staleAfter)
		if s.Snapshot(staleAfter).MessagesFlowing {
			t.Errorf("flowing = true, want false after staleAfter elapsed")
		}
	})
}
