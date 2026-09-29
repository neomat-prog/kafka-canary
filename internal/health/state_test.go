package health

import (
	"sync"
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
			t.Errorf("latencyMs = %d, want 0 before any consume", got.LastLatencyMs)
		}
		if len(got.Partitions) != 0 {
			t.Errorf("partitions = %v, want empty before any consume", got.Partitions)
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

func TestSnapshotMultiPartition(t *testing.T) {
	const staleAfter = 50 * time.Millisecond
	s := New(staleAfter)

	s.RecordConsume(0, 5*time.Millisecond)
	time.Sleep(2 * staleAfter)
	s.RecordConsume(1, 20*time.Millisecond)

	got := s.Snapshot()
	if got.MessagesFlowing {
		t.Errorf("flowing = true, want false while partition 0 is stale")
	}
	if len(got.StalePartitions) != 1 || got.StalePartitions[0] != 0 {
		t.Errorf("stalePartitions = %v, want [0]", got.StalePartitions)
	}
	if got.LastLatencyMs != 20 {
		t.Errorf("lastLatencyMs = %d, want 20 (max across partitions)", got.LastLatencyMs)
	}
	if len(got.Partitions) != 2 {
		t.Errorf("partitions = %v, want 2 entries", got.Partitions)
	}
}

func TestSetAssigned(t *testing.T) {
	const staleAfter = time.Minute
	s := New(staleAfter)
	s.RecordConsume(0, time.Millisecond)
	s.RecordConsume(1, time.Millisecond)
	s.RecordConsume(2, time.Millisecond)

	s.SetAssigned([]int32{1, 2, 3})

	got := s.Snapshot()
	if _, ok := got.Partitions[0]; ok {
		t.Errorf("partition 0 still present after rebalance dropped it: %v", got.Partitions)
	}
	if len(got.Partitions) != 2 {
		t.Errorf("partitions = %v, want only the 2 still-owned ones with data", got.Partitions)
	}
	if !got.MessagesFlowing {
		t.Errorf("flowing = false, want true: remaining partitions are fresh")
	}
}

func TestSetAssignedEmptyStopsFlowing(t *testing.T) {
	s := New(time.Minute)
	s.RecordConsume(0, time.Millisecond)
	s.SetAssigned(nil)
	if s.Snapshot().MessagesFlowing {
		t.Errorf("flowing = true, want false when this consumer owns no partitions")
	}
}

func TestStateConcurrent(t *testing.T) {
	s := New(time.Minute)
	const goroutines, iters = 8, 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				switch g % 3 {
				case 0:
					s.RecordConsume(int32(i%4), time.Duration(i)*time.Millisecond)
				case 1:
					s.Snapshot()
				default:
					s.SetAssigned([]int32{0, 1, 2, 3})
				}
			}
		}(g)
	}
	wg.Wait()

	if got := s.Snapshot(); len(got.Partitions) > 4 {
		t.Errorf("partitions = %v, want at most 4", got.Partitions)
	}
}
