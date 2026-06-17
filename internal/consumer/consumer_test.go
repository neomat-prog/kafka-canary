package consumer

import (
	"testing"
	"time"

	"github.com/neomat-prog/kafka-canary/internal/message"
	"github.com/neomat-prog/kafka-canary/internal/metrics"
)

func TestProcess(t *testing.T) {
	good, err := message.New("42").Encode()
	if err != nil {
		t.Fatalf("encode setup: %v", err)
	}

	tests := []struct {
		name         string
		value        []byte
		wantErr      bool
		wantConsumed uint64
		wantErrors   uint64
	}{
		{"valid probe", good, false, 1, 0},
		{"garbage", []byte("not-a-probe"), true, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := metrics.NewStats()
			h := handler{stats: st} // no log — process never logs

			err := h.process(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("process() err = %v, wantErr %v", err, tt.wantErr)
			}

			snap := st.Snapshot(time.Hour) // staleAfter irrelevant; just reading counts
			if snap.Consumed != tt.wantConsumed {
				t.Errorf("consumed = %d, want %d", snap.Consumed, tt.wantConsumed)
			}
			if snap.Errors != tt.wantErrors {
				t.Errorf("errors = %d, want %d", snap.Errors, tt.wantErrors)
			}
		})
	}
}
