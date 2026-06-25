package consumer

import (
	"io"
	"log/slog"
	"testing"

	"github.com/neomat-prog/kafka-canary/internal/health"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

func TestProcess(t *testing.T) {
	good, err := message.New("42").Encode()
	if err != nil {
		t.Fatalf("encode setup: %v", err)
	}

	tests := []struct {
		name        string
		value       []byte
		wantErr     bool
		wantFlowing bool
	}{
		{"valid probe", good, false, true},
		{"garbage", []byte("not-a-probe"), true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handler{
				state: health.New(),
				log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			lat, err := h.process(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("process() err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && lat <= 0 {
				t.Errorf("latency = %v, want > 0 for valid probe", lat)
			}
		})
	}
}
