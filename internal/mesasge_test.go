package internal

import (
	"testing"
	"time"
)

func TestProbeRoundTrip(t *testing.T) {
	in := New("abc")
	b, err := in.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := Decode(b)
	if out.ID != in.ID {
		t.Errorf("id: got %q want %q", out.ID, in.ID)
	}
	if out.ProducedAt != in.ProducedAt {
		t.Errorf("producedAt: got %d want %d", out.ProducedAt, in.ProducedAt)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("want error for bad json, got nil")
	}
}

func TestLatency(t *testing.T) {
	p := Probe{ID: "x", ProducedAt: time.Now().Add(-2 * time.Second).UnixNano()}
	got := p.Latency()
	if got < time.Second || got > 3*time.Second {
		t.Errorf("latency = %s, want ~2", got)
	}
}
