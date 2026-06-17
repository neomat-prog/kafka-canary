package internal

import "testing"

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
