package stress

import "testing"

func TestClamp(t *testing.T) {
	got := Opts{Rate: 1e9, SizeBytes: -5, DurationSec: 0}.Clamp()
	want := Opts{Rate: maxRate, SizeBytes: 1, DurationSec: 1}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}
