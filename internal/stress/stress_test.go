package stress

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestClamp(t *testing.T) {
	got := Stress{Rate: 1e9, SizeBytes: -5, DurationSec: 0}.Clamp()
	want := Stress{Rate: maxRate, SizeBytes: 1, DurationSec: 1}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestRunAgainstKafka(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	var buf bytes.Buffer
	r := New(strings.Split(brokers, ","), "stress-test", nil, slog.New(slog.NewTextHandler(&buf, nil)))
	r.run(context.Background(), Stress{Rate: 1000, SizeBytes: 512, DurationSec: 2}.Clamp())

	out := buf.String()
	if !strings.Contains(out, "stress done") || !strings.Contains(out, "failed=0") || strings.Contains(out, "sent=0 ") {
		t.Fatalf("bad run:\n%s", out)
	}
}
