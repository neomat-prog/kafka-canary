package consumer

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/health"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

func newHandler(out io.Writer) *handler {
	return &handler{
		state:        health.New(time.Minute),
		log:          slog.New(slog.NewTextHandler(out, nil)),
		latThreshold: time.Minute,
	}
}

func encode(t *testing.T, id string, seq int64) []byte {
	t.Helper()
	b, err := message.New(id, seq).Encode()
	if err != nil {
		t.Fatalf("encode setup: %v", err)
	}
	return b
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		wantErr bool
	}{
		{"valid probe", encode(t, "42", 1), false},
		{"garbage", []byte("not-a-probe"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHandler(io.Discard)

			lat, _, err := h.process(tt.value, 0)
			if (err != nil) != tt.wantErr {
				t.Fatalf("process() err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && lat <= 0 {
				t.Errorf("latency = %v, want > 0 for valid probe", lat)
			}
		})
	}
}

func TestProcessBadPayloadKeepsLastSeq(t *testing.T) {
	h := newHandler(io.Discard)
	_, seq, err := h.process([]byte("not-a-probe"), 7)
	if err == nil {
		t.Fatal("process() err = nil, want decode error")
	}
	if seq != 7 {
		t.Errorf("seq = %d, want 7: a bad payload must not reset gap tracking", seq)
	}
}

// feed runs seqs through one handler the way ConsumeClaim does and returns the logs.
func feed(t *testing.T, seqs ...int64) string {
	t.Helper()
	var buf bytes.Buffer
	h := newHandler(&buf)

	var lastSeq int64
	for _, s := range seqs {
		_, seq, err := h.process(encode(t, "probe", s), lastSeq)
		if err != nil {
			t.Fatalf("process(seq=%d): %v", s, err)
		}
		lastSeq = seq
	}
	return buf.String()
}

func TestProcessDetectsGap(t *testing.T) {
	tests := []struct {
		name        string
		seqs        []int64
		wantGap     bool
		wantMissing string
	}{
		{"contiguous", []int64{1, 2, 3}, false, ""},
		{"one missing", []int64{1, 2, 4}, true, "missing=1"},
		{"several missing", []int64{1, 5}, true, "missing=3"},
		{"first probe never reports a gap", []int64{9}, false, ""},
		{"restart mid-stream is still a gap", []int64{4, 9}, true, "missing=4"},
		{"duplicate is not a gap", []int64{3, 3}, false, ""},
		{"out of order is not a gap", []int64{5, 4}, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := feed(t, tt.seqs...)
			gotGap := strings.Contains(logs, "probe gap")

			if gotGap != tt.wantGap {
				t.Fatalf("probe gap logged = %v, want %v for seqs %v\nlogs: %s", gotGap, tt.wantGap, tt.seqs, logs)
			}
			if tt.wantMissing != "" && !strings.Contains(logs, tt.wantMissing) {
				t.Errorf("logs missing %q for seqs %v\nlogs: %s", tt.wantMissing, tt.seqs, logs)
			}
		})
	}
}

func TestProcessLatencySpikeLogged(t *testing.T) {
	var buf bytes.Buffer
	h := newHandler(&buf)
	h.latThreshold = time.Nanosecond

	if _, _, err := h.process(encode(t, "slow", 1), 0); err != nil {
		t.Fatalf("process: %v", err)
	}
	if !strings.Contains(buf.String(), "latency spike") {
		t.Errorf("no latency spike logged above threshold\nlogs: %s", buf.String())
	}
}

type fakeSession struct{ sarama.ConsumerGroupSession }

func (fakeSession) MarkMessage(*sarama.ConsumerMessage, string) {}

type fakeClaim struct {
	sarama.ConsumerGroupClaim
	ch chan *sarama.ConsumerMessage
}

func (c fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.ch }

type lockedWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// Sarama runs one ConsumeClaim goroutine per partition on a shared handler;
// gap tracking must stay per-partition. Run with -race.
func TestConsumeClaimConcurrentPartitions(t *testing.T) {
	var out lockedWriter
	h := newHandler(&out)

	var wg sync.WaitGroup
	for p := int32(0); p < 8; p++ {
		ch := make(chan *sarama.ConsumerMessage, 100)
		for s := int64(1); s <= 100; s++ {
			ch <- &sarama.ConsumerMessage{Partition: p, Offset: s, Value: encode(t, "probe", s)}
		}
		close(ch)
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ConsumeClaim(fakeSession{}, fakeClaim{ch: ch})
		}()
	}
	wg.Wait()

	if strings.Contains(out.buf.String(), "probe gap") {
		t.Fatalf("false gap: lastSeq shared across partitions\nlogs: %s", out.buf.String())
	}
}
