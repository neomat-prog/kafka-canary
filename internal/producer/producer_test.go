package producer

import (
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

type fakeClient struct {
	sarama.Client
	parts  []int32
	err    error
	closed bool
}

func (c *fakeClient) Partitions(string) ([]int32, error) { return c.parts, c.err }
func (c *fakeClient) Close() error                       { c.closed = true; return nil }

// fakeSync records sent messages; send fans out per partition, so it must be goroutine-safe.
type fakeSync struct {
	sarama.SyncProducer
	mu     sync.Mutex
	sent   []*sarama.ProducerMessage
	failOn map[int32]bool
	closed bool
}

func (s *fakeSync) SendMessage(m *sarama.ProducerMessage) (int32, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failOn[m.Partition] {
		return 0, 0, errors.New("broker down")
	}
	s.sent = append(s.sent, m)
	return m.Partition, int64(len(s.sent)), nil
}

func (s *fakeSync) Close() error { s.closed = true; return nil }

func newProducer(client *fakeClient, sp *fakeSync) *Producer {
	p := New(nil, "canary", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.client, p.sync = client, sp
	return p
}

func seqParts(n int32) []int32 {
	parts := make([]int32, n)
	for i := range n {
		parts[i] = i
	}
	return parts
}

func TestSendProbesEveryPartition(t *testing.T) {
	tests := []struct {
		name  string
		parts []int32
	}{
		{"single partition", []int32{0}},
		{"many partitions", []int32{0, 1, 2, 3, 4, 5, 6, 7}},
		{"more partitions than errgroup limit", seqParts(40)},
		{"no partitions", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &fakeSync{}
			p := newProducer(&fakeClient{parts: tt.parts}, sp)

			if err := p.send(3); err != nil {
				t.Fatalf("send() err = %v, want nil", err)
			}
			if len(sp.sent) != len(tt.parts) {
				t.Fatalf("sent %d messages, want %d", len(sp.sent), len(tt.parts))
			}

			var got []int32
			for _, m := range sp.sent {
				if m.Topic != "canary" {
					t.Errorf("topic = %q, want canary", m.Topic)
				}
				got = append(got, m.Partition)
			}
			slices.Sort(got)
			if !slices.Equal(got, tt.parts) {
				t.Errorf("partitions = %v, want %v", got, tt.parts)
			}
		})
	}
}

func TestSendEncodesProbe(t *testing.T) {
	sp := &fakeSync{}
	p := newProducer(&fakeClient{parts: []int32{5}}, sp)

	if err := p.send(42); err != nil {
		t.Fatalf("send: %v", err)
	}
	b, err := sp.sent[0].Value.Encode()
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}
	msg, err := message.Decode(b)
	if err != nil {
		t.Fatalf("payload does not decode: %v", err)
	}
	if msg.Seq != 42 {
		t.Errorf("seq = %d, want 42", msg.Seq)
	}
	if !strings.HasPrefix(msg.ID, "5-") {
		t.Errorf("id = %q, want partition prefix 5-", msg.ID)
	}
}

func TestSendPartialFailure(t *testing.T) {
	sp := &fakeSync{failOn: map[int32]bool{1: true, 3: true}}
	p := newProducer(&fakeClient{parts: []int32{0, 1, 2, 3}}, sp)

	err := p.send(1)
	if err == nil {
		t.Fatal("send() err = nil, want joined partition errors")
	}
	for _, want := range []string{"send part 1", "send part 3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err %q missing %q", err, want)
		}
	}
	if len(sp.sent) != 2 {
		t.Errorf("sent %d messages, want 2: one bad partition must not block the rest", len(sp.sent))
	}
	if p.sync == nil {
		t.Error("connection dropped on partition send failure, want kept")
	}
}

func TestSendPartitionsErrorDropsConnection(t *testing.T) {
	client := &fakeClient{err: errors.New("metadata unavailable")}
	sp := &fakeSync{}
	p := newProducer(client, sp)

	err := p.send(1)
	if err == nil || !strings.Contains(err.Error(), "partitions") {
		t.Fatalf("send() err = %v, want partitions error", err)
	}
	if !client.closed || !sp.closed {
		t.Errorf("client closed = %v, sync closed = %v, want both closed", client.closed, sp.closed)
	}
	if p.client != nil || p.sync != nil {
		t.Error("connection not reset, next tick would reuse dead client instead of reconnecting")
	}
}
