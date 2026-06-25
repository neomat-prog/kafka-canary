package consumer

import (
	"context"
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/health"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

type Consumer struct {
	brokers      []string
	groupID      string
	topic        string
	tls          *tls.Config
	state        *health.State
	log          *slog.Logger
	group        sarama.ConsumerGroup
	latThreshold time.Duration
}

func New(brokers []string, groupID, topic string, tlsCfg *tls.Config, latThreshold time.Duration, state *health.State, log *slog.Logger) *Consumer {
	return &Consumer{brokers: brokers, groupID: groupID, topic: topic, tls: tlsCfg, latThreshold: latThreshold, state: state, log: log}
}

func (c *Consumer) connect() (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	if c.tls != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = c.tls
	}
	return sarama.NewConsumerGroup(c.brokers, c.groupID, cfg)
}

func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		if c.group != nil {
			c.group.Close()
		}
	}()

	h := &handler{state: c.state, log: c.log, latThreshold: c.latThreshold}

	for {
		if ctx.Err() != nil {
			return nil
		}
		if c.group == nil {
			g, err := c.connect()
			if err != nil {
				c.log.Warn("consumer connect failed, retrying", "err", err)
				if sleep(ctx, 2*time.Second) {
					return nil // ctx cancelled during backoff
				}
				continue
			}
			c.group = g
		}
		if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
			c.log.Warn("consume error, reconnecting", "err", err)
			c.group.Close()
			c.group = nil // force reconnect next loop
			sleep(ctx, 2*time.Second)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}

type handler struct {
	state        *health.State
	log          *slog.Logger
	latThreshold time.Duration
	lastSeq      int64
}

func (h *handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCh := claim.Messages()
	for msg := range msgCh {
		lat, err := h.process(msg.Value)
		if err != nil {
			h.log.Warn("bad payload", "err", err)
		} else {
			h.state.RecordConsume(lat)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (h *handler) process(value []byte) (latency time.Duration, err error) {
	msg, err := message.Decode(value)
	if err != nil {
		return 0, err
	}
	latency = msg.Latency()

	if latency > h.latThreshold {
		h.log.Warn("latency spike", "latency", latency, "id", msg.ID)
	}

	if h.lastSeq != 0 && msg.Seq > h.lastSeq+1 {
		h.log.Warn("probe gap", "missing", msg.Seq-h.lastSeq-1, "from", h.lastSeq)
	}
	h.lastSeq = msg.Seq

	return latency, nil
}
