package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/health"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

type Consumer struct {
	brokers []string
	groupID string
	topic   string
	state   *health.State
	log     *slog.Logger
	group   sarama.ConsumerGroup // nil until first successful connect
}

func New(brokers []string, groupID, topic string, state *health.State, log *slog.Logger) *Consumer {
	return &Consumer{brokers: brokers, groupID: groupID, topic: topic, state: state, log: log}
}

func (c *Consumer) connect() (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	return sarama.NewConsumerGroup(c.brokers, c.groupID, cfg)
}

func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		if c.group != nil {
			c.group.Close()
		}
	}()
	h := handler{state: c.state, log: c.log}
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
	state *health.State
	log   *slog.Logger
}

func (handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCh := claim.Messages()
	for msg := range msgCh {
		if err := h.process(msg.Value); err != nil {
			h.log.Warn("bad payload", "err", err)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (h handler) process(value []byte) error {
	probe, err := message.Decode(value)
	if err != nil {
		return err
	}
	h.state.RecordConsume(probe.Latency())
	return nil
}
