package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/message"
	"github.com/neomat-prog/kafka-canary/internal/metrics"
)

type Consumer struct {
	group sarama.ConsumerGroup
	topic string
	stats *metrics.Stats
	log   *slog.Logger
}

func New(brokers []string, groupID, topic string, stats *metrics.Stats, log *slog.Logger) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}
	return &Consumer{group: group, topic: topic, stats: stats, log: log}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.group.Close()
	h := handler{stats: c.stats, log: c.log}
	for {
		if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

type handler struct {
	stats *metrics.Stats
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
		h.stats.RecordError()
		return err
	}
	h.stats.RecordConsume(probe.Latency())
	return nil
}
