package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

type Consumer struct {
	group sarama.ConsumerGroup
	topic string
}

func New(brokers []string, groupID, topic string) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}
	return &Consumer{group: group, topic: topic}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.group.Close()
	for {
		if err := c.group.Consume(ctx, []string{c.topic}, handler{}); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// handler implements sarama.ConsumerGroupHandler
type handler struct{}

// Setup runs once when sessions starts
func (handler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup runs on end
func (handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		probe, err := message.Decode(msg.Value)
		if err != nil {
			log.Printf("bad payload: %v", err)
			sess.MarkMessage(msg, "")
			continue
		}
		log.Printf("consumed id=%s latency=%s", probe.ID, probe.Latency())
		sess.MarkMessage(msg, "")
	}
	return nil
}
