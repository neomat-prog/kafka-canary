package producer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

type Producer struct {
	sync     sarama.SyncProducer
	topic    string
	interval time.Duration
}

func New(brokers []string, topic string, interval time.Duration) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	sp, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("new sync producer: %w", err)
	}
	return &Producer{sync: sp, topic: topic, interval: interval}, nil
}

// Run every probe
func (p *Producer) Run(ctx context.Context) error {
	defer p.sync.Close()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	tickCh := ticker.C
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tickCh:
			if err := p.send(); err != nil {
				log.Printf("produce error: %v", err)
			}
		}
	}
}

func (p *Producer) send() error {
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	b, err := message.New(id).Encode()
	if err != nil {
		return err
	}
	partition, offset, err := p.sync.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(b),
	})
	if err != nil {
		return nil
	}
	log.Printf("produced id=%s partition=%d offset=%d", id, partition, offset)
	return nil
}
