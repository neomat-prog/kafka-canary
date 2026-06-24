package producer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

type Producer struct {
	brokers  []string
	topic    string
	interval time.Duration
	log      *slog.Logger
	sync     sarama.SyncProducer // nil until first successful connect
}

func New(brokers []string, topic string, interval time.Duration, log *slog.Logger) *Producer {
	return &Producer{brokers: brokers, topic: topic, interval: interval, log: log}
}

func (p *Producer) connect() (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	return sarama.NewSyncProducer(p.brokers, cfg)
}

// Run sends one probe per interval. Kafka errors are logged, not fatal,
// a down broker means the next tick retries (re)connect, so the process stays up.
func (p *Producer) Run(ctx context.Context) error {
	defer func() {
		if p.sync != nil {
			p.sync.Close()
		}
	}()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.send(); err != nil {
				p.log.Warn("produce failed", "err", err)
			}
		}
	}
}

func (p *Producer) send() error {
	if p.sync == nil { // (re)connect on demand
		sp, err := p.connect()
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		p.sync = sp
	}

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
		p.sync.Close()
		p.sync = nil // drop dead client, reconnect next tick
		return fmt.Errorf("send: %w", err)
	}
	p.log.Info("produced", "id", id, "partition", partition, "offset", offset)
	return nil
}
