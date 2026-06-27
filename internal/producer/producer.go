package producer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/neomat-prog/kafka-canary/internal/message"
	"golang.org/x/sync/errgroup"
)

type Producer struct {
	brokers  []string
	topic    string
	interval time.Duration
	tls      *tls.Config
	log      *slog.Logger
	client   sarama.Client
	sync     sarama.SyncProducer
}

func New(brokers []string, topic string, interval time.Duration, tlsCfg *tls.Config, log *slog.Logger) *Producer {
	return &Producer{brokers: brokers, topic: topic, interval: interval, tls: tlsCfg, log: log}
}

func (p *Producer) connect() error {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	if p.tls != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = p.tls
	}
	client, err := sarama.NewClient(p.brokers, cfg)
	if err != nil {
		return err
	}
	sp, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		client.Close()
		return err
	}
	p.client, p.sync = client, sp
	return nil
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

func (p *Producer) partitions() ([]int32, error) {
	return p.client.Partitions(p.topic)
}

func (p *Producer) drop() {
	if p.sync != nil {
		p.sync.Close()
	}
	if p.client != nil {
		p.client.Close()
	}
	p.sync, p.client = nil, nil
}

func (p *Producer) sendOne(part int32) error {
	id := fmt.Sprintf("%d-%d", part, time.Now().UnixNano())
	b, err := message.New(id).Encode()
	if err != nil {
		return err
	}
	_, offset, err := p.sync.SendMessage(&sarama.ProducerMessage{
		Topic:     p.topic,
		Partition: part,
		Value:     sarama.ByteEncoder(b),
	})
	if err != nil {
		return fmt.Errorf("send part %d: %w", part, err)
	}

	p.log.Info("produced", "id", id, "partition", part, "offset", offset)

	return nil
}

func (p *Producer) send() error {
	if p.sync == nil {
		if err := p.connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
	}
	parts, err := p.partitions()
	if err != nil {
		p.drop()
		return fmt.Errorf("partitions: %w", err)
	}

	var (
		mu   sync.Mutex
		g    errgroup.Group
		errs []error
	)
	g.SetLimit(16)

	for _, part := range parts {
		g.Go(func() error {
			if err := p.sendOne(part); err != nil {
				p.log.Warn("partition probe failed", "partition", part, "err", err)
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
			return nil
		})
	}
	g.Wait()
	return errors.Join(errs...)
}
