package stress

import (
	"context"
	"crypto/tls"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// i believe the stress test should not obey consuming?
type Opts struct {
	Rate        int `json:"rate"`
	SizeBytes   int `json:"sizeBytes"`
	DurationSec int `json:"durationSec"`
}

const (
	maxRate        = 10000
	maxSizeBytes   = 100 << 10 // 100 KiB
	maxDurationSec = 60
)

func (o Opts) Clamp() Opts {
	o.Rate = min(max(o.Rate, 100), maxRate)
	o.SizeBytes = min(max(o.SizeBytes, 1), maxSizeBytes)
	o.DurationSec = min(max(o.DurationSec, 1), maxDurationSec)
	return o
}

// Runner is produce-only load
type Runner struct {
	brokers []string
	topic   string
	tls     *tls.Config
	log     *slog.Logger
	running atomic.Bool
}

func New(brokers []string, topic string, tlsCfg *tls.Config, log *slog.Logger) *Runner {
	return &Runner{
		brokers: brokers,
		topic:   topic,
		tls:     tlsCfg,
		log:     log,
	}
}

func (r *Runner) Start(ctx context.Context, o Opts) bool {
	if !r.running.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		defer r.running.Store(false)
		r.run(ctx, o)
	}()
	return true
}

func (r *Runner) run(ctx context.Context, o Opts) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	if r.tls != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = r.tls
	}
	ap, err := sarama.NewAsyncProducer(r.brokers, cfg)
	if err != nil {
		r.log.Warn("stress connect failed", "err", err)
		return
	}

	var sent, failed atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for range ap.Successes() {
			sent.Add(1)
		}
	})

	wg.Go(func() {
		for range ap.Errors() {
			failed.Add(1)
		}
	})

	ctx, cancel := context.WithTimeout(ctx, time.Duration(o.DurationSec)*time.Second)
	defer cancel()
	payload := sarama.ByteEncoder(make([]byte, o.SizeBytes))
	r.log.Info("stress started", "topic", r.topic, "rate", o.Rate, "sizeBytes", o.SizeBytes, "durationSec", o.DurationSec)
	start := time.Now()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-t.C:
			for range o.Rate / 100 {
				select {
				case ap.Input() <- &sarama.ProducerMessage{Topic: r.topic, Value: payload}:
				case <-ctx.Done(): // broker backpressure must not stretch the run past its duration
					break loop
				}
			}
		}
	}
	ap.AsyncClose() // flushes, then closes Successes/Errors
	wg.Wait()
	elapsed := time.Since(start)
	r.log.Info("stress done",
		"sent", sent.Load(), "failed", failed.Load(), "elapsed", elapsed,
		"msgPerSec", int(float64(sent.Load())/elapsed.Seconds()))
}
