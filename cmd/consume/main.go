package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	"github.com/neomat-prog/kafka-canary/internal/consumer"
	"github.com/neomat-prog/kafka-canary/internal/metrics"
)

func main() {
	stats := metrics.NewStats()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	c, err := consumer.New([]string{"localhost:9092"}, "canary-group", "__strimzi_canary", stats, logger)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		log.Fatalf("run: %v", err)
	}
}
