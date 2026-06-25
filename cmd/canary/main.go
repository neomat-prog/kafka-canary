package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/neomat-prog/kafka-canary/internal/config"
	"github.com/neomat-prog/kafka-canary/internal/consumer"
	"github.com/neomat-prog/kafka-canary/internal/health"
	"github.com/neomat-prog/kafka-canary/internal/producer"
	"github.com/neomat-prog/kafka-canary/internal/server"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)) // JSON = k8s log scrapers parse it

	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log.Info("config loaded",
		"brokers", cfg.Brokers, "topic", cfg.Topic,
		"interval", cfg.Interval, "addr", cfg.Addr)

	tlsCfg, err := config.BuildTLSConfig(cfg)
	if err != nil {
		return err
	}
	if tlsCfg != nil {
		log.Info("mTLS enabled", "ca", cfg.CACertPath, "cert", cfg.ClientCertPath)
	}

	state := health.New()

	prod := producer.New(cfg.Brokers, cfg.Topic, cfg.Interval, tlsCfg, log)
	cons := consumer.New(cfg.Brokers, cfg.Group, cfg.Topic, tlsCfg, state, log)
	srv := server.New(cfg.Addr, state, cfg.StaleAfter, log)

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return prod.Run(ctx) })
	g.Go(func() error { return cons.Run(ctx) })
	g.Go(func() error { return srv.Run(ctx) })

	log.Info("canary running")
	return g.Wait()
}
