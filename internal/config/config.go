package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

type Config struct {
	Brokers    []string
	Topic      string
	Group      string
	Interval   time.Duration
	Addr       string
	StaleAfter time.Duration
}

func Load() (Config, error) {
	c := Config{
		//TODO: check whether this localhost is correct
		Brokers:  splitCSV(env("CANARY_BROKERS", "localhost:9092")),
		Topic:    env("CANARY_TOPIC", "__strimzi_canary"),
		Group:    env("CANARY_CONSUMER_GROUP", "canary-group"),
		Interval: dur("CANARY_PRODUCE_INTERVAL", 5*time.Second),
		Addr:     env("CANARY_METRICS_ADDR", ":8080"),
	}
	if len(c.Brokers) == 0 {
		return Config{}, fmt.Errorf("CANARY_BROKERS is empty")
	}
	c.StaleAfter = 3 * c.Interval
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("bad duration, using default", "key", key, "value", v, "default", def)
		return def
	}
	return d
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
