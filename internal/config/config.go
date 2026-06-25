package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

type Config struct {
	Brokers      []string
	Topic        string
	Group        string
	Interval     time.Duration
	Addr         string
	StaleAfter   time.Duration
	LatThreshold time.Duration

	CACertPath     string // truststore .p12 (ca.p12)
	ClientCertPath string // keystore .p12 (user.p12)
	ClientKeyPath  string // unused with PKCS12; kept for PEM fallback

	TruststorePassword string
	KeystorePassword   string
}

func Load() (Config, error) {
	c := Config{
		Brokers:      splitCSV(env("CANARY_BROKERS", "localhost:9092")),
		Topic:        env("CANARY_TOPIC", "__strimzi_canary"),
		Group:        env("CANARY_CONSUMER_GROUP", "canary-group"),
		Interval:     dur("CANARY_PRODUCE_INTERVAL", 5*time.Second),
		LatThreshold: dur("CANARY_LATENCY_THRESHOLD", 500*time.Millisecond),
		Addr:         env("CANARY_METRICS_ADDR", ":8080"),

		CACertPath:     env("CANARY_CA_CERT", ""),
		ClientCertPath: env("CANARY_CLIENT_CERT", ""),
		ClientKeyPath:  env("CANARY_CLIENT_KEY", ""),

		TruststorePassword: env("CANARY_TRUSTSTORE_PASSWORD", ""),
		KeystorePassword:   env("CANARY_KEYSTORE_PASSWORD", ""),
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
