package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	t.Run("defaults when unset", func(t *testing.T) {
		t.Setenv("CANARY_BROKERS", "localhost:9092")
		c, err := Load()
		if err != nil {
			t.Fatalf("Load() err = %v", err)
		}
		if c.Topic != "__strimzi_canary" {
			t.Errorf("Topic = %q, want default", c.Topic)
		}
		if c.Interval != 5*time.Second {
			t.Errorf("Interval = %v, want 5s", c.Interval)
		}
		if c.StaleAfter != 15*time.Second {
			t.Errorf("StaleAfter = %v, want 3xinterval", c.StaleAfter)
		}
	})

	t.Run("empty brokers errors", func(t *testing.T) {
		t.Setenv("CANARY_BROKERS", "  ")
		if _, err := Load(); err == nil {
			t.Fatal("Load() err = nil, want error on empty brokers")
		}
	})

	t.Run("bad duration falls back", func(t *testing.T) {
		t.Setenv("CANARY_BROKERS", "localhost:9092")
		t.Setenv("CANARY_PRODUCE_INTERVAL", "not-a-duration")
		c, err := Load()
		if err != nil {
			t.Fatalf("Load() err = %v", err)
		}
		if c.Interval != 5*time.Second {
			t.Errorf("Interval = %v, want fallback 5s", c.Interval)
		}
	})

	t.Run("override parses", func(t *testing.T) {
		t.Setenv("CANARY_BROKERS", "a:9092,b:9092")
		t.Setenv("CANARY_PRODUCE_INTERVAL", "2s")
		c, _ := Load()
		if len(c.Brokers) != 2 {
			t.Errorf("Brokers = %v, want 2", c.Brokers)
		}
		if c.StaleAfter != 6*time.Second {
			t.Errorf("StaleAfter = %v, want 6s", c.StaleAfter)
		}

	})
}
