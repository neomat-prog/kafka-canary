package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/neomat-prog/kafka-canary/internal/health"
)

func newTestServer(state *health.State) *Server {
	return New(":0", state, 100*time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestReady(t *testing.T) {
	tests := []struct {
		name   string
		record bool // call RecordConsume before hitting /ready?
		want   int
	}{
		{"flowing → 200", true, http.StatusOK},
		{"never consumed → 503", false, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := health.New()
			if tt.record {
				st.RecordConsume(time.Millisecond)
			}
			s := newTestServer(st)

			rec := httptest.NewRecorder()
			s.handleReady(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

			if rec.Code != tt.want {
				t.Errorf("/ready code = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestHealthyAlways200(t *testing.T) {
	s := newTestServer(health.New()) // no consume ever
	rec := httptest.NewRecorder()
	s.handleHealthy(rec, httptest.NewRequest(http.MethodGet, "/healthy", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthy code = %d, want 200 even with no traffic", rec.Code)
	}
}

func TestStatusContentType(t *testing.T) {
	st := health.New()
	st.RecordConsume(5 * time.Millisecond)
	s := newTestServer(st)

	rec := httptest.NewRecorder()
	s.handleStatus(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}
