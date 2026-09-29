package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/neomat-prog/kafka-canary/internal/health"
)

func newTestServer(state *health.State) *Server {
	return New(":0", state, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
			st := health.New(100 * time.Millisecond)
			if tt.record {
				st.RecordConsume(0, time.Millisecond)
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
	s := newTestServer(health.New(100 * time.Millisecond)) // no consume ever
	rec := httptest.NewRecorder()
	s.handleHealthy(rec, httptest.NewRequest(http.MethodGet, "/healthy", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthy code = %d, want 200 even with no traffic", rec.Code)
	}
}

func TestStatusContentType(t *testing.T) {
	st := health.New(100 * time.Millisecond)
	st.RecordConsume(0, time.Millisecond)
	s := newTestServer(st)

	rec := httptest.NewRecorder()
	s.handleStatus(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func TestStatus(t *testing.T) {
	tests := []struct {
		name   string
		record bool
		want   int
	}{
		{"flowing → 200", true, http.StatusOK},
		{"never consumed → 503", false, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := health.New(100 * time.Millisecond)
			if tt.record {
				st.RecordConsume(0, 7*time.Millisecond)
			}
			s := newTestServer(st)

			rec := httptest.NewRecorder()
			s.handleStatus(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

			if rec.Code != tt.want {
				t.Errorf("/status code = %d, want %d", rec.Code, tt.want)
			}

			var got health.Status
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if got.MessagesFlowing != tt.record {
				t.Errorf("messagesFlowing = %v, want %v", got.MessagesFlowing, tt.record)
			}
			if tt.record && got.LastLatencyMs != 7 {
				t.Errorf("lastLatencyMs = %d, want 7", got.LastLatencyMs)
			}
		})
	}
}

func TestStatusStalePartitionsInBody(t *testing.T) {
	const staleAfter = 50 * time.Millisecond
	st := health.New(staleAfter)
	st.RecordConsume(3, time.Millisecond)
	time.Sleep(2 * staleAfter)
	s := newTestServer(st)

	rec := httptest.NewRecorder()
	s.handleStatus(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	var got health.Status
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(got.StalePartitions) != 1 || got.StalePartitions[0] != 3 {
		t.Errorf("stalePartitions = %v, want [3]", got.StalePartitions)
	}
}

func TestRoutes(t *testing.T) {
	st := health.New(time.Minute)
	st.RecordConsume(0, time.Millisecond)
	srv := httptest.NewServer(newTestServer(st).http.Handler)
	defer srv.Close()

	for _, path := range []string{"/healthy", "/ready", "/status", "/docs/", "/docs/openapi.yaml"} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + path)
			if err != nil {
				t.Fatalf("GET %s: %v", path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s = %d, want 200", path, resp.StatusCode)
			}
		})
	}
}
