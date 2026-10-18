package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/neomat-prog/kafka-canary/internal/health"
	"github.com/neomat-prog/kafka-canary/internal/stress"
)

type Server struct {
	http   *http.Server
	state  *health.State
	log    *slog.Logger
	stress *stress.Runner
}

func New(addr string, state *health.State, st *stress.Runner, log *slog.Logger) *Server {
	s := &Server{state: state, log: log, stress: st}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthy", s.handleHealthy)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/docs/", s.handleDocs)
	mux.HandleFunc("/docs/openapi.yaml", s.handleOpenAPI)
	mux.HandleFunc("POST /stress", s.handleStress)

	s.http = &http.Server{Addr: addr, Handler: mux}
	return s
}

func (s *Server) handleHealthy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}` + "\n"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.state.Snapshot().MessagesFlowing {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.state.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	if !snap.MessagesFlowing {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(snap)
}

func (s *Server) handleStress(w http.ResponseWriter, r *http.Request) {
	var req stress.Stress
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !s.stress.Start(context.Background(), req.Clamp()) {
		http.Error(w, "stress already running", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("http listening", "addr", s.http.Addr)
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.http.Shutdown(shutCtx)
	}
}
