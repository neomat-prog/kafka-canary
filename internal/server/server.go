package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/neomat-prog/kafka-canary/internal/health"
)

type Server struct {
	http       *http.Server
	state      *health.State
	staleAfter time.Duration
	log        *slog.Logger
}

func New(addr string, state *health.State, staleAfter time.Duration, log *slog.Logger) *Server {
	s := &Server{state: state, staleAfter: staleAfter, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthy", s.handleHealthy)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/status", s.handleStatus)

	s.http = &http.Server{Addr: addr, Handler: mux}
	return s
}

func (s *Server) handleHealthy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}` + `\n`))

}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.state.Snapshot(s.staleAfter).MessagesFlowing {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.state.Snapshot(s.staleAfter)
	w.Header().Set("Content-Type", "application/json")
	if !snap.MessagesFlowing {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(snap)
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
