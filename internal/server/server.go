package server

import (
	"context"
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
