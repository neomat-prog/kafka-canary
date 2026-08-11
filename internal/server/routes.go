package server

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/neomat-prog/kafka-canary/internal/health"
)

func Routes(addr string, state *health.State, staleAfter time.Duration, log *slog.Logger) *Server {
	s := &Server{state: state, staleAfter: staleAfter, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthy", s.handleHealthy)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/status", s.handleStatus)

	s.http = &http.Server{Addr: addr, Handler: mux}
	return s
}
