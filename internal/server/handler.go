package server

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleHealthy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}` + "\n"))
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
