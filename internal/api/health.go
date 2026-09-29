package api

import (
	"net/http"
	"time"
)

type healthBody struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	Time    string `json:"time"`

	Checks map[string]string `json:"checks,omitempty"`
}

var version = "dev"

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, healthBody{
		Status:  "ok",
		Version: version,
		Time:    s.now().Format(time.RFC3339),
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	body := healthBody{
		Status:  "ok",
		Version: version,
		Time:    s.now().Format(time.RFC3339),
		Checks:  map[string]string{},
	}

	if err := s.store.Ping(r.Context()); err != nil {
		body.Status = "unavailable"
		body.Checks["database"] = "unreachable"

		s.logger.WarnContext(r.Context(), "readiness probe failed", "error", err)

		s.writeJSON(w, r, http.StatusServiceUnavailable, body)
		return
	}

	body.Checks["database"] = "ok"

	s.writeJSON(w, r, http.StatusOK, body)
}
