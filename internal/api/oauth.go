package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/VibolSovichea/distributed-drive/internal/authflow"
)

type OAuthService interface {
	Start(ctx context.Context, nodeID string) (string, error)

	Complete(ctx context.Context, code, state string) error
}

type authorizeResponse struct {
	NodeID string `json:"nodeId"`

	URL string `json:"authorizationUrl"`

	ExpiresIn int `json:"expiresInSeconds"`
}

func (s *Server) handleStartAuthorization(w http.ResponseWriter, r *http.Request) {
	nodeID := chi.URLParam(r, "nodeID")
	if nodeID == "" {
		s.writeError(w, r, http.StatusBadRequest, codeBadRequest,
			"the route requires a node id")
		return
	}

	url, err := s.oauth.Start(r.Context(), nodeID)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	s.writeJSON(w, r, http.StatusOK, authorizeResponse{
		NodeID:    nodeID,
		URL:       url,
		ExpiresIn: int(authflow.DefaultStateTTL.Seconds()),
	})
}

func (s *Server) handleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	if denied := query.Get("error"); denied != "" {
		s.writeError(w, r, http.StatusBadRequest, codeBadRequest,
			"the provider did not grant access: "+sanitiseProviderError(denied))
		return
	}

	code := query.Get("code")
	state := query.Get("state")

	switch {
	case code == "":
		s.writeError(w, r, http.StatusBadRequest, codeBadRequest,
			"the callback carried no authorisation code")
		return
	case state == "":
		s.writeError(w, r, http.StatusBadRequest, codeBadRequest,
			"the callback carried no state")
		return
	}

	if err := s.oauth.Complete(r.Context(), code, state); err != nil {
		s.writeOAuthError(w, r, err)
		return
	}

	s.writeJSON(w, r, http.StatusOK, map[string]string{"status": "authorised"})
}

func (s *Server) writeOAuthError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, authflow.ErrUnknownState):

		s.writeError(w, r, http.StatusGone, codeBadRequest,
			"this authorisation link has already been used or does not exist; start a new one")
	case errors.Is(err, authflow.ErrStateExpired):
		s.writeError(w, r, http.StatusGone, codeBadRequest,
			"this authorisation link has expired; start a new one")
	case errors.Is(err, authflow.ErrStateMismatch):
		s.writeError(w, r, http.StatusForbidden, codeBadRequest,
			"this authorisation belongs to a different node")
	default:
		s.writeServiceError(w, r, err)
	}
}

func sanitiseProviderError(raw string) string {
	const maxLen = 64
	out := make([]rune, 0, maxLen)
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '-', r == '.':
			out = append(out, r)
		default:
			out = append(out, ' ')
		}
		if len(out) == maxLen {
			break
		}
	}
	if len(out) == 0 {
		return "no reason given"
	}
	return string(out)
}
