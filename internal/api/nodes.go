package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/node"
	"github.com/VibolSovichea/distributed-drive/internal/pool"
)


type NodeService interface {
	Register(ctx context.Context, input node.RegisterInput) (metadata.Node, error)
	Get(ctx context.Context, nodeID string) (metadata.Node, error)
	List(ctx context.Context) ([]metadata.Node, error)
	Delete(ctx context.Context, nodeID string) error
	Check(ctx context.Context, nodeID string) (metadata.Node, error)
	CheckAll(ctx context.Context) ([]metadata.Node, error)
}


type nodeResponse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Provider string `json:"provider"`
	
	
	Account   string `json:"account"`
	Status    string `json:"status"`
	Capacity  int64  `json:"capacity"`
	Used      int64  `json:"used"`
	Available int64  `json:"available"`
	LastSeen  string `json:"lastSeen,omitempty"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

func toNodeResponse(n metadata.Node) nodeResponse {
	out := nodeResponse{
		ID:        n.ID,
		Name:      n.Name,
		Provider:  string(n.Provider),
		Account:   n.AccountIdentifier,
		Status:    string(n.Status),
		Capacity:  n.Capacity,
		Used:      n.UsedCapacity,
		Available: n.Available(),
		CreatedAt: s(n.CreatedAt),
		UpdatedAt: s(n.UpdatedAt),
	}
	if n.LastSeen != nil {
		out.LastSeen = s(*n.LastSeen)
	}
	return out
}

func s(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}








func (s *Server) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	var body createNodeRequest
	if err := decodeJSON(w, r, &body); err != nil {
		s.writeDecodeError(w, r, err)
		return
	}

	record, err := s.nodes.Register(r.Context(), node.RegisterInput{
		Name:     body.Name,
		Provider: metadata.Provider(body.Provider),
		Root:     body.Root,
	})
	if err != nil {
		
		
		if isUnauthorisedNode(err) && record.ID != "" {
			s.logger.WarnContext(r.Context(), "node registered but not reachable",
				"nodeId", record.ID, "status", record.Status, "error", err)
			s.writeJSON(w, r, http.StatusCreated, toNodeResponse(record))
			return
		}
		s.writeServiceError(w, r, err)
		return
	}

	s.logger.InfoContext(r.Context(), "node registered",
		"nodeId", record.ID, "provider", record.Provider, "status", record.Status)
	s.writeJSON(w, r, http.StatusCreated, toNodeResponse(record))
}


func isUnauthorisedNode(err error) bool {
	return errors.Is(err, node.ErrUnauthorized) || errors.Is(err, node.ErrNoClient)
}


func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.nodes.List(r.Context())
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	out := make([]nodeResponse, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, toNodeResponse(n))
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"nodes": out})
}


func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	record, err := s.nodes.Get(r.Context(), chi.URLParam(r, "nodeID"))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, toNodeResponse(record))
}


func (s *Server) handleNodeStatus(w http.ResponseWriter, r *http.Request) {
	record, err := s.nodes.Check(r.Context(), chi.URLParam(r, "nodeID"))
	if err != nil {
		
		
		
		if record.ID != "" {
			s.logger.WarnContext(r.Context(), "node check reported a problem",
				"nodeId", record.ID, "status", record.Status, "error", err)
			s.writeJSON(w, r, http.StatusOK, toNodeResponse(record))
			return
		}
		s.writeServiceError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, toNodeResponse(record))
}


func (s *Server) handleCheckAllNodes(w http.ResponseWriter, r *http.Request) {
	records, err := s.nodes.CheckAll(r.Context())

	
	
	if err != nil {
		s.logger.WarnContext(r.Context(), "some nodes could not be checked", "error", err)
	}

	out := make([]nodeResponse, 0, len(records))
	for _, n := range records {
		out = append(out, toNodeResponse(n))
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"nodes": out})
}



func (s *Server) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	nodeID := chi.URLParam(r, "nodeID")
	if nodeID == "" {
		s.writeError(w, r, http.StatusBadRequest, codeBadRequest, "nodeID is required")
		return
	}

	if err := s.nodes.Delete(r.Context(), nodeID); err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}


func (s *Server) handleAttachNode(w http.ResponseWriter, r *http.Request) {
	var body attachNodeRequest
	if err := decodeJSON(w, r, &body); err != nil {
		s.writeDecodeError(w, r, err)
		return
	}

	poolID := chi.URLParam(r, "poolID")

	record, err := s.pools.AddNode(r.Context(), poolID, body.NodeID)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	
	
	
	capacity, err := s.pools.Capacity(r.Context(), poolID)
	if err != nil {
		s.logger.WarnContext(r.Context(), "node attached but capacity unavailable",
			"poolId", poolID, "error", err)
	}

	s.logger.InfoContext(r.Context(), "node attached to pool",
		"poolId", poolID, "nodeId", record.ID)
	s.writeJSON(w, r, http.StatusCreated,
		map[string]any{"node": toNodeResponse(record), "capacity": capacity})
}


func (s *Server) handleDetachNode(w http.ResponseWriter, r *http.Request) {
	poolID := chi.URLParam(r, "poolID")
	nodeID := chi.URLParam(r, "nodeID")

	if err := s.pools.RemoveNode(r.Context(), poolID, nodeID); err != nil {
		if errors.Is(err, pool.ErrHasData) {
			
			
			s.writeError(w, r, http.StatusConflict, codeConflict,
				"the node still holds data; delete the pool's files before detaching it")
			return
		}
		s.writeServiceError(w, r, err)
		return
	}

	s.logger.InfoContext(r.Context(), "node detached from pool", "poolId", poolID, "nodeId", nodeID)
	s.writeJSON(w, r, http.StatusOK, map[string]any{"detached": nodeID})
}


func (s *Server) handleListPoolNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.pools.ListNodes(r.Context(), chi.URLParam(r, "poolID"))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	out := make([]nodeResponse, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, toNodeResponse(n))
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"nodes": out})
}


type createNodeRequest struct {
	
	Name string `json:"name"`
	
	
	
	Provider string `json:"provider"`
	
	
	Root string `json:"root,omitempty"`
}


type attachNodeRequest struct {
	NodeID string `json:"nodeId"`
}
