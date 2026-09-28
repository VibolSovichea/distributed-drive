package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/pool"
)







type poolResponse struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	DataChunks   int    `json:"dataChunks"`
	ParityChunks int    `json:"parityChunks"`
	ChunkSize    int64  `json:"chunkSize"`
	
	ShardsPerStripe int    `json:"shardsPerStripe"`
	Redundancy      string `json:"redundancy"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
}

func newPoolResponse(p metadata.Pool) poolResponse {
	return poolResponse{
		ID:              p.ID,
		Name:            p.Name,
		DataChunks:      p.DataChunks,
		ParityChunks:    p.ParityChunks,
		ChunkSize:       p.ChunkSize,
		ShardsPerStripe: p.ShardsPerStripe(),
		Redundancy:      redundancy(p),
		CreatedAt:       formatTime(p.CreatedAt),
		UpdatedAt:       formatTime(p.UpdatedAt),
	}
}



func redundancy(p metadata.Pool) string {
	return strconv.Itoa(p.DataChunks) + "+" + strconv.Itoa(p.ParityChunks)
}




type createPoolRequest struct {
	Name         string `json:"name"`
	DataChunks   int    `json:"dataChunks"`
	ParityChunks int    `json:"parityChunks"`
	ChunkSize    int64  `json:"chunkSize"`
}


type listPoolsResponse struct {
	Pools []poolResponse `json:"pools"`
	Count int            `json:"count"`
}


func (s *Server) handleCreatePool(w http.ResponseWriter, r *http.Request) {
	var req createPoolRequest
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeDecodeError(w, r, err)
		return
	}

	created, err := s.pools.Create(r.Context(), pool.CreateInput{
		Name:         req.Name,
		DataChunks:   req.DataChunks,
		ParityChunks: req.ParityChunks,
		ChunkSize:    req.ChunkSize,
	})
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	s.logger.InfoContext(r.Context(), "pool created",
		"poolId", created.ID, "name", created.Name, "redundancy", redundancy(created))

	w.Header().Set("Location", "/api/pools/"+created.ID)
	s.writeJSON(w, r, http.StatusCreated, newPoolResponse(created))
}


func (s *Server) handleListPools(w http.ResponseWriter, r *http.Request) {
	pools, err := s.pools.List(r.Context())
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	
	
	body := listPoolsResponse{Pools: make([]poolResponse, 0, len(pools))}
	for _, p := range pools {
		body.Pools = append(body.Pools, newPoolResponse(p))
	}
	body.Count = len(body.Pools)

	s.writeJSON(w, r, http.StatusOK, body)
}


func (s *Server) handleGetPool(w http.ResponseWriter, r *http.Request) {
	found, err := s.pools.Get(r.Context(), chi.URLParam(r, "poolID"))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	s.writeJSON(w, r, http.StatusOK, newPoolResponse(found))
}


func (s *Server) handleDeletePool(w http.ResponseWriter, r *http.Request) {
	poolID := chi.URLParam(r, "poolID")

	if err := s.pools.Delete(r.Context(), poolID); err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	s.logger.InfoContext(r.Context(), "pool deleted", "poolId", poolID)

	
	w.WriteHeader(http.StatusNoContent)
}


func (s *Server) handlePoolCapacity(w http.ResponseWriter, r *http.Request) {
	capacity, err := s.pools.Capacity(r.Context(), chi.URLParam(r, "poolID"))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	s.writeJSON(w, r, http.StatusOK, capacity)
}






func (s *Server) writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError

	switch {
	case errors.As(err, &tooLarge):
		s.writeError(w, r, http.StatusRequestEntityTooLarge, codePayloadTooLarge,
			"the request body is too large")

	case errors.Is(err, errUnknownField):
		s.writeError(w, r, http.StatusBadRequest, codeUnprocessableJSON,
			"unknown field in the request body: "+publicMessage(err))

	default:
		s.writeError(w, r, http.StatusBadRequest, codeUnprocessableJSON,
			"invalid request body: "+publicMessage(err))
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
