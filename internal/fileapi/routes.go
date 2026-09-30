package fileapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/store"
	"github.com/go-chi/chi/v5"
)

type Config struct {
	Service Service
	Logger  *slog.Logger
	Now     func() time.Time
}

func RegisterRoutes(r chi.Router, cfg Config) {
	if cfg.Service == nil {
		panic("fileapi: Config.Service is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}

	h := &handlers{svc: cfg.Service, log: cfg.Logger, now: cfg.Now}

	r.Route("/api", func(api chi.Router) {
		api.Route("/pools/{poolID}/files", func(files chi.Router) {
			files.Post("/", h.handleUpload)
			files.Get("/", h.handleListFiles)
			files.Route("/{fileID}", func(file chi.Router) {
				file.Get("/", h.handleGetFile)
				file.Delete("/", h.handleDeleteFile)
				file.Get("/download", h.handleDownload)
				file.Post("/repair", h.handleRepairFile)
				file.Post("/scrub", h.handleScrubFile)
			})
		})

		api.Route("/pools/{poolID}/maintenance", func(maint chi.Router) {
			maint.Post("/repair", h.handleRepairPool)
			maint.Post("/scrub", h.handleScrubPool)
			maint.Post("/sweep", h.handleSweepAbandoned)
		})
	})
}

type handlers struct {
	svc Service
	log *slog.Logger
	now func() time.Time
}

func (h *handlers) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		h.log.ErrorContext(r.Context(), "encoding response failed", "error", err, "path", r.URL.Path)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		h.log.DebugContext(r.Context(), "writing response failed", "error", err)
	}
}

func (h *handlers) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	body := map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	}
	h.writeJSON(w, r, status, body)
}

func (h *handlers) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, metadata.ErrNotFound):
		h.writeError(w, r, http.StatusNotFound, "not_found", "the requested resource does not exist")
	case errors.Is(err, metadata.ErrInvalid):
		h.writeError(w, r, http.StatusBadRequest, "bad_request", err.Error())
	case errors.Is(err, store.ErrFileNotReadable):
		h.writeError(w, r, http.StatusPreconditionFailed, "not_readable", "the file is not readable in its current state")
	case errors.Is(err, store.ErrFileCorrupt):
		h.writeError(w, r, http.StatusConflict, "corrupt", "the file does not match its recorded checksum")
	case errors.Is(err, store.ErrTooFewShards):
		h.writeError(w, r, http.StatusConflict, "unrepairable", "too few shards remain to rebuild the file")
	case errors.Is(err, store.ErrUploadTruncated):
		h.writeError(w, r, http.StatusBadRequest, "truncated", "the upload ended early")
	case errors.Is(err, store.ErrNoUsableNodes):
		h.writeError(w, r, http.StatusServiceUnavailable, "unavailable", "the pool cannot hold a stripe")
	case errors.Is(err, context.Canceled):
		h.log.DebugContext(r.Context(), "request cancelled", "path", r.URL.Path)
	case errors.Is(err, context.DeadlineExceeded):
		h.writeError(w, r, http.StatusGatewayTimeout, "unavailable", "the operation timed out")
	default:
		h.log.ErrorContext(r.Context(), "request failed", "error", err, "method", r.Method, "path", r.URL.Path)
		h.writeError(w, r, http.StatusInternalServerError, "internal_error", "an unexpected error occurred")
	}
}

func (h *handlers) handleUpload(w http.ResponseWriter, r *http.Request) {
	poolID := chi.URLParam(r, "poolID")
	if poolID == "" {
		h.writeError(w, r, http.StatusBadRequest, "bad_request", "poolID is required")
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		name = r.Header.Get("X-File-Name")
	}
	if name == "" {
		name = "upload-" + h.now().Format("20060102150405")
	}

	size := int64(0)
	if sz := r.URL.Query().Get("size"); sz != "" {
		if v, err := strconv.ParseInt(sz, 10, 64); err == nil {
			size = v
		}
	}

	file, err := h.svc.Upload(r.Context(), chi.URLParam(r, "poolID"), name, r.Body, size)
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}

	h.writeJSON(w, r, http.StatusCreated, map[string]any{
		"id":           file.ID,
		"poolId":       file.PoolID,
		"name":         file.Name,
		"size":         file.Size,
		"contentHash":  file.ContentHash,
		"chunkSize":    file.ChunkSize,
		"dataChunks":   file.DataChunks,
		"parityChunks": file.ParityChunks,
		"status":       file.Status,
		"createdAt":    file.CreatedAt.Format(time.RFC3339),
	})
}

func (h *handlers) handleListFiles(w http.ResponseWriter, r *http.Request) {
	files, err := h.svc.ListFiles(r.Context(), chi.URLParam(r, "poolID"))
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}

	out := make([]map[string]any, len(files))
	for i, f := range files {
		out[i] = map[string]any{
			"id":           f.ID,
			"poolId":       f.PoolID,
			"name":         f.Name,
			"size":         f.Size,
			"contentHash":  f.ContentHash,
			"chunkSize":    f.ChunkSize,
			"dataChunks":   f.DataChunks,
			"parityChunks": f.ParityChunks,
			"status":       f.Status,
			"createdAt":    f.CreatedAt.Format(time.RFC3339),
			"updatedAt":    f.UpdatedAt.Format(time.RFC3339),
		}
	}
	h.writeJSON(w, r, http.StatusOK, out)
}

func (h *handlers) handleGetFile(w http.ResponseWriter, r *http.Request) {
	file, err := h.svc.GetFile(r.Context(), chi.URLParam(r, "fileID"))
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusOK, map[string]any{
		"id":           file.ID,
		"poolId":       file.PoolID,
		"name":         file.Name,
		"size":         file.Size,
		"contentHash":  file.ContentHash,
		"chunkSize":    file.ChunkSize,
		"dataChunks":   file.DataChunks,
		"parityChunks": file.ParityChunks,
		"status":       file.Status,
		"createdAt":    file.CreatedAt.Format(time.RFC3339),
		"updatedAt":    file.UpdatedAt.Format(time.RFC3339),
	})
}

func (h *handlers) handleDownload(w http.ResponseWriter, r *http.Request) {
	fileID := chi.URLParam(r, "fileID")

	file, err := h.svc.GetFile(r.Context(), fileID)
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}

	reader, err := h.svc.Download(r.Context(), fileID)
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(file.Size, 10))
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, file.Name))
	w.Header().Set("X-Content-SHA256", file.ContentHash)

	if _, err := io.Copy(w, reader); err != nil {
		h.log.ErrorContext(r.Context(), "download copy failed", "error", err)
	}
}

func (h *handlers) handleRepairFile(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.RepairFile(r.Context(), chi.URLParam(r, "fileID"))
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusOK, map[string]any{
		"fileId":     result.FileID,
		"restored":   result.Restored,
		"unrepaired": result.Unrepaired,
		"healthy":    result.Healthy,
	})
}

func (h *handlers) handleScrubFile(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.ScrubFile(r.Context(), chi.URLParam(r, "fileID"))
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusOK, map[string]any{
		"fileId":   result.FileID,
		"verified": result.Verified,
		"corrupt":  result.Corrupt,
		"missing":  result.Missing,
		"stripes":  result.Stripes,
		"healthy":  result.Healthy,
	})
}

func (h *handlers) handleRepairPool(w http.ResponseWriter, r *http.Request) {
	results, err := h.svc.RepairPool(r.Context(), chi.URLParam(r, "poolID"))
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	out := make([]map[string]any, len(results))
	for i, r := range results {
		out[i] = map[string]any{
			"fileId":     r.FileID,
			"restored":   r.Restored,
			"unrepaired": r.Unrepaired,
			"healthy":    r.Healthy,
		}
	}
	h.writeJSON(w, r, http.StatusOK, out)
}

func (h *handlers) handleScrubPool(w http.ResponseWriter, r *http.Request) {
	results, err := h.svc.ScrubPool(r.Context(), chi.URLParam(r, "poolID"))
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	out := make([]map[string]any, len(results))
	for i, r := range results {
		out[i] = map[string]any{
			"fileId":   r.FileID,
			"verified": r.Verified,
			"corrupt":  r.Corrupt,
			"missing":  r.Missing,
			"stripes":  r.Stripes,
			"healthy":  r.Healthy,
		}
	}
	h.writeJSON(w, r, http.StatusOK, out)
}

func (h *handlers) handleSweepAbandoned(w http.ResponseWriter, r *http.Request) {
	type sweepRequest struct {
		OlderThanSeconds int64 `json:"olderThanSeconds"`
	}
	var req sweepRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, "malformed_json", "invalid JSON body")
		return
	}
	olderThan := time.Duration(req.OlderThanSeconds) * time.Second
	if olderThan <= 0 {
		olderThan = 24 * time.Hour
	}

	count, err := h.svc.SweepAbandoned(r.Context(), olderThan)
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	h.writeJSON(w, r, http.StatusOK, map[string]any{
		"swept": count,
	})
}

func (h *handlers) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	h.writeError(w, r, http.StatusNotImplemented, "not_implemented", "file deletion not yet implemented")
}
