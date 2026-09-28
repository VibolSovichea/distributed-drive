package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/VibolSovichea/distributed-drive/internal/fileapi"
	"github.com/VibolSovichea/distributed-drive/internal/logging"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/pool"
)

const maxRequestBody = 1 << 20

type Pinger interface {
	Ping(ctx context.Context) error
}

type Config struct {
	Pools pool.Service

	Nodes NodeService

	OAuth OAuthService

	Store Pinger

	Files fileapi.Service

	MetaStore metadata.Store

	Logger *slog.Logger

	Now func() time.Time
}

type Server struct {
	pools  pool.Service
	nodes  NodeService
	oauth  OAuthService
	store  Pinger
	files  fileapi.Service
	meta   metadata.Store
	logger *slog.Logger
	now    func() time.Time
}

func New(cfg Config) (*Server, error) {
	if cfg.Pools == nil {
		return nil, errors.New("api: Config.Pools is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("api: Config.Store is required")
	}
	if cfg.Files != nil && cfg.MetaStore == nil {
		return nil, errors.New("api: Config.MetaStore is required when Files is set")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Server{
		pools:  cfg.Pools,
		nodes:  cfg.Nodes,
		oauth:  cfg.OAuth,
		store:  cfg.Store,
		files:  cfg.Files,
		meta:   cfg.MetaStore,
		logger: cfg.Logger,
		now:    cfg.Now,
	}, nil
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(s.recoverer)
	r.Use(s.requestID)
	r.Use(s.realIP)
	r.Use(s.logRequest)

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		s.writeError(w, r, http.StatusNotFound, codeNotFound, "no route matches the request")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		s.writeError(w, r, http.StatusMethodNotAllowed, codeMethodNotAllowed,
			"the route exists but not for this method")
	})

	r.Get("/health", s.handleHealth)
	r.Get("/health/ready", s.handleReady)

	r.Route("/api", func(api chi.Router) {
		api.Get("/pools", s.handleListPools)
		api.Post("/pools", s.handleCreatePool)
		api.Route("/pools/{poolID}", func(pool chi.Router) {
			pool.Get("/", s.handleGetPool)
			pool.Delete("/", s.handleDeletePool)
			pool.Get("/capacity", s.handlePoolCapacity)

			if s.nodes != nil {
				pool.Get("/nodes", s.handleListPoolNodes)
				pool.Post("/nodes", s.handleAttachNode)
				pool.Delete("/nodes/{nodeID}", s.handleDetachNode)
			}
		})

		if s.nodes != nil {
			api.Get("/nodes", s.handleListNodes)
			api.Post("/nodes", s.handleRegisterNode)
			api.Post("/nodes/check", s.handleCheckAllNodes)
			api.Get("/nodes/{nodeID}", s.handleGetNode)
			api.Get("/nodes/{nodeID}/status", s.handleNodeStatus)
			api.Delete("/nodes/{nodeID}", s.handleDeleteNode)

			if s.oauth != nil {
				api.Post("/nodes/{nodeID}/authorize", s.handleStartAuthorization)
			}
		}

		if s.oauth != nil {
			api.Get("/oauth/callback", s.handleOAuthCallback)
		}
	})

	if s.files != nil {
		fileapi.RegisterRoutes(r, fileapi.Config{
			Service: s.files,
			Logger:  s.logger,
			Now:     s.now,
		})
	}

	return r
}

func (s *Server) ListenAndServe(ctx context.Context, addr string, shutdownTimeout time.Duration) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("http server listening", "addr", addr)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("api: listen on %s: %w", addr, err)
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	s.logger.Info("http server shutting down", logging.Duration("timeout", shutdownTimeout))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("api: shutdown: %w", err)
	}

	return <-errCh
}

type errorBody struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"requestId,omitempty"`
	} `json:"error"`
}

const (
	codeBadRequest        = "bad_request"
	codeNotFound          = "not_found"
	codeConflict          = "conflict"
	codeInternal          = "internal_error"
	codeUnavailable       = "unavailable"
	codeMethodNotAllowed  = "method_not_allowed"
	codePayloadTooLarge   = "payload_too_large"
	codeUnprocessableJSON = "malformed_json"
)

var (
	errMalformedJSON = errors.New("malformed JSON")

	errUnknownField = errors.New("unknown field")

	errTrailingJSON = errors.New("trailing JSON")
)

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {

		s.logger.ErrorContext(r.Context(), "encoding response failed",
			"error", err, "path", r.URL.Path)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)

	if _, err := w.Write(body); err != nil {
		s.logger.DebugContext(r.Context(), "writing response failed", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	body.Error.RequestID = RequestIDFromContext(r.Context())

	s.writeJSON(w, r, status, body)
}

func (s *Server) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, metadata.ErrNotFound):
		s.writeError(w, r, http.StatusNotFound, codeNotFound, "the requested resource does not exist")

	case errors.Is(err, pool.ErrNotEmpty):
		s.writeError(w, r, http.StatusConflict, codeConflict,
			"the pool still contains files; delete them before deleting the pool")

	case errors.Is(err, metadata.ErrConflict):
		s.writeError(w, r, http.StatusConflict, codeConflict, "the resource already exists")

	case errors.Is(err, metadata.ErrInvalid):

		s.writeError(w, r, http.StatusBadRequest, codeBadRequest, publicMessage(err))

	case errors.Is(err, context.Canceled):

		s.logger.DebugContext(r.Context(), "request cancelled", "path", r.URL.Path)

	case errors.Is(err, context.DeadlineExceeded):
		s.writeError(w, r, http.StatusGatewayTimeout, codeUnavailable, "the operation timed out")

	default:
		s.logger.ErrorContext(r.Context(), "request failed",
			"error", err, "method", r.Method, "path", r.URL.Path)
		s.writeError(w, r, http.StatusInternalServerError, codeInternal, "an unexpected error occurred")
	}
}

func publicMessage(err error) string {
	msg := err.Error()
	for _, prefix := range []string{
		metadata.ErrInvalid.Error() + ": ",
		metadata.ErrConflict.Error() + ": ",
		metadata.ErrNotFound.Error() + ": ",
	} {
		if len(msg) > len(prefix) && msg[:len(prefix)] == prefix {
			return msg[len(prefix):]
		}
	}
	return msg
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		return classifyDecodeError(err)
	}

	var trailing json.RawMessage
	err := decoder.Decode(&trailing)
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil:
		return fmt.Errorf("%w: the request body must contain a single JSON object", errTrailingJSON)
	default:
		return classifyDecodeError(err)
	}
}

func classifyDecodeError(err error) error {

	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return err
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return fmt.Errorf("%w: field %q must be a %s, got %s",
			errMalformedJSON, typeErr.Field, typeErr.Type, typeErr.Value)
	}

	if strings.HasPrefix(err.Error(), "json: unknown field ") {
		return fmt.Errorf("%w: %s", errUnknownField, strings.TrimPrefix(err.Error(), "json: unknown field "))
	}

	return fmt.Errorf("%w: %w", errMalformedJSON, err)
}
