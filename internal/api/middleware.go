package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/VibolSovichea/distributed-drive/internal/logging"
)

type requestIDKey struct{}

const RequestIDHeader = "X-Request-ID"

func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !safeRequestID(id) {
			id = uuid.NewString()
		}

		w.Header().Set(RequestIDHeader, id)

		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func safeRequestID(id string) bool {
	const maxLen = 64
	if id == "" || len(id) > maxLen {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

func (s *Server) realIP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isLoopback(r.RemoteAddr) {
			if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
				r.RemoteAddr = firstForwarded(forwarded)
			}
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func firstForwarded(header string) string {
	first := header
	if i := strings.IndexByte(header, ','); i >= 0 {
		first = header[:i]
	}

	return strings.TrimSpace(first)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {

		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (s *Server) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}

		r = r.WithContext(logging.WithLogger(r.Context(), s.logger))

		next.ServeHTTP(rec, r)

		if rec.status == 0 {
			rec.status = http.StatusOK
		}

		level := slog.LevelInfo
		switch {
		case rec.status >= 500:
			level = slog.LevelError
		case rec.status >= 400:
			level = slog.LevelWarn
		}

		s.logger.LogAttrs(r.Context(), level, "http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Int("bytes", rec.bytes),
			logging.Duration("duration", time.Since(start)),
			slog.String("requestId", RequestIDFromContext(r.Context())),
			slog.String("remote", r.RemoteAddr),
		)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			var err error
			if e, ok := recovered.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", recovered)
			}

			if errors.Is(err, http.ErrAbortHandler) {
				panic(recovered)
			}

			s.logger.ErrorContext(r.Context(), "handler panicked",
				"panic", recovered,
				"method", r.Method,
				"path", r.URL.Path,
				"requestId", RequestIDFromContext(r.Context()),
				"stack", string(debug.Stack()),
			)

			s.writeError(w, r, http.StatusInternalServerError, codeInternal, "an unexpected error occurred")
		}()

		next.ServeHTTP(w, r)
	})
}
