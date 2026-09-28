
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)





func New(w io.Writer, level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts))
	case "text":
		return slog.New(slog.NewTextHandler(w, opts))
	default:
		panic(fmt.Sprintf("logging: unsupported format %q", format))
	}
}



func Default() *slog.Logger {
	return New(os.Stderr, "info", "json")
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}




func Handler(w io.Writer, level, format string) slog.Handler {
	return New(w, level, format).Handler()
}

type contextKey struct{}



func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, l)
}



func FromContext(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if l, ok := ctx.Value(contextKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	if fallback != nil {
		return fallback
	}
	return Default()
}







func Duration(key string, d time.Duration) slog.Attr {
	return slog.String(key, d.String())
}
