package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestNewJSONFormat(t *testing.T) {
	var buf bytes.Buffer

	New(&buf, "info", "json").Info("hello", "count", 3)

	got := buf.String()
	if !strings.HasPrefix(strings.TrimSpace(got), "{") {
		t.Fatalf("New(..., json) wrote %q, want a JSON object", got)
	}
	for _, want := range []string{`"msg":"hello"`, `"level":"INFO"`, `"count":3`} {
		if !strings.Contains(got, want) {
			t.Errorf("output = %q, want it to contain %q", got, want)
		}
	}
}

func TestNewTextFormat(t *testing.T) {
	var buf bytes.Buffer

	New(&buf, "info", "text").Info("hello", "count", 3)

	got := buf.String()
	if !strings.Contains(got, `msg=hello`) || !strings.Contains(got, `count=3`) {
		t.Fatalf("New(..., text) wrote %q, want key=value output", got)
	}
	if strings.HasPrefix(strings.TrimSpace(got), "{") {
		t.Fatalf("New(..., text) wrote %q, want logfmt rather than JSON", got)
	}
}

func TestNewRespectsLevel(t *testing.T) {
	tests := []struct {
		level  string
		emit   string
		logged bool
	}{
		{level: "debug", emit: "debug message", logged: true},
		{level: "info", emit: "debug message", logged: false},
		{level: "info", emit: "info message", logged: true},
		{level: "warn", emit: "info message", logged: false},
		{level: "error", emit: "warn message", logged: false},
		{level: "error", emit: "error message", logged: true},
	}

	for _, tt := range tests {
		t.Run(tt.level+"/"+tt.emit, func(t *testing.T) {
			var buf bytes.Buffer
			l := New(&buf, tt.level, "json")

			switch tt.emit {
			case "debug message":
				l.Debug("debug message")
			case "info message":
				l.Info("info message")
			case "warn message":
				l.Warn("warn message")
			case "error message":
				l.Error("error message")
			}

			if logged := strings.Contains(buf.String(), tt.emit); logged != tt.logged {
				t.Errorf("logged = %t, want %t (output %q)", logged, tt.logged, buf.String())
			}
		})
	}
}

func TestNewIsCaseAndSpaceInsensitive(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, "  DEBUG ", " JSON ").Debug("visible")

	if !strings.Contains(buf.String(), "visible") {
		t.Fatalf("output = %q, want the record to be emitted", buf.String())
	}
}

func TestNewPanicsOnUnsupportedFormat(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New() did not panic on an unsupported format")
		}
	}()

	New(&bytes.Buffer{}, "info", "yaml")
}

func TestContextRoundTrip(t *testing.T) {
	base := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	ctx := WithLogger(t.Context(), base)

	if got := FromContext(ctx, nil); got != base {
		t.Errorf("FromContext() = %v, want the logger that was stored", got)
	}
}

func TestFromContextFallsBack(t *testing.T) {
	fallback := slog.Default()

	if got := FromContext(t.Context(), fallback); got != fallback {
		t.Errorf("FromContext() with no stored logger = %v, want the fallback", got)
	}
	if got := FromContext(t.Context(), nil); got == nil {
		t.Error("FromContext(ctx, nil) = nil, want a usable logger")
	}
}

func TestDefaultDoesNotPanic(t *testing.T) {
	if got := Default(); got == nil {
		t.Fatal("Default() = nil, want a logger")
	}
}

func TestHandlerMatchesNew(t *testing.T) {
	var buf bytes.Buffer

	slog.New(Handler(&buf, "warn", "text")).Error("boom")

	if !strings.Contains(buf.String(), "boom") {
		t.Fatalf("output = %q, want the record to be emitted", buf.String())
	}
}

func TestDurationIsHumanReadable(t *testing.T) {
	
	
	var buf bytes.Buffer
	New(&buf, "info", "json").Info("test", Duration("timeout", 15*time.Second))

	if !strings.Contains(buf.String(), `"timeout":"15s"`) {
		t.Errorf("log line = %q, want a readable duration", buf.String())
	}
}
