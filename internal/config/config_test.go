package config

import (
	"strings"
	"testing"
	"time"
)

func env(pairs map[string]string) lookupFunc {
	return func(key string) (string, bool) {
		v, ok := pairs[key]
		return v, ok
	}
}

func TestLoadFromAppliesDefaultsWhenUnset(t *testing.T) {
	got, err := loadFrom(env(nil))
	if err != nil {
		t.Fatalf("loadFrom() error = %v, want nil", err)
	}

	if diff := got; diff != Defaults() {
		t.Fatalf("loadFrom() = %+v, want %+v", diff, Defaults())
	}
}

func TestLoadFromOverrides(t *testing.T) {
	envs := map[string]string{
		"DD_HTTP_ADDR":               ":9999",
		"DD_HTTP_READ_TIMEOUT":       "5s",
		"DD_HTTP_WRITE_TIMEOUT":      "90s",
		"DD_HTTP_IDLE_TIMEOUT":       "30s",
		"DD_HTTP_SHUTDOWN_TIMEOUT":   "3s",
		"DD_DATABASE_PATH":           "/tmp/custom.db",
		"DD_DATABASE_BUSY_TIMEOUT":   "2s",
		"DD_DATABASE_MAX_OPEN_CONNS": "8",
		"DD_MIGRATE_ON_START":        "false",
		"DD_LOG_LEVEL":               "debug",
		"DD_LOG_FORMAT":              "text",
	}

	got, err := loadFrom(env(envs))
	if err != nil {
		t.Fatalf("loadFrom() error = %v, want nil", err)
	}

	if got.HTTP.Addr != ":9999" {
		t.Errorf("HTTP.Addr = %q, want %q", got.HTTP.Addr, ":9999")
	}
	if got.HTTP.ReadTimeout != 5*time.Second {
		t.Errorf("HTTP.ReadTimeout = %v, want 5s", got.HTTP.ReadTimeout)
	}
	if got.HTTP.WriteTimeout != 90*time.Second {
		t.Errorf("HTTP.WriteTimeout = %v, want 90s", got.HTTP.WriteTimeout)
	}
	if got.HTTP.IdleTimeout != 30*time.Second {
		t.Errorf("HTTP.IdleTimeout = %v, want 30s", got.HTTP.IdleTimeout)
	}
	if got.HTTP.ShutdownTimeout != 3*time.Second {
		t.Errorf("HTTP.ShutdownTimeout = %v, want 3s", got.HTTP.ShutdownTimeout)
	}
	if got.Database.Path != "/tmp/custom.db" {
		t.Errorf("Database.Path = %q, want %q", got.Database.Path, "/tmp/custom.db")
	}
	if got.Database.BusyTimeout != 2*time.Second {
		t.Errorf("Database.BusyTimeout = %v, want 2s", got.Database.BusyTimeout)
	}
	if got.Database.MaxOpenConns != 8 {
		t.Errorf("Database.MaxOpenConns = %d, want 8", got.Database.MaxOpenConns)
	}
	if got.Database.MigrateOnOpen {
		t.Error("Database.MigrateOnOpen = true, want false")
	}
	if got.Logging.Level != "debug" {
		t.Errorf("Logging.Level = %q, want %q", got.Logging.Level, "debug")
	}
	if got.Logging.Format != "text" {
		t.Errorf("Logging.Format = %q, want %q", got.Logging.Format, "text")
	}
}

func TestLoadFromTrimsSurroundingWhitespace(t *testing.T) {
	got, err := loadFrom(env(map[string]string{
		"DD_HTTP_ADDR": "  :7000  ",
		"DD_LOG_LEVEL": "\tinfo\n",
	}))
	if err != nil {
		t.Fatalf("loadFrom() error = %v, want nil", err)
	}
	if got.HTTP.Addr != ":7000" {
		t.Errorf("HTTP.Addr = %q, want %q", got.HTTP.Addr, ":7000")
	}
	if got.Logging.Level != "info" {
		t.Errorf("Logging.Level = %q, want %q", got.Logging.Level, "info")
	}
}

func TestLoadFromReportsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		envs    map[string]string
		wantSub string
	}{
		{
			name:    "duration is not a duration",
			envs:    map[string]string{"DD_HTTP_READ_TIMEOUT": "soon"},
			wantSub: "DD_HTTP_READ_TIMEOUT",
		},
		{
			name:    "duration is empty",
			envs:    map[string]string{"DD_DATABASE_BUSY_TIMEOUT": ""},
			wantSub: "DD_DATABASE_BUSY_TIMEOUT",
		},
		{
			name:    "boolean is not a boolean",
			envs:    map[string]string{"DD_MIGRATE_ON_START": "yes-please"},
			wantSub: "DD_MIGRATE_ON_START",
		},
		{
			name:    "integer is not an integer",
			envs:    map[string]string{"DD_DATABASE_MAX_OPEN_CONNS": "many"},
			wantSub: "DD_DATABASE_MAX_OPEN_CONNS",
		},
		{
			name:    "integer is below the minimum",
			envs:    map[string]string{"DD_DATABASE_MAX_OPEN_CONNS": "0"},
			wantSub: "at least 1",
		},
		{
			name:    "unknown log level",
			envs:    map[string]string{"DD_LOG_LEVEL": "loud"},
			wantSub: "DD_LOG_LEVEL",
		},
		{
			name:    "unknown log format",
			envs:    map[string]string{"DD_LOG_FORMAT": "xml"},
			wantSub: "DD_LOG_FORMAT",
		},
		{
			name:    "empty address",
			envs:    map[string]string{"DD_HTTP_ADDR": "   "},
			wantSub: "DD_HTTP_ADDR",
		},
		{
			name:    "empty database path",
			envs:    map[string]string{"DD_DATABASE_PATH": ""},
			wantSub: "DD_DATABASE_PATH",
		},
		{
			name:    "negative write timeout",
			envs:    map[string]string{"DD_HTTP_WRITE_TIMEOUT": "-1s"},
			wantSub: "DD_HTTP_WRITE_TIMEOUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadFrom(env(tt.envs))
			if err == nil {
				t.Fatalf("loadFrom() error = nil, want error containing %q", tt.wantSub)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("loadFrom() error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

func TestLoadFromReportsEveryProblemAtOnce(t *testing.T) {
	_, err := loadFrom(env(map[string]string{
		"DD_LOG_LEVEL":         "loud",
		"DD_HTTP_ADDR":         "",
		"DD_HTTP_IDLE_TIMEOUT": "never",
	}))
	if err == nil {
		t.Fatal("loadFrom() error = nil, want error")
	}

	msg := err.Error()
	for _, want := range []string{"DD_LOG_LEVEL", "DD_HTTP_ADDR", "DD_HTTP_IDLE_TIMEOUT"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error = %q, want it to mention %q", msg, want)
		}
	}
}

func TestLoadReadsProcessEnvironment(t *testing.T) {
	t.Setenv("DD_HTTP_ADDR", ":4242")

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if got.HTTP.Addr != ":4242" {
		t.Fatalf("Load() HTTP.Addr = %q, want %q", got.HTTP.Addr, ":4242")
	}
}

func TestConfigStringRendersEverySection(t *testing.T) {
	got := Defaults().String()

	for _, want := range []string{"http{", "database{", "logging{", ":8080", "distributed-drive.db"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, want it to contain %q", got, want)
		}
	}
}
