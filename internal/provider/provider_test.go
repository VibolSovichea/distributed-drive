package provider

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestQuotaAvailable(t *testing.T) {
	tests := []struct {
		name  string
		quota Quota
		want  int64
		known bool
	}{
		{name: "room to spare", quota: Quota{Total: 100, Used: 40}, want: 60, known: true},
		{name: "unknown total", quota: Quota{Used: 40}, want: 0, known: false},
		{name: "exactly full", quota: Quota{Total: 100, Used: 100}, want: 0, known: true},
		{name: "over full is clamped", quota: Quota{Total: 100, Used: 130}, want: 0, known: true},
		{name: "nothing used", quota: Quota{Total: 100}, want: 100, known: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.quota.Available(); got != tt.want {
				t.Errorf("Available() = %d, want %d", got, tt.want)
			}
			if got := tt.quota.Known(); got != tt.known {
				t.Errorf("Known() = %t, want %t", got, tt.known)
			}
		})
	}
}

func TestIdentityDisplay(t *testing.T) {
	tests := []struct {
		name     string
		identity Identity
		want     string
	}{
		{
			name:     "email wins",
			identity: Identity{AccountID: "123", Email: "a@example.com", DisplayName: "Ada"},
			want:     "a@example.com",
		},
		{
			name:     "display name when there is no email",
			identity: Identity{AccountID: "123", DisplayName: "Ada"},
			want:     "Ada",
		},
		{
			name:     "id as a last resort",
			identity: Identity{AccountID: "123"},
			want:     "123",
		},
		{
			name:     "unknown when nothing is known",
			identity: Identity{},
			want:     "unknown account",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.identity.Display(); got != tt.want {
				t.Errorf("Display() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHealthOf(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Health
	}{
		{name: "auth", err: Authf(errors.New("token revoked")), want: HealthAuthError},
		{name: "unavailable", err: Unavailablef(errors.New("dial tcp")), want: HealthOffline},
		{name: "full", err: QuotaExceededf(errors.New("403 quota")), want: HealthFull},
		{name: "degraded", err: Degradedf(errors.New("checksum mismatch")), want: HealthDegraded},
		{name: "not found still means reachable", err: NotFound("abc"), want: HealthHealthy},
		{
			name: "invalid request is the caller's fault",
			err:  fmt.Errorf("%w: bad id", ErrInvalid),
			want: HealthHealthy,
		},
		{name: "too large", err: fmt.Errorf("%w: 5 EiB", ErrTooLarge), want: HealthFull},
		{name: "unrecognised defaults to offline", err: errors.New("what"), want: HealthOffline},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HealthOf(tt.err); got != tt.want {
				t.Errorf("HealthOf(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestHealthOfSeesThroughAddedContext(t *testing.T) {
	
	
	wrapped := fmt.Errorf("gdrive: files.get id=abc: %w", Authf(errors.New("invalid_grant")))

	if got := HealthOf(wrapped); got != HealthAuthError {
		t.Errorf("HealthOf() = %q, want %q", got, HealthAuthError)
	}
	if !errors.Is(wrapped, ErrAuth) {
		t.Error("the sentinel is not visible through the wrapping chain")
	}
}

func TestStatusErrorUnwrapsToItsSentinel(t *testing.T) {
	err := QuotaExceededf(errors.New("storageQuotaExceeded"))

	if !errors.Is(err, ErrQuotaExceeded) {
		t.Error("errors.Is(err, ErrQuotaExceeded) = false, want true")
	}
	
	if errors.Is(err, ErrUnavailable) {
		t.Error("a quota failure must not match ErrUnavailable")
	}
}

func TestStatusErrorMessageNamesTheHealth(t *testing.T) {
	
	
	err := Authf(errors.New("invalid_grant"))

	want := "provider: node is auth_error"
	if got := err.Error(); len(got) < len(want) || got[:len(want)] != want {
		t.Errorf("Error() = %q, want it to start with %q", got, want)
	}
}

func TestNotFoundNamesTheObject(t *testing.T) {
	err := NotFound("01ABC")

	if !errors.Is(err, ErrNotFound) {
		t.Error("errors.Is(err, ErrNotFound) = false, want true")
	}
	if got := err.Error(); got == "" {
		t.Error("Error() is empty, want the object id included")
	}
}

func TestHealthOfSaysNothingForACancelledCheck(t *testing.T) {
	
	
	
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := HealthOf(err); got != HealthUnknown {
			t.Errorf("HealthOf(%v) = %q, want %q", err, got, HealthUnknown)
		}
	}

	
	wrapped := fmt.Errorf("gdrive: files.list: %w", context.Canceled)
	if got := HealthOf(wrapped); got != HealthUnknown {
		t.Errorf("HealthOf(wrapped cancellation) = %q, want %q", got, HealthUnknown)
	}
}
