package id

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewIsValid(t *testing.T) {
	got := New()

	if len(got) != 26 {
		t.Fatalf("New() = %q, want 26 characters", got)
	}
	if !IsValid(got) {
		t.Errorf("IsValid(%q) = false, want true", got)
	}
}

func TestNewIsUnique(t *testing.T) {
	const n = 10_000

	seen := make(map[string]struct{}, n)
	for range n {
		seen[New()] = struct{}{}
	}

	if len(seen) != n {
		t.Fatalf("got %d distinct ids, want %d", len(seen), n)
	}
}

func TestNewIsMonotonicWithinAMillisecond(t *testing.T) {
	
	
	
	const n = 5_000

	prev := New()
	for range n {
		curr := New()
		if curr <= prev {
			t.Fatalf("New() produced %q then %q, want strictly increasing ids", prev, curr)
		}
		prev = curr
	}
}

func TestNewIsConcurrencySafe(t *testing.T) {
	const workers, each = 8, 500

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []string
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]string, 0, each)
			for range each {
				local = append(local, New())
			}
			mu.Lock()
			results = append(results, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	seen := make(map[string]struct{}, workers*each)
	for _, v := range results {
		if !IsValid(v) {
			t.Errorf("IsValid(%q) = false, want true", v)
		}
		seen[v] = struct{}{}
	}
	if len(seen) != workers*each {
		t.Fatalf("got %d distinct ids, want %d", len(seen), workers*each)
	}
}

func TestIsValidRejects(t *testing.T) {
	valid := New()

	tests := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "too short", in: valid[:25]},
		{name: "too long", in: valid + "0"},
		{name: "not base32", in: "ILLLLLLLLLLLLLLLLLLLLLLLLL"},
		{name: "uuid", in: "9f1c0b0e-1f2a-4b3c-8d9e-0f1a2b3c4d5e"},
		{name: "sql injection", in: "'; DROP TABLE pools; --"},
		{name: "whitespace only", in: "                          "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if IsValid(tt.in) {
				t.Errorf("IsValid(%q) = true, want false", tt.in)
			}
		})
	}
}

func TestIsValidAcceptsLowercaseAndSurroundingSpace(t *testing.T) {
	want := New()

	if !IsValid(strings.ToLower(want)) {
		t.Errorf("IsValid(%q) = false, want true", strings.ToLower(want))
	}
	if !IsValid("  " + want + "\n") {
		t.Errorf("IsValid with surrounding whitespace = false, want true")
	}
}

func TestTime(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	generated := New()
	after := time.Now().UTC().Add(time.Second)

	got, err := Time(generated)
	if err != nil {
		t.Fatalf("Time(%q) error = %v, want nil", generated, err)
	}
	if got.Before(before) || got.After(after) {
		t.Fatalf("Time(%q) = %v, want a value between %v and %v", generated, got, before, after)
	}
}

func TestTimeRejectsGarbage(t *testing.T) {
	if _, err := Time("nope"); err == nil {
		t.Fatal("Time(\"nope\") error = nil, want an error")
	}
}
