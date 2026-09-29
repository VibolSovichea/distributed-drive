package authflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeAuthorizer struct {
	mu sync.Mutex

	states []string

	authorizeErr error
	exchangeErr  error

	exchanged []string
}

func (f *fakeAuthorizer) NewState(nodeID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.states) == 0 {
		f.states = []string{"state-1", "state-2", "state-3", "state-4", "state-5"}
	}
	state := f.states[0]
	f.states = f.states[1:]

	return state + "@" + nodeID, nil
}

func (f *fakeAuthorizer) AuthorizeURL(state string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.authorizeErr != nil {
		return "", f.authorizeErr
	}
	return "https://provider.example/authorize?state=" + state, nil
}

func (f *fakeAuthorizer) Exchange(_ context.Context, code, state string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exchangeErr != nil {
		return f.exchangeErr
	}
	f.exchanged = append(f.exchanged, code+" "+state)
	return nil
}

func (f *fakeAuthorizer) NodeOf(state string) (string, bool) {
	nodeID, ok := strings.CutPrefix(state, "state-")
	if !ok {
		return "", false
	}
	nodeID, ok = strings.CutPrefix(nodeID[strings.Index(nodeID, "@"):], "@")
	if nodeID == "" {
		return "", false
	}
	return nodeID, true
}

func (f *fakeAuthorizer) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.exchanged...)
}

func newService(t *testing.T, auth Authorizer, now *time.Time) *Service {
	t.Helper()

	svc, err := New(auth, Options{
		StateTTL:   10 * time.Minute,
		MaxPending: 4,
		Now:        func() time.Time { return *now },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

func TestStartReturnsAConsentURL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{}
	svc := newService(t, auth, &now)

	url, err := svc.Start(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !strings.Contains(url, "state=state-1@node-1") {
		t.Errorf("the URL does not carry the state: %s", url)
	}
	if svc.Pending() != 1 {
		t.Errorf("pending = %d, want 1", svc.Pending())
	}
}

func TestCompleteExchangesTheCode(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{}
	svc := newService(t, auth, &now)

	if _, err := svc.Start(context.Background(), "node-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := svc.Complete(context.Background(), "the-code", "state-1@node-1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := auth.calls(); len(got) != 1 || got[0] != "the-code state-1@node-1" {
		t.Errorf("exchanged = %v, want one call with the code and state", got)
	}
}

func TestAStateIsAcceptedOnlyOnce(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{}
	svc := newService(t, auth, &now)

	if _, err := svc.Start(context.Background(), "node-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := svc.Complete(context.Background(), "code", "state-1@node-1"); err != nil {
		t.Fatalf("the first completion must succeed: %v", err)
	}

	err := svc.Complete(context.Background(), "code", "state-1@node-1")
	if !errors.Is(err, ErrUnknownState) {
		t.Errorf("error = %v, want ErrUnknownState", err)
	}
	if got := auth.calls(); len(got) != 1 {
		t.Errorf("exchanged %d times, want 1: the code was redeemed twice", len(got))
	}
}

func TestAStateForAnotherNodeIsRejected(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{}
	svc := newService(t, auth, &now)

	if _, err := svc.Start(context.Background(), "node-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	err := svc.Complete(context.Background(), "code", "state-2@node-2")
	if !errors.Is(err, ErrUnknownState) {
		t.Errorf("error = %v, want ErrUnknownState", err)
	}
	if got := auth.calls(); len(got) != 0 {
		t.Errorf("exchanged %v, want nothing", got)
	}
}

func TestAMisreportedNodeIsRefused(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{}
	lying := &liarAuthorizer{fakeAuthorizer: auth}
	svc := newService(t, lying, &now)

	if _, err := svc.Start(context.Background(), "node-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	err := svc.Complete(context.Background(), "code", lying.state)
	if !errors.Is(err, ErrStateMismatch) {
		t.Errorf("error = %v, want ErrStateMismatch", err)
	}
	if got := auth.calls(); len(got) != 0 {
		t.Errorf("exchanged %v, want nothing", got)
	}
}

type liarAuthorizer struct {
	*fakeAuthorizer
	state string
}

func (l *liarAuthorizer) NewState(nodeID string) (string, error) {
	state, err := l.fakeAuthorizer.NewState(nodeID)
	if err != nil {
		return "", err
	}
	l.state = strings.Replace(state, "@"+nodeID, "@node-elsewhere", 1)
	return l.state, nil
}

func TestAStateExpires(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{}
	svc := newService(t, auth, &now)

	if _, err := svc.Start(context.Background(), "node-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	now = now.Add(11 * time.Minute)

	err := svc.Complete(context.Background(), "code", "state-1@node-1")
	if !errors.Is(err, ErrStateExpired) {
		t.Errorf("error = %v, want ErrStateExpired", err)
	}
	if got := auth.calls(); len(got) != 0 {
		t.Errorf("exchanged %v, want nothing", got)
	}
}

func TestCompleteRequiresBothParameters(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newService(t, &fakeAuthorizer{}, &now)

	if _, err := svc.Start(context.Background(), "node-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for _, tc := range []struct{ name, code, state string }{
		{"no code", "", "state-1@node-1"},
		{"no state", "code", ""},
	} {
		if err := svc.Complete(context.Background(), tc.code, tc.state); err == nil {
			t.Errorf("%s was accepted", tc.name)
		}
	}
}

func TestPendingIsBounded(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newService(t, &fakeAuthorizer{states: manyStates(200)}, &now)

	for i := 0; i < 200; i++ {
		if _, err := svc.Start(context.Background(), "node-1"); err != nil {
			t.Fatalf("Start %d: %v", i, err)
		}
	}

	if got := svc.Pending(); got > 4 {
		t.Errorf("pending = %d, want at most the configured maximum of 4", got)
	}
}

func TestStartDoesNotLeakAStateWhenTheProviderFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{authorizeErr: errors.New("client id is not configured")}
	svc := newService(t, auth, &now)

	for i := 0; i < 10; i++ {
		if _, err := svc.Start(context.Background(), "node-1"); err == nil {
			t.Fatal("Start must fail when the provider cannot build a URL")
		}
	}
	if got := svc.Pending(); got != 0 {
		t.Errorf("pending = %d, want 0", got)
	}
}

func TestExpiredEntriesArePruned(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	auth := &fakeAuthorizer{}
	svc := newService(t, auth, &now)

	for i := 0; i < 3; i++ {
		if _, err := svc.Start(context.Background(), "node-1"); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}

	now = now.Add(time.Hour)
	if _, err := svc.Start(context.Background(), "node-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got := svc.Pending(); got != 1 {
		t.Errorf("pending = %d, want only the state just issued", got)
	}
}

func TestStartRequiresANode(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := newService(t, &fakeAuthorizer{}, &now)

	if _, err := svc.Start(context.Background(), ""); err == nil {
		t.Error("Start with no node must fail")
	}
}

func TestNewRequiresAnAuthorizer(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, Options{}); err == nil {
		t.Error("New with no authorizer must fail")
	}
}

func manyStates(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "state" + string(rune('a'+i%26)) + strings.Repeat("x", i%7) + "-" + itoa(i)
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
