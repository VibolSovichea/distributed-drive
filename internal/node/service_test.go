package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

type fakeStore struct {
	mu sync.Mutex

	nodes  map[string]metadata.Node
	order  []string
	updErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{nodes: map[string]metadata.Node{}}
}

func (s *fakeStore) CreateNode(_ context.Context, n metadata.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.nodes[n.ID]; exists {
		return fmt.Errorf("%w: node %s already exists", metadata.ErrConflict, n.ID)
	}
	s.nodes[n.ID] = n
	s.order = append(s.order, n.ID)
	return nil
}

func (s *fakeStore) GetNode(_ context.Context, id string) (metadata.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[id]
	if !ok {
		return metadata.Node{}, fmt.Errorf("%w: node %s", metadata.ErrNotFound, id)
	}
	return n, nil
}

func (s *fakeStore) ListNodes(context.Context) ([]metadata.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]metadata.Node, 0, len(s.nodes))
	for _, id := range s.order {
		out = append(out, s.nodes[id])
	}
	return out, nil
}

func (s *fakeStore) UpdateNode(_ context.Context, n metadata.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.updErr != nil {
		return s.updErr
	}
	if _, ok := s.nodes[n.ID]; !ok {
		return fmt.Errorf("%w: node %s", metadata.ErrNotFound, n.ID)
	}
	s.nodes[n.ID] = n
	return nil
}

func (s *fakeStore) DeleteNode(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.nodes[id]; !ok {
		return fmt.Errorf("%w: node %s", metadata.ErrNotFound, id)
	}
	delete(s.nodes, id)
	return nil
}

func (s *fakeStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.nodes)
}

type fakeNode struct {
	mu sync.Mutex

	identity    provider.Identity
	identityErr error

	quota    provider.Quota
	quotaErr error

	opened int
}

func (f *fakeNode) Upload(context.Context, io.Reader, provider.ObjectMetadata) (provider.RemoteObject, error) {
	return provider.RemoteObject{}, errors.New("not implemented")
}

func (f *fakeNode) Download(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeNode) Delete(context.Context, string) error { return errors.New("not implemented") }
func (f *fakeNode) Stat(context.Context, string) (provider.RemoteObject, error) {
	return provider.RemoteObject{}, errors.New("not implemented")
}

func (f *fakeNode) Quota(context.Context) (provider.Quota, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.quota, f.quotaErr
}

func (f *fakeNode) Identity(context.Context) (provider.Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.identity, f.identityErr
}

type fakeFactory struct {
	mu sync.Mutex

	nodes map[string]*fakeNode

	openErr error

	perNodeErr map[string]error

	opens int
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{
		nodes:      map[string]*fakeNode{},
		perNodeErr: map[string]error{},
	}
}

func (f *fakeFactory) add(name string, n *fakeNode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[name] = n
}

func (f *fakeFactory) Open(_ context.Context, record metadata.Node) (provider.StorageNode, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.opens++

	if err, ok := f.perNodeErr[record.ID]; ok {
		return nil, err
	}
	if f.openErr != nil {
		return nil, f.openErr
	}

	if n, ok := f.nodes[record.Name]; ok {
		n.mu.Lock()
		n.opened++
		n.mu.Unlock()
		return n, nil
	}
	return nil, fmt.Errorf("%w: no client for node %q", ErrUnauthorized, record.Name)
}

func (f *fakeFactory) openCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opens
}

func healthyNode(account string, quota provider.Quota) *fakeNode {
	return &fakeNode{
		identity: provider.Identity{AccountID: account, Email: account + "@example.com"},
		quota:    quota,
	}
}

func newTestRegistry(store *fakeStore, factory *fakeFactory) *Registry {
	return New(store, factory, Options{
		CheckTimeout: time.Second,
		Now:          func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
}

func TestRegisterRecordsAccountAndCapacity(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()
	node := healthyNode("user-1", provider.Quota{Total: 1000, Used: 400})
	factory.add("drive-1", node)

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if record.ID == "" {
		t.Error("Register assigned no id")
	}

	if record.AccountIdentifier != "user-1" {
		t.Errorf("account identifier = %q, want user-1", record.AccountIdentifier)
	}
	if record.Status != metadata.NodeStatusHealthy {
		t.Errorf("status = %q, want healthy", record.Status)
	}
	if record.Capacity != 1000 || record.UsedCapacity != 400 {
		t.Errorf("capacity = %d/%d, want 1000/400", record.UsedCapacity, record.Capacity)
	}
	if record.LastSeen == nil {
		t.Error("a successful check must record LastSeen")
	}
	if record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		t.Error("Register must timestamp the record")
	}
}

func TestRegisterWritesRecordEvenWhenCredentialsAreWrong(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()
	factory.openErr = ErrUnauthorized

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})

	if err == nil {
		t.Error("Register should report that the account could not be contacted")
	}
	if record.ID == "" {
		t.Fatal("no record was returned")
	}
	if store.count() != 1 {
		t.Fatalf("the node record was not persisted: %d rows", store.count())
	}

	stored, err := store.GetNode(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if stored.Status != metadata.NodeStatusAuthError {
		t.Errorf("status = %q, want auth_error", stored.Status)
	}
	if stored.AccountIdentifier != "" {
		t.Errorf("account identifier = %q, want empty: the account was never reached",
			stored.AccountIdentifier)
	}
}

func TestRegisterRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	reg := newTestRegistry(store, newFakeFactory())

	cases := []struct {
		name  string
		input RegisterInput
	}{
		{"no name", RegisterInput{Provider: metadata.ProviderGoogleDrive}},
		{"no provider", RegisterInput{Name: "n"}},
		{"unknown provider", RegisterInput{Name: "n", Provider: metadata.Provider("nope")}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := reg.Register(context.Background(), tc.input); !errors.Is(err, metadata.ErrInvalid) {
				t.Errorf("error = %v, want ErrInvalid", err)
			}
		})
	}

	if store.count() != 0 {
		t.Errorf("%d rows were written for invalid input", store.count())
	}
}

func TestCheckRecordsStatusFromProviderHealth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		identityErr error
		quota       provider.Quota
		quotaErr    error
		wantStatus  metadata.NodeStatus
		wantErrIs   error
	}{
		{
			name:       "healthy",
			quota:      provider.Quota{Total: 10, Used: 1},
			wantStatus: metadata.NodeStatusHealthy,
		},
		{
			name:       "offline",
			quotaErr:   provider.Unavailablef(errors.New("no route to host")),
			wantStatus: metadata.NodeStatusOffline,
			wantErrIs:  provider.ErrUnavailable,
		},
		{
			name:        "auth error",
			identityErr: provider.Authf(errors.New("token revoked")),
			wantStatus:  metadata.NodeStatusAuthError,
			wantErrIs:   provider.ErrAuth,
		},
		{
			name:       "full",
			quotaErr:   provider.QuotaExceededf(errors.New("storage full")),
			wantStatus: metadata.NodeStatusFull,
			wantErrIs:  provider.ErrQuotaExceeded,
		},
		{
			name:       "degraded",
			quotaErr:   provider.Degradedf(errors.New("odd response")),
			wantStatus: metadata.NodeStatusDegraded,
			wantErrIs:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			factory := newFakeFactory()

			node := healthyNode("user-1", tc.quota)
			node.identityErr = tc.identityErr
			node.quotaErr = tc.quotaErr
			factory.add("drive-1", node)

			reg := newTestRegistry(store, factory)

			record, err := reg.Register(context.Background(),
				RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})

			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Errorf("error = %v, want it to wrap %v", err, tc.wantErrIs)
			}
			if record.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", record.Status, tc.wantStatus)
			}
		})
	}
}

func TestCheckRecordsFullWhenQuotaIsExhausted(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()

	node := healthyNode("user-1", provider.Quota{Total: 100, Used: 100})
	factory.add("drive-1", node)

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if record.Status != metadata.NodeStatusFull {
		t.Errorf("status = %q, want full", record.Status)
	}
	if record.Status.Usable() {
		t.Error("a full node must not be usable for placement")
	}
}

func TestCheckLeavesStatusAloneWhenContextIsCancelled(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()

	node := healthyNode("user-1", provider.Quota{Total: 100, Used: 1})
	factory.add("drive-1", node)

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	node.mu.Lock()
	node.identityErr = context.Canceled
	node.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	after, err := reg.Check(ctx, record.ID)
	if err == nil {
		t.Fatal("a cancelled check must report an error")
	}

	if after.Status != metadata.NodeStatusHealthy {
		t.Errorf("status = %q, want it left at healthy", after.Status)
	}
}

func TestCheckDoesNotOverwriteLastSeenOnFailure(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()

	node := healthyNode("user-1", provider.Quota{Total: 100, Used: 1})
	factory.add("drive-1", node)

	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reg := New(store, factory, Options{
		Now: func() time.Time { return clock },
	})

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	firstSeen := *record.LastSeen

	clock = clock.Add(time.Hour)
	node.mu.Lock()
	node.quotaErr = provider.Unavailablef(errors.New("down"))
	node.mu.Unlock()

	after, err := reg.Check(context.Background(), record.ID)
	if err == nil {
		t.Fatal("Check should report the failure")
	}

	if after.LastSeen == nil || !after.LastSeen.Equal(firstSeen) {
		t.Errorf("LastSeen = %v, want it left at %v", after.LastSeen, firstSeen)
	}
	if after.UpdatedAt.Equal(firstSeen) {
		t.Error("UpdatedAt should have moved even though the check failed")
	}
}

func TestCheckRecoversFromAuthError(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()

	node := healthyNode("user-1", provider.Quota{Total: 100, Used: 1})
	node.identityErr = provider.Authf(errors.New("revoked"))
	factory.add("drive-1", node)

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if !errors.Is(err, provider.ErrAuth) {
		t.Fatalf("error = %v, want ErrAuth", err)
	}
	if record.Status != metadata.NodeStatusAuthError {
		t.Fatalf("status = %q, want auth_error", record.Status)
	}

	node.mu.Lock()
	node.identityErr = nil
	node.mu.Unlock()

	after, err := reg.Check(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("Check after re-authorisation: %v", err)
	}
	if after.Status != metadata.NodeStatusHealthy {
		t.Errorf("status = %q, want healthy", after.Status)
	}
}

func TestClientCachesTheProviderClient(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()
	factory.add("drive-1", healthyNode("user-1", provider.Quota{Total: 100}))

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	first := factory.openCount()
	for i := 0; i < 5; i++ {
		if _, err := reg.Client(context.Background(), record.ID); err != nil {
			t.Fatalf("Client: %v", err)
		}
	}

	if got := factory.openCount(); got != first {
		t.Errorf("the factory was called %d more times; the client is not cached", got-first)
	}
}

func TestClientIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()
	factory.add("drive-1", healthyNode("user-1", provider.Quota{Total: 100}))

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	before := factory.openCount()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := reg.Client(context.Background(), record.ID); err != nil {
				t.Errorf("Client: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := factory.openCount(); got != before {
		t.Errorf("%d clients were built concurrently for one node", got-before)
	}
}

func TestClientForUnknownNode(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(newFakeStore(), newFakeFactory())

	_, err := reg.Client(context.Background(), "nope")
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestClientWithoutAuthorisationIsDistinct(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()
	factory.openErr = ErrUnauthorized

	reg := newTestRegistry(store, factory)

	_, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if !errors.Is(err, ErrUnauthorized) {
		t.Errorf("error = %v, want it to wrap ErrUnauthorized", err)
	}

	record, _ := store.ListNodes(context.Background())
	if len(record) != 1 {
		t.Fatalf("%d rows, want 1", len(record))
	}
	_, err = reg.Client(context.Background(), record[0].ID)
	if !errors.Is(err, ErrNoClient) {
		t.Errorf("Client error = %v, want ErrNoClient", err)
	}
}

func TestBrokenFactoryLeavesStatusAlone(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()

	node := healthyNode("user-1", provider.Quota{Total: 100, Used: 1})
	factory.add("drive-1", node)

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	factory.mu.Lock()
	factory.openErr = errors.New("misconfigured")
	factory.mu.Unlock()
	reg.Forget(record.ID)

	after, err := reg.Check(context.Background(), record.ID)
	if err == nil {
		t.Fatal("Check should report the failure")
	}
	if after.Status != metadata.NodeStatusHealthy {
		t.Errorf("status = %q, want it left at healthy", after.Status)
	}
}

func TestForgetDropsTheCachedClient(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()
	factory.add("drive-1", healthyNode("user-1", provider.Quota{Total: 100}))

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	before := factory.openCount()
	reg.Forget(record.ID)

	if _, err := reg.Client(context.Background(), record.ID); err != nil {
		t.Fatalf("Client: %v", err)
	}
	if factory.openCount() != before+1 {
		t.Error("Forget did not drop the cached client")
	}
}

func TestCheckAllVisitsEveryNodeAndKeepsGoing(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()

	factory.add("a", healthyNode("user-good", provider.Quota{Total: 100, Used: 1}))

	bad := healthyNode("user-bad", provider.Quota{Total: 100})
	bad.quotaErr = provider.Unavailablef(errors.New("unreachable"))
	factory.add("b", bad)

	factory.add("c", healthyNode("user-also-good", provider.Quota{Total: 100, Used: 2}))

	reg := newTestRegistry(store, factory)

	ids := make([]string, 0, 3)
	for _, name := range []string{"a", "b", "c"} {
		record, err := reg.Register(context.Background(),
			RegisterInput{Name: name, Provider: metadata.ProviderGoogleDrive})

		if record.ID == "" {
			t.Fatalf("Register(%s) produced no record: %v", name, err)
		}
		ids = append(ids, record.ID)
	}

	if store.count() != 3 {
		t.Fatalf("%d nodes registered, want 3", store.count())
	}

	results, err := reg.CheckAll(context.Background())
	if err == nil {
		t.Error("CheckAll should report the unreachable node")
	}

	if len(results) != 3 {
		t.Fatalf("CheckAll returned %d results, want 3", len(results))
	}

	statuses := map[string]metadata.NodeStatus{}
	for _, r := range results {
		statuses[r.ID] = r.Status
	}
	if statuses[ids[0]] != metadata.NodeStatusHealthy {
		t.Errorf("node a status = %q, want healthy", statuses[ids[0]])
	}
	if statuses[ids[1]] != metadata.NodeStatusOffline {
		t.Errorf("node b status = %q, want offline", statuses[ids[1]])
	}
	if statuses[ids[2]] != metadata.NodeStatusHealthy {
		t.Errorf("node c status = %q, want healthy", statuses[ids[2]])
	}
}

func TestCheckAllIsConcurrencySafe(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()

	for i := 0; i < 25; i++ {
		factory.add(fmt.Sprintf("n%d", i),
			healthyNode(fmt.Sprintf("user-%d", i), provider.Quota{Total: 100, Used: 1}))
	}

	reg := newTestRegistry(store, factory)

	for i := 0; i < 25; i++ {
		if _, err := reg.Register(context.Background(),
			RegisterInput{Name: fmt.Sprintf("n%d", i), Provider: metadata.ProviderGoogleDrive}); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}

	if _, err := reg.CheckAll(context.Background()); err != nil {
		t.Fatalf("CheckAll: %v", err)
	}

	if _, err := reg.CheckAll(context.Background()); err != nil {
		t.Fatalf("CheckAll again: %v", err)
	}
}

func TestCheckAllOnEmptyRegistry(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(newFakeStore(), newFakeFactory())

	results, err := reg.CheckAll(context.Background())
	if err != nil {
		t.Fatalf("CheckAll: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want none", len(results))
	}
}

func TestStatusForCoversEveryProviderHealth(t *testing.T) {
	t.Parallel()

	cases := map[provider.Health]metadata.NodeStatus{
		provider.HealthHealthy:   metadata.NodeStatusHealthy,
		provider.HealthDegraded:  metadata.NodeStatusDegraded,
		provider.HealthOffline:   metadata.NodeStatusOffline,
		provider.HealthFull:      metadata.NodeStatusFull,
		provider.HealthAuthError: metadata.NodeStatusAuthError,
	}

	for health, want := range cases {
		if got := statusFor(health); got != want {
			t.Errorf("statusFor(%q) = %q, want %q", health, got, want)
		}
	}

	if got := statusFor(provider.HealthUnknown); got == metadata.NodeStatusUnknown {
		t.Error("statusFor(HealthUnknown) produced the zero status")
	}
}

func TestFailedStatusUpdateIsReported(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	factory := newFakeFactory()
	factory.add("drive-1", healthyNode("user-1", provider.Quota{Total: 100, Used: 1}))

	reg := newTestRegistry(store, factory)

	record, err := reg.Register(context.Background(),
		RegisterInput{Name: "drive-1", Provider: metadata.ProviderGoogleDrive})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	store.mu.Lock()
	store.updErr = errors.New("disk full")
	store.mu.Unlock()

	if _, err := reg.Check(context.Background(), record.ID); err == nil {
		t.Error("a failed status write must be reported")
	}
}
