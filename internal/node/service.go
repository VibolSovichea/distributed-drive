








package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/id"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)






var ErrNoClient = errors.New("node: no client for this node")







var ErrUnauthorized = errors.New("node: the account is not authorised")






type Factory interface {
	
	
	
	Open(ctx context.Context, n metadata.Node) (provider.StorageNode, error)
}


type Store interface {
	CreateNode(ctx context.Context, node metadata.Node) error
	GetNode(ctx context.Context, id string) (metadata.Node, error)
	ListNodes(ctx context.Context) ([]metadata.Node, error)
	UpdateNode(ctx context.Context, node metadata.Node) error
	DeleteNode(ctx context.Context, id string) error
}


type Service interface {
	
	
	
	
	
	
	
	Register(ctx context.Context, input RegisterInput) (metadata.Node, error)

	
	Get(ctx context.Context, nodeID string) (metadata.Node, error)

	
	List(ctx context.Context) ([]metadata.Node, error)

	
	
	Delete(ctx context.Context, nodeID string) error

	
	Client(ctx context.Context, nodeID string) (provider.StorageNode, error)

	
	
	
	Check(ctx context.Context, nodeID string) (metadata.Node, error)

	
	
	
	CheckAll(ctx context.Context) ([]metadata.Node, error)

	
	
	Forget(nodeID string)
}

var _ Service = (*Registry)(nil)


type Registry struct {
	store   Store
	factory Factory

	
	now func() time.Time

	
	
	checkTimeout time.Duration

	
	
	
	checkConcurrency int

	
	
	
	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	
	
	
	
	mu     sync.Mutex
	client provider.StorageNode
}


type Options struct {
	
	CheckTimeout time.Duration
	
	CheckConcurrency int
	
	Now func() time.Time
}


func New(store Store, factory Factory, opts Options) *Registry {
	if opts.CheckTimeout <= 0 {
		opts.CheckTimeout = 30 * time.Second
	}
	if opts.CheckConcurrency <= 0 {
		opts.CheckConcurrency = 8
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Registry{
		store:            store,
		factory:          factory,
		now:              opts.Now,
		checkTimeout:     opts.CheckTimeout,
		checkConcurrency: opts.CheckConcurrency,
		entries:          make(map[string]*entry),
	}
}




type RegisterInput struct {
	Name     string
	Provider metadata.Provider
	
	
	Root string
}


func (r *Registry) Register(ctx context.Context, input RegisterInput) (metadata.Node, error) {
	now := r.now().UTC()

	record := metadata.Node{
		ID:        id.New(),
		Name:      input.Name,
		Provider:  input.Provider,
		Status:    metadata.NodeStatusOffline,
		CreatedAt: now,
		UpdatedAt: now,
	}

	
	
	
	if err := record.Validate(); err != nil {
		return metadata.Node{}, err
	}

	
	
	
	if err := r.store.CreateNode(ctx, record); err != nil {
		return metadata.Node{}, err
	}

	
	return r.Check(ctx, record.ID)
}


func (r *Registry) Get(ctx context.Context, nodeID string) (metadata.Node, error) {
	return r.store.GetNode(ctx, nodeID)
}


func (r *Registry) List(ctx context.Context) ([]metadata.Node, error) {
	return r.store.ListNodes(ctx)
}



func (r *Registry) Delete(ctx context.Context, nodeID string) error {
	
	r.slot(nodeID).mu.Lock()
	r.slot(nodeID).client = nil
	r.slot(nodeID).mu.Unlock()

	return r.store.DeleteNode(ctx, nodeID)
}


func (r *Registry) Client(ctx context.Context, nodeID string) (provider.StorageNode, error) {
	record, err := r.store.GetNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}

	slot := r.slot(nodeID)
	slot.mu.Lock()
	defer slot.mu.Unlock()

	if slot.client != nil {
		return slot.client, nil
	}

	client, err := r.factory.Open(ctx, record)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoClient, err)
	}

	slot.client = client
	return client, nil
}


func (r *Registry) Forget(nodeID string) {
	slot := r.slot(nodeID)

	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.client = nil
}


func (r *Registry) Check(ctx context.Context, nodeID string) (metadata.Node, error) {
	record, err := r.store.GetNode(ctx, nodeID)
	if err != nil {
		return metadata.Node{}, err
	}

	client, err := r.Client(ctx, nodeID)
	if err != nil {
		
		
		
		
		if !errors.Is(err, ErrUnauthorized) {
			return record, err
		}

		updated, recordErr := r.record(ctx, record, metadata.NodeStatusAuthError,
			provider.Identity{}, provider.Quota{}, time.Time{})
		if recordErr != nil {
			return record, recordErr
		}

		
		
		
		
		return updated, err
	}

	ctx, cancel := context.WithTimeout(ctx, r.checkTimeout)
	defer cancel()

	
	
	
	identity, quota, probeErr := r.probe(ctx, client)

	if probeErr != nil {
		health := provider.HealthOf(probeErr)

		
		
		
		if health == provider.HealthUnknown {
			return record, probeErr
		}

		updated, recordErr := r.record(ctx, record, statusFor(health), identity, quota, time.Time{})
		if recordErr != nil {
			return record, recordErr
		}
		return updated, probeErr
	}

	
	
	
	status := metadata.NodeStatusHealthy
	if quota.Known() && quota.Available() == 0 {
		status = metadata.NodeStatusFull
	}

	return r.record(ctx, record, status, identity, quota, r.now())
}


func (r *Registry) probe(ctx context.Context, client provider.StorageNode) (
	provider.Identity, provider.Quota, error) {

	var identity provider.Identity
	var quota provider.Quota

	
	
	
	idErr := error(nil)
	identity, idErr = client.Identity(ctx)
	if idErr != nil {
		return identity, quota, idErr
	}

	var qErr error
	quota, qErr = client.Quota(ctx)
	if qErr != nil {
		return identity, quota, qErr
	}

	return identity, quota, nil
}






func (r *Registry) record(
	ctx context.Context,
	record metadata.Node,
	status metadata.NodeStatus,
	identity provider.Identity,
	quota provider.Quota,
	
	
	
	seenAt time.Time,
) (metadata.Node, error) {

	updated := record
	updated.Status = status
	updated.UpdatedAt = r.now().UTC()

	
	
	
	
	if identity.AccountID != "" {
		updated.AccountIdentifier = identity.AccountID
	}
	if quota.Known() {
		updated.Capacity = quota.Total
		updated.UsedCapacity = quota.Used
	}
	if !seenAt.IsZero() {
		seen := seenAt.UTC()
		updated.LastSeen = &seen
	}

	if err := r.store.UpdateNode(ctx, updated); err != nil {
		return metadata.Node{}, err
	}

	return updated, nil
}







func (r *Registry) CheckAll(ctx context.Context) ([]metadata.Node, error) {
	records, err := r.store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}

	results := make([]metadata.Node, len(records))
	errs := make([]error, len(records))

	limit := r.checkConcurrency
	if limit < 1 {
		limit = 1
	}
	sem := make(chan struct{}, limit)

	var wg sync.WaitGroup
	for i, record := range records {
		wg.Add(1)
		go func(i int, nodeID string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			updated, checkErr := r.Check(ctx, nodeID)
			results[i] = updated
			errs[i] = checkErr
		}(i, record.ID)
	}
	wg.Wait()

	
	
	
	var failures []error
	var reachable int
	for i := range results {
		if errs[i] != nil {
			failures = append(failures, fmt.Errorf("%s: %w", records[i].Name, errs[i]))
			continue
		}
		reachable++
	}

	return results, errors.Join(failures...)
}





func statusFor(health provider.Health) metadata.NodeStatus {
	switch health {
	case provider.HealthDegraded:
		return metadata.NodeStatusDegraded
	case provider.HealthOffline:
		return metadata.NodeStatusOffline
	case provider.HealthFull:
		return metadata.NodeStatusFull
	case provider.HealthAuthError:
		return metadata.NodeStatusAuthError
	case provider.HealthHealthy:
		return metadata.NodeStatusHealthy
	default:
		
		
		return metadata.NodeStatusDegraded
	}
}


func (r *Registry) slot(nodeID string) *entry {
	r.mu.Lock()
	defer r.mu.Unlock()

	slot, ok := r.entries[nodeID]
	if !ok {
		slot = &entry{}
		r.entries[nodeID] = slot
	}
	return slot
}
