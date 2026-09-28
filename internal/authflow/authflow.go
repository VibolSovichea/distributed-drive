













package authflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)




var (
	
	ErrUnknownState = errors.New("authflow: unknown or already used state")
	
	ErrStateExpired = errors.New("authflow: the authorisation expired")
	
	
	
	
	
	ErrStateMismatch = errors.New("authflow: the authorisation belongs to a different node")
	
	ErrNotConfigured = errors.New("authflow: no provider is configured")
)




const (
	DefaultStateTTL     = 10 * time.Minute
	DefaultMaxPending   = 128
	DefaultClockSkewPad = 0
)


type Authorizer interface {
	
	
	
	
	
	
	NewState(nodeID string) (string, error)
	
	AuthorizeURL(state string) (string, error)
	
	
	
	
	Exchange(ctx context.Context, code, state string) error

	
	
	
	
	
	NodeOf(state string) (nodeID string, ok bool)
}


type Options struct {
	
	StateTTL time.Duration
	
	
	
	MaxPending int
	
	Now func() time.Time
	
	
	AfterPrune func()
}


type Service struct {
	auth Authorizer
	opts Options

	mu sync.Mutex
	
	
	pending map[string]pending
	
	order []string
}

type pending struct {
	nodeID    string
	expiresAt time.Time
}


func New(auth Authorizer, opts Options) (*Service, error) {
	if auth == nil {
		return nil, errors.New("authflow: an Authorizer is required")
	}
	if opts.StateTTL <= 0 {
		opts.StateTTL = DefaultStateTTL
	}
	if opts.MaxPending <= 0 {
		opts.MaxPending = DefaultMaxPending
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}

	return &Service{
		auth:    auth,
		opts:    opts,
		pending: make(map[string]pending),
	}, nil
}






func (s *Service) Start(_ context.Context, nodeID string) (string, error) {
	if nodeID == "" {
		return "", errors.New("authflow: a node id is required")
	}

	state, err := s.auth.NewState(nodeID)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.opts.Now()
	s.pruneLocked(now)

	
	
	for len(s.pending) >= s.opts.MaxPending && len(s.order) > 0 {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.pending, oldest)
	}

	s.pending[state] = pending{nodeID: nodeID, expiresAt: now.Add(s.opts.StateTTL)}
	s.order = append(s.order, state)

	url, err := s.auth.AuthorizeURL(state)
	if err != nil {
		
		
		delete(s.pending, state)
		s.order = s.order[:len(s.order)-1]
		return "", fmt.Errorf("authflow: build the authorisation URL: %w", err)
	}

	return url, nil
}







func (s *Service) Complete(ctx context.Context, code, state string) error {
	if code == "" {
		return errors.New("authflow: the callback carried no code")
	}
	if state == "" {
		return errors.New("authflow: the callback carried no state")
	}

	s.mu.Lock()
	entry, ok := s.pending[state]
	if ok {
		delete(s.pending, state)
		s.removeOrderLocked(state)
	}
	now := s.opts.Now()
	s.mu.Unlock()

	if !ok {
		
		
		return ErrUnknownState
	}
	if now.After(entry.expiresAt) {
		return ErrStateExpired
	}
	
	
	
	
	if decoded, ok := s.auth.NodeOf(state); ok && decoded != entry.nodeID {
		return ErrStateMismatch
	}

	return s.auth.Exchange(ctx, code, state)
}


func (s *Service) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}


func (s *Service) pruneLocked(now time.Time) {
	kept := s.order[:0]
	for _, state := range s.order {
		entry, ok := s.pending[state]
		if !ok {
			continue
		}
		if now.After(entry.expiresAt) {
			delete(s.pending, state)
			continue
		}
		kept = append(kept, state)
	}
	s.order = kept
}



func (s *Service) removeOrderLocked(state string) {
	for i, existing := range s.order {
		if existing == state {
			s.order = append(s.order[:i], s.order[i+1:]...)
			return
		}
	}
}
