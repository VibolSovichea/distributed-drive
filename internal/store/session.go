package store

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

var nowFunc = time.Now

type session struct {
	resolver NodeResolver
	meta     metadataStore

	mu     sync.Mutex
	opened map[string]provider.StorageNode
}

func newSession(resolver NodeResolver, meta metadataStore) *session {
	return &session{
		resolver: resolver,
		meta:     meta,
		opened:   make(map[string]provider.StorageNode),
	}
}

func (s *session) node(ctx context.Context, nodeID string) (provider.StorageNode, error) {
	s.mu.Lock()
	if node, ok := s.opened[nodeID]; ok {
		s.mu.Unlock()
		return node, nil
	}
	s.mu.Unlock()

	node, err := s.meta.GetNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}

	handle, err := s.resolver.Open(ctx, node)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.opened[nodeID]; ok {
		return existing, nil
	}
	s.opened[nodeID] = handle

	return handle, nil
}

func (s *session) delete(ctx context.Context, nodeID, remoteID string) error {
	handle, err := s.node(ctx, nodeID)
	if err != nil {
		return err
	}
	if err := handle.Delete(ctx, remoteID); err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

type metadataStore interface {
	GetNode(ctx context.Context, id string) (metadata.Node, error)
}

func runConcurrently(n, limit int, fn func(i int)) {
	if n == 0 {
		return
	}
	if limit < 1 || limit > n {
		limit = n
	}

	tokens := make(chan struct{}, limit)
	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)
		tokens <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-tokens }()
			fn(i)
		}(i)
	}

	wg.Wait()
}

func isNotFound(err error) bool { return errors.Is(err, provider.ErrNotFound) }

func readFull(ctx context.Context, r io.Reader, buf []byte) (int, error) {
	read := 0
	for read < len(buf) {
		if err := ctx.Err(); err != nil {
			return read, err
		}
		n, err := r.Read(buf[read:])
		read += n
		if err != nil {
			if err == io.EOF && read < len(buf) {
				return read, io.ErrUnexpectedEOF
			}
			return read, err
		}
	}
	return read, nil
}
