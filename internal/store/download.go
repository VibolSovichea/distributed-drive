package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/VibolSovichea/distributed-drive/internal/chunker"
	"github.com/VibolSovichea/distributed-drive/internal/ec"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

func (s *Store) Download(ctx context.Context, fileID string) (io.ReadCloser, error) {
	file, err := s.meta.GetFile(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("store: load the file: %w", err)
	}
	switch file.Status {
	case metadata.FileStatusCommitted, metadata.FileStatusDegraded:
	default:
		return nil, fmt.Errorf("%w: it is %s", ErrFileNotReadable, file.Status)
	}

	chunks, err := s.meta.GetFileChunks(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("store: load the file's shards: %w", err)
	}

	stripes, err := indexStripes(file, chunks)
	if err != nil {
		return nil, err
	}

	params := ec.Params{Data: file.DataChunks, Parity: file.ParityChunks}
	codec, err := ec.New(params)
	if err != nil {
		return nil, fmt.Errorf("store: the file's erasure parameters are unusable: %w", err)
	}

	d := &download{
		ctx:     ctx,
		store:   s,
		file:    file,
		stripes: stripes,
		params:  params,
		codec:   codec,
		sess:    newSession(s.nodes, s.meta),
	}

	plan := make([]chunker.Descriptor, 0, len(stripes)*file.DataChunks)
	for stripeIndex := range stripes {
		for i := range file.DataChunks {
			plan = append(plan, chunker.Descriptor{
				Index: int64(stripeIndex*file.DataChunks + i),
				Size:  stripes[stripeIndex][i].Size,
			})
		}
	}

	joiner, err := chunker.NewJoiner(d, plan)
	if err != nil {
		return nil, fmt.Errorf("store: plan the download: %w", err)
	}

	return &verifyingReader{
		inner:  joiner,
		digest: sha256.New(),
		want:   file.ContentHash,

		onComplete: func() { s.markIntact(ctx, file) },
	}, nil
}

func (s *Store) markIntact(ctx context.Context, file metadata.File) {
	if file.Status != metadata.FileStatusDegraded {
		return
	}
	file.Status = metadata.FileStatusCommitted
	file.UpdatedAt = s.now()
	_ = s.meta.UpdateFile(ctx, file)
}

func indexStripes(file metadata.File, chunks []metadata.Chunk) ([][]metadata.Chunk, error) {
	perStripe := file.DataChunks + file.ParityChunks
	if perStripe <= 0 {
		return nil, fmt.Errorf("store: %w: file %s has no shards at all", metadata.ErrInvalid, file.ID)
	}

	byIndex := make(map[int]map[int]metadata.Chunk)
	highest := -1
	for _, chunk := range chunks {
		if chunk.StripeIndex < 0 {
			return nil, fmt.Errorf("store: %w: file %s has a shard with a negative stripe %d",
				metadata.ErrInvalid, file.ID, chunk.StripeIndex)
		}
		if chunk.Index < 0 || chunk.Index >= perStripe {
			return nil, fmt.Errorf("store: %w: file %s has a shard at stripe %d index %d, outside 0..%d",
				metadata.ErrInvalid, file.ID, chunk.StripeIndex, chunk.Index, perStripe-1)
		}

		indexes, ok := byIndex[chunk.StripeIndex]
		if !ok {
			indexes = make(map[int]metadata.Chunk, perStripe)
			byIndex[chunk.StripeIndex] = indexes
		}
		indexes[chunk.Index] = chunk

		if chunk.StripeIndex > highest {
			highest = chunk.StripeIndex
		}
	}

	out := make([][]metadata.Chunk, 0, highest+1)
	for stripeIndex := range highest + 1 {
		indexes := byIndex[stripeIndex]
		group := make([]metadata.Chunk, perStripe)
		nodes := make(map[string]bool, perStripe)

		for i := range perStripe {
			chunk, ok := indexes[i]
			if !ok {
				continue
			}
			if nodes[chunk.NodeID] {
				return nil, fmt.Errorf(
					"store: %w: file %s stripe %d has two shards on node %s, so it has less redundancy than it claims",
					metadata.ErrInvalid, file.ID, stripeIndex, chunk.NodeID)
			}
			nodes[chunk.NodeID] = true
			group[i] = chunk
		}
		out = append(out, group)
	}

	return out, nil
}

type download struct {
	ctx     context.Context
	store   *Store
	file    metadata.File
	stripes [][]metadata.Chunk
	params  ec.Params
	codec   ec.Codec
	sess    *session

	mu     sync.Mutex
	held   int
	shards [][]byte
}

func (d *download) Open(index int64) (io.ReadCloser, error) {
	stripeIndex := int(index) / d.file.DataChunks
	shardIndex := int(index) % d.file.DataChunks

	if stripeIndex < 0 || stripeIndex >= len(d.stripes) {
		return nil, fmt.Errorf("store: %w: chunk %d is in stripe %d, but the file has %d",
			metadata.ErrInvalid, index, stripeIndex, len(d.stripes))
	}

	if err := d.load(stripeIndex); err != nil {
		return nil, err
	}

	d.mu.Lock()
	chunk := d.stripes[stripeIndex][shardIndex]
	data := d.shards[shardIndex][:chunk.Size]
	d.mu.Unlock()

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (d *download) load(stripeIndex int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.shards != nil && d.held == stripeIndex {
		return nil
	}

	loaded, err := loadStripe(d.ctx, d.sess, d.store.cipher, d.file, d.params, d.codec, d.stripes[stripeIndex], stripeIndex)
	if err != nil {
		return err
	}

	d.held = stripeIndex
	d.shards = loaded.Shards
	return nil
}

type verifyingReader struct {
	inner  io.ReadCloser
	digest hash.Hash
	want   string

	onComplete func()

	eofSeen bool

	closed bool
}

func (r *verifyingReader) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {

		r.digest.Write(p[:n])
	}

	if !errors.Is(err, io.EOF) {
		return n, err
	}

	if !r.eofSeen {
		r.eofSeen = true

		if r.want != "" {
			if got := hex.EncodeToString(r.digest.Sum(nil)); got != r.want {
				return n, fmt.Errorf("%w: got sha256 %s, want %s", ErrFileCorrupt, got, r.want)
			}
		}
		if r.onComplete != nil {
			r.onComplete()
			r.onComplete = nil
		}
	}

	return n, io.EOF
}

func (r *verifyingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.inner.Close()
}
