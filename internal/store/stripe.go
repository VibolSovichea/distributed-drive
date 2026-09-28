package store

import (
	"context"
	"fmt"

	"github.com/VibolSovichea/distributed-drive/internal/chunker"
	"github.com/VibolSovichea/distributed-drive/internal/crypto"
	"github.com/VibolSovichea/distributed-drive/internal/ec"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)



type LoadedStripe struct {
	
	
	
	
	Shards [][]byte

	
	
	
	
	
	
	
	Rebuilt []int
}


func (l LoadedStripe) Healthy() bool { return len(l.Rebuilt) == 0 }












func (s *Store) LoadStripe(
	ctx context.Context,
	sess *session,
	file metadata.File,
	group []metadata.Chunk,
	stripeIndex int,
) (LoadedStripe, error) {
	params := ec.Params{Data: file.DataChunks, Parity: file.ParityChunks}
	codec, err := ec.New(params)
	if err != nil {
		return LoadedStripe{}, fmt.Errorf("store: the file's erasure parameters are unusable: %w", err)
	}
	return loadStripe(ctx, sess, s.cipher, file, params, codec, group, stripeIndex)
}



func loadStripe(
	ctx context.Context,
	sess *session,
	cipher *crypto.CipherConfig,
	file metadata.File,
	params ec.Params,
	codec ec.Codec,
	group []metadata.Chunk,
	stripeIndex int,
) (LoadedStripe, error) {
	width := int(file.ChunkSize)
	shards := params.Alloc(width)

	
	var fileKey []byte
	if cipher.Enabled() && file.Encrypted {
		key, err := crypto.DeriveKey(cipher.MasterKey, file.ID)
		if err != nil {
			return LoadedStripe{}, fmt.Errorf("store: derive file key: %w", err)
		}
		fileKey = key
	}

	
	
	
	fetched := make([]bool, len(shards))
	
	
	unusable := make([]bool, len(shards))
	
	
	
	
	verified := make([]bool, len(shards))
	
	var rebuilt []int

	for range params.Parity + 1 {
		
		for i := range shards {
			if unusable[i] || fetched[i] {
				continue
			}
			chunk := group[i]
			if chunk.RemoteFileID == "" {
				
				unusable[i] = true
				continue
			}
			if _, err := fetchShard(ctx, sess, chunk, shards[i], file, fileKey, cipher, stripeIndex, i); err != nil {
				unusable[i] = true
				continue
			}
			fetched[i] = true
		}

		
		for i := range shards {
			if unusable[i] || !fetched[i] || verified[i] {
				continue
			}
			if chunker.Hash(shards[i]) != group[i].Hash {
				unusable[i] = true
				continue
			}
			verified[i] = true
		}

		condemned := 0
		for _, bad := range unusable {
			if bad {
				condemned++
			}
		}

		switch {
		case condemned == 0:
			return LoadedStripe{Shards: shards, Rebuilt: rebuilt}, nil
		case condemned > params.Parity:
			
			return LoadedStripe{}, fmt.Errorf(
				"%w: a stripe of file %s has %d unusable shards, and only %d can be rebuilt",
				ErrTooFewShards, file.ID, condemned, params.Parity)
		}

		
		
		for i := range shards {
			if unusable[i] {
				shards[i] = nil
			}
		}
		if err := codec.Reconstruct(shards); err != nil {
			return LoadedStripe{}, fmt.Errorf("store: rebuild a stripe of file %s: %w", file.ID, err)
		}
		
		
		
		
		
		
		
		
		for i := range shards {
			if unusable[i] {
				unusable[i] = false
				fetched[i] = true
				verified[i] = false
				rebuilt = append(rebuilt, i)
			}
		}
	}

	return LoadedStripe{}, fmt.Errorf(
		"%w: a stripe of file %s still fails its checksums after reconstruction",
		ErrFileCorrupt, file.ID)
}











func fetchShard(
	ctx context.Context,
	sess *session,
	chunk metadata.Chunk,
	buf []byte,
	file metadata.File,
	fileKey []byte,
	cipher *crypto.CipherConfig,
	stripeIndex, shardIndex int,
) (int, error) {
	handle, err := sess.node(ctx, chunk.NodeID)
	if err != nil {
		return 0, err
	}

	reader, err := handle.Download(ctx, chunk.RemoteFileID)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	
	
	
	if cipher.Enabled() && file.Encrypted && len(fileKey) > 0 {
		
		ciphertext := make([]byte, len(buf)+cipher.Cipher.Overhead())
		if _, err := readFull(ctx, reader, ciphertext); err != nil {
			return 0, err
		}
		plaintext, err := crypto.DecryptShard(cipher.Cipher, fileKey, ciphertext, stripeIndex, shardIndex)
		if err != nil {
			return 0, err
		}
		copy(buf, plaintext)
		return len(plaintext), nil
	}

	return readFull(ctx, reader, buf)
}
