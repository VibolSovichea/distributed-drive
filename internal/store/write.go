package store

import (
	"bytes"
	"context"
	"fmt"

	"github.com/VibolSovichea/distributed-drive/internal/chunker"
	"github.com/VibolSovichea/distributed-drive/internal/crypto"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)














func (s *Store) storeShard(
	ctx context.Context,
	sess *session,
	in UploadInput,
	file metadata.File,
	pool metadata.Pool,
	nodeID string,
	stripe, index int,
	plaintext []byte,
	fileKey []byte,
) (provider.RemoteObject, error) {
	handle, err := sess.node(ctx, nodeID)
	if err != nil {
		return provider.RemoteObject{}, err
	}

	var ciphertext []byte
	if s.cipher.Enabled() && file.Encrypted && len(fileKey) > 0 {
		ct, err := crypto.EncryptShard(s.cipher.Cipher, fileKey, plaintext, stripe, index)
		if err != nil {
			return provider.RemoteObject{}, fmt.Errorf("store: encrypt stripe %d shard %d: %w",
				stripe, index, err)
		}
		ciphertext = ct
	} else {
		ciphertext = plaintext
	}

	
	
	remote, err := handle.Upload(ctx, bytes.NewReader(ciphertext), provider.ObjectMetadata{
		Name:        shardName(file.Name, stripe, index),
		Description: shardDescription(file.ID, pool.ID, chunker.Hash(plaintext)),
		ContentType: in.ContentType,
		Size:        int64(len(ciphertext)),
	})
	if err != nil {
		return provider.RemoteObject{}, fmt.Errorf("store: upload stripe %d shard %d to node %s: %w",
			stripe, index, nodeID, err)
	}
	return remote, nil
}
