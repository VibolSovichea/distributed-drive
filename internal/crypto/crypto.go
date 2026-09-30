package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

type Cipher interface {
	Seal(key []byte, plaintext []byte, nonce []byte) ([]byte, error)

	Open(key []byte, ciphertext []byte, nonce []byte) ([]byte, error)

	Overhead() int

	NonceSize() int
}

type AESGCM struct{}

func (AESGCM) Seal(key []byte, plaintext []byte, nonce []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return aead.Seal(nil, nonce, plaintext, nil), nil
}

func (AESGCM) Open(key []byte, ciphertext []byte, nonce []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return aead.Open(nil, nonce, ciphertext, nil)
}

func (AESGCM) Overhead() int  { return 16 }
func (AESGCM) NonceSize() int { return 12 }

type XChaCha20Poly1305 struct{}

func (XChaCha20Poly1305) Seal(key []byte, plaintext []byte, nonce []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	return aead.Seal(nil, nonce, plaintext, nil), nil
}

func (XChaCha20Poly1305) Open(key []byte, ciphertext []byte, nonce []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	return aead.Open(nil, nonce, ciphertext, nil)
}

func (XChaCha20Poly1305) Overhead() int  { return 24 }
func (XChaCha20Poly1305) NonceSize() int { return 24 }

var DefaultCipher Cipher = AESGCM{}

const KeySize = 32

var ErrInvalidKey = errors.New("crypto: key must be 32 bytes")

var ErrInvalidNonce = errors.New("crypto: nonce has wrong size")

var ErrDecrypt = errors.New("crypto: decryption failed")

func DeriveKey(masterKey []byte, fileID string) ([]byte, error) {
	if len(masterKey) != KeySize {
		return nil, ErrInvalidKey
	}
	hkdf := hkdf.New(sha256.New, masterKey, []byte(fileID), []byte("distributed-drive-shard-key-v1"))
	key := make([]byte, KeySize)
	if _, err := hkdf.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

func DeriveNonce(fileKey []byte, stripe, index int, nonceSize int) ([]byte, error) {
	if len(fileKey) != KeySize {
		return nil, ErrInvalidKey
	}

	context := make([]byte, 8)
	context[0] = byte(stripe >> 24)
	context[1] = byte(stripe >> 16)
	context[2] = byte(stripe >> 8)
	context[3] = byte(stripe)
	context[4] = byte(index >> 24)
	context[5] = byte(index >> 16)
	context[6] = byte(index >> 8)
	context[7] = byte(index)

	hkdf := hkdf.New(sha256.New, fileKey, context, []byte("distributed-drive-shard-nonce-v1"))
	nonce := make([]byte, nonceSize)
	if _, err := hkdf.Read(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

func EncryptShard(c Cipher, fileKey []byte, plaintext []byte, stripe, index int) ([]byte, error) {
	nonce, err := DeriveNonce(fileKey, stripe, index, c.NonceSize())
	if err != nil {
		return nil, err
	}
	return c.Seal(fileKey, plaintext, nonce)
}

func DecryptShard(c Cipher, fileKey []byte, ciphertext []byte, stripe, index int) ([]byte, error) {
	nonce, err := DeriveNonce(fileKey, stripe, index, c.NonceSize())
	if err != nil {
		return nil, err
	}
	plaintext, err := c.Open(fileKey, ciphertext, nonce)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return plaintext, nil
}

func RandomKey() ([]byte, error) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

func HashKey(key []byte) string {
	h := sha256.Sum256(key)
	return fmt.Sprintf("sha256:%x", h[:8])
}

func MAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func VerifyMAC(key, data, mac []byte) bool {
	return hmac.Equal(MAC(key, data), mac)
}

type CipherConfig struct {
	MasterKey []byte

	Cipher Cipher
}

func NewCipherConfig(masterKey []byte) *CipherConfig {
	return &CipherConfig{
		MasterKey: masterKey,
		Cipher:    DefaultCipher,
	}
}

func (cc *CipherConfig) Enabled() bool {
	return cc.MasterKey != nil
}
