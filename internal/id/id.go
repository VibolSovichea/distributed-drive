package id

import (
	"crypto/rand"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	entropyMu sync.Mutex
	entropy   = ulid.Monotonic(rand.Reader, 0)
)

func New() string {
	now := ulid.Timestamp(time.Now())

	entropyMu.Lock()
	defer entropyMu.Unlock()

	return ulid.MustNew(now, entropy).String()
}

func IsValid(s string) bool {
	_, err := parse(s)
	return err == nil
}

func Time(id string) (time.Time, error) {
	parsed, err := parse(id)
	if err != nil {
		return time.Time{}, err
	}
	return ulid.Time(parsed.Time()), nil
}

func parse(s string) (ulid.ULID, error) {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) != ulid.EncodedSize {
		return ulid.ULID{}, errInvalid
	}
	return ulid.ParseStrict(strings.ToUpper(trimmed))
}

type invalidIDError struct{}

func (invalidIDError) Error() string { return "id: not a valid ULID" }

var errInvalid error = invalidIDError{}
