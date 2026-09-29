package chunker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

const testSize int64 = 1024

func newSplitter(t *testing.T, size int64) *Splitter {
	t.Helper()

	s, err := NewSplitter(size)
	if err != nil {
		t.Fatalf("NewSplitter(%d): %v", size, err)
	}
	return s
}

func drain(t *testing.T, s *Splitter, r io.Reader) ([]byte, int64) {
	t.Helper()

	var out bytes.Buffer
	var total int64

	for {
		chunk, err := s.Next(r)
		if errors.Is(err, io.EOF) {
			return out.Bytes(), total
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if chunk.Index != s.Count()-1 {
			t.Errorf("chunk index = %d, want %d", chunk.Index, s.Count()-1)
		}
		out.Write(chunk.Data)
		total += chunk.Size
	}
}

func TestEmptyFileProducesNoChunks(t *testing.T) {
	t.Parallel()

	got, total := drain(t, newSplitter(t, testSize), strings.NewReader(""))
	if len(got) != 0 {
		t.Errorf("got %d bytes, want none", len(got))
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
}

func TestTinyFileIsOnePartialChunk(t *testing.T) {
	t.Parallel()

	payload := "small"
	got, total := drain(t, newSplitter(t, testSize), strings.NewReader(payload))

	if string(got) != payload {
		t.Errorf("got %q, want %q", got, payload)
	}
	if total != int64(len(payload)) {
		t.Errorf("total = %d, want %d", total, len(payload))
	}
}

func TestExactChunkBoundary(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("x"), int(testSize)*3)
	s := newSplitter(t, testSize)

	got, total := drain(t, s, bytes.NewReader(payload))
	if !bytes.Equal(got, payload) {
		t.Error("the reassembled bytes differ from the input")
	}
	if total != int64(len(payload)) {
		t.Errorf("total = %d, want %d", total, len(payload))
	}
	if s.Count() != 3 {
		t.Errorf("chunks = %d, want 3", s.Count())
	}
}

func TestPartialFinalChunk(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("y"), int(testSize)*2+137)
	s := newSplitter(t, testSize)

	got, _ := drain(t, s, bytes.NewReader(payload))
	if !bytes.Equal(got, payload) {
		t.Error("the reassembled bytes differ from the input")
	}
	if s.Count() != 3 {
		t.Errorf("chunks = %d, want 3", s.Count())
	}
}

func TestOneByteMoreThanAChunk(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("z"), int(testSize)+1)
	s := newSplitter(t, testSize)

	got, _ := drain(t, s, bytes.NewReader(payload))
	if !bytes.Equal(got, payload) {
		t.Error("the reassembled bytes differ from the input")
	}
	if s.Count() != 2 {
		t.Errorf("chunks = %d, want 2", s.Count())
	}
}

func TestEveryChunkIsHashed(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize) * 2)
	payload = append(payload, bytes.Repeat([]byte("h"), 9)...)
	s := newSplitter(t, testSize)
	r := bytes.NewReader(payload)

	seen := map[string]bool{}
	for {
		chunk, err := s.Next(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}

		if want := Hash(chunk.Data); chunk.SHA256 != want {
			t.Errorf("chunk %d hash = %s, want %s", chunk.Index, chunk.SHA256, want)
		}
		if chunk.SHA256 == "" {
			t.Errorf("chunk %d has no hash", chunk.Index)
		}
		if seen[chunk.SHA256] {
			t.Errorf("chunk %d repeats a hash, so identical chunks are indistinguishable", chunk.Index)
		}
		seen[chunk.SHA256] = true
	}
}

func variedBytes(length int) []byte {
	out := make([]byte, length)
	for i := range out {
		out[i] = byte(i*7+i/251) ^ byte(i>>3)
	}
	return out
}

func TestIdenticalChunksHashIdentically(t *testing.T) {
	t.Parallel()

	s := newSplitter(t, testSize)
	r := bytes.NewReader(bytes.Repeat([]byte("k"), int(testSize)*2))

	first, err := s.Next(r)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	second, err := s.Next(r)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	if first.Index == second.Index {
		t.Error("two chunks shared an index")
	}
	if first.SHA256 != second.SHA256 {
		t.Error("identical bytes hashed differently")
	}
}

func TestIndexesAreStableAcrossRuns(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("s"), int(testSize)*2+5)

	first := indexAndHashes(t, payload)
	second := indexAndHashes(t, payload)

	if len(first) != len(second) {
		t.Fatalf("run lengths differ: %d and %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("chunk %d differs between runs: %v and %v", i, first[i], second[i])
		}
	}
}

func indexAndHashes(t *testing.T, payload []byte) []string {
	t.Helper()

	var out []string
	s := newSplitter(t, testSize)
	r := bytes.NewReader(payload)

	for {
		chunk, err := s.Next(r)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, fmt.Sprintf("%d:%s", chunk.Index, chunk.SHA256))
	}
}

func TestChunkDataIsOnlyValidUntilTheNextRead(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("a"), int(testSize)*2)
	s := newSplitter(t, testSize)
	r := bytes.NewReader(payload)

	first, err := s.Next(r)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	firstHash := first.SHA256

	if _, err := s.Next(r); err != nil {
		t.Fatalf("Next: %v", err)
	}

	if first.SHA256 != firstHash {
		t.Error("the recorded hash changed, which would make it untrustworthy")
	}
}

func TestLargeFileStreamsWithoutBufferingItAll(t *testing.T) {
	t.Parallel()

	const size = 40 << 20
	generated := &repeatingReader{remaining: size, fill: 'q'}

	s := newSplitter(t, DefaultSize)

	var total int64
	for {
		chunk, err := s.Next(generated)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		total += chunk.Size
	}

	if total != size {
		t.Errorf("total = %d, want %d", total, size)
	}
	if want := (size + DefaultSize - 1) / DefaultSize; s.Count() != want {
		t.Errorf("chunks = %d, want %d", s.Count(), want)
	}
}

type repeatingReader struct {
	remaining int64
	fill      byte
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = r.fill
	}
	r.remaining -= int64(len(p))
	return len(p), nil
}

func TestNewSplitterRejectsAnUnusableSize(t *testing.T) {
	t.Parallel()

	for _, size := range []int64{0, -1, 1, MinSize - 1} {
		if _, err := NewSplitter(size); err == nil {
			t.Errorf("NewSplitter(%d) was accepted", size)
		}
	}
	if _, err := NewSplitter(MinSize); err != nil {
		t.Errorf("NewSplitter(%d) = %v, want it accepted", MinSize, err)
	}
}

func TestSplitReportsShapeWithoutRetaining(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("m"), int(testSize)*4+11)

	total, chunks, err := Split(bytes.NewReader(payload), testSize)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if total != int64(len(payload)) {
		t.Errorf("total = %d, want %d", total, len(payload))
	}
	if chunks != 5 {
		t.Errorf("chunks = %d, want 5", chunks)
	}
}

func TestSplitRejectsABadSize(t *testing.T) {
	t.Parallel()

	if _, _, err := Split(strings.NewReader("x"), 1); err == nil {
		t.Error("Split with an unusable size must fail")
	}
}

func TestNextPropagatesAReadFailure(t *testing.T) {
	t.Parallel()

	boom := errors.New("the disk went away")
	s := newSplitter(t, testSize)

	if _, err := s.Next(failingReader{err: boom}); !errors.Is(err, boom) {
		t.Errorf("error = %v, want it to wrap the read failure", err)
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }
