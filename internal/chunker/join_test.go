package chunker

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

type memorySource struct {
	chunks map[int64][]byte

	corrupt map[int64][]byte

	failOn map[int64]error
	opened []int64
}

func (m *memorySource) Open(index int64) (io.ReadCloser, error) {
	m.opened = append(m.opened, index)

	if err, ok := m.failOn[index]; ok {
		return nil, err
	}
	if data, ok := m.corrupt[index]; ok {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	data, ok := m.chunks[index]
	if !ok {
		return nil, errors.New("no such chunk")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func splitToPlan(t *testing.T, payload []byte, size int64) (*memorySource, []Descriptor) {
	t.Helper()

	s := newSplitter(t, size)
	src := &memorySource{
		chunks:  map[int64][]byte{},
		corrupt: map[int64][]byte{},
		failOn:  map[int64]error{},
	}

	var plan []Descriptor
	r := bytes.NewReader(payload)
	for {
		chunk, err := s.Next(r)
		if errors.Is(err, io.EOF) {
			return src, plan
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}

		src.chunks[chunk.Index] = append([]byte(nil), chunk.Data...)
		plan = append(plan, Descriptor{
			Index:  chunk.Index,
			Size:   chunk.Size,
			SHA256: chunk.SHA256,
		})
	}
}

func TestJoinReassemblesInIndexOrder(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize) * 3)
	payload = append(payload, bytes.Repeat([]byte("t"), 250)...)
	src, plan := splitToPlan(t, payload, testSize)

	for i, j := 0, len(plan)-1; i < j; i, j = i+1, j-1 {
		plan[i], plan[j] = plan[j], plan[i]
	}

	joiner, err := NewJoiner(src, plan)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	got, err := io.ReadAll(joiner)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the reassembled file differs from the input")
	}
	if joiner.Offset() != int64(len(payload)) {
		t.Errorf("offset = %d, want %d", joiner.Offset(), len(payload))
	}
}

func TestJoinRejectsACorruptChunk(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize) * 3)
	src, plan := splitToPlan(t, payload, testSize)

	altered := append([]byte(nil), src.chunks[1]...)
	altered[0] ^= 0xff
	src.corrupt[1] = altered

	joiner, err := NewJoiner(src, plan)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	_, err = io.ReadAll(joiner)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}

	if !strings.Contains(err.Error(), "chunk 1") {
		t.Errorf("error = %v, want it to name chunk 1", err)
	}
}

func TestJoinRejectsATruncatedChunk(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize) * 2)
	src, plan := splitToPlan(t, payload, testSize)

	src.corrupt[1] = src.chunks[1][:len(src.chunks[1])-10]

	joiner, err := NewJoiner(src, plan)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	if _, err := io.ReadAll(joiner); !errors.Is(err, ErrCorrupt) {
		t.Errorf("error = %v, want ErrCorrupt", err)
	}
}

func TestJoinReportsAnOpenFailure(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize) * 2)
	src, plan := splitToPlan(t, payload, testSize)

	boom := errors.New("the node is offline")
	src.failOn[1] = boom

	joiner, err := NewJoiner(src, plan)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	if _, err := io.ReadAll(joiner); !errors.Is(err, boom) {
		t.Errorf("error = %v, want it to wrap the open failure", err)
	}
}

func TestJoinRejectsAGapInThePlan(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize) * 4)
	_, plan := splitToPlan(t, payload, testSize)

	gapped := make([]Descriptor, 0, len(plan)-1)
	gapped = append(gapped, plan[:1]...)
	gapped = append(gapped, plan[2:]...)

	_, err := NewJoiner(mustSource(t, payload, testSize), gapped)
	if err == nil {
		t.Fatal("a plan with a gap must be rejected")
	}
	if !strings.Contains(err.Error(), "not contiguous") {
		t.Errorf("error = %v, want it to name the gap", err)
	}
}

func TestJoinRejectsADuplicateIndex(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize) * 2)
	_, plan := splitToPlan(t, payload, testSize)

	doubled := make([]Descriptor, len(plan)+1)
	copy(doubled, plan)
	doubled[len(plan)] = plan[0]
	if _, err := NewJoiner(mustSource(t, payload, testSize), doubled); err == nil {
		t.Error("a plan with a duplicate index must be rejected")
	}
}

func TestJoinOfAnEmptyFileIsEmpty(t *testing.T) {
	t.Parallel()

	joiner, err := NewJoiner(&memorySource{}, nil)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	got, err := io.ReadAll(joiner)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d bytes, want none", len(got))
	}
}

func TestJoinSkipsVerificationWhenNoHashIsStored(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize))
	src, plan := splitToPlan(t, payload, testSize)

	plan[0].SHA256 = ""

	joiner, err := NewJoiner(src, plan)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	got, err := io.ReadAll(joiner)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the reassembled file differs from the input")
	}
}

func TestJoinReadsInSmallPieces(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize)*2 + 77)
	src, plan := splitToPlan(t, payload, testSize)

	joiner, err := NewJoiner(src, plan)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	var out bytes.Buffer
	buf := make([]byte, 7)
	for {
		n, err := joiner.Read(buf)
		out.Write(buf[:n])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}

	if !bytes.Equal(out.Bytes(), payload) {
		t.Error("the reassembled file differs from the input")
	}
}

func TestNewJoinerValidatesItsArguments(t *testing.T) {
	t.Parallel()

	if _, err := NewJoiner(nil, nil); err == nil {
		t.Error("NewJoiner with no source must fail")
	}
	if _, err := NewJoiner(&memorySource{}, []Descriptor{{Index: 0, Size: -1}}); err == nil {
		t.Error("a negative size must be rejected")
	}
}

func TestJoinCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	payload := variedBytes(int(testSize))
	src, plan := splitToPlan(t, payload, testSize)

	joiner, err := NewJoiner(src, plan)
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}

	if _, err := joiner.Read(make([]byte, 10)); err != nil {
		t.Fatalf("Read: %v", err)
	}

	if err := joiner.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := joiner.Close(); err != nil {
		t.Errorf("the second Close: %v", err)
	}
}

func TestSourceFuncAdaptsAFunction(t *testing.T) {
	t.Parallel()

	var opened int64
	src := SourceFunc(func(index int64) (io.ReadCloser, error) {
		opened++
		return io.NopCloser(strings.NewReader("body")), nil
	})

	joiner, err := NewJoiner(src, []Descriptor{{Index: 0, Size: 4}})
	if err != nil {
		t.Fatalf("NewJoiner: %v", err)
	}
	defer func() { _ = joiner.Close() }()

	got, err := io.ReadAll(joiner)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "body" {
		t.Errorf("got %q, want body", got)
	}
	if opened != 1 {
		t.Errorf("opened %d chunks, want 1", opened)
	}
}

func mustSource(t *testing.T, payload []byte, size int64) *memorySource {
	t.Helper()

	src, _ := splitToPlan(t, payload, size)
	return src
}
