










package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)















const DefaultSize int64 = 1 << 20












const MinSize int64 = 1 << 10







var ErrCorrupt = errors.New("chunker: the chunk does not match its checksum")







type Chunk struct {
	
	
	
	Index int64
	
	Size int64
	
	
	
	SHA256 string
	Data   []byte
}





type Splitter struct {
	size  int64
	index int64
	buf   []byte
}


func NewSplitter(size int64) (*Splitter, error) {
	if size < MinSize {
		return nil, fmt.Errorf("chunker: a chunk size of %d is too small; the minimum is %d",
			size, MinSize)
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("chunker: a chunk size of %d cannot be held in memory", size)
	}

	return &Splitter{
		size: size,
		
		
		buf: make([]byte, size),
	}, nil
}


func (s *Splitter) Size() int64 { return s.size }







func (s *Splitter) Next(r io.Reader) (Chunk, error) {
	n, err := io.ReadFull(r, s.buf)

	switch {
	case errors.Is(err, io.EOF) && n == 0:
		
		
		
		return Chunk{}, io.EOF
	case err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF):
		return Chunk{}, fmt.Errorf("chunker: read chunk %d: %w", s.index, err)
	}

	digest := sha256.Sum256(s.buf[:n])
	chunk := Chunk{
		Index:  s.index,
		Size:   int64(n),
		SHA256: hex.EncodeToString(digest[:]),
		Data:   s.buf[:n],
	}
	s.index++

	return chunk, nil
}


func (s *Splitter) Count() int64 { return s.index }







func Split(r io.Reader, size int64) (total int64, chunks int64, err error) {
	s, err := NewSplitter(size)
	if err != nil {
		return 0, 0, err
	}

	for {
		chunk, err := s.Next(r)
		if errors.Is(err, io.EOF) {
			return total, chunks, nil
		}
		if err != nil {
			return total, chunks, err
		}
		total += chunk.Size
		chunks++
	}
}



type Source interface {
	Open(index int64) (io.ReadCloser, error)
}


type SourceFunc func(index int64) (io.ReadCloser, error)


func (f SourceFunc) Open(index int64) (io.ReadCloser, error) { return f(index) }


type Descriptor struct {
	
	
	
	
	Index int64
	
	Size int64
	
	
	SHA256 string
}





type Joiner struct {
	src  Source
	plan []Descriptor

	
	next int
	
	current io.ReadCloser
	
	pending []byte
	
	offset int64
	closed bool
}







func NewJoiner(src Source, plan []Descriptor) (*Joiner, error) {
	if src == nil {
		return nil, errors.New("chunker: a chunk source is required")
	}

	sorted := make([]Descriptor, len(plan))
	copy(sorted, plan)
	
	
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].Index < sorted[j-1].Index; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	for i, d := range sorted {
		if d.Size < 0 {
			return nil, fmt.Errorf("chunker: chunk %d has a negative size", d.Index)
		}
		if i > 0 && d.Index == sorted[i-1].Index {
			return nil, fmt.Errorf("chunker: chunk %d appears twice", d.Index)
		}
		if i > 0 && d.Index != sorted[i-1].Index+1 {
			return nil, fmt.Errorf("chunker: chunk %d is missing; the plan is not contiguous", d.Index)
		}
	}

	return &Joiner{src: src, plan: sorted}, nil
}


func (j *Joiner) Read(p []byte) (int, error) {
	for {
		if len(j.pending) > 0 {
			n := copy(p, j.pending)
			j.pending = j.pending[n:]
			j.offset += int64(n)
			return n, nil
		}

		if j.closed {
			return 0, io.EOF
		}
		if j.next >= len(j.plan) {
			j.closed = true
			return 0, io.EOF
		}

		if err := j.openNext(); err != nil {
			return 0, err
		}
	}
}







func (j *Joiner) openNext() error {
	if j.current != nil {
		_ = j.current.Close()
		j.current = nil
	}

	desc := j.plan[j.next]
	j.next++

	body, err := j.src.Open(desc.Index)
	if err != nil {
		return fmt.Errorf("chunker: open chunk %d: %w", desc.Index, err)
	}

	data, err := io.ReadAll(body)
	closeErr := body.Close()
	if err != nil {
		return fmt.Errorf("chunker: read chunk %d: %w", desc.Index, err)
	}
	if closeErr != nil {
		return fmt.Errorf("chunker: close chunk %d: %w", desc.Index, closeErr)
	}

	if int64(len(data)) != desc.Size {
		return fmt.Errorf("%w: chunk %d is %d bytes, expected %d",
			ErrCorrupt, desc.Index, len(data), desc.Size)
	}

	if desc.SHA256 != "" {
		digest := sha256.Sum256(data)
		if got := hex.EncodeToString(digest[:]); got != desc.SHA256 {
			
			
			
			return fmt.Errorf("%w: chunk %d hashes to %s, expected %s",
				ErrCorrupt, desc.Index, got, desc.SHA256)
		}
	}

	j.pending = data
	return nil
}


func (j *Joiner) Offset() int64 { return j.offset }


func (j *Joiner) Close() error {
	j.closed = true
	if j.current == nil {
		return nil
	}
	err := j.current.Close()
	j.current = nil
	return err
}




func Hash(b []byte) string {
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:])
}
