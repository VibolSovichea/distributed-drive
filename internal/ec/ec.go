














package ec

import (
	"errors"
	"fmt"

	"github.com/klauspost/reedsolomon"
)









const MaxShards = 256







var (
	
	
	ErrInvalidParams = errors.New("ec: invalid erasure coding parameters")

	
	ErrShardCount = errors.New("ec: wrong number of shards")

	
	
	ErrShardSize = errors.New("ec: shards must be the same size")

	
	
	ErrTooFewShards = errors.New("ec: not enough surviving shards to reconstruct")

	
	
	
	
	ErrNoShardsPresent = errors.New("ec: no shards survive")

	
	
	
	
	
	
	
	ErrNoData = errors.New("ec: every shard is empty")
)


type Params struct {
	
	Data int
	
	
	Parity int
}


func (p Params) Shards() int { return p.Data + p.Parity }












func (p Params) Alloc(width int) [][]byte {
	if p.Data < 1 || width <= 0 {
		return nil
	}
	shards := make([][]byte, p.Shards())
	for i := range shards {
		shards[i] = make([]byte, width)
	}
	return shards
}




func (p Params) Tolerates() int { return p.Parity }


func (p Params) Validate() error {
	switch {
	case p.Data < 1:
		return fmt.Errorf("%w: %d data shards cannot form a stripe", ErrInvalidParams, p.Data)
	case p.Parity < 0:
		return fmt.Errorf("%w: %d parity shards is negative", ErrInvalidParams, p.Parity)
	case p.Data+p.Parity > MaxShards:
		
		
		return fmt.Errorf("%w: %d data plus %d parity exceeds the %d shard limit",
			ErrInvalidParams, p.Data, p.Parity, MaxShards)
	}
	return nil
}







type Codec interface {
	
	Params() Params

	
	
	
	
	
	
	
	
	
	
	
	
	Encode(shards [][]byte) error

	
	
	
	
	
	
	
	
	
	
	Reconstruct(shards [][]byte) error

	
	
	
	
	
	
	Verify(shards [][]byte) (bool, error)
}


type reedSolomon struct {
	params  Params
	encoder reedsolomon.Encoder
}


func New(params Params) (Codec, error) {
	if err := params.Validate(); err != nil {
		return nil, err
	}

	
	
	
	enc, err := reedsolomon.New(params.Data, params.Parity)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidParams, err)
	}

	return &reedSolomon{params: params, encoder: enc}, nil
}

func (c *reedSolomon) Params() Params { return c.params }









func (c *reedSolomon) check(shards [][]byte) (size int, anyPresent bool, err error) {
	if len(shards) != c.params.Shards() {
		return 0, false, fmt.Errorf("%w: have %d, want %d data plus %d parity = %d",
			ErrShardCount, len(shards), c.params.Data, c.params.Parity, c.params.Shards())
	}

	
	
	
	for i, shard := range shards {
		if len(shard) == 0 {
			continue
		}
		if !anyPresent {
			size, anyPresent = len(shard), true
			continue
		}
		if len(shard) != size {
			return 0, false, fmt.Errorf("%w: shard %d is %d bytes, want the %d of the others",
				ErrShardSize, i, len(shard), size)
		}
	}

	return size, anyPresent, nil
}

func (c *reedSolomon) Encode(shards [][]byte) error {
	_, present, err := c.check(shards)
	if err != nil {
		return err
	}
	if !present {
		
		
		
		return ErrNoData
	}
	if err := c.encoder.Encode(shards); err != nil {
		return fmt.Errorf("ec: encode %d+%d shards: %w",
			c.params.Data, c.params.Parity, err)
	}
	return nil
}

func (c *reedSolomon) Reconstruct(shards [][]byte) error {
	
	
	
	if _, _, err := c.check(shards); err != nil {
		return err
	}

	present := 0
	for _, shard := range shards {
		if len(shard) != 0 {
			present++
		}
	}

	switch {
	case present == 0:
		
		
		
		return ErrNoShardsPresent
	case present == len(shards):
		
		
		return nil
	case present < c.params.Data:
		
		
		
		return fmt.Errorf("%w: %d of %d shards survive and %d are needed",
			ErrTooFewShards, present, len(shards), c.params.Data)
	}

	
	
	
	if err := c.encoder.Reconstruct(shards); err != nil {
		return fmt.Errorf("ec: reconstruct: %w", err)
	}
	if err := c.rebuildMissingParity(shards); err != nil {
		return err
	}
	return nil
}








func (c *reedSolomon) rebuildMissingParity(shards [][]byte) error {
	size := 0
	for _, shard := range shards {
		if len(shard) != 0 {
			size = len(shard)
			break
		}
	}
	if size == 0 {
		return nil
	}

	missing := false
	for i := c.params.Data; i < len(shards); i++ {
		if len(shards[i]) == 0 {
			missing = true
			break
		}
	}
	if !missing {
		return nil
	}

	
	
	for i := c.params.Data; i < len(shards); i++ {
		if len(shards[i]) == 0 {
			shards[i] = make([]byte, size)
		}
	}

	
	
	
	
	if err := c.encoder.Encode(shards); err != nil {
		return fmt.Errorf("ec: rebuild parity: %w", err)
	}
	return nil
}

func (c *reedSolomon) Verify(shards [][]byte) (bool, error) {
	_, present, err := c.check(shards)
	if err != nil {
		return false, err
	}
	if !present {
		return false, ErrNoData
	}
	ok, err := c.encoder.Verify(shards)
	if err != nil {
		return false, fmt.Errorf("ec: verify: %w", err)
	}
	return ok, nil
}
