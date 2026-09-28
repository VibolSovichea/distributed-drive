package chunker

import (
	"fmt"
)




















type Layout struct {
	
	
	ChunkSize int64

	
	DataChunks int

	
	Total int64

	
	
	Stripes int

	
	
	
	
	DataInLast int
}








func Plan(total, chunkSize int64, k int) (Layout, error) {
	if k < 1 {
		return Layout{}, fmt.Errorf("chunker: %d data chunks cannot form a stripe", k)
	}
	if chunkSize < MinSize {
		return Layout{}, fmt.Errorf("chunker: a chunk size of %d is below the minimum of %d",
			chunkSize, MinSize)
	}
	if total < 0 {
		return Layout{}, fmt.Errorf("chunker: a file of %d bytes cannot exist", total)
	}

	l := Layout{ChunkSize: chunkSize, DataChunks: k, Total: total}
	if total == 0 {
		return l, nil
	}

	
	
	
	
	perStripe := chunkSize * int64(k)
	l.Stripes = int((total + perStripe - 1) / perStripe)

	
	
	consumed := int64(l.Stripes-1) * perStripe
	used := total - consumed
	l.DataInLast = int((used + chunkSize - 1) / chunkSize)

	return l, nil
}


func (l Layout) Splitter() (*Splitter, error) { return NewSplitter(l.ChunkSize) }






func (l Layout) DataShardCount() int {
	if l.Total <= 0 || l.ChunkSize <= 0 {
		return 0
	}
	return int((l.Total + l.ChunkSize - 1) / l.ChunkSize)
}








func (l Layout) StripeOf(shard int) (stripe, index int) {
	if l.DataChunks < 1 {
		return 0, 0
	}
	return shard / l.DataChunks, shard % l.DataChunks
}



func (l Layout) DataShardsInStripe(stripe int) int {
	switch {
	case stripe < 0 || stripe >= l.Stripes:
		return 0
	case stripe < l.Stripes-1:
		return l.DataChunks
	default:
		return l.DataInLast
	}
}







func (l Layout) DataShardSize(stripe, index int) int64 {
	if l.ChunkSize <= 0 || l.DataChunks < 1 {
		return 0
	}
	if index < 0 || index >= l.DataChunks {
		return 0
	}

	used := l.DataShardsInStripe(stripe)
	if index >= used {
		return 0
	}

	
	
	if stripe == l.Stripes-1 && index == used-1 {
		return l.LastDataShardSize()
	}
	return l.ChunkSize
}







func (l Layout) ParityShardSize() int64 { return l.ChunkSize }



func (l Layout) LastDataShardSize() int64 {
	if l.Total <= 0 || l.ChunkSize <= 0 {
		return 0
	}
	if r := l.Total % l.ChunkSize; r != 0 {
		return r
	}
	return l.ChunkSize
}
