package ec

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)




func shardData(k, m, width int) [][]byte {
	shards := make([][]byte, k+m)
	for i := range shards {
		buf := make([]byte, width)
		for j := range buf {
			
			
			x := uint32(i*2654435761) ^ uint32(j*2246822519)
			x ^= x >> 13
			x *= 2654435761
			buf[j] = byte(x >> 16)
		}
		shards[i] = buf
	}
	return shards
}



func cloneShards(shards [][]byte) [][]byte {
	out := make([][]byte, len(shards))
	for i, s := range shards {
		if s == nil {
			continue
		}
		out[i] = bytes.Clone(s)
	}
	return out
}




func sameShards(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}




func TestRecoverFromEveryLossUpToM(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 4096

	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reference := shardData(k, m, width)
	if err := codec.Encode(reference); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	
	for lost := range k + m {
		t.Run(fmt.Sprintf("one missing/index=%d", lost), func(t *testing.T) {
			shards := cloneShards(reference)
			shards[lost] = nil

			if err := codec.Reconstruct(shards); err != nil {
				t.Fatalf("Reconstruct: %v", err)
			}
			if !sameShards(shards, reference) {
				t.Error("the reconstructed stripe differs from the original")
			}
		})
	}

	
	for a := range k + m {
		for b := a + 1; b < k+m; b++ {
			t.Run(fmt.Sprintf("two missing/%d+%d", a, b), func(t *testing.T) {
				shards := cloneShards(reference)
				shards[a] = nil
				shards[b] = nil

				if err := codec.Reconstruct(shards); err != nil {
					t.Fatalf("Reconstruct: %v", err)
				}
				if !sameShards(shards, reference) {
					t.Error("the reconstructed stripe differs from the original")
				}
			})
		}
	}

	
	
	t.Run("three missing is refused", func(t *testing.T) {
		shards := cloneShards(reference)
		shards[0] = nil
		shards[1] = nil
		shards[2] = nil

		err := codec.Reconstruct(shards)
		if !errors.Is(err, ErrTooFewShards) {
			t.Fatalf("Reconstruct error = %v, want ErrTooFewShards", err)
		}
		
		
		for i, shard := range shards {
			if i < 3 {
				continue
			}
			if !bytes.Equal(shard, reference[i]) {
				t.Errorf("shard %d was modified by a failed reconstruction", i)
			}
		}
	})
}

func TestRecoverFromEveryCombinationOfDataAndParityLoss(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 2048

	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reference := shardData(k, m, width)
	if err := codec.Encode(reference); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	
	
	
	
	cases := []struct {
		name string
		lost []int
	}{
		{name: "two data", lost: []int{0, 1}},
		{name: "one data one parity", lost: []int{1, 4}},
		{name: "other data other parity", lost: []int{3, 5}},
		{name: "two parity", lost: []int{4, 5}},
		{name: "first and last data", lost: []int{0, 3}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			shards := cloneShards(reference)
			for _, i := range tc.lost {
				shards[i] = nil
			}

			if err := codec.Reconstruct(shards); err != nil {
				t.Fatalf("Reconstruct: %v", err)
			}
			if !sameShards(shards, reference) {
				t.Errorf("lost %v and the reconstruction is wrong", tc.lost)
			}
		})
	}
}





func TestReconstructRebuildsMissingParity(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 1024

	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reference := shardData(k, m, width)
	if err := codec.Encode(reference); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	
	
	shards := cloneShards(reference)
	shards[2] = nil
	shards[k] = nil

	if err := codec.Reconstruct(shards); err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}

	for i := k; i < k+m; i++ {
		if len(shards[i]) != width {
			t.Fatalf("parity shard %d is %d bytes, want %d: the recovery left the "+
				"stripe incomplete", i, len(shards[i]), width)
		}
	}
	if !sameShards(shards, reference) {
		t.Error("the rebuilt stripe differs from the original")
	}

	
	
	ok, err := codec.Verify(shards)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !ok {
		t.Error("the reconstructed stripe does not verify")
	}
}

func TestReconstructIsANoOpWhenNothingIsMissing(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 512
	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	shards := shardData(k, m, width)
	if err := codec.Encode(shards); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	
	
	before := cloneShards(shards)
	if err := codec.Reconstruct(shards); err != nil {
		t.Fatalf("Reconstruct with nothing missing: %v", err)
	}
	if !sameShards(shards, before) {
		t.Error("Reconstruct modified a complete stripe")
	}
}

func TestReconstructTreatsZeroLengthAsMissing(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 512
	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reference := shardData(k, m, width)
	if err := codec.Encode(reference); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	
	
	shards := cloneShards(reference)
	shards[1] = []byte{}

	if err := codec.Reconstruct(shards); err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if !sameShards(shards, reference) {
		t.Error("an empty buffer was not treated as a missing shard")
	}
}

func TestReconstructRefusesWhenNothingSurvives(t *testing.T) {
	t.Parallel()

	codec, err := New(Params{Data: 4, Parity: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	shards := make([][]byte, 6)
	for i := range shards {
		shards[i] = nil
	}

	
	
	if err := codec.Reconstruct(shards); !errors.Is(err, ErrNoShardsPresent) {
		t.Errorf("Reconstruct error = %v, want ErrNoShardsPresent", err)
	}
}

func TestVerifyDetectsAFlippedBit(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 1024
	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	shards := shardData(k, m, width)
	if err := codec.Encode(shards); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	ok, err := codec.Verify(shards)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !ok {
		t.Fatal("a freshly encoded stripe must verify")
	}

	
	
	shards[1][17] ^= 0x01
	ok, err = codec.Verify(shards)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if ok {
		t.Error("Verify accepted a stripe with a flipped data bit")
	}
}

func TestVerifyDetectsDamagedParity(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 1024
	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	shards := shardData(k, m, width)
	if err := codec.Encode(shards); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	shards[k+1][0] ^= 0xff
	ok, err := codec.Verify(shards)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if ok {
		t.Error("Verify accepted a stripe with a damaged parity shard")
	}
}

func TestEncodeRejectsTheWrongNumberOfShards(t *testing.T) {
	t.Parallel()

	codec, err := New(Params{Data: 4, Parity: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for _, n := range []int{0, 1, 5, 7} {
		shards := make([][]byte, n)
		for i := range shards {
			shards[i] = make([]byte, 64)
		}
		if err := codec.Encode(shards); !errors.Is(err, ErrShardCount) {
			t.Errorf("Encode with %d shards = %v, want ErrShardCount", n, err)
		}
		if _, err := codec.Verify(shards); !errors.Is(err, ErrShardCount) {
			t.Errorf("Verify with %d shards = %v, want ErrShardCount", n, err)
		}
		if err := codec.Reconstruct(shards); !errors.Is(err, ErrShardCount) {
			t.Errorf("Reconstruct with %d shards = %v, want ErrShardCount", n, err)
		}
	}
}

func TestEncodeRejectsUnequalShardSizes(t *testing.T) {
	t.Parallel()

	codec, err := New(Params{Data: 4, Parity: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	
	
	shards := shardData(4, 2, 128)
	shards[3] = make([]byte, 64)
	if err := codec.Encode(shards); !errors.Is(err, ErrShardSize) {
		t.Errorf("Encode error = %v, want ErrShardSize", err)
	}
}

func TestNewRejectsUnusableParameters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		params Params
	}{
		{name: "no data shards", params: Params{Data: 0, Parity: 2}},
		{name: "negative data", params: Params{Data: -1, Parity: 2}},
		{name: "negative parity", params: Params{Data: 4, Parity: -1}},
		{name: "too many shards", params: Params{Data: 200, Parity: 200}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := New(tc.params); !errors.Is(err, ErrInvalidParams) {
				t.Errorf("New(%+v) error = %v, want ErrInvalidParams", tc.params, err)
			}
		})
	}
}

func TestEncodeLeavesTheDataShardsAlone(t *testing.T) {
	t.Parallel()

	const k, m, width = 4, 2, 256
	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	data := shardData(k, 0, width)
	before := cloneShards(data)
	shards := Params{Data: k, Parity: m}.Alloc(width)
	copy(shards, data)

	if err := codec.Encode(shards); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	
	
	for i := range before {
		if !bytes.Equal(shards[i], before[i]) {
			t.Errorf("Encode modified data shard %d", i)
		}
	}
}

func TestAZeroLengthStripeIsRefusedWithAClearError(t *testing.T) {
	t.Parallel()

	
	
	
	
	codec, err := New(Params{Data: 4, Parity: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	shards := make([][]byte, 6)
	for i := range shards {
		shards[i] = make([]byte, 0)
	}

	if err := codec.Encode(shards); !errors.Is(err, ErrNoData) {
		t.Errorf("Encode of an empty stripe = %v, want ErrNoData", err)
	}
	if _, err := codec.Verify(shards); !errors.Is(err, ErrNoData) {
		t.Errorf("Verify of an empty stripe = %v, want ErrNoData", err)
	}
}

func TestAllocReturnsAFullyShapedZeroedSlice(t *testing.T) {
	t.Parallel()

	p := Params{Data: 4, Parity: 2}
	shards := p.Alloc(256)

	if len(shards) != 6 {
		t.Fatalf("Alloc returned %d shards, want 6", len(shards))
	}
	for i, shard := range shards {
		if len(shard) != 256 {
			t.Errorf("shard %d is %d bytes, want 256", i, len(shard))
		}
		
		
		if !bytes.Equal(shard, make([]byte, 256)) {
			t.Errorf("shard %d is not zeroed", i)
		}
	}

	
	
	codec, err := New(p)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := codec.Encode(shards); err != nil {
		t.Errorf("Encode of an Alloc slice: %v", err)
	}

	
	
	if got := p.Alloc(0); got != nil {
		t.Errorf("Alloc(0) = %v, want nil", got)
	}
	if got := (Params{Data: 0}).Alloc(256); got != nil {
		t.Errorf("Alloc with no data shards = %v, want nil", got)
	}
}

func TestParamsArithmetic(t *testing.T) {
	t.Parallel()

	p := Params{Data: 4, Parity: 2}
	if got, want := p.Shards(), 6; got != want {
		t.Errorf("Shards() = %d, want %d", got, want)
	}
	
	
	if got, want := p.Tolerates(), 2; got != want {
		t.Errorf("Tolerates() = %d, want %d", got, want)
	}

	noParity := Params{Data: 4, Parity: 0}
	if got := noParity.Tolerates(); got != 0 {
		t.Errorf("Tolerates() = %d, want 0 with no parity", got)
	}
}

func TestNoParityMeansNoRecovery(t *testing.T) {
	t.Parallel()

	
	
	
	codec, err := New(Params{Data: 4, Parity: 0})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	shards := shardData(4, 0, 128)
	if err := codec.Encode(shards); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	shards[2] = nil

	if err := codec.Reconstruct(shards); !errors.Is(err, ErrTooFewShards) {
		t.Errorf("Reconstruct error = %v, want ErrTooFewShards", err)
	}
}







func TestMaxShardsAgreesWithTheDomainModel(t *testing.T) {
	t.Parallel()

	if MaxShards != metadata.MaxShards {
		t.Errorf("ec.MaxShards = %d, metadata.MaxShards = %d; they must agree",
			MaxShards, metadata.MaxShards)
	}
}

func TestCodecIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	
	
	
	const k, m, width = 4, 2, 4096
	codec, err := New(Params{Data: k, Parity: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reference := shardData(k, m, width)
	if err := codec.Encode(reference); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	const workers = 8
	errs := make(chan error, workers)
	for w := range workers {
		go func(w int) {
			shards := cloneShards(reference)
			shards[w%2] = nil
			shards[(w+1)%(k+m)] = nil
			if err := codec.Reconstruct(shards); err != nil {
				errs <- err
				return
			}
			if !sameShards(shards, reference) {
				errs <- errors.New("the reconstructed stripe differs from the original")
				return
			}
			errs <- nil
		}(w)
	}
	for range workers {
		if err := <-errs; err != nil {
			t.Errorf("concurrent reconstruction: %v", err)
		}
	}
}

func TestDifferentWidthsRoundTrip(t *testing.T) {
	t.Parallel()

	
	
	
	const k, m = 4, 2
	for _, width := range []int{1, 2, 7, 31, 32, 33, 63, 64, 65, 1000, 4096} {
		codec, err := New(Params{Data: k, Parity: m})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		reference := shardData(k, m, width)
		if err := codec.Encode(reference); err != nil {
			t.Fatalf("Encode at width %d: %v", width, err)
		}

		shards := cloneShards(reference)
		shards[0] = nil
		shards[k+1] = nil
		if err := codec.Reconstruct(shards); err != nil {
			t.Fatalf("Reconstruct at width %d: %v", width, err)
		}
		if !sameShards(shards, reference) {
			t.Errorf("width %d did not round trip", width)
		}
	}
}
