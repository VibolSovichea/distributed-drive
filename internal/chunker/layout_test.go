package chunker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

const stripeSize int64 = 64 << 10

func TestPlanAgreesWithTheSplitter(t *testing.T) {
	t.Parallel()

	totals := []int64{
		0, 1, stripeSize - 1, stripeSize, stripeSize + 1,
		2 * stripeSize, 2*stripeSize + 1, 2*stripeSize - 1,
		4 * stripeSize, 4*stripeSize + 7, 4*stripeSize - 1,
		12*stripeSize + 3, 100 * stripeSize,
	}
	counts := []int{1, 2, 3, 4, 6, 8}

	for _, total := range totals {
		for _, k := range counts {
			t.Run(fmt.Sprintf("total=%d/k=%d", total, k), func(t *testing.T) {
				t.Parallel()

				l, err := Plan(total, stripeSize, k)
				if err != nil {
					t.Fatalf("Plan: %v", err)
				}

				if got, want := l.DataShardCount(), int((total+stripeSize-1)/stripeSize); total > 0 && got != want {
					t.Errorf("DataShardCount() = %d, want %d", got, want)
				}
				if total == 0 && l.DataShardCount() != 0 {
					t.Errorf("DataShardCount() = %d, want 0 for an empty file", l.DataShardCount())
				}

				payload := make([]byte, total)
				for i := range payload {
					payload[i] = byte(i*31 + i/97)
				}

				s, err := l.Splitter()
				if err != nil {
					t.Fatalf("Splitter: %v", err)
				}

				var assembled bytes.Buffer
				shard := 0
				r := bytes.NewReader(payload)
				for {
					chunk, err := s.Next(r)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("Next: %v", err)
					}

					stripe, index := l.StripeOf(shard)
					want := l.DataShardSize(stripe, index)
					if chunk.Size != want {
						t.Errorf("shard %d (stripe %d, index %d): the splitter read %d bytes, "+
							"the layout says %d", shard, stripe, index, chunk.Size, want)
					}

					assembled.Write(chunk.Data)
					shard++
				}

				if shard != l.DataShardCount() {
					t.Errorf("the splitter produced %d shards, the layout claims %d",
						shard, l.DataShardCount())
				}
				if !bytes.Equal(assembled.Bytes(), payload) {
					t.Error("the reassembled bytes differ from the input")
				}

				var sum int64
				for i := 0; i < l.DataShardCount(); i++ {
					stripe, index := l.StripeOf(i)
					sum += l.DataShardSize(stripe, index)
				}
				if sum != total {
					t.Errorf("the shard sizes sum to %d, want %d", sum, total)
				}
			})
		}
	}
}

func TestPlanStripeBoundaries(t *testing.T) {
	t.Parallel()

	const k = 4
	per := k * stripeSize

	tests := []struct {
		name       string
		total      int64
		wantStripe int
		wantInLast int
	}{
		{name: "empty", total: 0, wantStripe: 0, wantInLast: 0},
		{name: "one byte", total: 1, wantStripe: 1, wantInLast: 1},
		{name: "one shard", total: stripeSize, wantStripe: 1, wantInLast: 1},
		{name: "one byte over a shard", total: stripeSize + 1, wantStripe: 1, wantInLast: 2},
		{name: "a full stripe minus one", total: per - 1, wantStripe: 1, wantInLast: k},
		{name: "exactly one stripe", total: per, wantStripe: 1, wantInLast: k},
		{name: "one byte over a stripe", total: per + 1, wantStripe: 2, wantInLast: 1},
		{name: "two stripes", total: 2 * per, wantStripe: 2, wantInLast: k},
		{name: "two stripes and a bit", total: 2*per + 1, wantStripe: 3, wantInLast: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l, err := Plan(tt.total, stripeSize, k)
			if err != nil {
				t.Fatalf("Plan: %v", err)
			}
			if l.Stripes != tt.wantStripe {
				t.Errorf("Stripes = %d, want %d", l.Stripes, tt.wantStripe)
			}
			if l.DataInLast != tt.wantInLast {
				t.Errorf("DataInLast = %d, want %d", l.DataInLast, tt.wantInLast)
			}
		})
	}
}

func TestPlanRejectsAnUnusableStripe(t *testing.T) {
	t.Parallel()

	for _, k := range []int{0, -1} {
		if _, err := Plan(1024, stripeSize, k); err == nil {
			t.Errorf("Plan with k=%d was accepted", k)
		}
	}
	if _, err := Plan(-1, stripeSize, 4); err == nil {
		t.Error("Plan with a negative size was accepted")
	}
	if _, err := Plan(1024, MinSize-1, 4); err == nil {
		t.Error("Plan with a chunk size below the minimum was accepted")
	}
}

func TestDataShardsInStripeIsFullUntilTheLast(t *testing.T) {
	t.Parallel()

	const k = 4
	per := int64(k) * stripeSize

	l, err := Plan(2*per+1, stripeSize, k)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if l.Stripes != 3 {
		t.Fatalf("Stripes = %d, want 3", l.Stripes)
	}

	for stripe := 0; stripe < 2; stripe++ {
		if got := l.DataShardsInStripe(stripe); got != k {
			t.Errorf("DataShardsInStripe(%d) = %d, want %d", stripe, got, k)
		}
	}

	if got := l.DataShardsInStripe(2); got != 1 {
		t.Errorf("DataShardsInStripe(2) = %d, want 1", got)
	}

	if got := l.DataShardsInStripe(3); got != 0 {
		t.Errorf("DataShardsInStripe(3) = %d, want 0", got)
	}
	if got := l.DataShardsInStripe(-1); got != 0 {
		t.Errorf("DataShardsInStripe(-1) = %d, want 0", got)
	}
}

func TestOnlyTheFinalDataShardIsShort(t *testing.T) {
	t.Parallel()

	const k = 4
	per := int64(k) * stripeSize

	l, err := Plan(2*per+3, stripeSize, k)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	short := 0
	for i := 0; i < l.DataShardCount(); i++ {
		stripe, index := l.StripeOf(i)
		if l.DataShardSize(stripe, index) < l.ChunkSize {
			short++
			if want := int64(3); l.DataShardSize(stripe, index) != want {
				t.Errorf("the short shard is %d bytes, want %d",
					l.DataShardSize(stripe, index), want)
			}
			if i != l.DataShardCount()-1 {
				t.Errorf("shard %d is short but is not the last", i)
			}
		}
	}
	if short != 1 {
		t.Errorf("%d shards are short, want exactly 1", short)
	}
}

func TestAnEvenlyDividingFileHasNoShortShard(t *testing.T) {
	t.Parallel()

	const k = 4
	per := int64(k) * stripeSize

	l, err := Plan(2*per, stripeSize, k)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := l.LastDataShardSize(), stripeSize; got != want {
		t.Errorf("LastDataShardSize() = %d, want a full %d", got, want)
	}
	for i := 0; i < l.DataShardCount(); i++ {
		stripe, index := l.StripeOf(i)
		if got := l.DataShardSize(stripe, index); got != stripeSize {
			t.Errorf("shard %d is %d bytes, want %d", i, got, stripeSize)
		}
	}
}

func TestParityShardsAreAlwaysFullWidth(t *testing.T) {
	t.Parallel()

	const k = 4
	per := int64(k) * stripeSize

	for _, total := range []int64{0, 1, stripeSize, per, per + 1, 3*per + 7} {
		l, err := Plan(total, stripeSize, k)
		if err != nil {
			t.Fatalf("Plan(%d): %v", total, err)
		}
		if got := l.ParityShardSize(); got != stripeSize {
			t.Errorf("Plan(%d).ParityShardSize() = %d, want %d", total, got, stripeSize)
		}
	}
}

func TestStripeOfWalksStripesInOrder(t *testing.T) {
	t.Parallel()

	const k = 4

	total := int64(3*k*stripeSize) + stripeSize + 5
	l, err := Plan(total, stripeSize, k)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if l.Stripes != 4 {
		t.Fatalf("Stripes = %d, want 4", l.Stripes)
	}

	want := [][2]int{
		{0, 0}, {0, 1}, {0, 2}, {0, 3},
		{1, 0}, {1, 1}, {1, 2}, {1, 3},
		{2, 0}, {2, 1}, {2, 2}, {2, 3},
		{3, 0}, {3, 1},
	}
	if got := l.DataShardCount(); got != len(want) {
		t.Fatalf("DataShardCount() = %d, want %d", got, len(want))
	}
	for shard, w := range want {
		stripe, index := l.StripeOf(shard)
		if stripe != w[0] || index != w[1] {
			t.Errorf("StripeOf(%d) = (%d, %d), want (%d, %d)", shard, stripe, index, w[0], w[1])
		}
	}
}

func TestEmptyFileHasNoShardsAndNoStripes(t *testing.T) {
	t.Parallel()

	l, err := Plan(0, stripeSize, 4)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if l.Stripes != 0 {
		t.Errorf("Stripes = %d, want 0", l.Stripes)
	}
	if got := l.DataShardCount(); got != 0 {
		t.Errorf("DataShardCount() = %d, want 0", got)
	}
	if got := l.LastDataShardSize(); got != 0 {
		t.Errorf("LastDataShardSize() = %d, want 0", got)
	}
	if got := l.DataShardsInStripe(0); got != 0 {
		t.Errorf("DataShardsInStripe(0) = %d, want 0", got)
	}

	s, err := l.Splitter()
	if err != nil {
		t.Fatalf("Splitter: %v", err)
	}
	if _, err := s.Next(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Errorf("Next on an empty source = %v, want io.EOF", err)
	}
}

func TestDataShardSizeIgnoresAnIndexPastTheDataShards(t *testing.T) {
	t.Parallel()

	l, err := Plan(4*stripeSize, stripeSize, 2)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	if got := l.DataShardSize(0, 2); got != 0 {
		t.Errorf("DataShardSize(0, 2) = %d, want 0 for a parity slot", got)
	}
	if got := l.DataShardSize(0, -1); got != 0 {
		t.Errorf("DataShardSize(0, -1) = %d, want 0", got)
	}
}

func TestLayoutOfOneChunkIsASingleStripe(t *testing.T) {
	t.Parallel()

	l, err := Plan(stripeSize+5, stripeSize, 1)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if l.Stripes != 2 {
		t.Errorf("Stripes = %d, want 2", l.Stripes)
	}
	if l.DataInLast != 1 {
		t.Errorf("DataInLast = %d, want 1", l.DataInLast)
	}
	if got := l.DataShardSize(1, 0); got != 5 {
		t.Errorf("DataShardSize(1, 0) = %d, want 5", got)
	}
	if got := l.DataShardSize(0, 0); got != stripeSize {
		t.Errorf("DataShardSize(0, 0) = %d, want %d", got, stripeSize)
	}
}
