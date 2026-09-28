package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/chunker"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)




const smallChunk = metadata.MinChunkSize

func TestARoundTripReturnsTheOriginalBytes(t *testing.T) {
	t.Parallel()

	
	
	
	for _, size := range []int64{
		0,
		1,
		smallChunk - 1,
		smallChunk,
		smallChunk + 1,
		2 * smallChunk,
		2*smallChunk + 1,
		5*smallChunk + 123,
	} {
		h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
		body := pattern(size, byte(size))

		file := h.upload(t, "f.bin", body)
		if file.Size != int64(size) {
			t.Errorf("size %d: file.Size = %d, want %d", size, file.Size, size)
		}
		if file.Status != metadata.FileStatusCommitted {
			t.Errorf("size %d: status = %s, want committed", size, file.Status)
		}

		got, err := h.download(t, file.ID)
		if err != nil {
			t.Errorf("size %d: Download() = %v", size, err)
			continue
		}
		if !bytes.Equal(got, body) {
			t.Errorf("size %d: read back %d bytes, want the original %d", size, len(got), size)
		}
	}
}

func TestAnEmptyFileHasNoShardsAndStillDownloads(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	file := h.upload(t, "empty.bin", nil)

	
	
	
	if chunks := h.chunks(t, file.ID); len(chunks) != 0 {
		t.Errorf("an empty file has %d shard rows, want 0", len(chunks))
	}
	if file.Size != 0 {
		t.Errorf("file.Size = %d, want 0", file.Size)
	}
	
	
	if want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; file.ContentHash != want {
		t.Errorf("ContentHash = %q, want the digest of no bytes", file.ContentHash)
	}

	got, err := h.download(t, file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("read back %d bytes, want 0", len(got))
	}
}

func TestTheFileSpansAsManyStripesAsItsSizeRequires(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})

	tests := []struct {
		size    int64
		stripes int
	}{
		{0, 0},
		{1, 1},
		{smallChunk, 1},
		{2 * smallChunk, 1},
		{2*smallChunk + 1, 2},
		{4 * smallChunk, 2},
		{4*smallChunk + 1, 3},
	}

	for _, tc := range tests {
		file := h.upload(t, fmt.Sprintf("f-%d.bin", tc.size), pattern(tc.size, byte(tc.size)))

		
		
		if got, want := len(h.chunks(t, file.ID)), tc.stripes*4; got != want {
			t.Errorf("size %d: %d shard rows, want %d (%d stripes of 4)",
				tc.size, got, want, tc.stripes)
		}
	}
}

func TestEveryShardOfAStripeLandsOnItsOwnAccount(t *testing.T) {
	t.Parallel()

	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 6})
	file := h.upload(t, "f.bin", pattern(5*smallChunk+7, 3))

	perStripe := make(map[int]map[string]bool)
	for _, chunk := range h.chunks(t, file.ID) {
		nodes, ok := perStripe[chunk.StripeIndex]
		if !ok {
			nodes = make(map[string]bool)
			perStripe[chunk.StripeIndex] = nodes
		}
		if nodes[chunk.NodeID] {
			t.Errorf("stripe %d has two shards on node %s", chunk.StripeIndex, chunk.NodeID)
		}
		nodes[chunk.NodeID] = true
	}

	if len(perStripe) != 3 {
		t.Fatalf("got %d stripes, want 3", len(perStripe))
	}
	for stripe, nodes := range perStripe {
		if len(nodes) != 4 {
			t.Errorf("stripe %d used %d accounts, want 4", stripe, len(nodes))
		}
	}
}

func TestShardSizesAreTheTrueOnesNotThePaddedOnes(t *testing.T) {
	t.Parallel()

	
	const tail = 100
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	file := h.upload(t, "f.bin", pattern(4*smallChunk+tail, 9))

	byStripe := make(map[int]map[int]metadata.Chunk)
	for _, chunk := range h.chunks(t, file.ID) {
		if byStripe[chunk.StripeIndex] == nil {
			byStripe[chunk.StripeIndex] = make(map[int]metadata.Chunk)
		}
		byStripe[chunk.StripeIndex][chunk.Index] = chunk
	}

	
	for stripe := range 2 {
		for i := range 2 {
			chunk, ok := byStripe[stripe][i]
			if !ok {
				t.Fatalf("stripe %d has no data shard %d", stripe, i)
			}
			if chunk.Size != smallChunk {
				t.Errorf("stripe %d data %d: size %d, want %d", stripe, i, chunk.Size, smallChunk)
			}
		}
	}

	
	
	
	last := byStripe[2]
	if got := last[0].Size; got != tail {
		t.Errorf("last stripe data 0: size %d, want the file's tail of %d", got, tail)
	}
	if got := last[1].Size; got != 0 {
		t.Errorf("last stripe data 1: size %d, want 0 for a padding slot", got)
	}

	
	
	
	for i := 2; i < 4; i++ {
		for stripe := range byStripe {
			chunk, ok := byStripe[stripe][i]
			if !ok {
				t.Fatalf("stripe %d has no shard %d", stripe, i)
			}
			if chunk.Size != smallChunk {
				t.Errorf("stripe %d shard %d: size %d, want %d", stripe, i, chunk.Size, smallChunk)
			}
			if chunk.ChunkType != metadata.ChunkTypeParity {
				t.Errorf("stripe %d shard %d: type %s, want parity", stripe, i, chunk.ChunkType)
			}
		}
	}
}

func TestEveryStripeIsStoredAtFullWidth(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	file := h.upload(t, "f.bin", pattern(2*smallChunk+10, 4))

	for _, chunk := range h.chunks(t, file.ID) {
		node := h.nodes[chunk.NodeID]
		node.mu.Lock()
		body := node.objects[chunk.RemoteFileID]
		node.mu.Unlock()

		if int64(len(body)) != smallChunk {
			t.Errorf("stripe %d shard %d stored %d bytes, want the full %d",
				chunk.StripeIndex, chunk.Index, len(body), smallChunk)
		}
	}
}

func TestShardDigestsCoverTheStoredBytes(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	file := h.upload(t, "f.bin", pattern(2*smallChunk+10, 5))

	for _, chunk := range h.chunks(t, file.ID) {
		node := h.nodes[chunk.NodeID]
		node.mu.Lock()
		body := append([]byte(nil), node.objects[chunk.RemoteFileID]...)
		node.mu.Unlock()

		if want := chunker.Hash(body); chunk.Hash != want {
			t.Errorf("stripe %d shard %d: hash %s, want %s",
				chunk.StripeIndex, chunk.Index, chunk.Hash, want)
		}
	}
}

func TestAFileSurvivesLosingUpToMShardsPerStripe(t *testing.T) {
	t.Parallel()

	
	
	for lost := 1; lost <= 2; lost++ {
		h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
		body := pattern(3*smallChunk+50, byte(lost))
		file := h.upload(t, "f.bin", body)

		
		
		chunks := h.chunks(t, file.ID)
		stripes := make(map[int][]metadata.Chunk)
		for _, chunk := range chunks {
			stripes[chunk.StripeIndex] = append(stripes[chunk.StripeIndex], chunk)
		}
		for stripe := range stripes {
			group := stripes[stripe]
			for i := range lost {
				h.take(t, group[i])
			}
		}

		got, err := h.download(t, file.ID)
		if err != nil {
			t.Errorf("losing %d per stripe: Download() = %v", lost, err)
			continue
		}
		if !bytes.Equal(got, body) {
			t.Errorf("losing %d per stripe: read back %d bytes, want %d", lost, len(got), len(body))
		}
	}
}

func TestLosingMoreThanMShardsIsReported(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk+10, 6)
	file := h.upload(t, "f.bin", body)

	group := h.chunks(t, file.ID)
	for _, chunk := range group[:3] {
		h.take(t, chunk)
	}

	_, err := h.download(t, file.ID)
	if !errors.Is(err, ErrTooFewShards) {
		t.Errorf("Download() = %v, want ErrTooFewShards", err)
	}
	if errors.Is(err, ErrFileCorrupt) {
		t.Error("a lost shard is not corruption")
	}
}

func TestLosingAWholeStripeIsNotRecoverable(t *testing.T) {
	t.Parallel()

	
	
	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(3*smallChunk, 7)
	file := h.upload(t, "f.bin", body)

	lost := 0
	for _, chunk := range h.chunks(t, file.ID) {
		if chunk.StripeIndex == 1 {
			h.take(t, chunk)
			lost++
		}
	}
	if lost != 4 {
		t.Fatalf("only took %d shards, want all 4 of stripe 1", lost)
	}

	if _, err := h.download(t, file.ID); !errors.Is(err, ErrTooFewShards) {
		t.Errorf("Download() = %v, want ErrTooFewShards", err)
	}
}

func TestACorruptShardIsRebuiltRatherThanBelieved(t *testing.T) {
	t.Parallel()

	
	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(2*smallChunk+9, 8)
	file := h.upload(t, "f.bin", body)

	
	
	group := h.chunks(t, file.ID)
	h.scramble(t, group[0])
	h.scramble(t, group[3])

	got, err := h.download(t, file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("read back %d bytes, want %d", len(got), len(body))
	}
}

func TestCorruptionBeyondTheParityIsReportedNotSilentlyRebuilt(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk+3, 9)
	file := h.upload(t, "f.bin", body)

	group := h.chunks(t, file.ID)
	h.scramble(t, group[0])
	h.scramble(t, group[1])
	h.scramble(t, group[2])

	_, err := h.download(t, file.ID)
	if !errors.Is(err, ErrTooFewShards) && !errors.Is(err, ErrFileCorrupt) {
		t.Errorf("Download() = %v, want a reported failure", err)
	}
}

func TestAMissingRowIsTreatedAsALostShard(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk+21, 10)
	file := h.upload(t, "f.bin", body)

	group := h.chunks(t, file.ID)
	
	
	group[0].RemoteFileID = "obj-does-not-exist"
	if err := h.meta.ReplaceChunk(t.Context(), group[0]); err != nil {
		t.Fatalf("ReplaceChunk() = %v", err)
	}

	got, err := h.download(t, file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("read back %d bytes, want %d", len(got), len(body))
	}
}

func TestTwoShardsOnOneAccountAreRefused(t *testing.T) {
	t.Parallel()

	
	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk, 11)
	file := h.upload(t, "f.bin", body)

	group := h.chunks(t, file.ID)
	group[1].NodeID = group[0].NodeID
	group[1].RemoteFileID = group[0].RemoteFileID
	if err := h.meta.ReplaceChunk(t.Context(), group[1]); err != nil {
		t.Fatalf("ReplaceChunk() = %v", err)
	}

	_, err := h.download(t, file.ID)
	if !errors.Is(err, metadata.ErrInvalid) {
		t.Errorf("Download() = %v, want metadata.ErrInvalid", err)
	}
}

func TestAWrongWholeFileDigestIsCaught(t *testing.T) {
	t.Parallel()

	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk+5, 12)
	file := h.upload(t, "f.bin", body)

	
	file.ContentHash = chunker.Hash([]byte("not this file"))
	if err := h.meta.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() = %v", err)
	}

	_, err := h.download(t, file.ID)
	if !errors.Is(err, ErrFileCorrupt) {
		t.Errorf("Download() = %v, want ErrFileCorrupt", err)
	}
}

func TestAnUnfinishedFileCannotBeDownloaded(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	file := h.upload(t, "f.bin", pattern(smallChunk, 13))

	file.Status = metadata.FileStatusUploading
	if err := h.meta.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() = %v", err)
	}

	if _, err := h.download(t, file.ID); !errors.Is(err, ErrFileNotReadable) {
		t.Errorf("Download() = %v, want ErrFileNotReadable", err)
	}
}

func TestADegradedFileBecomesCommittedOnceItReadsBackWhole(t *testing.T) {
	t.Parallel()

	
	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk+7, 14)
	file := h.upload(t, "f.bin", body)

	file.Status = metadata.FileStatusDegraded
	if err := h.meta.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() = %v", err)
	}

	if _, err := h.download(t, file.ID); err != nil {
		t.Fatalf("Download() = %v", err)
	}

	got, err := h.meta.GetFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFile() = %v", err)
	}
	if got.Status != metadata.FileStatusCommitted {
		t.Errorf("status = %s, want committed after a verified read", got.Status)
	}
}

func TestAnAbandonedDownloadChangesNothing(t *testing.T) {
	t.Parallel()

	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(3*smallChunk, 15)
	file := h.upload(t, "f.bin", body)

	file.Status = metadata.FileStatusDegraded
	if err := h.meta.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() = %v", err)
	}

	reader, err := h.store.Download(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if _, err := reader.Read(make([]byte, 16)); err != nil {
		t.Fatalf("Read() = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	got, err := h.meta.GetFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFile() = %v", err)
	}
	if got.Status != metadata.FileStatusDegraded {
		t.Errorf("status = %s, want it left degraded", got.Status)
	}
}

func TestATruncatedUploadIsCaughtByCounting(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})

	_, err := h.store.Upload(t.Context(), UploadInput{
		PoolID: h.pool.ID,
		Name:   "f.bin",
		Source: bytes.NewReader(pattern(smallChunk, 16)),
		Size:   3 * smallChunk,
	})
	if !errors.Is(err, ErrUploadTruncated) {
		t.Errorf("Upload() = %v, want ErrUploadTruncated", err)
	}
}

func TestAPoolTooSmallForAStripeRefusesBeforeWritingAnything(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 3, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk, 17)

	_, err := h.store.Upload(t.Context(), UploadInput{
		PoolID: h.pool.ID,
		Name:   "f.bin",
		Source: bytes.NewReader(body),
	})
	if !errors.Is(err, ErrNoUsableNodes) {
		t.Errorf("Upload() = %v, want ErrNoUsableNodes", err)
	}

	
	for id, node := range h.nodes {
		if live := node.live(); live != 0 {
			t.Errorf("node %s holds %d objects, want 0", id, live)
		}
	}
}

func TestAFailedWriteRollsBackTheShardsThatLanded(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4, concurr: 1})

	
	
	h.nodes["node-2"].failWrite = true

	_, err := h.store.Upload(t.Context(), UploadInput{
		PoolID: h.pool.ID,
		Name:   "f.bin",
		Source: bytes.NewReader(pattern(smallChunk, 18)),
	})
	if err == nil {
		t.Fatal("Upload() = nil, want a write failure")
	}

	total := 0
	for id, node := range h.nodes {
		live := node.live()
		total += live
		if live != 0 {
			t.Errorf("node %s still holds %d objects after a failed upload", id, live)
		}
	}
	if total != 0 {
		t.Errorf("%d orphaned objects left behind", total)
	}
}

func TestAnUncommittedFileRowIsLeftBehindOnPurpose(t *testing.T) {
	t.Parallel()

	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	h.nodes["node-2"].failWrite = true

	_, err := h.store.Upload(t.Context(), UploadInput{
		PoolID: h.pool.ID,
		Name:   "f.bin",
		Source: bytes.NewReader(pattern(smallChunk, 19)),
	})
	if err == nil {
		t.Fatal("Upload() = nil, want a failure")
	}

	files, err := h.meta.ListPoolFiles(t.Context(), h.pool.ID)
	if err != nil {
		t.Fatalf("ListPoolFiles() = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("got %d file rows, want 1 recording the attempt", len(files))
	}
	if files[0].Status != metadata.FileStatusUploading {
		t.Errorf("status = %s, want uploading", files[0].Status)
	}
}

func TestTheContentHashIsOfTheOriginalBytes(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(3*smallChunk+11, 20)

	file := h.upload(t, "f.bin", body)
	if want := chunker.Hash(body); file.ContentHash != want {
		t.Errorf("ContentHash = %s, want %s", file.ContentHash, want)
	}
}

func TestAPoolWithNoParityRefusesToDownloadOnceAShardIsLost(t *testing.T) {
	t.Parallel()

	
	
	h := newHarness(t, harnessOpts{k: 2, m: 0, chunk: smallChunk, nodes: 3})
	body := pattern(smallChunk, 21)
	file := h.upload(t, "f.bin", body)

	h.take(t, h.chunks(t, file.ID)[0])

	if _, err := h.download(t, file.ID); !errors.Is(err, ErrTooFewShards) {
		t.Errorf("Download() = %v, want ErrTooFewShards", err)
	}
}

func TestEachStripeIsRebuiltOnlyOncePerDownload(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(5*smallChunk+13, 22)
	file := h.upload(t, "f.bin", body)

	reader, err := h.store.Download(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	defer reader.Close()

	
	var got []byte
	buf := make([]byte, 1)
	for {
		n, err := reader.Read(buf)
		got = append(got, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read() = %v", err)
		}
	}

	if !bytes.Equal(got, body) {
		t.Errorf("read back %d bytes, want %d", len(got), len(body))
	}
}

func TestAMultiStripeFileSurvivesLossInEveryStripeAtOnce(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 3, m: 2, chunk: smallChunk, nodes: 6})
	body := pattern(6*smallChunk+77, 23)
	file := h.upload(t, "f.bin", body)

	stripes := make(map[int][]metadata.Chunk)
	for _, chunk := range h.chunks(t, file.ID) {
		stripes[chunk.StripeIndex] = append(stripes[chunk.StripeIndex], chunk)
	}
	
	
	if len(stripes) != 3 {
		t.Fatalf("got %d stripes, want 3", len(stripes))
	}
	
	h.take(t, stripes[0][0])
	h.take(t, stripes[1][0])
	h.take(t, stripes[1][4])

	got, err := h.download(t, file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("read back %d bytes, want %d", len(got), len(body))
	}
}

func TestAProviderFailureDoesNotLookLikeCorruption(t *testing.T) {
	t.Parallel()

	
	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(smallChunk+6, 24)
	file := h.upload(t, "f.bin", body)

	group := h.chunks(t, file.ID)
	node := h.nodes[group[0].NodeID]
	node.mu.Lock()
	node.failRead[group[0].RemoteFileID] = provider.Unavailablef(fmt.Errorf("injected"))
	node.mu.Unlock()

	got, err := h.download(t, file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Error("a lost shard was not rebuilt")
	}
}

func TestDetectLostFindsMissingObjects(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(3*smallChunk, 100)
	file := h.upload(t, "f.bin", body)

	
	group := h.chunks(t, file.ID)
	h.take(t, group[0])

	lost, err := h.store.DetectLost(t.Context(), h.pool.ID)
	if err != nil {
		t.Fatalf("DetectLost() = %v", err)
	}
	if len(lost) != 1 {
		t.Fatalf("got %d lost shards, want 1", len(lost))
	}
	if lost[0].FileID != file.ID || lost[0].StripeIndex != group[0].StripeIndex || lost[0].Index != group[0].Index {
		t.Errorf("lost shard = %+v, want file=%s stripe=%d index=%d", lost[0], file.ID, group[0].StripeIndex, group[0].Index)
	}
}

func TestDetectLostIgnoresUploadingFiles(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	file := h.upload(t, "f.bin", pattern(smallChunk, 101))

	
	file.Status = metadata.FileStatusUploading
	if err := h.meta.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() = %v", err)
	}

	lost, err := h.store.DetectLost(t.Context(), h.pool.ID)
	if err != nil {
		t.Fatalf("DetectLost() = %v", err)
	}
	if len(lost) != 0 {
		t.Errorf("got %d lost shards for uploading file, want 0", len(lost))
	}
}

func TestRepairFileRestoresLostShard(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 5})
	body := pattern(3*smallChunk, 102)
	file := h.upload(t, "f.bin", body)

	
	group := h.chunks(t, file.ID)
	h.take(t, group[0])

	
	lost, _ := h.store.DetectLost(t.Context(), h.pool.ID)
	if len(lost) != 1 {
		t.Fatalf("DetectLost() = %d, want 1", len(lost))
	}

	
	result, err := h.store.RepairFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("RepairFile() = %v", err)
	}
	if result.Restored != 1 {
		t.Errorf("Restored = %d, want 1", result.Restored)
	}
	if !result.Healthy {
		t.Error("file should be healthy after repair")
	}
	if file.Status != metadata.FileStatusCommitted {
		t.Errorf("status = %s, want committed", file.Status)
	}

	
	got, err := h.download(t, file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("repaired file content mismatch")
	}

	
	lost, _ = h.store.DetectLost(t.Context(), h.pool.ID)
	if len(lost) != 0 {
		t.Errorf("DetectLost() = %d, want 0 after repair", len(lost))
	}
}

func TestRepairFileHandlesCorruptShard(t *testing.T) {
	t.Parallel()

	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 5})
	body := pattern(2*smallChunk+10, 103)
	file := h.upload(t, "f.bin", body)

	
	group := h.chunks(t, file.ID)
	h.scramble(t, group[0])

	
	result, err := h.store.RepairFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("RepairFile() = %v", err)
	}
	if result.Restored != 1 {
		t.Errorf("Restored = %d, want 1", result.Restored)
	}
	if !result.Healthy {
		t.Error("file should be healthy after repair")
	}

	got, err := h.download(t, file.ID)
	if err != nil {
		t.Fatalf("Download() = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("repaired file content mismatch")
	}
}

func TestRepairFileFailsWhenTooManyLost(t *testing.T) {
	t.Parallel()

	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 5})
	body := pattern(smallChunk, 104)
	file := h.upload(t, "f.bin", body)

	group := h.chunks(t, file.ID)
	for _, chunk := range group[:3] {
		h.take(t, chunk)
	}

	result, err := h.store.RepairFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("RepairFile() = %v", err)
	}
	if result.Healthy {
		t.Error("file should not be healthy")
	}
	if len(result.Unrepaired) != 1 {
		t.Errorf("Unrepaired = %v, want [0]", result.Unrepaired)
	}

	
	file, err = h.meta.GetFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFile() = %v", err)
	}
	if file.Status != metadata.FileStatusDegraded {
		t.Errorf("status = %s, want degraded", file.Status)
	}
}

func TestScrubFileDetectsCorruption(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(2*smallChunk+10, 105)
	file := h.upload(t, "f.bin", body)

	
	group := h.chunks(t, file.ID)
	h.scramble(t, group[0])

	result, err := h.store.ScrubFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("ScrubFile() = %v", err)
	}
	if result.Corrupt != 1 {
		t.Errorf("Corrupt = %d, want 1", result.Corrupt)
	}
	if result.Healthy {
		t.Error("file should not be healthy")
	}
}

func TestScrubFileHealthyFile(t *testing.T) {
	t.Parallel()

	
	
	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})
	body := pattern(3*smallChunk, 106)
	file := h.upload(t, "f.bin", body)

	result, err := h.store.ScrubFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("ScrubFile() = %v", err)
	}
	if result.Corrupt != 0 || result.Missing != 0 {
		t.Errorf("Corrupt = %d, Missing = %d, want 0", result.Corrupt, result.Missing)
	}
	if !result.Healthy {
		t.Error("file should be healthy")
	}
	if result.Verified != 8 { 
		t.Errorf("Verified = %d, want 8", result.Verified)
	}
}

func TestRepairPoolRepairsMultipleFiles(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 5})
	file1 := h.upload(t, "f1.bin", pattern(2*smallChunk, 107))
	file2 := h.upload(t, "f2.bin", pattern(3*smallChunk, 108))

	
	group1 := h.chunks(t, file1.ID)
	h.take(t, group1[0])
	group2 := h.chunks(t, file2.ID)
	h.take(t, group2[1])

	results, err := h.store.RepairPool(t.Context(), h.pool.ID)
	if err != nil {
		t.Fatalf("RepairPool() = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	for _, r := range results {
		if r.Restored != 1 {
			t.Errorf("Restored = %d, want 1", r.Restored)
		}
		if !r.Healthy {
			t.Errorf("file %s should be healthy", r.FileID)
		}
	}
}

func TestSweepAbandonedRemovesUploadingFile(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})

	
	file := h.upload(t, "abandoned.bin", pattern(smallChunk, 109))

	
	
	oldTime := h.now.Add(-2 * time.Hour)
	file.Status = metadata.FileStatusUploading
	file.UpdatedAt = oldTime
	if err := h.meta.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() = %v", err)
	}

	
	swept, err := h.store.SweepAbandoned(t.Context(), time.Hour)
	if err != nil {
		t.Fatalf("SweepAbandoned() = %v", err)
	}
	if swept != 1 {
		t.Errorf("swept = %d, want 1", swept)
	}

	
	files, _ := h.meta.ListPoolFiles(t.Context(), h.pool.ID)
	for _, f := range files {
		if f.ID == file.ID {
			t.Error("abandoned file should have been swept")
		}
	}
}

func TestSweepAbandonedSkipsRecentUploads(t *testing.T) {
	t.Parallel()

	h := newHarness(t, harnessOpts{k: 2, m: 2, chunk: smallChunk, nodes: 4})

	file := h.upload(t, "recent.bin", pattern(smallChunk, 110))
	file.Status = metadata.FileStatusUploading
	file.UpdatedAt = h.now.Add(-10 * time.Minute)
	if err := h.meta.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() = %v", err)
	}

	swept, err := h.store.SweepAbandoned(t.Context(), time.Hour)
	if err != nil {
		t.Fatalf("SweepAbandoned() = %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0", swept)
	}

	
	files, _ := h.meta.ListPoolFiles(t.Context(), h.pool.ID)
	found := false
	for _, f := range files {
		if f.ID == file.ID {
			found = true
		}
	}
	if !found {
		t.Error("recent upload should not have been swept")
	}
}
