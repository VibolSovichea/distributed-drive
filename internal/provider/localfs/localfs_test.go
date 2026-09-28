package localfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

var fixedNow = time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)

func newNode(t *testing.T) *Node {
	t.Helper()

	node, err := New(Config{
		Root:      t.TempDir(),
		Quota:     1_000,
		AccountID: "test-account",
		Now:       func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	return node
}




func payload(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func mustUpload(t *testing.T, node *Node, name string, data []byte) provider.RemoteObject {
	t.Helper()

	obj, err := node.Upload(t.Context(), bytes.NewReader(data), provider.ObjectMetadata{
		Name:        name,
		Description: "chunk of report.pdf",
		ContentType: "application/octet-stream",
		Size:        int64(len(data)),
	})
	if err != nil {
		t.Fatalf("Upload(%q) error = %v, want nil", name, err)
	}

	return obj
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	node := newNode(t)
	data := payload(64 << 10)

	created := mustUpload(t, node, "shard-0", data)

	if created.ID != "shard-0" {
		t.Errorf("ID = %q, want %q", created.ID, "shard-0")
	}
	if created.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", created.Size, len(data))
	}

	reader, err := node.Download(t.Context(), "shard-0")
	if err != nil {
		t.Fatalf("Download() error = %v, want nil", err)
	}
	defer func() { _ = reader.Close() }()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading the object: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("downloaded %d bytes, want the %d that were uploaded", len(got), len(data))
	}
}

func TestUploadRecordsTheDigestOfWhatItWrote(t *testing.T) {
	node := newNode(t)
	data := payload(4096)

	created := mustUpload(t, node, "shard-0", data)

	want := sha256.Sum256(data)
	if created.Checksum != hex.EncodeToString(want[:]) {
		t.Errorf("Checksum = %q, want the SHA-256 of the content", created.Checksum)
	}
}

func TestStatReportsTheRecordedMetadata(t *testing.T) {
	node := newNode(t)
	data := payload(1024)
	created := mustUpload(t, node, "shard-0", data)

	found, err := node.Stat(t.Context(), "shard-0")
	if err != nil {
		t.Fatalf("Stat() error = %v, want nil", err)
	}

	if found.ID != created.ID {
		t.Errorf("ID = %q, want %q", found.ID, created.ID)
	}
	if found.Size != created.Size {
		t.Errorf("Size = %d, want %d", found.Size, created.Size)
	}
	if found.Checksum != created.Checksum {
		t.Errorf("Checksum = %q, want %q", found.Checksum, created.Checksum)
	}
	if !found.CreatedAt.Equal(fixedNow) {
		t.Errorf("CreatedAt = %v, want %v", found.CreatedAt, fixedNow)
	}
}

func TestStatOfAMissingObjectIsNotFound(t *testing.T) {
	node := newNode(t)

	_, err := node.Stat(t.Context(), "absent")
	if !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("Stat() error = %v, want provider.ErrNotFound", err)
	}
}

func TestDownloadOfAMissingObjectIsNotFound(t *testing.T) {
	node := newNode(t)

	_, err := node.Download(t.Context(), "absent")
	if !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("Download() error = %v, want provider.ErrNotFound", err)
	}
}

func TestDeleteRemovesObjectAndMetadata(t *testing.T) {
	node := newNode(t)
	mustUpload(t, node, "shard-0", payload(512))

	if err := node.Delete(t.Context(), "shard-0"); err != nil {
		t.Fatalf("Delete() error = %v, want nil", err)
	}

	if _, err := node.Stat(t.Context(), "shard-0"); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("Stat() after delete error = %v, want provider.ErrNotFound", err)
	}

	
	
	if _, err := os.Stat(node.path("shard-0" + sidecarSuffix)); !errors.Is(err, os.ErrNotExist) {
		t.Error("the metadata file survived the delete")
	}
}

func TestDeleteOfAMissingObjectIsNotFound(t *testing.T) {
	
	
	node := newNode(t)

	err := node.Delete(t.Context(), "absent")
	if !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("Delete() error = %v, want provider.ErrNotFound", err)
	}
}

func TestUploadRejectsAShortSource(t *testing.T) {
	
	
	node := newNode(t)

	_, err := node.Upload(t.Context(), bytes.NewReader(payload(10)), provider.ObjectMetadata{
		Name: "truncated",
		Size: 100,
	})
	if err == nil {
		t.Fatal("Upload() = nil, want an error for a short source")
	}
	if !errors.Is(err, provider.ErrInvalid) {
		t.Errorf("error = %v, want it to wrap provider.ErrInvalid", err)
	}

	
	
	names, listErr := node.Objects()
	if listErr != nil {
		t.Fatalf("Objects() error = %v, want nil", listErr)
	}
	if len(names) != 0 {
		t.Errorf("Objects() = %v, want nothing after a failed upload", names)
	}
}

func TestUploadAcceptsAnUnknownSize(t *testing.T) {
	
	
	node := newNode(t)
	data := payload(300)

	created, err := node.Upload(t.Context(), bytes.NewReader(data), provider.ObjectMetadata{
		Name: "streamed",
		Size: -1,
	})
	if err != nil {
		t.Fatalf("Upload() error = %v, want nil", err)
	}
	if created.Size != 300 {
		t.Errorf("Size = %d, want 300", created.Size)
	}
}

func TestUploadGeneratesANameWhenNoneIsGiven(t *testing.T) {
	node := newNode(t)

	created, err := node.Upload(t.Context(), bytes.NewReader(payload(8)), provider.ObjectMetadata{})
	if err != nil {
		t.Fatalf("Upload() error = %v, want nil", err)
	}
	if created.ID == "" {
		t.Error("ID is empty, want a generated name")
	}
}



type erroringReader struct {
	data    []byte
	read    int
	failAt  int
	emitted bool
}

func (r *erroringReader) Read(p []byte) (int, error) {
	if r.read >= r.failAt && !r.emitted {
		r.emitted = true
		return 0, errors.New("connection reset by peer")
	}
	if r.read >= len(r.data) {
		return 0, io.EOF
	}

	n := copy(p, r.data[r.read:])
	r.read += n
	return n, nil
}

func TestAFailedUploadLeavesNothingBehind(t *testing.T) {
	node := newNode(t)
	data := payload(1000)

	_, err := node.Upload(t.Context(), &erroringReader{data: data, failAt: 400},
		provider.ObjectMetadata{Name: "broken", Size: int64(len(data))})
	if err == nil {
		t.Fatal("Upload() = nil, want the read error")
	}

	
	
	names, listErr := node.Objects()
	if listErr != nil {
		t.Fatalf("Objects() error = %v, want nil", listErr)
	}
	if len(names) != 0 {
		t.Errorf("Objects() = %v, want nothing after a failed upload", names)
	}

	entries, readErr := os.ReadDir(node.root)
	if readErr != nil {
		t.Fatalf("ReadDir() error = %v, want nil", readErr)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".upload-") {
			t.Errorf("temp file %q was left behind", entry.Name())
		}
	}
}

func TestUploadHonoursCancellation(t *testing.T) {
	node := newNode(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := node.Upload(ctx, bytes.NewReader(payload(1024)), provider.ObjectMetadata{
		Name: "cancelled",
		Size: 1024,
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Upload() error = %v, want context.Canceled", err)
	}
}

func TestOperationsHonourAnAlreadyCancelledContext(t *testing.T) {
	node := newNode(t)
	mustUpload(t, node, "shard-0", payload(16))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	operations := map[string]func() error{
		"Download": func() error { _, err := node.Download(ctx, "shard-0"); return err },
		"Delete":   func() error { return node.Delete(ctx, "shard-0") },
		"Stat":     func() error { _, err := node.Stat(ctx, "shard-0"); return err },
	}

	for name, op := range operations {
		if err := op(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s() error = %v, want context.Canceled", name, err)
		}
	}
}

func TestNamesThatCouldEscapeTheRootAreRejected(t *testing.T) {
	
	
	tests := []struct {
		name   string
		object string
	}{
		{name: "parent directory", object: "../escape"},
		{name: "nested path", object: "a/b"},
		{name: "absolute path", object: "/etc/passwd"},
		{name: "dot", object: "."},
		{name: "dot dot", object: ".."},
		{name: "reserved temp prefix", object: ".upload-x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newNode(t)

			_, err := node.Upload(t.Context(), bytes.NewReader(payload(8)),
				provider.ObjectMetadata{Name: tt.object, Size: 8})
			if !errors.Is(err, provider.ErrInvalid) {
				t.Errorf("Upload(%q) error = %v, want provider.ErrInvalid", tt.object, err)
			}

			if tt.object != "" {
				if _, err := node.Stat(t.Context(), tt.object); !errors.Is(err, provider.ErrInvalid) {
					t.Errorf("Stat(%q) error = %v, want provider.ErrInvalid", tt.object, err)
				}
			}
			if err := node.Delete(t.Context(), tt.object); !errors.Is(err, provider.ErrInvalid) {
				t.Errorf("Delete(%q) error = %v, want provider.ErrInvalid", tt.object, err)
			}
		})
	}
}

func TestAFileOutsideTheRootIsUntouched(t *testing.T) {
	
	
	parent := t.TempDir()
	outside := filepath.Join(parent, "secret")
	if err := os.WriteFile(outside, []byte("do not touch"), 0o600); err != nil {
		t.Fatalf("planting the file: %v", err)
	}

	node, err := New(Config{Root: filepath.Join(parent, "node")})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	if _, err := node.Upload(t.Context(), bytes.NewReader(payload(8)),
		provider.ObjectMetadata{Name: "../secret", Size: 8}); err == nil {
		t.Fatal("Upload() = nil, want the traversal to be refused")
	}

	got, readErr := os.ReadFile(outside)
	if readErr != nil {
		t.Fatalf("the file outside the root is unreadable: %v", readErr)
	}
	if string(got) != "do not touch" {
		t.Errorf("the file outside the root = %q, want it untouched", got)
	}
}

func TestQuotaAndIdentity(t *testing.T) {
	node := newNode(t)

	quota, err := node.Quota(t.Context())
	if err != nil {
		t.Fatalf("Quota() error = %v, want nil", err)
	}
	if quota.Total != 1000 {
		t.Errorf("Total = %d, want 1000", quota.Total)
	}

	identity, err := node.Identity(t.Context())
	if err != nil {
		t.Fatalf("Identity() error = %v, want nil", err)
	}
	if identity.AccountID != "test-account" {
		t.Errorf("AccountID = %q, want %q", identity.AccountID, "test-account")
	}
}

func TestAnUnknownQuotaIsReportedAsUnknown(t *testing.T) {
	
	
	
	node, err := New(Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	quota, err := node.Quota(t.Context())
	if err != nil {
		t.Fatalf("Quota() error = %v, want nil", err)
	}
	if quota.Known() {
		t.Error("Known() = true, want false when no quota was configured")
	}
	if quota.Available() != 0 {
		t.Errorf("Available() = %d, want 0 for an unknown quota", quota.Available())
	}
}

func TestNewRejectsAnEmptyRoot(t *testing.T) {
	if _, err := New(Config{Root: "  "}); err == nil {
		t.Error("New() with a blank root = nil, want an error")
	}
}

func TestNewCreatesTheRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a", "b", "node")

	if _, err := New(Config{Root: root}); err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("the root was not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("the root is not a directory")
	}
}

func TestObjectsExcludesMetadataAndInFlightUploads(t *testing.T) {
	node := newNode(t)
	mustUpload(t, node, "shard-0", payload(16))
	mustUpload(t, node, "shard-1", payload(16))

	
	
	if err := os.WriteFile(node.path(".upload-pending"+sidecarSuffix), []byte("{}"), 0o600); err != nil {
		t.Fatalf("planting a temp file: %v", err)
	}

	names, err := node.Objects()
	if err != nil {
		t.Fatalf("Objects() error = %v, want nil", err)
	}

	want := []string{"shard-0", "shard-1"}
	if len(names) != len(want) {
		t.Fatalf("Objects() = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("Objects()[%d] = %q, want %q", i, names[i], want[i])
		}
	}
}

func TestUploadDoesNotBufferTheWholeSource(t *testing.T) {
	
	
	
	
	node := newNode(t)

	const chunk = 256 << 10
	peak := 0
	var live int

	source := &countingReader{
		reader: bytes.NewReader(payload(chunk * 8)),
		onRead: func(n int) {
			live += n
			if live > peak {
				peak = live
			}
		},
		onRelease: func(n int) { live -= n },
	}

	if _, err := node.Upload(t.Context(), source, provider.ObjectMetadata{
		Name: "streamed",
		Size: int64(chunk * 8),
	}); err != nil {
		t.Fatalf("Upload() error = %v, want nil", err)
	}

	
	
	
	if peak > chunk {
		t.Errorf("peak in-flight bytes = %d, want at most the %d-byte buffer", peak, chunk)
	}
}

type countingReader struct {
	reader    io.Reader
	onRead    func(int)
	onRelease func(int)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	if n > 0 && c.onRead != nil {
		c.onRead(n)
		if c.onRelease != nil {
			
			c.onRelease(n)
		}
	}
	return n, err
}

func TestProviderInterfaceIsSatisfied(t *testing.T) {
	
	
	
	var node provider.StorageNode = newNode(t)
	if node == nil {
		t.Fatal("the node does not satisfy provider.StorageNode")
	}
}
