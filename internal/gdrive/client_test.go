package gdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"golang.org/x/oauth2"

	"github.com/VibolSovichea/distributed-drive/internal/provider"
)







type fakeDrive struct {
	mu sync.Mutex

	
	files map[string][]byte

	
	sessions map[string]*fakeSession

	
	quota    quotaResponse
	identity identityResponse

	
	nextID int

	
	
	requests map[string]int

	
	failNextChunks int

	
	
	
	nextSessionLoseAt   int
	nextSessionExpireAt int
}

type quotaResponse struct {
	Limit string `json:"limit"`
	Usage string `json:"usage"`
}

type identityResponse struct {
	User struct {
		DisplayName  string `json:"displayName"`
		EmailAddress string `json:"emailAddress"`
		PermissionID string `json:"permissionId"`
	} `json:"user"`
}

type fakeSession struct {
	
	received []byte
	
	declared string
	
	
	startTotal string
	
	chunks int
	
	
	
	loseAt int
	
	
	expireAt int
	
	finished bool
}

func newFakeDrive() *fakeDrive {
	return &fakeDrive{
		files:               map[string][]byte{},
		sessions:            map[string]*fakeSession{},
		requests:            map[string]int{},
		quota:               quotaResponse{Limit: "1099511627776", Usage: "1073741824"},
		nextSessionLoseAt:   -1,
		nextSessionExpireAt: -1,
	}
}

func (f *fakeDrive) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = map[string]int{}
	f.failNextChunks = 0
}


func (f *fakeDrive) start(t *testing.T) *Client {
	t.Helper()

	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	client, err := New(Options{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: "test-token",
			TokenType:   "Bearer",
		}),
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	client.base = srv.URL
	client.upload = srv.URL

	return client
}

func (f *fakeDrive) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/files", f.handleFiles)
	mux.HandleFunc("/about", f.handleAbout)
	mux.HandleFunc("/upload/files", f.handleFiles)
	mux.HandleFunc("/upload/sessions/", f.handleSession)
	mux.HandleFunc("/files/", f.handleFileByID)
	mux.HandleFunc("/sessions/", f.handleSession)
	return mux
}


func (f *fakeDrive) handleFiles(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests["files"]++
	failNext := f.failNextChunks
	f.failNextChunks = 0
	f.mu.Unlock()

	if failNext > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if r.Method == http.MethodPost && r.URL.Query().Get("uploadType") == "resumable" {
		f.startSession(w, r)
		return
	}

	w.WriteHeader(http.StatusBadRequest)
}


func (f *fakeDrive) startSession(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	
	
	
	declared := r.Header.Get("X-Upload-Content-Length")
	if declared != "" {
		if n, err := strconv.Atoi(declared); err != nil || n < 0 {
			http.Error(w, "X-Upload-Content-Length must be a byte count",
				http.StatusBadRequest)
			return
		}
	}

	
	
	var meta map[string]any
	if err := json.Unmarshal(body, &meta); err == nil {
		if _, ok := meta["size"]; ok {
			http.Error(w, "size is not a writable file field", http.StatusBadRequest)
			return
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	path := fmt.Sprintf("/sessions/%d", len(f.sessions)+1)
	f.sessions[path] = &fakeSession{
		startTotal: declared,
		loseAt:     f.nextSessionLoseAt,
		expireAt:   f.nextSessionExpireAt,
	}
	f.nextSessionLoseAt, f.nextSessionExpireAt = -1, -1

	w.Header().Set("Location", path)
	w.WriteHeader(http.StatusOK)
}


func (f *fakeDrive) handleSession(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests["session"]++
	failNext := f.failNextChunks
	f.failNextChunks = 0
	f.mu.Unlock()

	if failNext > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	path := r.URL.Path

	f.mu.Lock()
	session, ok := f.sessions[path]
	f.mu.Unlock()

	if !ok || session.finished {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	contentRange := r.Header.Get("Content-Range")
	chunk, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	
	if contentRange == "bytes */*" {
		f.finishUpload(w, path)
		return
	}

	
	
	
	if strings.HasPrefix(contentRange, "bytes */") {
		f.reportStatus(w, path)
		return
	}

	offset, last, total, err := parseContentRange(contentRange)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if offset != int64(len(session.received)) {
		
		
		
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	session.received = append(session.received, chunk...)
	session.declared = total
	chunkIndex := session.chunks
	session.chunks++

	complete := last+1 == int64(len(session.received)) && total != "" && strconv.FormatInt(last+1, 10) == total
	if complete {
		f.finishUpload(w, path)
		return
	}

	
	
	
	f.mu.Lock()
	lose := session.loseAt == chunkIndex
	if session.expireAt == chunkIndex {
		
		
		
		delete(f.sessions, path)
		f.mu.Unlock()
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(statusResumeIncomplete)
		return
	}
	f.mu.Unlock()
	if lose {
		
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(statusResumeIncomplete)
		return
	}

	
	w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", last))
	w.WriteHeader(statusResumeIncomplete)
}


func (f *fakeDrive) reportStatus(w http.ResponseWriter, path string) {
	f.mu.Lock()
	session, ok := f.sessions[path]
	f.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	f.mu.Lock()
	finished := session.finished
	f.mu.Unlock()

	if finished {
		
		
		
		f.mu.Lock()
		defer f.mu.Unlock()
		for id, data := range f.files {
			if bytes.Equal(data, session.received) {
				writeFileJSON(w, id, len(data))
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	held := int64(len(session.received))
	if held == 0 {
		
		w.WriteHeader(statusResumeIncomplete)
		return
	}

	w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", held-1))
	w.WriteHeader(statusResumeIncomplete)
}


func (f *fakeDrive) finishUpload(w http.ResponseWriter, path string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	session, ok := f.sessions[path]
	if !ok || session.finished {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	
	
	
	declared := session.declared
	if declared == "" || declared == "*" {
		declared = session.startTotal
	}
	if declared != "" &&
		strconv.Itoa(len(session.received)) != declared {
		
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	f.nextID++
	id := fmt.Sprintf("file-%d", f.nextID)
	f.files[id] = session.received
	session.finished = true

	writeFileJSON(w, id, len(session.received))
}


func writeFileJSON(w http.ResponseWriter, id string, size int) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"id":%q,"name":"shard","size":%q,"md5Checksum":"abc123",`+
		`"createdTime":"2026-01-01T00:00:00Z","modifiedTime":"2026-01-01T00:00:01Z"}`,
		id, strconv.Itoa(size))
}

func parseContentRange(raw string) (offset, last int64, total string, err error) {
	if !strings.HasPrefix(raw, "bytes ") {
		return 0, 0, "", errors.New("malformed content range")
	}
	spec := strings.TrimPrefix(raw, "bytes ")

	dash := strings.Index(spec, "-")
	if dash < 0 {
		return 0, 0, "", errors.New("malformed content range")
	}
	offset, err = strconv.ParseInt(spec[:dash], 10, 64)
	if err != nil {
		return 0, 0, "", err
	}

	rest := spec[dash+1:]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return 0, 0, "", errors.New("malformed content range")
	}
	last, err = strconv.ParseInt(rest[:slash], 10, 64)
	if err != nil {
		return 0, 0, "", err
	}
	total = rest[slash+1:]

	return offset, last, total, nil
}


func (f *fakeDrive) handleFileByID(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests["file-"+r.Method]++

	
	id := r.URL.Path
	if idx := strings.LastIndex(id, "/"); idx >= 0 {
		id = id[idx+1:]
	}

	data, ok := f.files[id]
	if !ok {
		writeDriveError(w, http.StatusNotFound, "notFound")
		return
	}

	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("alt") == "media" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":%q,"name":"shard","size":%q,"md5Checksum":"abc123",`+
			`"createdTime":"2026-01-01T00:00:00Z","modifiedTime":"2026-01-01T00:00:01Z"}`,
			id, strconv.Itoa(len(data)))

	case http.MethodDelete:
		delete(f.files, id)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeDrive) handleAbout(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fields := r.URL.Query().Get("fields")
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(fields, "storageQuota"):
		fmt.Fprintf(w, `{"storageQuota":{"limit":%q,"usage":%q}}`, f.quota.Limit, f.quota.Usage)
	case strings.Contains(fields, "user"):
		f.identity.User.DisplayName = "Test Person"
		f.identity.User.EmailAddress = "test@example.com"
		f.identity.User.PermissionID = "user-123"
		fmt.Fprintf(w, `{"user":{"displayName":%q,"emailAddress":%q,"permissionId":%q}}`,
			f.identity.User.DisplayName, f.identity.User.EmailAddress, f.identity.User.PermissionID)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func writeDriveError(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":{"errors":[{"reason":%q}],"code":%d}}`, reason, status)
}

func TestUploadKnownSizeRoundTrips(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	payload := bytes.Repeat([]byte("shard"), 1000) 

	obj, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Name: "shard", Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	if obj.ID == "" {
		t.Fatal("Upload returned no id")
	}
	if obj.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", obj.Size, len(payload))
	}
	if obj.Checksum != "abc123" {
		t.Errorf("checksum = %q, want abc123", obj.Checksum)
	}
	if obj.CreatedAt.IsZero() {
		t.Error("CreatedAt was not parsed")
	}

	
	rc, err := client.Download(context.Background(), obj.ID)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read the download: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("the downloaded bytes differ from the upload")
	}
}

func TestUploadMultiChunkUsesResumableProtocol(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	
	payload := bytes.Repeat([]byte("x"), resumableChunkSize+4096)

	obj, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", obj.Size, len(payload))
	}

	drive.mu.Lock()
	chunkRequests := drive.requests["session"]
	drive.mu.Unlock()

	if chunkRequests < 2 {
		t.Errorf("session requests = %d, want at least 2 for a multi-chunk upload", chunkRequests)
	}
}

func TestUploadExactChunkMultipleFinalises(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	
	
	
	payload := bytes.Repeat([]byte("y"), resumableChunkSize)

	obj, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", obj.Size, len(payload))
	}
}

func TestUploadUnknownSizeFinalisesWithExplicitRequest(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	payload := []byte("a small shard")

	
	
	obj, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", obj.Size, len(payload))
	}
}

func TestUploadEmptyObject(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	obj, err := client.Upload(context.Background(), bytes.NewReader(nil),
		provider.ObjectMetadata{})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if obj.ID == "" {
		t.Fatal("an empty upload must still produce a file id")
	}
}

func TestUploadShortSourceIsRejected(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	
	
	_, err := client.Upload(context.Background(), bytes.NewReader([]byte("0123456789")),
		provider.ObjectMetadata{Size: 100})
	if err == nil {
		t.Fatal("a truncated upload must not succeed")
	}
	if !errors.Is(err, provider.ErrInvalid) {
		t.Errorf("error = %v, want it to wrap ErrInvalid", err)
	}
}

func TestUploadSessionStartWithoutLocationFails(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) 
	}))
	defer srv.Close()

	client, err := New(Options{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"}),
		HTTPClient:  srv.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.base, client.upload = srv.URL, srv.URL

	if _, err := client.Upload(context.Background(), bytes.NewReader([]byte("x")),
		provider.ObjectMetadata{}); err == nil {
		t.Fatal("a session with no Location must fail rather than upload nowhere")
	}
}

func TestStatReportsSizeAndChecksum(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	payload := []byte("some bytes")
	obj, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	st, err := client.Stat(context.Background(), obj.ID)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.ID != obj.ID {
		t.Errorf("id = %q, want %q", st.ID, obj.ID)
	}
	if st.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", st.Size, len(payload))
	}
}

func TestStatMissingObjectIsNotFoundAndNodeIsHealthy(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	_, err := client.Stat(context.Background(), "does-not-exist")
	if !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}

	
	
	if got := provider.HealthOf(err); got != provider.HealthHealthy {
		t.Errorf("health = %q, want %q", got, provider.HealthHealthy)
	}
}

func TestDeleteRemovesObject(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	obj, err := client.Upload(context.Background(), bytes.NewReader([]byte("bye")),
		provider.ObjectMetadata{})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	if err := client.Delete(context.Background(), obj.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := client.Stat(context.Background(), obj.ID); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("after Delete, Stat error = %v, want ErrNotFound", err)
	}

	
	
	if err := client.Delete(context.Background(), obj.ID); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("second Delete error = %v, want ErrNotFound", err)
	}
}

func TestQuotaReportsAllowance(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	q, err := client.Quota(context.Background())
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if q.Total != 1099511627776 {
		t.Errorf("total = %d, want 1099511627776", q.Total)
	}
	if !q.Known() {
		t.Error("a reported allowance must count as known")
	}
	if q.Available() != 1099511627776-1073741824 {
		t.Errorf("available = %d, want %d", q.Available(), 1099511627776-1073741824)
	}
}

func TestIdentityUsesStableAccountID(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	id, err := client.Identity(context.Background())
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}
	if id.AccountID != "user-123" {
		t.Errorf("account id = %q, want user-123", id.AccountID)
	}
	if id.Display() != "test@example.com" {
		t.Errorf("display = %q, want the email", id.Display())
	}
}

func TestObjectIDValidation(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	
	for _, id := range []string{"", "   ", "a/b", "a?b=1", "a#b"} {
		t.Run(id, func(t *testing.T) {
			if _, err := client.Stat(context.Background(), id); !errors.Is(err, provider.ErrInvalid) {
				t.Errorf("Stat(%q) error = %v, want ErrInvalid", id, err)
			}
			if err := client.Delete(context.Background(), id); !errors.Is(err, provider.ErrInvalid) {
				t.Errorf("Delete(%q) error = %v, want ErrInvalid", id, err)
			}
		})
	}
}

func TestForbiddenQuotaReasonMarksNodeFull(t *testing.T) {
	t.Parallel()

	
	
	
	cases := []struct {
		reason      string
		wantHealth  provider.Health
		wantErrorIs error
	}{
		{"storageQuotaExceeded", provider.HealthFull, provider.ErrQuotaExceeded},
		{"rateLimitExceeded", provider.HealthFull, provider.ErrQuotaExceeded},
		{"insufficientFilePermissions", provider.HealthAuthError, provider.ErrAuth},
		{"", provider.HealthAuthError, provider.ErrAuth},
	}

	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeDriveError(w, http.StatusForbidden, tc.reason)
			}))
			defer srv.Close()

			client, err := New(Options{
				TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"}),
				HTTPClient:  srv.Client(),
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			client.base = srv.URL

			_, err = client.Quota(context.Background())
			if !errors.Is(err, tc.wantErrorIs) {
				t.Errorf("error = %v, want it to wrap %v", err, tc.wantErrorIs)
			}
			if got := provider.HealthOf(err); got != tc.wantHealth {
				t.Errorf("health = %q, want %q", got, tc.wantHealth)
			}
		})
	}
}

func TestUnauthorizedMarksNodeNeedingReauth(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeDriveError(w, http.StatusUnauthorized, "authError")
	}))
	defer srv.Close()

	client, err := New(Options{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "expired"}),
		HTTPClient:  srv.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.base = srv.URL

	_, err = client.Stat(context.Background(), "any")
	if !errors.Is(err, provider.ErrAuth) {
		t.Errorf("error = %v, want ErrAuth", err)
	}
	if got := provider.HealthOf(err); got != provider.HealthAuthError {
		t.Errorf("health = %q, want %q", got, provider.HealthAuthError)
	}
}

func TestServerErrorIsUnavailable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client, err := New(Options{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"}),
		HTTPClient:  srv.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.base = srv.URL

	_, err = client.Stat(context.Background(), "any")
	if !errors.Is(err, provider.ErrUnavailable) {
		t.Errorf("error = %v, want ErrUnavailable", err)
	}
	if got := provider.HealthOf(err); got != provider.HealthOffline {
		t.Errorf("health = %q, want %q", got, provider.HealthOffline)
	}
}

func TestCancelledContextLeavesHealthUnknown(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	
	
	_, err := client.Stat(ctx, "any")
	if err == nil {
		t.Fatal("a cancelled context must fail the call")
	}
	if got := provider.HealthOf(err); got != provider.HealthUnknown {
		t.Errorf("health = %q, want %q", got, provider.HealthUnknown)
	}
}

func TestUploadHonoursCancellation(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Upload(ctx, bytes.NewReader(bytes.Repeat([]byte("z"), 1024)),
		provider.ObjectMetadata{Size: 1024})
	if err == nil {
		t.Fatal("a cancelled upload must fail")
	}
	if got := provider.HealthOf(err); got != provider.HealthUnknown {
		t.Errorf("health = %q, want %q", got, provider.HealthUnknown)
	}
}

func TestContentRangeForms(t *testing.T) {
	t.Parallel()

	
	
	if got := contentRange(0, 100, 0); got != "bytes 0-99/*" {
		t.Errorf("unknown total = %q, want %q", got, "bytes 0-99/*")
	}
	
	
	if got := contentRange(0, 100, 250); got != "bytes 0-99/250" {
		t.Errorf("known total, first chunk = %q, want %q", got, "bytes 0-99/250")
	}
	if got := contentRange(100, 50, 150); got != "bytes 100-149/150" {
		t.Errorf("known total, second chunk = %q, want %q", got, "bytes 100-149/150")
	}
	
	if got := contentRange(0, 250, 250); got != "bytes 0-249/250" {
		t.Errorf("known total, one chunk = %q, want %q", got, "bytes 0-249/250")
	}
}

func TestNewRequiresTokenSource(t *testing.T) {
	t.Parallel()

	if _, err := New(Options{}); err == nil {
		t.Fatal("a client with no credentials must not be constructible")
	}
}

func TestUploadResumesAfterALostAcknowledgement(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	
	
	
	payload := bytes.Repeat([]byte("a"), resumableChunkSize*2+1234)

	drive.loseSessionResponse(1)

	obj, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Name: "shard", Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("Upload must recover from a lost acknowledgement: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", obj.Size, len(payload))
	}
	if obj.ID == "" {
		t.Fatal("Upload returned no id")
	}

	stored, err := client.Download(context.Background(), obj.ID)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer func() { _ = stored.Close() }()

	got, err := io.ReadAll(stored)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("the stored object is %d bytes, want %d, and its contents %s",
			len(got), len(payload), "differ")
	}

	
	drive.mu.Lock()
	ids := len(drive.files)
	drive.mu.Unlock()
	if ids != 1 {
		t.Errorf("%d objects were stored, want exactly 1", ids)
	}
}

func TestUploadDoesNotDuplicateAnObjectFinalisedBlind(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	
	
	payload := bytes.Repeat([]byte("b"), resumableChunkSize+77)

	drive.loseSessionResponse(2)

	obj, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Name: "shard", Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	drive.mu.Lock()
	ids := len(drive.files)
	var body []byte
	var storedID string
	for id, data := range drive.files {
		body, storedID = data, id
	}
	drive.mu.Unlock()

	if ids != 1 {
		t.Fatalf("%d objects were stored, want exactly 1", ids)
	}
	
	
	if obj.ID != storedID {
		t.Errorf("Upload returned %q but the stored object is %q", obj.ID, storedID)
	}
	if !bytes.Equal(body, payload) {
		t.Errorf("the stored object is %d bytes, want %d", len(body), len(payload))
	}
}

func TestUploadGivesUpWhenTheSessionIsGone(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	
	
	payload := bytes.Repeat([]byte("c"), resumableChunkSize*3)

	
	
	
	drive.expireSession(1)

	_, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Name: "shard", Size: int64(len(payload))})
	if err == nil {
		t.Fatal("Upload must fail when the session cannot be recovered")
	}
	if !strings.Contains(err.Error(), "retry the upload") {
		t.Errorf("error = %v, want it to say the upload has to be retried", err)
	}
}

func TestSessionStartDeclaresTheObjectSize(t *testing.T) {
	t.Parallel()

	drive := newFakeDrive()
	client := drive.start(t)

	payload := bytes.Repeat([]byte("d"), 4096)
	if _, err := client.Upload(context.Background(), bytes.NewReader(payload),
		provider.ObjectMetadata{Name: "shard", Size: int64(len(payload))}); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	drive.mu.Lock()
	defer drive.mu.Unlock()
	for _, session := range drive.sessions {
		if session.startTotal == strconv.Itoa(len(payload)) {
			return
		}
	}
	t.Errorf("no session declared the object size; got %v", drive.sessionTotals())
}



func (f *fakeDrive) loseSessionResponse(chunk int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSessionLoseAt = chunk
}



func (f *fakeDrive) expireSession(chunk int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSessionExpireAt = chunk
}

func (f *fakeDrive) sessionTotals() map[string]string {
	out := map[string]string{}
	for path, s := range f.sessions {
		out[path] = s.startTotal
	}
	return out
}
