package gdrive

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

const (
	defaultBaseURL   = "https://www.googleapis.com/drive/v3"
	defaultUploadURL = "https://www.googleapis.com/upload/drive/v3"
)

const DriveScope = "https://www.googleapis.com/auth/drive"

const fileFields = "id,name,size,md5Checksum,createdTime,modifiedTime"

const resumableChunkSize = 8 << 20

const statusResumeIncomplete = 308

const defaultTimeout = 10 * time.Minute

type Client struct {
	http    *http.Client
	account string

	base   string
	upload string
}

var _ provider.StorageNode = (*Client)(nil)

type Options struct {
	TokenSource oauth2.TokenSource

	HTTPClient *http.Client

	Account string

	Timeout time.Duration
}

func New(opts Options) (*Client, error) {
	if opts.TokenSource == nil {
		return nil, errors.New("gdrive: a token source is required")
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = defaultTimeout
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	return &Client{
		http:    httpClient,
		account: opts.Account,
		base:    defaultBaseURL,
		upload:  defaultUploadURL,
	}, nil
}

func (c *Client) Account() string { return c.account }

type driveFile struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Size         string `json:"size"`
	MD5Checksum  string `json:"md5Checksum"`
	CreatedTime  string `json:"createdTime"`
	ModifiedTime string `json:"modifiedTime"`
}

func (f driveFile) toRemote() (provider.RemoteObject, error) {
	size, err := parseSize(f.Size)
	if err != nil {
		return provider.RemoteObject{}, err
	}

	created, _ := parseTime(f.CreatedTime)
	modified, _ := parseTime(f.ModifiedTime)

	return provider.RemoteObject{
		ID:         f.ID,
		Name:       f.Name,
		Size:       size,
		Checksum:   f.MD5Checksum,
		CreatedAt:  created,
		ModifiedAt: modified,
	}, nil
}

func parseSize(raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	size, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("gdrive: unreadable size %q: %w", raw, err)
	}
	if size < 0 {
		return 0, fmt.Errorf("gdrive: negative size %q", raw)
	}
	return size, nil
}

func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("gdrive: unreadable timestamp %q: %w", raw, err)
	}
	return parsed.UTC(), nil
}

func (c *Client) Upload(ctx context.Context, r io.Reader, meta provider.ObjectMetadata) (provider.RemoteObject, error) {
	if r == nil {
		return provider.RemoteObject{}, provider.Degradedf(errors.New("gdrive: upload needs a reader"))
	}

	sessionURL, err := c.startUploadSession(ctx, meta)
	if err != nil {
		return provider.RemoteObject{}, err
	}

	file, err := c.pumpUpload(ctx, sessionURL, r, meta.Size)
	if err != nil {
		return provider.RemoteObject{}, err
	}

	return file.toRemote()
}

type uploadRequest struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

func contentType(meta provider.ObjectMetadata) string {
	if meta.ContentType != "" {
		return meta.ContentType
	}

	return "application/octet-stream"
}

func (c *Client) startUploadSession(ctx context.Context, meta provider.ObjectMetadata) (string, error) {
	body, err := json.Marshal(uploadRequest{
		Name:        meta.Name,
		Description: meta.Description,
		MimeType:    contentType(meta),
	})
	if err != nil {
		return "", provider.Degradedf(fmt.Errorf("gdrive: encode the upload request: %w", err))
	}

	endpoint := c.upload + "/files?uploadType=resumable&fields=" + url.QueryEscape(fileFields)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", provider.Degradedf(err)
	}

	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("X-Upload-Content-Type", contentType(meta))

	if meta.Size > 0 {
		req.Header.Set("X-Upload-Content-Length", strconv.FormatInt(meta.Size, 10))
	}

	resp, err := c.send(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", c.apiError("start the upload session", resp)
	}

	location := resp.Header.Get("Location")
	if location == "" {
		return "", provider.Degradedf(
			errors.New("gdrive: the upload session response carried no Location header"))
	}

	sessionURL, err := req.URL.Parse(location)
	if err != nil {
		return "", provider.Degradedf(
			fmt.Errorf("gdrive: the upload session URL %q is unusable: %w", location, err))
	}

	return sessionURL.String(), nil
}

func (c *Client) pumpUpload(ctx context.Context, sessionURL string, r io.Reader, total int64) (driveFile, error) {
	buf := make([]byte, resumableChunkSize)
	known := total > 0

	var offset int64

	for {
		if err := ctx.Err(); err != nil {
			return driveFile{}, err
		}

		n, readErr := fill(buf, r)

		if n == 0 {
			if !errors.Is(readErr, io.EOF) {
				return driveFile{}, provider.Degradedf(fmt.Errorf("gdrive: read the upload source: %w", readErr))
			}

			if known {
				return driveFile{}, provider.Degradedf(fmt.Errorf(
					"%w: the upload source ended after %d of %d bytes",
					provider.ErrInvalid, offset, total))
			}
			return c.finalizeSession(ctx, sessionURL, offset)
		}

		file, done, err := c.putChunkResumable(ctx, sessionURL, buf[:n], offset, total)
		if err != nil {
			return driveFile{}, err
		}

		offset += int64(n)

		if done {
			return file, nil
		}

		if readErr != nil {

			if !errors.Is(readErr, io.EOF) {
				return driveFile{}, provider.Degradedf(fmt.Errorf("gdrive: read the upload source: %w", readErr))
			}

			if known {
				return driveFile{}, provider.Degradedf(fmt.Errorf(
					"%w: the upload source ended after %d of %d bytes",
					provider.ErrInvalid, offset, total))
			}

			return c.finalizeSession(ctx, sessionURL, offset)
		}
	}
}

func fill(buf []byte, r io.Reader) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

const maxResumeAttempts = 2

type uploadOutcome bool

const (
	uploadPending uploadOutcome = false

	uploadComplete uploadOutcome = true
)

var errSessionGone = errors.New("gdrive: the upload session is gone")

type transportFailure struct{ err error }

func (e *transportFailure) Error() string { return e.err.Error() }
func (e *transportFailure) Unwrap() error { return e.err }

type sessionState struct {
	file driveFile

	offset int64
}

func (c *Client) putChunkResumable(ctx context.Context, sessionURL string, chunk []byte, offset, total int64) (driveFile, uploadOutcome, error) {
	start := offset
	sent := int64(0)

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return driveFile{}, false, err
		}

		file, done, err := c.putChunk(ctx, sessionURL, chunk[sent:], start+sent, total)
		if err == nil {
			return file, done, nil
		}

		var tf *transportFailure
		if !errors.As(err, &tf) {
			return driveFile{}, uploadPending, err
		}
		if attempt >= maxResumeAttempts {
			return driveFile{}, uploadPending, err
		}

		state, qerr := c.queryOffset(ctx, sessionURL, total)
		if qerr != nil {
			if errors.Is(qerr, errSessionGone) {

				return driveFile{}, uploadPending, provider.Degradedf(fmt.Errorf(
					"gdrive: the upload session expired after %d bytes; retry the upload",
					start+sent))
			}

			return driveFile{}, uploadPending, err
		}

		if state.file.ID != "" {
			return state.file, uploadComplete, nil
		}

		got := state.offset - start
		if got < sent {
			return driveFile{}, uploadPending, provider.Degradedf(fmt.Errorf(
				"gdrive: the upload session holds %d bytes after %d were accepted; "+
					"the session was discarded", state.offset, start+sent))
		}
		if got > int64(len(chunk)) {
			return driveFile{}, uploadPending, provider.Degradedf(fmt.Errorf(
				"gdrive: the upload session holds %d bytes, more than the %d sent at offset %d",
				state.offset, len(chunk), start))
		}

		if got == int64(len(chunk)) {

			return driveFile{}, uploadPending, nil
		}

		sent = got
	}
}

func (c *Client) queryOffset(ctx context.Context, sessionURL string, total int64) (sessionState, error) {
	if total <= 0 {
		return sessionState{}, errors.New("gdrive: cannot query a session of unknown length")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sessionURL, bytes.NewReader(nil))
	if err != nil {
		return sessionState{}, provider.Degradedf(err)
	}
	req.ContentLength = 0
	req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", total))

	resp, err := c.send(req)
	if err != nil {
		return sessionState{}, &transportFailure{err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		var created driveFile
		if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
			return sessionState{}, provider.Degradedf(
				fmt.Errorf("gdrive: decode the upload status response: %w", err))
		}
		if created.ID == "" {
			return sessionState{}, provider.Degradedf(
				errors.New("gdrive: the session reported completion with no file id"))
		}
		return sessionState{file: created}, nil

	case statusResumeIncomplete:
		offset, err := acknowledgedOffset(resp)
		if err != nil {
			return sessionState{}, err
		}
		return sessionState{offset: offset}, nil

	case http.StatusRequestedRangeNotSatisfiable, http.StatusNotFound:
		return sessionState{}, errSessionGone

	default:
		return sessionState{}, c.apiError("check the upload status", resp)
	}
}

func (c *Client) putChunk(ctx context.Context, sessionURL string, chunk []byte, offset, total int64) (file driveFile, outcome uploadOutcome, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sessionURL, bytes.NewReader(chunk))
	if err != nil {
		return driveFile{}, uploadPending, provider.Degradedf(err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(chunk))

	req.Header.Set("Content-Range", contentRange(offset, int64(len(chunk)), total))

	resp, err := c.send(req)
	if err != nil {

		return driveFile{}, uploadPending, &transportFailure{err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		var created driveFile
		if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
			return driveFile{}, uploadPending, provider.Degradedf(
				fmt.Errorf("gdrive: decode the upload response: %w", err))
		}
		if created.ID == "" {
			return driveFile{}, uploadPending, provider.Degradedf(
				errors.New("gdrive: the upload completed with no file id"))
		}
		return created, uploadComplete, nil

	case statusResumeIncomplete:

		return driveFile{}, uploadPending, checkOffset(resp, offset+int64(len(chunk)))

	case http.StatusRequestedRangeNotSatisfiable, http.StatusNotFound:

		return driveFile{}, uploadPending, provider.Degradedf(fmt.Errorf(
			"gdrive: the upload session expired after %d bytes; retry the upload", offset))

	default:
		return driveFile{}, uploadPending, c.apiError("upload a chunk", resp)
	}
}

func checkOffset(resp *http.Response, want int64) error {
	raw := resp.Header.Get("Range")
	if raw == "" {

		return nil
	}

	got, err := acknowledgedOffset(resp)
	if err != nil {
		return err
	}

	if got != want {
		return provider.Degradedf(fmt.Errorf(
			"gdrive: Drive acknowledged %d bytes but %d were sent; retry the upload", got, want))
	}

	return nil
}

func acknowledgedOffset(resp *http.Response) (int64, error) {
	raw := resp.Header.Get("Range")
	if raw == "" {
		return 0, nil
	}

	const prefix = "bytes=0-"
	if !strings.HasPrefix(raw, prefix) {
		return 0, provider.Degradedf(fmt.Errorf("gdrive: unreadable acknowledged range %q", raw))
	}

	got, err := strconv.ParseInt(strings.TrimPrefix(raw, prefix), 10, 64)
	if err != nil {
		return 0, provider.Degradedf(
			fmt.Errorf("gdrive: unreadable acknowledged range %q: %w", raw, err))
	}

	return got + 1, nil
}

func (c *Client) finalizeSession(ctx context.Context, sessionURL string, offset int64) (driveFile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sessionURL, bytes.NewReader(nil))
	if err != nil {
		return driveFile{}, provider.Degradedf(err)
	}
	req.Header.Set("Content-Range", "bytes */*")

	resp, err := c.send(req)
	if err != nil {
		return driveFile{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return driveFile{}, c.apiError("finalize the upload", resp)
	}

	var created driveFile
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return driveFile{}, provider.Degradedf(
			fmt.Errorf("gdrive: decode the final upload response: %w", err))
	}
	if created.ID == "" {
		return driveFile{}, provider.Degradedf(fmt.Errorf(
			"gdrive: the upload was finalised with no file id after %d bytes", offset))
	}

	return created, nil
}

func contentRange(offset, length, total int64) string {
	last := offset + length - 1

	if total > 0 {
		return fmt.Sprintf("bytes %d-%d/%d", offset, last, total)
	}

	return fmt.Sprintf("bytes %d-%d/*", offset, last)
}

func (c *Client) Download(ctx context.Context, id string) (io.ReadCloser, error) {
	if err := validateFileID(id); err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s/files/%s?alt=media", c.base, url.PathEscape(id))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, provider.Degradedf(err)
	}

	resp, err := c.send(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		defer func() { _ = resp.Body.Close() }()
		return nil, provider.NotFound(id)
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		return nil, c.apiError("download the object", resp)
	}

	return resp.Body, nil
}

func (c *Client) Delete(ctx context.Context, id string) error {
	if err := validateFileID(id); err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s/files/%s", c.base, url.PathEscape(id))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return provider.Degradedf(err)
	}

	resp, err := c.send(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK:
		return nil
	case http.StatusNotFound:
		return provider.NotFound(id)
	default:
		return c.apiError("delete the object", resp)
	}
}

func (c *Client) Stat(ctx context.Context, id string) (provider.RemoteObject, error) {
	if err := validateFileID(id); err != nil {
		return provider.RemoteObject{}, err
	}

	endpoint := fmt.Sprintf("%s/files/%s?fields=%s", c.base, url.PathEscape(id), url.QueryEscape(fileFields))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return provider.RemoteObject{}, provider.Degradedf(err)
	}

	resp, err := c.send(req)
	if err != nil {
		return provider.RemoteObject{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return provider.RemoteObject{}, provider.NotFound(id)
	}
	if resp.StatusCode != http.StatusOK {
		return provider.RemoteObject{}, c.apiError("stat the object", resp)
	}

	var file driveFile
	if err := json.NewDecoder(resp.Body).Decode(&file); err != nil {
		return provider.RemoteObject{}, provider.Degradedf(
			fmt.Errorf("gdrive: decode the file resource: %w", err))
	}

	return file.toRemote()
}

func (c *Client) Quota(ctx context.Context) (provider.Quota, error) {
	var body struct {
		StorageQuota struct {
			Limit string `json:"limit"`
			Usage string `json:"usage"`
		} `json:"storageQuota"`
	}

	if err := c.getJSON(ctx, c.base+"/about?fields=storageQuota", &body); err != nil {
		return provider.Quota{}, err
	}

	limit, err := parseSize(body.StorageQuota.Limit)
	if err != nil {
		return provider.Quota{}, err
	}
	used, err := parseSize(body.StorageQuota.Usage)
	if err != nil {
		return provider.Quota{}, err
	}

	return provider.Quota{Total: limit, Used: used}, nil
}

func (c *Client) Identity(ctx context.Context) (provider.Identity, error) {
	var body struct {
		User struct {
			DisplayName  string `json:"displayName"`
			EmailAddress string `json:"emailAddress"`
			PermissionID string `json:"permissionId"`
		} `json:"user"`
	}

	if err := c.getJSON(ctx, c.base+"/about?fields=user", &body); err != nil {
		return provider.Identity{}, err
	}

	return provider.Identity{
		AccountID:   body.User.PermissionID,
		Email:       body.User.EmailAddress,
		DisplayName: body.User.DisplayName,
	}, nil
}

func (c *Client) getJSON(ctx context.Context, endpoint string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return provider.Degradedf(err)
	}

	resp, err := c.send(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.apiError("read the account information", resp)
	}

	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		return provider.Degradedf(fmt.Errorf("gdrive: decode the response: %w", err))
	}

	return nil
}

func (c *Client) send(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, classifyTransportError(req, err)
	}
	return resp, nil
}

func classifyTransportError(req *http.Request, err error) error {

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	if isTLSError(err) {
		return provider.Degradedf(fmt.Errorf("gdrive: TLS failure talking to Drive: %w", err))
	}

	where := "the request"
	if req != nil && req.URL != nil {
		where = req.Method + " " + req.URL.Path
	}

	return provider.Unavailablef(fmt.Errorf("gdrive: %s failed: %w", where, err))
}

func isTLSError(err error) bool {
	var recordErr *tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return true
	}

	return strings.Contains(err.Error(), "x509:") ||
		strings.Contains(err.Error(), "tls:")
}

func (c *Client) apiError(op string, resp *http.Response) error {
	status := resp.StatusCode
	reason := apiErrorReason(resp)

	err := fmt.Errorf("gdrive: %s: HTTP %d %s", op, status, strings.TrimSpace(reason))

	switch {
	case status == http.StatusNotFound:
		return fmt.Errorf("gdrive %s: %w", op, err)

	case status == http.StatusUnauthorized:
		return provider.Authf(err)

	case status == http.StatusForbidden:

		if isQuotaReason(reason) {
			return provider.QuotaExceededf(err)
		}
		return provider.Authf(err)

	case status == http.StatusRequestEntityTooLarge:
		return provider.Degradedf(fmt.Errorf("gdrive %s: %w", op, err))

	case status == http.StatusBadRequest:
		return provider.Degradedf(fmt.Errorf("gdrive %s: %w", op, err))

	case status == http.StatusTooManyRequests:

		return provider.Degradedf(fmt.Errorf("gdrive %s: %w", op, err))

	case retryableStatus(status):
		return provider.Unavailablef(err)

	default:
		return provider.Degradedf(err)
	}
}

func apiErrorReason(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}

	var body struct {
		Error struct {
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return ""
	}
	if len(body.Error.Errors) == 0 {
		return ""
	}

	return body.Error.Errors[0].Reason
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func isQuotaReason(reason string) bool {
	switch reason {
	case "storageQuotaExceeded", "quotaExceeded", "rateLimitExceeded", "userRateLimitExceeded":
		return true
	default:
		return false
	}
}

func validateFileID(id string) error {
	if strings.TrimSpace(id) == "" {
		return provider.Degradedf(fmt.Errorf(
			"%w: an object id must not be empty", provider.ErrInvalid))
	}
	if strings.ContainsAny(id, "/?#") {
		return provider.Degradedf(fmt.Errorf(
			"%w: object id %q must not contain a URL separator", provider.ErrInvalid, id))
	}
	return nil
}
