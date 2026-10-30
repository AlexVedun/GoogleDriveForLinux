package driveapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDriveOperationsAgainstFakeServer(t *testing.T) {
	var created FileMetadata
	var patched []struct {
		Query url.Values
		Body  FilePatch
	}
	var generated int
	var uploadCalls atomic.Int32
	server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/files":
			if request.URL.Query().Get("spaces") != "drive" || request.URL.Query().Get("q") != "name contains 'a'" || !strings.Contains(request.URL.Query().Get("fields"), "capabilities(canEdit,canTrash,canDownload,canRename,canMoveItemWithinDrive,canAddChildren,canRemoveChildren)") {
				http.Error(writer, `{"error":"missing query fields"}`, http.StatusBadRequest)
				return
			}
			if request.URL.Query().Get("pageToken") == "page-2" {
				_, _ = io.WriteString(writer, `{"files":[{"id":"file-2","name":"two","size":"0"}]}`)
				return
			}
			_, _ = io.WriteString(writer, `{"nextPageToken":"page-2","files":[{"id":"file-1","name":"one","mimeType":"text/plain","parents":["root"],"size":"42","version":"7","modifiedTime":"2026-10-30T10:00:00Z","webViewLink":"https://drive.google.com/file/d/file-1","md5Checksum":"abc","capabilities":{"canEdit":true,"canDownload":true}}]}`)
		case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/files/file-1" && request.URL.Query().Get("alt") == "media":
			writer.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(writer, "download-data")
		case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/files/file-1":
			_, _ = io.WriteString(writer, `{"id":"file-1","name":"one","size":"42","capabilities":{"canEdit":true,"canTrash":true}}`)
		case request.Method == http.MethodPost && request.URL.Path == "/drive/v3/files":
			if err := json.NewDecoder(request.Body).Decode(&created); err != nil {
				t.Errorf("decode create metadata: %v", err)
			}
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(writer, `{"id":"created-id","name":"created"}`)
		case request.Method == http.MethodPatch && request.URL.Path == "/drive/v3/files/file-1":
			var body FilePatch
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode update patch: %v", err)
			}
			patched = append(patched, struct {
				Query url.Values
				Body  FilePatch
			}{request.URL.Query(), body})
			_, _ = io.WriteString(writer, `{"id":"file-1","name":"renamed","trashed":true}`)
		case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/changes/startPageToken":
			_, _ = io.WriteString(writer, `{"startPageToken":"start-99"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/changes":
			query := request.URL.Query()
			if query.Get("pageToken") != "start-99" || query.Get("restrictToMyDrive") != "true" || query.Get("includeRemoved") != "true" {
				http.Error(writer, `{"error":"missing change query"}`, http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(writer, `{"newStartPageToken":"start-100","changes":[{"fileId":"file-1","time":"2026-10-30T10:00:00Z","file":{"id":"file-1","name":"one","size":"42"}}]}`)
		case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/files/generateIds":
			generated = 2
			_, _ = io.WriteString(writer, `{"ids":["reserved-1","reserved-2"]}`)
		case request.Method == http.MethodPost && request.URL.Path == "/upload/drive/v3/files":
			if request.URL.Query().Get("uploadType") != "resumable" || request.Header.Get("X-Upload-Content-Length") != "8" || request.Header.Get("X-Upload-Content-Type") != "text/plain" {
				http.Error(writer, `{"error":"missing resumable headers"}`, http.StatusBadRequest)
				return
			}
			var metadata FileMetadata
			if err := json.NewDecoder(request.Body).Decode(&metadata); err != nil || metadata.ID != "reserved-1" {
				t.Errorf("upload metadata = %#v, decode err = %v", metadata, err)
			}
			writer.Header().Set("Location", serverURL(request)+"/upload/session?opaque=secret-session")
			writer.WriteHeader(http.StatusOK)
		case request.Method == http.MethodPut && request.URL.Path == "/upload/session":
			switch request.Header.Get("Content-Range") {
			case "bytes 0-3/8":
				if request.Header.Get("Content-Length") != "4" {
					t.Errorf("chunk Content-Length = %q, want 4", request.Header.Get("Content-Length"))
				}
				body, _ := io.ReadAll(request.Body)
				if string(body) != "abcd" {
					t.Errorf("chunk body = %q, want abcd", body)
				}
				writer.Header().Set("Range", "bytes=0-3")
				writer.WriteHeader(http.StatusPermanentRedirect)
			case "bytes */8":
				writer.Header().Set("Range", "bytes=0-5")
				writer.WriteHeader(http.StatusPermanentRedirect)
			case "bytes 6-7/8":
				_, _ = io.WriteString(writer, `{"id":"reserved-1","name":"upload.txt"}`)
			default:
				t.Errorf("unexpected upload Content-Range %q", request.Header.Get("Content-Range"))
				http.Error(writer, `{"error":"invalid range"}`, http.StatusBadRequest)
			}
			uploadCalls.Add(1)
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL)
			http.NotFound(writer, request)
		}
	}))
	client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 1}})
	ctx := context.Background()

	first, err := client.ListFiles(ctx, FileListOptions{Query: "name contains 'a'", PageSize: 2})
	if err != nil || len(first.Files) != 1 || first.NextPageToken != "page-2" || first.Files[0].Size != 42 || first.Files[0].Version != "7" || !first.Files[0].Capabilities.CanEdit {
		t.Fatalf("first file page = %#v, err = %v", first, err)
	}
	second, err := client.ListFiles(ctx, FileListOptions{Query: "name contains 'a'", PageSize: 2, PageToken: first.NextPageToken})
	if err != nil || len(second.Files) != 1 || second.Files[0].ID != "file-2" {
		t.Fatalf("second file page = %#v, err = %v", second, err)
	}
	file, err := client.GetFile(ctx, "file-1")
	if err != nil || file.Size != 42 || !file.Capabilities.CanTrash {
		t.Fatalf("GetFile = %#v, err = %v", file, err)
	}
	createdFile, err := client.CreateFile(ctx, FileMetadata{Name: "created", MIMEType: "text/plain", Parents: []string{"root"}})
	if err != nil || createdFile.ID != "created-id" || created.Name != "created" || created.ID != "" {
		t.Fatalf("CreateFile = %#v, metadata = %#v, err = %v", createdFile, created, err)
	}
	if _, err := client.MoveRename(ctx, "file-1", "renamed", "new-parent", "old-parent"); err != nil {
		t.Fatalf("MoveRename: %v", err)
	}
	if _, err := client.TrashFile(ctx, "file-1"); err != nil {
		t.Fatalf("TrashFile: %v", err)
	}
	if len(patched) != 2 || patched[0].Body.Name == nil || *patched[0].Body.Name != "renamed" || patched[0].Query.Get("addParents") != "new-parent" || patched[0].Query.Get("removeParents") != "old-parent" {
		t.Fatalf("move/rename request = %#v", patched)
	}
	if patched[1].Body.Trashed == nil || !*patched[1].Body.Trashed {
		t.Fatalf("trash patch = %#v", patched[1].Body)
	}

	download, err := client.DownloadFile(ctx, "file-1")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(download.Body)
	if err != nil || string(data) != "download-data" || download.ContentType != "text/plain" || download.ContentLength != int64(len(data)) {
		t.Fatalf("download = %q, type=%q size=%d err=%v", data, download.ContentType, download.ContentLength, err)
	}
	_ = download.Body.Close()

	startToken, err := client.GetStartPageToken(ctx)
	if err != nil || startToken != "start-99" {
		t.Fatalf("start page token = %q, err = %v", startToken, err)
	}
	changes, err := client.ListChanges(ctx, startToken)
	if err != nil || changes.NewStartPageToken != "start-100" || len(changes.Changes) != 1 || changes.Changes[0].File == nil || changes.Changes[0].File.Size != 42 {
		t.Fatalf("change page = %#v, err = %v", changes, err)
	}
	ids, err := client.GenerateIDs(ctx, 2)
	if err != nil || generated != 2 || len(ids) != 2 || ids[0] != "reserved-1" {
		t.Fatalf("generated IDs = %#v, err = %v", ids, err)
	}

	sessionURL, err := client.StartResumableUpload(ctx, UploadRequest{
		Metadata: FileMetadata{ID: "reserved-1", Name: "upload.txt"}, Size: 8, MIMEType: "text/plain",
	})
	if err != nil || !strings.Contains(sessionURL, "opaque=secret-session") {
		t.Fatalf("upload session = %q, err = %v", sessionURL, err)
	}
	progress, err := client.UploadChunk(ctx, sessionURL, UploadChunkRequest{Start: 0, End: 3, Total: 8, MIMEType: "text/plain", Body: strings.NewReader("abcd")})
	if err != nil || progress.Complete || progress.NextOffset != 4 {
		t.Fatalf("first upload chunk progress = %#v, err = %v", progress, err)
	}
	progress, err = client.QueryUploadStatus(ctx, sessionURL, 8)
	if err != nil || progress.Complete || progress.NextOffset != 6 {
		t.Fatalf("upload status progress = %#v, err = %v", progress, err)
	}
	progress, err = client.UploadChunk(ctx, sessionURL, UploadChunkRequest{Start: 6, End: 7, Total: 8, MIMEType: "text/plain", Body: strings.NewReader("gh")})
	if err != nil || !progress.Complete || progress.File == nil || progress.File.ID != "reserved-1" || uploadCalls.Load() != 3 {
		t.Fatalf("completed upload progress = %#v, calls=%d err=%v", progress, uploadCalls.Load(), err)
	}
}

func TestHTTPAndDriveReasonsMapToDistinctErrorsWithoutExposingMessages(t *testing.T) {
	cases := []struct {
		name   string
		status int
		reason string
		want   error
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, want: ErrUnauthenticated},
		{name: "permission denied", status: http.StatusForbidden, reason: "insufficientFilePermissions", want: ErrForbidden},
		{name: "user rate limit", status: http.StatusForbidden, reason: "userRateLimitExceeded", want: ErrRateLimited},
		{name: "storage quota", status: http.StatusForbidden, reason: "storageQuotaExceeded", want: ErrQuotaExceeded},
		{name: "not found", status: http.StatusNotFound, want: ErrNotFound},
		{name: "too many requests", status: http.StatusTooManyRequests, want: ErrRateLimited},
		{name: "backend failure", status: http.StatusBadGateway, want: ErrTemporary},
		{name: "bad request", status: http.StatusBadRequest, want: ErrInvalidRequest},
		{name: "invalid page token reason", status: http.StatusBadRequest, reason: "invalidPageToken", want: ErrInvalidPageToken},
		{name: "gone generic request", status: http.StatusGone, want: ErrInvalidRequest},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(testCase.status)
				if testCase.reason == "" {
					fmt.Fprintf(writer, `{"error":{"code":%d,"message":"private filename: secret.txt"}}`, testCase.status)
				} else {
					fmt.Fprintf(writer, `{"error":{"code":%d,"message":"private filename: secret.txt","errors":[{"reason":%q}]}}`, testCase.status, testCase.reason)
				}
			}))
			client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 1}})
			_, err := client.GetFile(context.Background(), "file-1")
			if !errors.Is(err, testCase.want) {
				t.Fatalf("GetFile error = %v, want %v", err, testCase.want)
			}
			if strings.Contains(err.Error(), "secret.txt") {
				t.Fatalf("API error exposed response message: %v", err)
			}
			var apiErr *APIError
			if errors.As(err, &apiErr) && testCase.reason != "" && (len(apiErr.Reasons) != 1 || apiErr.Reasons[0] != testCase.reason) {
				t.Fatalf("parsed reasons = %#v, want %q", apiErr.Reasons, testCase.reason)
			}
		})
	}
}

func TestListChangesGoneMapsToInvalidPageToken(t *testing.T) {
	server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/drive/v3/changes" || request.URL.Query().Get("pageToken") != "stale-token" {
			t.Errorf("unexpected changes request: %s %s", request.Method, request.URL)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusGone)
		_, _ = io.WriteString(writer, `{"error":{"code":410,"message":"cursor no longer accepted"}}`)
	}))
	client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 1}})
	if _, err := client.ListChanges(context.Background(), "stale-token"); !errors.Is(err, ErrInvalidPageToken) {
		t.Fatalf("ListChanges error = %v, want invalid page token", err)
	}
}

func TestRetrySafeRequestsRetryRateLimits5xxAndNetworkDrops(t *testing.T) {
	t.Run("403 rate limit and 503", func(t *testing.T) {
		var count atomic.Int32
		server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			attempt := count.Add(1)
			if attempt == 1 {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(writer, `{"error":{"errors":[{"reason":"rateLimitExceeded"}]}}`)
				return
			}
			if attempt == 2 {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"id":"file-1","name":"ok"}`)
		}))
		client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}})
		file, err := client.GetFile(context.Background(), "file-1")
		if err != nil || file.Name != "ok" || count.Load() != 3 {
			t.Fatalf("GetFile = %#v, attempts=%d, err=%v", file, count.Load(), err)
		}
	})

	t.Run("connection reset", func(t *testing.T) {
		var count atomic.Int32
		server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if count.Add(1) == 1 {
				connection, _, err := writer.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"id":"file-1","name":"recovered"}`)
		}))
		client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}})
		file, err := client.GetFile(context.Background(), "file-1")
		if err != nil || file.Name != "recovered" || count.Load() != 2 {
			t.Fatalf("GetFile = %#v, attempts=%d, err=%v", file, count.Load(), err)
		}
	})
}

func TestMutationsAreNotRetriedAfterAmbiguousServerFailure(t *testing.T) {
	var count atomic.Int32
	server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		count.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}})
	_, err := client.CreateFile(context.Background(), FileMetadata{Name: "one"})
	if !errors.Is(err, ErrTemporary) || count.Load() != 1 {
		t.Fatalf("CreateFile attempts=%d, err=%v; want one request and temporary failure", count.Load(), err)
	}
}

func TestRetryDelayCanBeCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var count atomic.Int32
	server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		count.Add(1)
		cancel()
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 4, BaseDelay: time.Second, MaxDelay: time.Second}})
	_, err := client.GetFile(ctx, "file-1")
	if !errors.Is(err, context.Canceled) || count.Load() != 1 {
		t.Fatalf("GetFile attempts=%d, err=%v; want cancelled wait after one attempt", count.Load(), err)
	}
}

func TestRequestConcurrencyIsBounded(t *testing.T) {
	const concurrencyLimit = 2
	var active atomic.Int32
	var maximum atomic.Int32
	var signalOnce sync.Once
	ready := make(chan struct{})
	release := make(chan struct{})
	server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
		}
		if current == concurrencyLimit {
			signalOnce.Do(func() { close(ready) })
		}
		<-release
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"id":"file-1","name":"ok"}`)
	}))
	client := newClientForServer(t, server, Config{MaxConcurrent: concurrencyLimit, Retry: RetryPolicy{MaxAttempts: 1}})
	var group sync.WaitGroup
	errorsFound := make(chan error, 6)
	for range 6 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := client.GetFile(context.Background(), "file-1")
			errorsFound <- err
		}()
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("fake server did not observe the expected concurrent requests")
	}
	close(release)
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("GetFile error = %v", err)
		}
	}
	if maximum.Load() != concurrencyLimit {
		t.Fatalf("maximum in-flight requests = %d, want %d", maximum.Load(), concurrencyLimit)
	}
}

func TestExpiredUploadSessionAndUntrustedLocationAreRejected(t *testing.T) {
	server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			writer.Header().Set("Location", "https://attacker.invalid/session")
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusNotFound)
	}))
	client := newClientForServer(t, server, Config{Retry: RetryPolicy{MaxAttempts: 1}})
	if _, err := client.StartResumableUpload(context.Background(), UploadRequest{Metadata: FileMetadata{ID: "id"}, Size: 0}); !errors.Is(err, ErrUnexpectedResponse) {
		t.Fatalf("untrusted session location error = %v", err)
	}
	if _, err := client.QueryUploadStatus(context.Background(), server.URL+"/upload/session", 10); !errors.Is(err, ErrUploadSessionExpired) {
		t.Fatalf("expired upload status error = %v", err)
	}
	if _, err := client.QueryUploadStatus(context.Background(), "https://attacker.invalid/session", 10); err == nil {
		t.Fatal("accepted a session URL on another origin")
	}
}

func TestCrossOriginRedirectIsNotFollowed(t *testing.T) {
	var attackerRequests atomic.Int32
	attacker := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attackerRequests.Add(1)
		writer.WriteHeader(http.StatusOK)
	}))
	api := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, attacker.URL+"/steal", http.StatusFound)
	}))
	client := newClientForServer(t, api, Config{Retry: RetryPolicy{MaxAttempts: 1}})
	_, err := client.GetFile(context.Background(), "file-1")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("redirect response error = %v, want rejected response", err)
	}
	if attackerRequests.Load() != 0 {
		t.Fatalf("followed a cross-origin redirect %d times", attackerRequests.Load())
	}
}

func TestInputValidationAndContextTimeout(t *testing.T) {
	if _, err := NewClient(Config{BaseURL: "file:///tmp/drive"}); err == nil {
		t.Fatal("accepted a non-HTTP API URL")
	}
	server := newFakeServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(50 * time.Millisecond)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"id":"file-1"}`)
	}))
	client := newClientForServer(t, server, Config{
		RequestTimeout: 10 * time.Millisecond,
		Retry:          RetryPolicy{MaxAttempts: 1},
	})
	if _, err := client.GetFile(context.Background(), "bad/id"); err == nil {
		t.Fatal("accepted a file ID containing a path separator")
	}
	if _, err := client.ListFiles(context.Background(), FileListOptions{PageSize: 1001}); err == nil {
		t.Fatal("accepted an oversized page")
	}
	if _, err := client.UpdateFile(context.Background(), "file-1", FilePatch{}, nil, nil); err == nil {
		t.Fatal("accepted an empty update")
	}
	if _, err := client.GetFile(context.Background(), "file-1"); !errors.Is(err, ErrTemporary) {
		t.Fatalf("timed request error = %v, want ErrTemporary", err)
	}
}

func newClientForServer(t *testing.T, server *httptest.Server, config Config) *Client {
	t.Helper()
	config.BaseURL = server.URL + "/drive/v3"
	config.UploadURL = server.URL + "/upload/drive/v3"
	if config.HTTPClient == nil {
		config.HTTPClient = server.Client()
	}
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newFakeServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &httptest.Server{
		Config:   &http.Server{Handler: handler},
		Listener: listener,
	}
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func serverURL(request *http.Request) string {
	return "http://" + request.Host
}
