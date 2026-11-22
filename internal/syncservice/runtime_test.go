package syncservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google-drive-linux/internal/auth"
	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

func TestRuntimeIndexesDriveAndAppliesCacheLimit(t *testing.T) {
	database := filepath.Join(t.TempDir(), "metadata.sqlite3")
	runtime, err := New(Config{
		Database: database, ContentRoot: filepath.Join(t.TempDir(), "content"),
		CacheLimit: 64 << 20, Drive: fakeDrive{},
		PollInterval: 20 * time.Millisecond, WorkInterval: 10 * time.Millisecond, PinInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status := runtime.Status()
		if status.NetworkState == "online" && status.IndexState == "ready" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	status := runtime.Status()
	if status.NetworkState != "online" || status.IndexState != "ready" {
		t.Fatalf("runtime did not complete initial index: %+v", status)
	}
	if err := runtime.SetCacheLimit(128 << 20); err != nil {
		t.Fatal(err)
	}
	if got := runtime.Cache().MaxBytes(); got != 128<<20 {
		t.Fatalf("cache limit = %d, want %d", got, 128<<20)
	}
}

func TestNetworkStateRequiresAuthorizationAfterInvalidGrant(t *testing.T) {
	if got := networkState(auth.ErrReauthorizationRequired); got != "reauthorization-required" {
		t.Fatalf("network state = %q, want reauthorization-required", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRuntimeSurfacesReauthorizationRequiredAndRecoversOnNotifyAuthChanged(t *testing.T) {
	ctx := context.Background()
	database := filepath.Join(t.TempDir(), "metadata.sqlite3")
	metadata, err := store.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.BeginInitialIndex(ctx, "snapshot-0", "root-id"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-id", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-0"); err != nil {
		t.Fatal(err)
	}
	folder, op, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "retry-folder")
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.SetOperationState(ctx, op.ID, domain.OperationRetryableError,
		"authorization_required", "Sign in again", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	_ = folder
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	var authorized atomic.Bool
	var createdFolder atomic.Bool
	client, err := driveapi.NewClient(driveapi.Config{
		BaseURL:   "https://drive.example.invalid/drive/v3",
		UploadURL: "https://upload.example.invalid/upload/drive/v3",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if !authorized.Load() {
					return nil, fmt.Errorf("oauth2: %w", auth.ErrReauthorizationRequired)
				}
				status := http.StatusOK
				body := `{}`
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/drive/v3/changes/startPageToken":
					body = `{"startPageToken":"snapshot-1"}`
				case req.Method == http.MethodGet && req.URL.Path == "/drive/v3/files/root":
					body = `{"id":"root-id","name":"My Drive","mimeType":"application/vnd.google-apps.folder"}`
				case req.Method == http.MethodGet && req.URL.Path == "/drive/v3/files/generateIds":
					body = `{"ids":["reserved-retry-1"]}`
				case req.Method == http.MethodGet && req.URL.Path == "/drive/v3/files/reserved-retry-1":
					if !createdFolder.Load() {
						status = http.StatusNotFound
						body = `{"error":{"code":404,"errors":[{"reason":"notFound"}]}}`
					} else {
						body = fmt.Sprintf(`{"id":"reserved-retry-1","name":"retry-folder","mimeType":"application/vnd.google-apps.folder","parents":["root-id"],"size":"0","version":"1","appProperties":{"gdlOperationId":%q}}`, op.ID)
					}
				case req.Method == http.MethodPost && req.URL.Path == "/drive/v3/files":
					createdFolder.Store(true)
					body = fmt.Sprintf(`{"id":"reserved-retry-1","name":"retry-folder","mimeType":"application/vnd.google-apps.folder","parents":["root-id"],"size":"0","version":"1","appProperties":{"gdlOperationId":%q}}`, op.ID)
				case req.Method == http.MethodGet && req.URL.Path == "/drive/v3/files":
					body = `{"files":[]}`
				case req.Method == http.MethodGet && req.URL.Path == "/drive/v3/changes":
					body = `{"newStartPageToken":"live-1"}`
				}
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Content-Type": {"application/json"}},
					Body:       io.NopCloser(strings.NewReader(body)),
				}, nil
			}),
		},
		Retry: driveapi.RetryPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(Config{
		Database: database, ContentRoot: filepath.Join(t.TempDir(), "content"),
		CacheLimit: 64 << 20, Drive: client,
		PollInterval: 30 * time.Second, WorkInterval: 30 * time.Second, PinInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.Status().NetworkState == "reauthorization-required" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status := runtime.Status(); status.NetworkState != "reauthorization-required" {
		t.Fatalf("network status before reauth = %+v, want reauthorization-required", status)
	}

	authorized.Store(true)
	runtime.NotifyAuthChanged()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status := runtime.Status()
		updatedOp, opErr := runtime.metadata.OperationByID(context.Background(), op.ID)
		if status.NetworkState == "online" && status.IndexState == "ready" && opErr == nil && updatedOp.State == domain.OperationCommitted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status := runtime.Status(); status.NetworkState != "online" || status.IndexState != "ready" {
		t.Fatalf("runtime did not recover immediately after NotifyAuthChanged: %+v", status)
	}
	updatedOp, err := runtime.metadata.OperationByID(context.Background(), op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updatedOp.State != domain.OperationCommitted || !updatedOp.NextAttemptAt.IsZero() {
		t.Fatalf("authorization_required operation did not recover after NotifyAuthChanged: %+v", updatedOp)
	}
}

type fakeDrive struct{}

func (fakeDrive) ListFiles(context.Context, driveapi.FileListOptions) (driveapi.FileList, error) {
	return driveapi.FileList{}, nil
}

func (fakeDrive) GetFile(_ context.Context, fileID string) (driveapi.File, error) {
	if fileID != "root" {
		return driveapi.File{}, errors.New("unexpected file ID")
	}
	return driveapi.File{ID: "root-id", Name: "My Drive", MIMEType: "application/vnd.google-apps.folder"}, nil
}

func (fakeDrive) CreateFile(context.Context, driveapi.FileMetadata) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected create")
}

func (fakeDrive) UpdateFile(context.Context, string, driveapi.FilePatch, []string, []string) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected update")
}

func (fakeDrive) DownloadFile(context.Context, string) (driveapi.Download, error) {
	return driveapi.Download{}, errors.New("unexpected download")
}

func (fakeDrive) TrashFile(context.Context, string) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected trash")
}

func (fakeDrive) MoveRename(context.Context, string, string, string, string) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected move")
}

func (fakeDrive) GetStartPageToken(context.Context) (string, error) { return "snapshot", nil }

func (fakeDrive) ListChanges(context.Context, string) (driveapi.ChangeList, error) {
	return driveapi.ChangeList{NewStartPageToken: "live"}, nil
}

func (fakeDrive) GenerateIDs(context.Context, int) ([]string, error) {
	return nil, errors.New("unexpected ID generation")
}

func (fakeDrive) StartResumableUpload(context.Context, driveapi.UploadRequest) (string, error) {
	return "", errors.New("unexpected upload")
}

func (fakeDrive) UploadChunk(context.Context, string, driveapi.UploadChunkRequest) (driveapi.UploadProgress, error) {
	return driveapi.UploadProgress{}, errors.New("unexpected upload chunk")
}

func (fakeDrive) QueryUploadStatus(context.Context, string, int64) (driveapi.UploadProgress, error) {
	return driveapi.UploadProgress{}, errors.New("unexpected upload status")
}
