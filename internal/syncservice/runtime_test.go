package syncservice

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"google-drive-linux/internal/auth"
	"google-drive-linux/internal/driveapi"
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
