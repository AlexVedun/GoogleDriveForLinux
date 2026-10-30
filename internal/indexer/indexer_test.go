package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

func TestInitialIndexWalksThousandsOfFilesThenCatchesUpBeforeReady(t *testing.T) {
	metadata, err := store.Open(t.TempDir() + "/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()

	files := make([]driveapi.File, 0, 2008)
	for index := range 2005 {
		name := fmt.Sprintf("file-%04d.txt", index)
		if index == 4 || index == 5 {
			name = "duplicate.txt"
		}
		files = append(files, remoteFile(fmt.Sprintf("file-%04d", index), name, "text/plain", "root-drive", int64(index)))
	}
	longName := strings.Repeat("long", 45) + ".txt"
	shared := remoteFile("shared-drive-file", "shared.txt", "text/plain", "root-drive", 1)
	shared.DriveID = "shared-drive-id"
	outside := remoteFile("outside-file", "outside.txt", "text/plain", "somewhere-else", 1)
	trashed := remoteFile("trashed-file", "trashed.txt", "text/plain", "root-drive", 1)
	trashed.Trashed = true
	files = append(files,
		remoteFile("empty-folder", "empty", driveFolderMIMEType, "root-drive", 0),
		remoteFile("long-file", longName, "text/plain", "root-drive", 1),
		remoteFile("slash-name", "a/b.txt", "text/plain", "root-drive", 1),
		remoteFile("dot-name", ".", "text/plain", "root-drive", 1),
		remoteFile("dotdot-name", "..", "text/plain", "root-drive", 1),
		remoteFile("nul-name", "bad\x00name.txt", "text/plain", "root-drive", 1),
		shared, outside, trashed,
	)

	newFile := remoteFile("late-file", "late.txt", "text/plain", "root-drive", 9)
	newFolder := remoteFile("late-folder", "late-folder", driveFolderMIMEType, "root-drive", 0)
	changed := remoteFile("file-0002", "updated.txt", "text/plain", "root-drive", 77)
	changed.Version = "2"
	changed.WebViewLink = "https://drive.google.com/file/d/file-0002"
	changed.Capabilities.CanEdit = true
	nested := remoteFile("nested-late", "nested.txt", "text/plain", "late-folder", 3)
	moved := remoteFile("file-0003", "moved.txt", "text/plain", "late-folder", 5)
	drive := &fakeDrive{
		root: driveapi.File{ID: "root-drive", Name: "My Drive", MIMEType: driveFolderMIMEType},
		filesByParent: map[string][]driveapi.File{
			"root-drive":   files,
			"empty-folder": nil,
			"late-folder":  {nested, moved},
		},
		pageSize: 300,
		changePages: map[string]driveapi.ChangeList{
			"snapshot-token": {
				Changes: []driveapi.Change{
					{FileID: "late-file", File: &newFile},
					{FileID: "late-folder", File: &newFolder},
					{FileID: "file-0002", File: &changed},
				},
				NextPageToken: "catchup-page-2",
			},
			"catchup-page-2": {
				Changes: []driveapi.Change{
					{FileID: "nested-late", File: &nested},
					{FileID: "file-0001", Removed: true},
					{FileID: "file-0003", File: &moved},
				},
				NewStartPageToken: "live-token",
			},
			"live-token": {NewStartPageToken: "next-token"},
		},
	}
	idx, err := New(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	status, err := metadata.InitialIndexStatus(context.Background())
	if err != nil || status.State != "ready" || status.Phase != "ready" || status.ChangeToken != "next-token" || status.SnapshotToken != "" {
		t.Fatalf("initial index status = %#v, err = %v", status, err)
	}
	if len(drive.calls) < 2 || drive.calls[0] != "start-token" || drive.calls[1] != "get-root" {
		t.Fatalf("Drive call order = %#v, want start token before root/list", drive.calls[:min(2, len(drive.calls))])
	}
	if drive.listCalls < 8 {
		t.Fatalf("file list calls = %d, want multiple pages for thousands of files and folders", drive.listCalls)
	}
	if drive.changeCalls != 3 {
		t.Fatalf("change calls = %d, want two catch-up pages and a post-folder scan catch-up", drive.changeCalls)
	}

	rootChildren, err := metadata.Children(context.Background(), domain.RootObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootChildren) != 2005+2+4 {
		t.Fatalf("visible root child count = %d, want 2011 after deletion and move", len(rootChildren))
	}
	for _, child := range rootChildren {
		if child.ObjectID == "drive:shared-drive-file" || child.ObjectID == "drive:outside-file" || child.ObjectID == "drive:trashed-file" || child.ObjectID == "drive:file-0001" || child.ObjectID == "drive:file-0003" {
			t.Errorf("excluded/deleted/moved object remains in root: %#v", child)
		}
	}
	duplicates := filterRemoteName(rootChildren, "duplicate.txt")
	if len(duplicates) != 2 || duplicates[0].DisplayName == duplicates[1].DisplayName || !strings.Contains(duplicates[0].DisplayName, "(") || !strings.Contains(duplicates[1].DisplayName, "(") {
		t.Fatalf("duplicate visible names = %#v, want stable unique suffixes", duplicates)
	}
	longEntries := filterRemoteName(rootChildren, longName)
	if len(longEntries) != 1 || longEntries[0].DisplayName != longName {
		t.Fatalf("long filename entry = %#v", longEntries)
	}
	for remote, display := range map[string]string{
		"a/b.txt":         "a／b.txt",
		".":               "．",
		"..":              "．．",
		"bad\x00name.txt": "bad�name.txt",
	} {
		entries := filterRemoteName(rootChildren, remote)
		if len(entries) != 1 || entries[0].DisplayName != display {
			t.Errorf("unsafe remote name %q maps to %#v, want display %q", remote, entries, display)
		}
	}
	if _, err := metadata.ObjectByID(context.Background(), "drive:shared-drive-file"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("shared-drive object lookup error = %v, want absent", err)
	}
	if _, err := metadata.ObjectByID(context.Background(), "drive:outside-file"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("out-of-tree object lookup error = %v, want absent", err)
	}
	deleted, err := metadata.ObjectByID(context.Background(), "drive:file-0001")
	if err != nil || !deleted.Deleted {
		t.Fatalf("deleted catch-up object = %#v, err = %v", deleted, err)
	}
	updated, err := metadata.ObjectByID(context.Background(), "drive:file-0002")
	if err != nil || updated.Size != 77 || updated.DriveVersion != "2" || !updated.ModifiedAt.Equal(changed.ModifiedTime) || !strings.Contains(updated.CapabilitiesJSON, "canEdit") {
		t.Fatalf("updated catch-up metadata = %#v, err = %v", updated, err)
	}

	emptyFolderChildren, err := metadata.Children(context.Background(), "drive:empty-folder")
	if err != nil || len(emptyFolderChildren) != 0 {
		t.Fatalf("empty folder children = %#v, err = %v", emptyFolderChildren, err)
	}
	lateFolderChildren, err := metadata.Children(context.Background(), "drive:late-folder")
	if err != nil || len(lateFolderChildren) != 2 {
		t.Fatalf("late folder children = %#v, err = %v", lateFolderChildren, err)
	}
	if _, err := metadata.DirectoryEntry(context.Background(), "drive:late-folder", "moved.txt"); err != nil {
		t.Fatalf("moved file was not placed under its new parent: %v", err)
	}
	if _, err := metadata.DirectoryEntry(context.Background(), domain.RootObjectID, "moved.txt"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("old moved path lookup error = %v, want absent", err)
	}

	before := append([]domain.DirectoryEntry(nil), rootChildren...)
	beforeCalls := len(drive.calls)
	if err := idx.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := metadata.Children(context.Background(), domain.RootObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || len(drive.calls) != beforeCalls {
		t.Fatalf("ready index rerun changed state: children %d->%d, API calls %d->%d", len(before), len(after), beforeCalls, len(drive.calls))
	}
	for index := range before {
		if before[index] != after[index] {
			t.Fatalf("root entry %d changed after rerun: %#v -> %#v", index, before[index], after[index])
		}
	}
}

func TestInterruptedScanResumesSavedPageAndCatchupCursor(t *testing.T) {
	for _, failedPageToken := range []string{"", "root-drive:2", "root-drive:4"} {
		t.Run("page_"+strings.ReplaceAll(failedPageToken, ":", "_"), func(t *testing.T) {
			databasePath := t.TempDir() + "/metadata.db"
			metadata, err := store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if metadata != nil {
					_ = metadata.Close()
				}
			}()
			files := []driveapi.File{
				remoteFile("first", "first.txt", "text/plain", "root-drive", 1),
				remoteFile("second", "second.txt", "text/plain", "root-drive", 2),
				remoteFile("third", "third.txt", "text/plain", "root-drive", 3),
				remoteFile("fourth", "fourth.txt", "text/plain", "root-drive", 4),
				remoteFile("fifth", "fifth.txt", "text/plain", "root-drive", 5),
				remoteFile("sixth", "sixth.txt", "text/plain", "root-drive", 6),
			}
			drive := &fakeDrive{
				root:          driveapi.File{ID: "root-drive", Name: "My Drive", MIMEType: driveFolderMIMEType},
				filesByParent: map[string][]driveapi.File{"root-drive": files},
				pageSize:      2,
				failOnce:      map[string]error{"root-drive|" + failedPageToken: errors.New("injected page interruption")},
				changePages: map[string]driveapi.ChangeList{
					"snapshot-token": {NextPageToken: "changes-page-2"},
					"changes-page-2": {NewStartPageToken: "live-token"},
				},
				changeFailOnce: map[string]error{"snapshot-token": errors.New("injected catch-up interruption")},
			}
			idx, err := New(drive, metadata)
			if err != nil {
				t.Fatal(err)
			}

			if err := idx.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "injected page interruption") {
				t.Fatalf("first Run() error = %v, want injected page interruption", err)
			}
			status, _ := metadata.InitialIndexStatus(context.Background())
			folders, err := metadata.InitialIndexFolders(context.Background())
			if err != nil || status.State != "indexing" || status.Phase != "scanning" || len(folders) != 1 || folders[0].PageToken != failedPageToken {
				t.Fatalf("saved scan progress = %#v folders=%#v err=%v", status, folders, err)
			}
			startCalls, rootCalls := drive.startCalls, drive.rootCalls
			if err := metadata.Close(); err != nil {
				t.Fatal(err)
			}
			metadata = nil
			metadata, err = store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			idx, err = New(drive, metadata)
			if err != nil {
				t.Fatal(err)
			}

			if err := idx.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "injected catch-up interruption") {
				t.Fatalf("resumed Run() error = %v, want injected catch-up interruption", err)
			}
			status, _ = metadata.InitialIndexStatus(context.Background())
			if status.State != "indexing" || status.Phase != "catching-up" || status.CatchupPageToken != "snapshot-token" {
				t.Fatalf("saved catch-up progress = %#v", status)
			}
			if drive.startCalls != startCalls || drive.rootCalls != rootCalls {
				t.Fatalf("resume recaptured snapshot or root: start calls %d->%d root %d->%d", startCalls, drive.startCalls, rootCalls, drive.rootCalls)
			}
			if err := metadata.Close(); err != nil {
				t.Fatal(err)
			}
			metadata = nil
			metadata, err = store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			idx, err = New(drive, metadata)
			if err != nil {
				t.Fatal(err)
			}

			if err := idx.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			status, _ = metadata.InitialIndexStatus(context.Background())
			if status.State != "ready" || status.ChangeToken != "live-token" {
				t.Fatalf("resumed final status = %#v", status)
			}
			children, err := metadata.Children(context.Background(), domain.RootObjectID)
			if err != nil || len(children) != 6 {
				t.Fatalf("resumed root children = %d, err = %v", len(children), err)
			}
		})
	}
}

func TestStartPageTokenIsRequiredBeforeAnyTraversal(t *testing.T) {
	metadata, err := store.Open(t.TempDir() + "/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	drive := &fakeDrive{
		root:          driveapi.File{ID: "root-drive", Name: "My Drive", MIMEType: driveFolderMIMEType},
		filesByParent: map[string][]driveapi.File{"root-drive": nil},
		changePages:   map[string]driveapi.ChangeList{"snapshot-token": {NewStartPageToken: "live-token"}},
	}
	drive.emptyStartToken = true
	idx, err := New(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Run(context.Background()); err == nil {
		t.Fatal("accepted an empty snapshot token")
	}
	if len(drive.calls) != 1 || drive.calls[0] != "start-token" || drive.listCalls != 0 {
		t.Fatalf("empty token still traversed Drive: calls=%#v lists=%d", drive.calls, drive.listCalls)
	}
}

func TestRejectedSavedPageTokenRestartsFolderWithoutDuplicateObjects(t *testing.T) {
	metadata, err := store.Open(t.TempDir() + "/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	drive := &fakeDrive{
		root: driveapi.File{ID: "root-drive", Name: "My Drive", MIMEType: driveFolderMIMEType},
		filesByParent: map[string][]driveapi.File{"root-drive": {
			remoteFile("first", "first.txt", "text/plain", "root-drive", 1),
			remoteFile("second", "second.txt", "text/plain", "root-drive", 2),
			remoteFile("third", "third.txt", "text/plain", "root-drive", 3),
			remoteFile("fourth", "fourth.txt", "text/plain", "root-drive", 4),
		}},
		pageSize: 2,
		failOnce: map[string]error{
			"root-drive|root-drive:2": &driveapi.APIError{StatusCode: 400, Kind: driveapi.FailureInvalidRequest},
		},
		changePages: map[string]driveapi.ChangeList{"snapshot-token": {NewStartPageToken: "live-token"}},
	}
	idx, err := New(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := metadata.InitialIndexStatus(context.Background())
	if err != nil || status.State != "ready" || status.ChangeToken != "live-token" {
		t.Fatalf("index status after rejected token = %#v, err = %v", status, err)
	}
	children, err := metadata.Children(context.Background(), domain.RootObjectID)
	if err != nil || len(children) != 4 {
		t.Fatalf("children after safe replay = %d, err = %v", len(children), err)
	}
	if drive.startCalls != 1 || drive.listCalls < 4 {
		t.Fatalf("start calls=%d list calls=%d; want one snapshot and a replay from page one", drive.startCalls, drive.listCalls)
	}
	for _, id := range []string{"first", "second", "third", "fourth"} {
		if _, err := metadata.ObjectByID(context.Background(), domain.ObjectID("drive:"+id)); err != nil {
			t.Errorf("object %q missing or duplicated after replay: %v", id, err)
		}
	}
}

func remoteFile(id, name, mimeType, parent string, size int64) driveapi.File {
	return driveapi.File{
		ID: id, Name: name, MIMEType: mimeType, Parents: []string{parent}, Size: size,
		Version: "1", ModifiedTime: time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC),
		WebViewLink: "https://drive.google.com/file/d/" + id,
	}
}

func filterRemoteName(entries []domain.DirectoryEntry, remoteName string) []domain.DirectoryEntry {
	var filtered []domain.DirectoryEntry
	for _, entry := range entries {
		if entry.RemoteName == remoteName {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

type fakeDrive struct {
	mu              sync.Mutex
	root            driveapi.File
	filesByParent   map[string][]driveapi.File
	pageSize        int
	changePages     map[string]driveapi.ChangeList
	failOnce        map[string]error
	changeFailOnce  map[string]error
	calls           []string
	startCalls      int
	rootCalls       int
	listCalls       int
	changeCalls     int
	emptyStartToken bool
}

func (f *fakeDrive) ListFiles(ctx context.Context, options driveapi.FileListOptions) (driveapi.FileList, error) {
	if err := ctx.Err(); err != nil {
		return driveapi.FileList{}, err
	}
	parent := strings.TrimPrefix(options.Query, "'")
	parent, _, _ = strings.Cut(parent, "' in parents")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	f.calls = append(f.calls, "list:"+parent+":"+options.PageToken)
	if err := f.failOnce[parent+"|"+options.PageToken]; err != nil {
		delete(f.failOnce, parent+"|"+options.PageToken)
		return driveapi.FileList{}, err
	}
	files := f.filesByParent[parent]
	pageSize := f.pageSize
	if pageSize <= 0 {
		pageSize = len(files)
	}
	start := 0
	if options.PageToken != "" {
		var parsedParent string
		separator := strings.LastIndex(options.PageToken, ":")
		if separator <= 0 {
			return driveapi.FileList{}, fmt.Errorf("invalid fake page token %q", options.PageToken)
		}
		parsedParent = options.PageToken[:separator]
		parsedOffset, parseErr := strconv.Atoi(options.PageToken[separator+1:])
		if parseErr != nil || parsedParent != parent {
			return driveapi.FileList{}, fmt.Errorf("invalid fake page token %q", options.PageToken)
		}
		start = parsedOffset
	}
	if start >= len(files) {
		return driveapi.FileList{}, nil
	}
	end := min(start+pageSize, len(files))
	page := driveapi.FileList{Files: append([]driveapi.File(nil), files[start:end]...)}
	if end < len(files) {
		page.NextPageToken = fmt.Sprintf("%s:%d", parent, end)
	}
	return page, nil
}

func (f *fakeDrive) GetFile(ctx context.Context, fileID string) (driveapi.File, error) {
	if err := ctx.Err(); err != nil {
		return driveapi.File{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if fileID == "root" {
		f.rootCalls++
		f.calls = append(f.calls, "get-root")
		return f.root, nil
	}
	return driveapi.File{}, sql.ErrNoRows
}

func (f *fakeDrive) CreateFile(context.Context, driveapi.FileMetadata) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected CreateFile call")
}
func (f *fakeDrive) UpdateFile(context.Context, string, driveapi.FilePatch, []string, []string) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected UpdateFile call")
}
func (f *fakeDrive) DownloadFile(context.Context, string) (driveapi.Download, error) {
	return driveapi.Download{}, errors.New("unexpected DownloadFile call")
}
func (f *fakeDrive) TrashFile(context.Context, string) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected TrashFile call")
}
func (f *fakeDrive) MoveRename(context.Context, string, string, string, string) (driveapi.File, error) {
	return driveapi.File{}, errors.New("unexpected MoveRename call")
}
func (f *fakeDrive) GenerateIDs(context.Context, int) ([]string, error) {
	return nil, errors.New("unexpected GenerateIDs call")
}
func (f *fakeDrive) StartResumableUpload(context.Context, driveapi.UploadRequest) (string, error) {
	return "", errors.New("unexpected StartResumableUpload call")
}
func (f *fakeDrive) UploadChunk(context.Context, string, driveapi.UploadChunkRequest) (driveapi.UploadProgress, error) {
	return driveapi.UploadProgress{}, errors.New("unexpected UploadChunk call")
}
func (f *fakeDrive) QueryUploadStatus(context.Context, string, int64) (driveapi.UploadProgress, error) {
	return driveapi.UploadProgress{}, errors.New("unexpected QueryUploadStatus call")
}

func (f *fakeDrive) GetStartPageToken(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startCalls++
	f.calls = append(f.calls, "start-token")
	if f.emptyStartToken {
		return "", nil
	}
	return "snapshot-token", nil
}

func (f *fakeDrive) ListChanges(ctx context.Context, pageToken string) (driveapi.ChangeList, error) {
	if err := ctx.Err(); err != nil {
		return driveapi.ChangeList{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.changeCalls++
	f.calls = append(f.calls, "changes:"+pageToken)
	if err := f.changeFailOnce[pageToken]; err != nil {
		delete(f.changeFailOnce, pageToken)
		return driveapi.ChangeList{}, err
	}
	page, exists := f.changePages[pageToken]
	if !exists {
		return driveapi.ChangeList{}, fmt.Errorf("unexpected changes token %q", pageToken)
	}
	return page, nil
}
