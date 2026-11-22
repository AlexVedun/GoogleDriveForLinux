package contentcache

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

func TestOpenStreamsLargeContentByOffsetAndSharesImmutableCache(t *testing.T) {
	ctx := context.Background()
	metadata, content := cacheFixture(t, "object-large", "version-1", 9<<20)
	defer metadata.Close()
	payload := make([]byte, 9<<20)
	for index := range payload {
		payload[index] = byte(index*31 + index/101)
	}
	remote := remoteForObject("file-object-large", "version-1", payload)
	drive := &cacheFakeDrive{files: []driveapi.File{remote}, payload: payload}
	cache, err := New(drive, metadata, content, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	object, err := metadata.ObjectByID(ctx, "object-large")
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.Open(ctx, object)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Open(ctx, object)
	if err != nil {
		t.Fatal(err)
	}
	if first.Size() != int64(len(payload)) || second.Size() != int64(len(payload)) {
		t.Fatalf("opened sizes = %d and %d; want %d", first.Size(), second.Size(), len(payload))
	}
	for _, offset := range []int64{0, 17, 1<<20 + 37, int64(len(payload)) - 1000} {
		readBuffer := make([]byte, 1000)
		read, err := first.ReadAt(readBuffer, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("read offset %d: %v", offset, err)
		}
		if !bytes.Equal(readBuffer[:read], payload[offset:int(offset)+read]) {
			t.Fatalf("read at offset %d returned wrong bytes", offset)
		}
	}

	var wait sync.WaitGroup
	errCh := make(chan error, 8)
	for reader := range 8 {
		wait.Add(1)
		go func(reader int) {
			defer wait.Done()
			buffer := make([]byte, 4096)
			for offset := int64(reader * 71); offset < int64(len(payload)); offset += 1 << 18 {
				n, err := second.ReadAt(buffer, offset)
				if err != nil && !errors.Is(err, io.EOF) {
					errCh <- err
					return
				}
				if !bytes.Equal(buffer[:n], payload[offset:int(offset)+n]) {
					errCh <- fmt.Errorf("reader %d got incorrect data at offset %d", reader, offset)
					return
				}
			}
		}(reader)
	}
	wait.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	_ = first.Close()
	_ = second.Close()
	if drive.downloadCalls != 1 || drive.getCalls != 2 {
		t.Fatalf("Drive calls = download %d, metadata %d; want 1 and 2", drive.downloadCalls, drive.getCalls)
	}
}

func TestFailedOrChangedDownloadLeavesNoPartialCache(t *testing.T) {
	t.Run("interrupted body", func(t *testing.T) {
		metadata, content := cacheFixture(t, "object-broken", "version-1", 4096)
		defer metadata.Close()
		payload := bytes.Repeat([]byte("x"), 4096)
		drive := &cacheFakeDrive{files: []driveapi.File{remoteForObject("file-object-broken", "version-1", payload)},
			body: &brokenBody{source: bytes.NewReader(payload), failAfter: 1500}, bodyLength: int64(len(payload))}
		cache, _ := New(drive, metadata, content, 1<<20)
		object, _ := metadata.ObjectByID(context.Background(), "object-broken")
		if _, err := cache.Open(context.Background(), object); err == nil {
			t.Fatal("Open succeeded after a mid-download read failure")
		}
		assertNoCachedContent(t, metadata, content)
	})

	t.Run("remote version changed", func(t *testing.T) {
		metadata, content := cacheFixture(t, "object-race", "version-1", 4)
		defer metadata.Close()
		before := remoteForObject("file-race", "version-1", []byte("old!"))
		after := remoteForObject("file-race", "version-2", []byte("new!"))
		drive := &cacheFakeDrive{fileByID: map[string][]driveapi.File{"file-object-race": {before, after}}, payload: []byte("old!")}
		cache, _ := New(drive, metadata, content, 1<<20)
		object, _ := metadata.ObjectByID(context.Background(), "object-race")
		if _, err := cache.Open(context.Background(), object); !errors.Is(err, ErrStaleRemoteVersion) {
			t.Fatalf("Open error = %v, want ErrStaleRemoteVersion", err)
		}
		assertNoCachedContent(t, metadata, content)
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		metadata, content := cacheFixture(t, "object-checksum", "version-1", 4)
		defer metadata.Close()
		remote := remoteForObject("file-object-checksum", "version-1", []byte("good"))
		drive := &cacheFakeDrive{files: []driveapi.File{remote}, payload: []byte("evil")}
		cache, _ := New(drive, metadata, content, 1<<20)
		object, _ := metadata.ObjectByID(context.Background(), "object-checksum")
		if _, err := cache.Open(context.Background(), object); err == nil {
			t.Fatal("Open accepted bytes with the wrong MD5 checksum")
		}
		assertNoCachedContent(t, metadata, content)
	})
}

func TestOfflineOpenUsesOnlyVerifiedCompleteCache(t *testing.T) {
	ctx := context.Background()
	metadata, content := cacheFixture(t, "object-offline", "version-1", len("trusted"))
	defer metadata.Close()
	payload := []byte("trusted")
	drive := &cacheFakeDrive{files: []driveapi.File{remoteForObject("file-object-offline", "version-1", payload)}, payload: payload}
	cache, err := New(drive, metadata, content, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	object, _ := metadata.ObjectByID(ctx, "object-offline")
	first, err := cache.Open(ctx, object)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	version, err := metadata.FindCachedContentVersion(ctx, object.ID, object.DriveVersion)
	if err != nil {
		t.Fatal(err)
	}
	drive.getError = errors.New("network unavailable")
	offline, err := cache.Open(ctx, object)
	if err != nil {
		t.Fatalf("open verified cache while offline: %v", err)
	}
	if got := string(readAllAt(t, offline)); got != "trusted" {
		t.Fatalf("offline cached bytes = %q", got)
	}
	_ = offline.Close()

	if err := os.WriteFile(filepath.Join(content.Paths().Root, filepath.FromSlash(version.LocalPath)), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Open(ctx, object); err == nil {
		t.Fatal("offline open served a truncated cache entry")
	}
	assertNoCachedContent(t, metadata, content)
}

func TestCacheEvictionHonorsQuotaAndOpenHandles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	metadata, err := store.Open(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	content, err := store.NewContentStore(metadata, filepath.Join(root, "content"))
	if err != nil {
		t.Fatal(err)
	}
	firstObject := createCacheObject(t, metadata, "object-first", "file-first", "first-v", []byte("123"))
	secondObject := createCacheObject(t, metadata, "object-second", "file-second", "second-v", []byte("4567"))
	drive := &cacheFakeDrive{files: []driveapi.File{
		remoteForObject("file-first", "first-v", []byte("123")),
		remoteForObject("file-second", "second-v", []byte("4567")),
	}, payloadByID: map[string][]byte{"file-first": []byte("123"), "file-second": []byte("4567")}}
	cache, err := New(drive, metadata, content, 5)
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.Open(ctx, firstObject)
	if err != nil {
		t.Fatal(err)
	}
	firstVersion, err := metadata.FindCachedContentVersion(ctx, firstObject.ID, firstObject.DriveVersion)
	if err != nil {
		t.Fatal(err)
	}

	second, err := cache.Open(ctx, secondObject)
	if err != nil {
		t.Fatal(err)
	}
	if bytesRead := readAllAt(t, second); string(bytesRead) != "4567" {
		t.Fatalf("second object data = %q", bytesRead)
	}
	if _, err := metadata.FindCachedContentVersion(ctx, firstObject.ID, firstObject.DriveVersion); err != nil {
		t.Fatalf("open cached descriptor was evicted while active: %v", err)
	}
	if _, err := os.Stat(filepath.Join(content.Paths().Transient, ".missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected transient staging state: %v", err)
	}
	_ = second.Close()
	_ = first.Close()

	thirdObject := createCacheObject(t, metadata, "object-third", "file-third", "third-v", []byte("891"))
	drive.files = append(drive.files, remoteForObject("file-third", "third-v", []byte("891")))
	drive.payloadByID["file-third"] = []byte("891")
	third, err := cache.Open(ctx, thirdObject)
	if err != nil {
		t.Fatal(err)
	}
	_ = third.Close()
	if _, err := metadata.FindCachedContentVersion(ctx, firstObject.ID, firstObject.DriveVersion); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("oldest unpinned cache entry still exists after quota eviction: %v", err)
	}
	if _, err := metadata.FindCachedContentVersion(ctx, secondObject.ID, secondObject.DriveVersion); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second cache entry should be evicted to fit boundary entry: %v", err)
	}
	if _, err := metadata.FindCachedContentVersion(ctx, thirdObject.ID, thirdObject.DriveVersion); err != nil {
		t.Fatalf("boundary cache entry missing: %v", err)
	}
	used, err := metadata.CachedContentBytes(ctx)
	if err != nil || used != 3 {
		t.Fatalf("cached bytes = %d, err = %v; want 3", used, err)
	}
	if _, err := os.Stat(filepath.Join(content.Paths().Transient, filepath.Base(firstVersion.LocalPath))); err == nil {
		t.Fatal("cache eviction unexpectedly left a transient copy")
	}
}

func TestCacheLimitCanBeUpdatedWhileInUse(t *testing.T) {
	metadata, content := cacheFixture(t, "object-cache-limit", "version-cache-limit", 0)
	defer metadata.Close()
	drive := &cacheFakeDrive{}
	cache, err := New(drive, metadata, content, 256)
	if err != nil {
		t.Fatal(err)
	}
	if got := cache.MaxBytes(); got != 256 {
		t.Fatalf("initial cache limit = %d", got)
	}
	if err := cache.SetMaxBytes(512); err != nil {
		t.Fatal(err)
	}
	if got := cache.MaxBytes(); got != 512 {
		t.Fatalf("updated cache limit = %d", got)
	}
	if err := cache.SetMaxBytes(-1); err == nil {
		t.Fatal("negative cache limit was accepted")
	}
}

func TestPinManagerDownloadsOutsideCacheLimitAndReportsProgress(t *testing.T) {
	ctx := context.Background()
	metadata, content := cacheFixture(t, "object-pinned", "pinned-v1", 3)
	defer metadata.Close()
	payload := []byte("pin")
	drive := &cacheFakeDrive{files: []driveapi.File{remoteForObject("file-object-pinned", "pinned-v1", payload)}, payload: payload}
	cache, err := New(drive, metadata, content, 1)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewPinManager(metadata, cache)
	if err != nil {
		t.Fatal(err)
	}
	object, err := metadata.ObjectByID(ctx, "object-pinned")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Pin(ctx, object.ID, false); err != nil {
		t.Fatal(err)
	}
	var progress []PinProgress
	if err := manager.Sync(ctx, func(value PinProgress) { progress = append(progress, value) }); err != nil {
		t.Fatal(err)
	}
	if drive.downloadCalls != 1 {
		t.Fatalf("download calls = %d, want 1", drive.downloadCalls)
	}
	version, err := metadata.FindCachedContentVersion(ctx, object.ID, object.DriveVersion)
	if err != nil {
		t.Fatal(err)
	}
	if version.LocalPath != filepath.ToSlash(filepath.Join("cache", "pinned", string(version.ID)+".blob")) {
		t.Fatalf("pinned content path = %q, want pinned cache directory", version.LocalPath)
	}
	file, _, err := content.OpenContentVersion(ctx, version.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || string(got) != string(payload) {
		t.Fatalf("pinned bytes = %q, readErr=%v closeErr=%v", got, readErr, closeErr)
	}
	if len(progress) < 3 || progress[len(progress)-1].State != "ready" ||
		progress[len(progress)-1].CompletedFiles != 1 || progress[len(progress)-1].CompletedBytes != int64(len(payload)) {
		t.Fatalf("pin progress = %#v", progress)
	}
	if overage, err := manager.OverLimitBytes(ctx); err != nil || overage != 2 {
		t.Fatalf("pinned content cache overage = %d, err = %v; want 2", overage, err)
	}
	if err := manager.Unpin(ctx, object.ID); err != nil {
		t.Fatal(err)
	}
	candidates, err := metadata.EvictableCachedVersions(ctx)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("unpinned cached versions = %#v, err = %v", candidates, err)
	}
	if err := content.EvictCachedContent(ctx, candidates[0]); err != nil {
		t.Fatalf("evict unpinned content from its prior pinned directory: %v", err)
	}
}

func cacheFixture(t *testing.T, objectID, version string, size int) (*store.Store, *store.ContentStore) {
	t.Helper()
	root := t.TempDir()
	metadata, err := store.Open(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	driveID := domain.DriveFileID("file-" + objectID)
	if _, err := metadata.CreateObject(context.Background(), domain.Object{
		ID: domain.ObjectID(objectID), DriveFileID: &driveID, MIMEType: "text/plain",
		Size: int64(size), DriveVersion: version, CapabilitiesJSON: "{}",
	}, domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: domain.ObjectID(objectID), RemoteName: "file", DisplayName: "file"}); err != nil {
		_ = metadata.Close()
		t.Fatal(err)
	}
	content, err := store.NewContentStore(metadata, filepath.Join(root, "content"))
	if err != nil {
		_ = metadata.Close()
		t.Fatal(err)
	}
	return metadata, content
}

func createCacheObject(t *testing.T, metadata *store.Store, objectID, fileID, version string, data []byte) domain.Object {
	t.Helper()
	driveID := domain.DriveFileID(fileID)
	object, err := metadata.CreateObject(context.Background(), domain.Object{
		ID: domain.ObjectID(objectID), DriveFileID: &driveID, MIMEType: "text/plain", Size: int64(len(data)),
		DriveVersion: version, CapabilitiesJSON: "{}",
	}, domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: domain.ObjectID(objectID), RemoteName: objectID, DisplayName: objectID})
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func remoteForObject(fileID, version string, data []byte) driveapi.File {
	sum := md5.Sum(data)
	return driveapi.File{ID: fileID, MIMEType: "text/plain", Version: version, Size: int64(len(data)),
		MD5Checksum: hex.EncodeToString(sum[:]), Capabilities: driveapi.Capabilities{CanDownload: true}}
}

func assertNoCachedContent(t *testing.T, metadata *store.Store, content *store.ContentStore) {
	t.Helper()
	used, err := metadata.CachedContentBytes(context.Background())
	if err != nil || used != 0 {
		t.Fatalf("cached bytes = %d, err = %v; want 0", used, err)
	}
	entries, err := os.ReadDir(content.Paths().Transient)
	if err != nil || len(entries) != 0 {
		t.Fatalf("transient entries = %v, err = %v; want empty", entries, err)
	}
	entries, err = os.ReadDir(content.Paths().EvictableCache)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cache entries = %v, err = %v; want empty", entries, err)
	}
}

func readAllAt(t *testing.T, handle *Handle) []byte {
	t.Helper()
	contents := make([]byte, handle.Size())
	count, err := handle.ReadAt(contents, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return contents[:count]
}

type cacheFakeDrive struct {
	driveapi.API
	mu            sync.Mutex
	files         []driveapi.File
	fileByID      map[string][]driveapi.File
	fileReads     map[string]int
	getCalls      int
	downloadCalls int
	payload       []byte
	payloadByID   map[string][]byte
	body          io.ReadCloser
	bodyLength    int64
	getError      error
}

func (d *cacheFakeDrive) GetFile(ctx context.Context, fileID string) (driveapi.File, error) {
	if err := ctx.Err(); err != nil {
		return driveapi.File{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.getError != nil {
		return driveapi.File{}, d.getError
	}
	d.getCalls++
	if d.fileByID != nil {
		sequence := d.fileByID[fileID]
		if len(sequence) == 0 {
			return driveapi.File{}, fmt.Errorf("unexpected Drive file %q", fileID)
		}
		if d.fileReads == nil {
			d.fileReads = make(map[string]int)
		}
		index := min(d.fileReads[fileID], len(sequence)-1)
		d.fileReads[fileID]++
		return sequence[index], nil
	}
	for _, file := range d.files {
		if file.ID == fileID {
			return file, nil
		}
	}
	return driveapi.File{}, fmt.Errorf("unexpected Drive file %q", fileID)
}

func (d *cacheFakeDrive) DownloadFile(ctx context.Context, fileID string) (driveapi.Download, error) {
	if err := ctx.Err(); err != nil {
		return driveapi.Download{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.downloadCalls++
	if d.body != nil {
		return driveapi.Download{Body: d.body, ContentLength: d.bodyLength}, nil
	}
	payload := d.payload
	if d.payloadByID != nil {
		payload = d.payloadByID[fileID]
	}
	return driveapi.Download{Body: io.NopCloser(bytes.NewReader(payload)), ContentLength: int64(len(payload))}, nil
}

type brokenBody struct {
	source    *bytes.Reader
	failAfter int64
	read      int64
}

func (b *brokenBody) Read(buffer []byte) (int, error) {
	if b.read >= b.failAfter {
		return 0, errors.New("injected network loss")
	}
	limit := int64(len(buffer))
	if remaining := b.failAfter - b.read; remaining < limit {
		limit = remaining
	}
	n, err := b.source.Read(buffer[:limit])
	b.read += int64(n)
	return n, err
}

func (b *brokenBody) Close() error { return nil }

func TestOpenDownloadsUpdatedRemoteVersionForLocallyCreatedFile(t *testing.T) {
	for _, viaChangeFeed := range []bool{true, false} {
		name := "retired-by-change-feed"
		if !viaChangeFeed {
			name = "legacy-unretired-row"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			metadata, err := store.Open(filepath.Join(root, "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer metadata.Close()
			if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
				t.Fatal(err)
			}
			if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", nil); err != nil {
				t.Fatal(err)
			}
			if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
				t.Fatal(err)
			}
			content, err := store.NewContentStore(metadata, filepath.Join(root, "content"))
			if err != nil {
				t.Fatal(err)
			}

			localObj, err := metadata.CreateLocalFile(ctx, domain.RootObjectID, "shared.txt", "text/plain")
			if err != nil {
				t.Fatal(err)
			}
			written, err := content.WritePending(ctx, store.WriteRequest{
				ObjectID: localObj.ID, Kind: domain.OperationCreate,
				IdempotencyKey: "create-shared", ExpectedSize: 8,
			}, strings.NewReader("local v1"))
			if err != nil {
				t.Fatal(err)
			}
			claimed, found, err := metadata.ClaimNextOperation(ctx, time.Now())
			if err != nil || !found || claimed.ID != written.Operation.ID {
				t.Fatalf("ClaimNextOperation = (%#v, %v, %v)", claimed, found, err)
			}
			if err := metadata.CommitRemoteOperation(ctx, claimed.ID, store.RemoteCommit{
				DriveFileID: "drive-shared-1", MIMEType: "text/plain", Size: 8, DriveVersion: "v1", CapabilitiesJSON: `{"canEdit":true,"canDownload":true}`,
			}); err != nil {
				t.Fatal(err)
			}

			drive := &cacheFakeDrive{getError: errors.New("offline")}
			cache, err := New(drive, metadata, content, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			createdObj, err := metadata.ObjectByID(ctx, localObj.ID)
			if err != nil {
				t.Fatal(err)
			}
			localHandle, err := cache.Open(ctx, createdObj)
			if err != nil {
				t.Fatalf("offline open of locally created v1 failed: %v", err)
			}
			if got := string(readAllAt(t, localHandle)); got != "local v1" {
				t.Fatalf("offline open got %q, want %q", got, "local v1")
			}
			_ = localHandle.Close()

			remotePayload := []byte("edited on another pc")
			driveID := domain.DriveFileID("drive-shared-1")
			drive.getError = nil
			drive.files = []driveapi.File{remoteForObject("drive-shared-1", "v2", remotePayload)}
			drive.payload = remotePayload

			var updatedObj domain.Object
			if viaChangeFeed {
				pageToken, err := metadata.BeginChangePoll(ctx)
				if err != nil {
					t.Fatal(err)
				}
				change := store.RemoteEntry{
					Object: domain.Object{
						ID: domain.ObjectID("drive:drive-shared-1"), DriveFileID: &driveID,
						MIMEType: "text/plain", Size: int64(len(remotePayload)), DriveVersion: "v2",
						CapabilitiesJSON: `{"canEdit":true,"canDownload":true}`,
					},
					RemoteName: "shared.txt", ParentDriveFileID: "root-drive",
				}
				if err := metadata.CommitChangePage(ctx, pageToken, []store.RemoteChange{{DriveFileID: "drive-shared-1", Entry: &change}}, "", "live-2"); err != nil {
					t.Fatal(err)
				}
				updatedObj, err = metadata.ObjectByID(ctx, localObj.ID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				// Simulate an existing DB where objects was updated to v2 while old content_versions kept is_current = 1, drive_version = 'v1'.
				rawDB, err := sql.Open("google-drive-sqlite3", filepath.Join(root, "metadata.db")+"?_busy_timeout=5000")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rawDB.ExecContext(ctx, `UPDATE objects SET drive_version = 'v2', size_bytes = ? WHERE object_id = ?`,
					len(remotePayload), string(localObj.ID)); err != nil {
					_ = rawDB.Close()
					t.Fatal(err)
				}
				if err := rawDB.Close(); err != nil {
					t.Fatal(err)
				}
				updatedObj, err = metadata.ObjectByID(ctx, localObj.ID)
				if err != nil {
					t.Fatal(err)
				}
			}

			remoteHandle, err := cache.Open(ctx, updatedObj)
			if err != nil {
				t.Fatalf("open updated remote v2 failed: %v", err)
			}
			defer remoteHandle.Close()
			if got := string(readAllAt(t, remoteHandle)); got != string(remotePayload) {
				t.Fatalf("Open returned %q, want %q", got, remotePayload)
			}
			if drive.downloadCalls != 1 {
				t.Fatalf("downloadCalls = %d, want 1", drive.downloadCalls)
			}
		})
	}
}
