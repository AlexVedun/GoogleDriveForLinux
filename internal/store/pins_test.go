package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
)

func TestRecursiveOfflinePinCoversNewChildrenAndProtectsCachedContent(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	folder := indexedRemoteEntry("pinned-folder", "offline", "root-drive")
	folder.Object.MIMEType = rootMIMEType
	first := indexedRemoteEntry("pinned-first", "first.txt", "pinned-folder")
	first.Object.DriveVersion = "first-v1"
	first.Object.Size = int64(len("first"))
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{folder}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "pinned-folder", "", []RemoteEntry{first}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.SetOfflinePin(ctx, folder.Object.ID, true); err != nil {
		t.Fatal(err)
	}
	for _, objectID := range []domain.ObjectID{folder.Object.ID, first.Object.ID} {
		pinned, err := metadata.IsOfflinePinned(ctx, objectID)
		if err != nil || !pinned {
			t.Fatalf("object %q inherited offline pin = %v, err = %v", objectID, pinned, err)
		}
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	future := indexedRemoteEntry("pinned-future", "later.txt", "pinned-folder")
	future.Object.DriveVersion = "future-v1"
	future.Object.Size = int64(len("later"))
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "pinned-future", Entry: &future}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	pinnedFiles, err := metadata.OfflinePinnedFiles(ctx)
	if err != nil || len(pinnedFiles) != 2 {
		t.Fatalf("pinned files after child discovery = %#v, err = %v", pinnedFiles, err)
	}

	content, err := NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	staged, err := content.StageDownload(ctx, strings.NewReader("first"), 5, "")
	if err != nil {
		t.Fatal(err)
	}
	version, err := content.PublishCachedContent(ctx, first.Object.ID, first.Object.DriveVersion, staged)
	if err != nil {
		t.Fatal(err)
	}
	if version.LocalPath != content.relativePinnedBlobPath(version.ID) {
		t.Fatalf("recursive pin cache path = %q, want pinned directory", version.LocalPath)
	}
	if candidates, err := metadata.EvictableCachedVersions(ctx); err != nil || len(candidates) != 0 {
		t.Fatalf("recursively pinned cache candidates = %#v, err = %v", candidates, err)
	}
	if err := content.EvictCachedContent(ctx, version); !errors.Is(err, ErrContentNotEvictable) {
		t.Fatalf("evict recursively pinned content error = %v", err)
	}
	const simulatedPinnedBytes = (1 << 30) + 4096
	if _, err := metadata.db.ExecContext(ctx, `UPDATE content_versions SET size_bytes = ? WHERE version_id = ?`,
		simulatedPinnedBytes, string(version.ID)); err != nil {
		t.Fatal(err)
	}
	if overage, err := metadata.ProtectedContentOverage(ctx, 1<<30); err != nil || overage != 4096 {
		t.Fatalf("simulated >1 GiB pinned content overage = %d, err = %v; want 4096", overage, err)
	}
	if err := metadata.RemoveOfflinePin(ctx, folder.Object.ID); err != nil {
		t.Fatal(err)
	}
	if pinned, err := metadata.IsOfflinePinned(ctx, first.Object.ID); err != nil || pinned {
		t.Fatalf("child remained pinned after folder unpin: %v, err = %v", pinned, err)
	}
	if candidates, err := metadata.EvictableCachedVersions(ctx); err != nil || len(candidates) != 1 {
		t.Fatalf("unpinned cache candidates = %#v, err = %v", candidates, err)
	}
	if err := content.EvictCachedContent(ctx, version); err != nil {
		t.Fatalf("evict cache after inherited unpin: %v", err)
	}
}

func TestOfflinePinRequiresRecursiveFolderChoice(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	file, err := metadata.CreateObject(ctx, domain.Object{ID: "pin-file", MIMEType: "text/plain"}, domain.DirectoryEntry{
		ParentID: domain.RootObjectID, ObjectID: "pin-file", RemoteName: "file", DisplayName: "file",
	})
	if err != nil {
		t.Fatal(err)
	}
	folder, err := metadata.CreateObject(ctx, domain.Object{ID: "pin-folder", MIMEType: rootMIMEType}, domain.DirectoryEntry{
		ParentID: domain.RootObjectID, ObjectID: "pin-folder", RemoteName: "folder", DisplayName: "folder",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.SetOfflinePin(ctx, file.ID, true); err == nil {
		t.Fatal("recursive pin on a file was accepted")
	}
	if err := metadata.SetOfflinePin(ctx, folder.ID, false); err == nil {
		t.Fatal("non-recursive pin on a folder was accepted")
	}
	if err := metadata.SetOfflinePin(ctx, folder.ID, true); err != nil {
		t.Fatal(err)
	}
	pins, err := metadata.OfflinePins(ctx)
	if err != nil || len(pins) != 1 || pins[0].ObjectID != folder.ID || !pins[0].Recursive || pins[0].PinnedAt.IsZero() || pins[0].PinnedAt.After(time.Now()) {
		t.Fatalf("stored offline pins = %#v, err = %v", pins, err)
	}
}
