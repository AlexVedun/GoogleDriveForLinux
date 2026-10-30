//go:build linux

package fusefs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/store"
)

func TestIndexedNamespaceEnumeratesThousandsAndKeepsStableInodes(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 2005)
	defer metadata.Close()
	root, err := NewIndexedRoot(metadata)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})

	stream, errno := root.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("root Readdir errno = %v", errno)
	}
	entries := readDirectoryStream(t, stream)
	if len(entries) != 2007 {
		t.Fatalf("root entry count = %d, want 2007", len(entries))
	}
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name }) {
		t.Fatal("Readdir entries are not in deterministic name order")
	}
	byName := make(map[string]fuse.DirEntry, len(entries))
	for _, entry := range entries {
		if _, exists := byName[entry.Name]; exists {
			t.Fatalf("duplicate displayed name in readdir: %q", entry.Name)
		}
		byName[entry.Name] = entry
	}
	folderEntry, exists := byName["folder"]
	if !exists || folderEntry.Mode != fuse.S_IFDIR {
		t.Fatalf("folder readdir entry = %#v, exists=%v", folderEntry, exists)
	}
	docEntry, exists := byName["native-doc"]
	if !exists || docEntry.Mode != 0 {
		t.Fatalf("unsupported native document should have unknown dirent type: %#v, exists=%v", docEntry, exists)
	}
	folderInode, errno := root.Lookup(ctx, "folder", &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("folder lookup errno = %v", errno)
	}
	folderNode := folderInode.Operations().(*IndexedNode)
	folderEntries, errno := folderNode.Readdir(ctx)
	if errno != 0 || len(readDirectoryStream(t, folderEntries)) != 0 {
		t.Fatalf("empty folder listing errno = %v", errno)
	}

	var rootAttr fuse.AttrOut
	if errno := root.Getattr(ctx, nil, &rootAttr); errno != 0 || rootAttr.Ino != uint64(store.RootInode) || rootAttr.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		t.Fatalf("root attrs = %#v, errno = %v", rootAttr.Attr, errno)
	}
	fileObject, err := metadata.ObjectByID(ctx, "drive:file-0000")
	if err != nil {
		t.Fatal(err)
	}
	fileInode, errno := root.Lookup(ctx, "file-0000.txt", &fuse.EntryOut{})
	if errno != 0 || fileInode.StableAttr().Ino != uint64(fileObject.Inode) {
		t.Fatalf("file lookup inode = %v, errno = %v; want %d", fileInode, errno, fileObject.Inode)
	}
	fileNode := fileInode.Operations().(*IndexedNode)
	var fileAttr fuse.AttrOut
	if errno := fileNode.Getattr(ctx, nil, &fileAttr); errno != 0 || fileAttr.Size != uint64(fileObject.Size) || fileAttr.Mode&syscall.S_IFMT != syscall.S_IFREG {
		t.Fatalf("file attrs = %#v, errno = %v", fileAttr.Attr, errno)
	}
	secondRoot, err := NewIndexedRoot(metadata)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(secondRoot, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	secondLookup, errno := secondRoot.Lookup(ctx, "file-0000.txt", &fuse.EntryOut{})
	if errno != 0 || secondLookup.StableAttr().Ino != fileInode.StableAttr().Ino {
		t.Fatalf("inode changed after constructing a fresh FUSE tree: first=%d second=%d errno=%v",
			fileInode.StableAttr().Ino, secondLookup.StableAttr().Ino, errno)
	}

	if _, errno := root.Lookup(ctx, "native-doc", &fuse.EntryOut{}); errno != syscall.EOPNOTSUPP {
		t.Fatalf("native Docs lookup errno = %v, want EOPNOTSUPP", errno)
	}
	if _, _, _, errno := root.Create(ctx, "new.txt", 0, 0o644, &fuse.EntryOut{}); errno != syscall.EROFS {
		t.Fatalf("create errno = %v, want EROFS", errno)
	}
	if _, errno := root.Mkdir(ctx, "new-folder", 0o755, &fuse.EntryOut{}); errno != syscall.EROFS {
		t.Fatalf("mkdir errno = %v, want EROFS", errno)
	}
	if errno := root.Unlink(ctx, "file-0000.txt"); errno != syscall.EROFS {
		t.Fatalf("unlink errno = %v, want EROFS", errno)
	}
	if errno := root.Rmdir(ctx, "folder"); errno != syscall.EROFS {
		t.Fatalf("rmdir errno = %v, want EROFS", errno)
	}
	if errno := root.Rename(ctx, "file-0000.txt", root, "renamed.txt", 0); errno != syscall.EROFS {
		t.Fatalf("rename errno = %v, want EROFS", errno)
	}
	if _, errno := fileNode.Write(ctx, nil, []byte("no"), 0); errno != syscall.EROFS {
		t.Fatalf("write errno = %v, want EROFS", errno)
	}
	if _, _, errno := fileNode.Open(ctx, 0); errno != syscall.EOPNOTSUPP {
		t.Fatalf("read open errno = %v, want EOPNOTSUPP until content reads are implemented", errno)
	}

	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, pageToken, []store.RemoteChange{{DriveFileID: "file-0000", Removed: true}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	if errno := fileNode.Getattr(ctx, nil, &fuse.AttrOut{}); errno != syscall.ENOENT {
		t.Fatalf("getattr for removed object errno = %v, want ENOENT", errno)
	}
	if _, errno := root.Lookup(ctx, "file-0000.txt", &fuse.EntryOut{}); errno != syscall.ENOENT {
		t.Fatalf("lookup for removed object errno = %v, want ENOENT", errno)
	}
	afterDelete, errno := root.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir after remote deletion errno = %v", errno)
	}
	if got := len(readDirectoryStream(t, afterDelete)); got != 2006 {
		t.Fatalf("root entry count after remote deletion = %d, want 2006", got)
	}
}

func TestIndexedReaddirCanRunConcurrentlyWithIndexUpdates(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 25)
	defer metadata.Close()
	root, err := NewIndexedRoot(metadata)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})

	var wait sync.WaitGroup
	errCh := make(chan error, 8)
	for reader := range 8 {
		wait.Add(1)
		go func(reader int) {
			defer wait.Done()
			for iteration := range 30 {
				stream, errno := root.Readdir(ctx)
				if errno != 0 {
					errCh <- fmt.Errorf("reader %d iteration %d: readdir errno %v", reader, iteration, errno)
					return
				}
				entries, err := collectDirectoryStream(stream)
				if err != nil {
					errCh <- err
					return
				}
				if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name }) {
					errCh <- fmt.Errorf("reader %d observed an unsorted directory snapshot", reader)
					return
				}
			}
		}(reader)
	}
	for index := range 30 {
		pageToken, err := metadata.BeginChangePoll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		entry := indexedTestEntry(fmt.Sprintf("live-%02d", index), fmt.Sprintf("live-%02d.txt", index), "root-drive", "text/plain", int64(index))
		if err := metadata.CommitChangePage(ctx, pageToken, []store.RemoteChange{{DriveFileID: fmt.Sprintf("live-%02d", index), Entry: &entry}}, "", fmt.Sprintf("live-token-%02d", index+1)); err != nil {
			t.Fatal(err)
		}
	}
	wait.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestValidateMountpointRequiresEmptyExistingDirectory(t *testing.T) {
	base := t.TempDir()
	mountpoint := filepath.Join(base, "mount")
	if err := os.Mkdir(mountpoint, 0o700); err != nil {
		t.Fatal(err)
	}
	resolved, err := ValidateMountpoint(mountpoint)
	if err != nil || resolved != mountpoint {
		t.Fatalf("ValidateMountpoint() = %q, %v", resolved, err)
	}
	if err := os.WriteFile(filepath.Join(mountpoint, "user-data"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateMountpoint(mountpoint); err == nil {
		t.Fatal("ValidateMountpoint accepted a non-empty directory")
	}
}

func openIndexedTestStore(t *testing.T, count int) *store.Store {
	t.Helper()
	metadata, err := store.Open(t.TempDir() + "/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	files := make([]store.RemoteEntry, 0, count+2)
	for index := range count {
		name := fmt.Sprintf("file-%04d.txt", index)
		if index == 4 || index == 5 {
			name = "duplicate.txt"
		}
		files = append(files, indexedTestEntry(fmt.Sprintf("file-%04d", index), name, "root-drive", "text/plain", int64(index)))
	}
	folder := indexedTestEntry("folder", "folder", "root-drive", googleFolderMIMEType, 0)
	doc := indexedTestEntry("native-doc", "native-doc", "root-drive", "application/vnd.google-apps.document", 0)
	files = append(files, folder, doc)
	ctx := context.Background()
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		metadata.Close()
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", files); err != nil {
		metadata.Close()
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "folder", "", nil); err != nil {
		metadata.Close()
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		metadata.Close()
		t.Fatal(err)
	}
	return metadata
}

func indexedTestEntry(id, name, parent, mimeType string, size int64) store.RemoteEntry {
	driveID := domain.DriveFileID(id)
	return store.RemoteEntry{
		Object: domain.Object{
			ID: domain.ObjectID("drive:" + id), DriveFileID: &driveID,
			MIMEType: mimeType, Size: size, CapabilitiesJSON: "{}",
		},
		RemoteName: name, ParentDriveFileID: parent,
	}
}

func readDirectoryStream(t *testing.T, stream fs.DirStream) []fuse.DirEntry {
	t.Helper()
	entries, err := collectDirectoryStream(stream)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func collectDirectoryStream(stream fs.DirStream) ([]fuse.DirEntry, error) {
	defer stream.Close()
	var entries []fuse.DirEntry
	for stream.HasNext() {
		entry, errno := stream.Next()
		if errno != 0 {
			return nil, fmt.Errorf("read FUSE directory stream: %v", errno)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func TestIndexedRootRequiresReadyIndex(t *testing.T) {
	metadata, err := store.Open(t.TempDir() + "/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if _, err := NewIndexedRoot(metadata); !errors.Is(err, ErrIndexNotReady) {
		t.Fatalf("NewIndexedRoot error = %v, want ErrIndexNotReady", err)
	}
}
