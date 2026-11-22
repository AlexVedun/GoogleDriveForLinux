//go:build linux

package fusefs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"

	"google-drive-linux/internal/contentcache"
	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

func TestFUSEOfflineCreateWriteFlushFsyncAndRestart(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 0)
	databasePath := metadata.Path()
	contentPath := filepath.Join(t.TempDir(), "content")
	content, err := store.NewContentStore(metadata, contentPath)
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, nil, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	inode, handle, flags, errno := root.Create(ctx, "offline.txt", uint32(os.O_RDWR), 0o644, &fuse.EntryOut{})
	if errno != 0 || flags&fuse.FOPEN_DIRECT_IO == 0 {
		t.Fatalf("Create errno=%v flags=%d", errno, flags)
	}
	node := inode.Operations().(*IndexedNode)
	if written, errno := node.Write(ctx, handle, []byte("durable local contents"), 0); errno != 0 || written != uint32(len("durable local contents")) {
		t.Fatalf("Write = %d, %v", written, errno)
	}
	if errno := node.Fsync(ctx, handle, 0); errno != 0 {
		t.Fatalf("Fsync errno = %v", errno)
	}
	if errno := node.Flush(ctx, handle); errno != 0 {
		t.Fatalf("Flush errno = %v", errno)
	}
	if errno := node.Fsync(ctx, handle, 0); errno != 0 {
		t.Fatalf("repeat Fsync errno = %v", errno)
	}
	object, err := metadata.ObjectByInode(ctx, domain.Inode(inode.StableAttr().Ino))
	if err != nil {
		t.Fatal(err)
	}
	assertPendingContent(t, ctx, metadata, content, object.ID, "durable local contents", 1)
	if errno := node.Release(ctx, handle); errno != 0 {
		t.Fatalf("Release errno = %v", errno)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	metadata, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	content, err = store.NewContentStore(metadata, contentPath)
	if err != nil {
		t.Fatal(err)
	}
	root, err = NewIndexedRootWithWriter(metadata, nil, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	inode, errno = root.Lookup(ctx, "offline.txt", &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	node = inode.Operations().(*IndexedNode)
	handle, _, errno = node.Open(ctx, uint32(os.O_RDWR))
	if errno != 0 {
		t.Fatal(errno)
	}
	result, errno := node.Read(ctx, handle, make([]byte, 64), 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	actual, status := result.Bytes(make([]byte, 64))
	if status != fuse.OK || string(actual) != "durable local contents" {
		t.Fatalf("reopened bytes = %q, status=%v", actual, status)
	}
	if count, errno := node.Write(ctx, handle, []byte("new"), 0); errno != 0 || count != 3 {
		t.Fatalf("reopened Write = %d, %v", count, errno)
	}
	if errno := node.Fsync(ctx, handle, 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno := node.Flush(ctx, handle); errno != 0 {
		t.Fatal(errno)
	}
	assertPendingContent(t, ctx, metadata, content, object.ID, "newable local contents", 2)
	if errno := node.Release(ctx, handle); errno != 0 {
		t.Fatalf("reopened Release errno = %v", errno)
	}
}

func TestFUSEWritersShareWorkingGenerationAndFsyncOnlyOnce(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 0)
	defer metadata.Close()
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, nil, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	inode, first, _, errno := root.Create(ctx, "concurrent.txt", uint32(os.O_RDWR), 0o644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	node := inode.Operations().(*IndexedNode)
	second, _, errno := node.Open(ctx, uint32(os.O_RDWR))
	if errno != 0 {
		t.Fatal(errno)
	}
	var wait sync.WaitGroup
	errCh := make(chan error, 2)
	for _, write := range []struct {
		handle fs.FileHandle
		bytes  []byte
		offset int64
	}{{first, []byte("left"), 0}, {second, []byte("right"), 4}} {
		wait.Add(1)
		go func(write struct {
			handle fs.FileHandle
			bytes  []byte
			offset int64
		}) {
			defer wait.Done()
			count, errno := node.Write(ctx, write.handle, write.bytes, write.offset)
			if errno != 0 || count != uint32(len(write.bytes)) {
				errCh <- fmt.Errorf("Write = %d, %v", count, errno)
			}
		}(write)
	}
	wait.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if errno := node.Fsync(ctx, first, 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno := node.Flush(ctx, second); errno != 0 {
		t.Fatal(errno)
	}
	if errno := node.Fsync(ctx, second, 0); errno != 0 {
		t.Fatal(errno)
	}
	object, err := metadata.ObjectByInode(ctx, domain.Inode(inode.StableAttr().Ino))
	if err != nil {
		t.Fatal(err)
	}
	assertPendingContent(t, ctx, metadata, content, object.ID, "leftright", 1)
	if errno := node.Release(ctx, second); errno != 0 {
		t.Fatal(errno)
	}
	if errno := node.Release(ctx, first); errno != 0 {
		t.Fatal(errno)
	}
}

func TestFUSESetattrTruncatesAndQueuesTheResult(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 0)
	defer metadata.Close()
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, nil, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	inode, handle, _, errno := root.Create(ctx, "truncate.txt", uint32(os.O_RDWR), 0o644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	node := inode.Operations().(*IndexedNode)
	if _, errno := node.Write(ctx, handle, []byte("abcdef"), 0); errno != 0 {
		t.Fatal(errno)
	}
	input := &fuse.SetAttrIn{}
	input.Valid = fuse.FATTR_SIZE
	input.Size = 3
	var output fuse.AttrOut
	if errno := node.Setattr(ctx, handle, input, &output); errno != 0 {
		t.Fatalf("Setattr truncate errno = %v", errno)
	}
	if output.Size != 3 {
		t.Fatalf("Setattr size = %d, want 3", output.Size)
	}
	if errno := node.Fsync(ctx, handle, 0); errno != 0 {
		t.Fatal(errno)
	}
	object, err := metadata.ObjectByInode(ctx, domain.Inode(inode.StableAttr().Ino))
	if err != nil {
		t.Fatal(err)
	}
	assertPendingContent(t, ctx, metadata, content, object.ID, "abc", 1)
	if errno := node.Release(ctx, handle); errno != 0 {
		t.Fatal(errno)
	}
}

func TestFUSEOfflineEditDoesNotWaitForNetworkOnFsync(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 1)
	defer metadata.Close()
	entry := indexedTestEntry("file-0000", "file-0000.txt", "root-drive", "text/plain", int64(len("original")))
	entry.Object.DriveVersion = "editable-v1"
	entry.Object.CapabilitiesJSON = `{"canEdit":true}`
	page, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, page, []store.RemoteChange{{DriveFileID: "file-0000", Entry: &entry}}, "", "live-editable"); err != nil {
		t.Fatal(err)
	}
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	remote := driveapi.File{ID: "file-0000", MIMEType: "text/plain", Size: 8, Version: "editable-v1",
		MD5Checksum: checksumString([]byte("original")), Capabilities: driveapi.Capabilities{CanDownload: true, CanEdit: true}}
	drive := &indexedReadDrive{file: remote, payload: []byte("original")}
	reader, err := contentcache.New(drive, metadata, content, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, reader, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	inode, errno := root.Lookup(ctx, "file-0000.txt", &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	node := inode.Operations().(*IndexedNode)
	handle, _, errno := node.Open(ctx, uint32(os.O_RDWR))
	if errno != 0 {
		t.Fatal(errno)
	}
	getCalls, downloadCalls := drive.getCalls, drive.downloadCalls
	if count, errno := node.Write(ctx, handle, []byte("offline!"), 0); errno != 0 || count != 8 {
		t.Fatalf("Write = %d, %v", count, errno)
	}
	if errno := node.Fsync(ctx, handle, 0); errno != 0 {
		t.Fatalf("Fsync errno = %v", errno)
	}
	if drive.getCalls != getCalls || drive.downloadCalls != downloadCalls {
		t.Fatalf("Fsync made network calls: get %d->%d download %d->%d", getCalls, drive.getCalls, downloadCalls, drive.downloadCalls)
	}
	assertPendingContent(t, ctx, metadata, content, "drive:file-0000", "offline!", 1)
	if errno := node.Release(ctx, handle); errno != 0 {
		t.Fatal(errno)
	}
}

func TestFUSERejectsEditingDriveFileWithoutCanEdit(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 1)
	defer metadata.Close()
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, nil, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	inode, errno := root.Lookup(ctx, "file-0000.txt", &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	if _, _, errno := inode.Operations().(*IndexedNode).Open(ctx, uint32(os.O_WRONLY)); errno != syscall.EACCES {
		t.Fatalf("Open without canEdit errno = %v, want EACCES", errno)
	}
}

func TestFUSEReportsNoSpaceWhenDurableQueueCannotReserveStorage(t *testing.T) {
	if errno := contentErrno(store.ErrInsufficientDiskSpace); errno != syscall.ENOSPC {
		t.Fatalf("durable storage error maps to %v, want ENOSPC", errno)
	}
}

func TestFUSEMkdirRenameReplaceUnlinkAndRecursiveRmdir(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 0)
	defer metadata.Close()
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, nil, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})

	workInode, errno := root.Mkdir(ctx, "work", 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	work := workInode.Operations().(*IndexedNode)
	target, _ := createFUSEFile(t, ctx, root, metadata, "target.txt", "old")
	source, _ := createFUSEFile(t, ctx, work, metadata, "editor.tmp", "new contents")
	if errno := work.Rename(ctx, "editor.tmp", root, "target.txt", 0); errno != 0 {
		t.Fatalf("atomic editor rename errno = %v", errno)
	}
	if errno := work.Rename(ctx, "target.txt", root, "already-there.txt", uint32(unix.RENAME_NOREPLACE)); errno != syscall.ENOENT {
		t.Fatalf("rename missing source errno = %v, want ENOENT", errno)
	}
	if _, errno := root.Lookup(ctx, "target.txt", &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("replacement target lookup errno = %v", errno)
	}
	if _, errno := work.Lookup(ctx, "editor.tmp", &fuse.EntryOut{}); errno != syscall.ENOENT {
		t.Fatalf("old editor temp lookup errno = %v, want ENOENT", errno)
	}
	if live, err := metadata.ObjectByID(ctx, source.ID); err != nil || live.DriveFileID != nil {
		t.Fatalf("renamed local object lost its identity: %#v err=%v", live, err)
	}
	if replaced, err := metadata.ObjectByID(ctx, target.ID); err != nil || !replaced.Deleted {
		t.Fatalf("replaced object deleted=%v err=%v", replaced.Deleted, err)
	}
	sourceOps, err := metadata.OperationsForObject(ctx, source.ID)
	if err != nil || len(sourceOps) != 2 || sourceOps[1].Kind != domain.OperationRename {
		t.Fatalf("source operations = %#v err=%v; want create then rename", sourceOps, err)
	}
	targetOps, err := metadata.OperationsForObject(ctx, target.ID)
	if err != nil || len(targetOps) != 2 || targetOps[1].Kind != domain.OperationTrash {
		t.Fatalf("replaced target operations = %#v err=%v; want create then trash", targetOps, err)
	}
	deps, err := metadata.OperationDependencies(ctx, sourceOps[1].ID)
	if err != nil || !containsOperationID(deps, targetOps[1].ID) {
		t.Fatalf("rename dependencies = %v err=%v; want destination trash %s", deps, err, targetOps[1].ID)
	}
	assertPendingContent(t, ctx, metadata, content, source.ID, "new contents", 2)
	assertPendingContent(t, ctx, metadata, content, target.ID, "old", 2)

	folderInode, errno := root.Mkdir(ctx, "recursive", 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	folder := folderInode.Operations().(*IndexedNode)
	child, _ := createFUSEFile(t, ctx, folder, metadata, "child.txt", "child")
	if errno := root.Rmdir(ctx, "recursive"); errno != syscall.ENOTEMPTY {
		t.Fatalf("rmdir non-empty folder errno = %v, want ENOTEMPTY", errno)
	}
	if errno := root.Unlink(ctx, "recursive"); errno != syscall.EISDIR {
		t.Fatalf("unlink folder errno = %v, want EISDIR", errno)
	}
	if errno := folder.Unlink(ctx, "child.txt"); errno != 0 {
		t.Fatalf("unlink child errno = %v", errno)
	}
	folderObject, err := metadata.ObjectByInode(ctx, domain.Inode(folderInode.StableAttr().Ino))
	if err != nil {
		t.Fatal(err)
	}
	folderOps, err := metadata.OperationsForObject(ctx, folderObject.ID)
	if err != nil || len(folderOps) != 1 || folderOps[0].Kind != domain.OperationMkdir {
		t.Fatalf("folder operations before rmdir = %#v err=%v", folderOps, err)
	}
	if errno := root.Rmdir(ctx, "recursive"); errno != 0 {
		t.Fatalf("rmdir empty folder errno = %v", errno)
	}
	childOps, err := metadata.OperationsForObject(ctx, child.ID)
	if err != nil || len(childOps) != 2 || childOps[1].Kind != domain.OperationTrash {
		t.Fatalf("child operations = %#v err=%v", childOps, err)
	}
	deps, err = metadata.OperationDependencies(ctx, childOps[0].ID)
	if err != nil || !containsOperationID(deps, folderOps[0].ID) {
		t.Fatalf("child create dependencies = %v err=%v; want folder mkdir %s", deps, err, folderOps[0].ID)
	}
	folderOps, err = metadata.OperationsForObject(ctx, folderObject.ID)
	if err != nil || len(folderOps) != 2 || folderOps[1].Kind != domain.OperationTrash {
		t.Fatalf("folder operations after rmdir = %#v err=%v", folderOps, err)
	}
	deps, err = metadata.OperationDependencies(ctx, folderOps[1].ID)
	if err != nil || !containsOperationID(deps, childOps[1].ID) {
		t.Fatalf("folder trash dependencies = %v err=%v; want child trash %s", deps, err, childOps[1].ID)
	}
	folderEntry, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "recursive")
	if err == nil || folderEntry.ObjectID != "" {
		t.Fatalf("recursive folder entry remains visible: %#v err=%v", folderEntry, err)
	}
}

func TestFUSERemoteMoveKeepsDriveIDAndHonorsCapabilities(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 1)
	defer metadata.Close()
	entry := indexedTestEntry("file-0000", "file-0000.txt", "root-drive", "text/plain", 0)
	entry.Object.DriveVersion = "move-version"
	entry.Object.CapabilitiesJSON = `{"canEdit":true,"canTrash":false,"canRename":true,"canMoveItemWithinDrive":true,"canAddChildren":true,"canRemoveChildren":true}`
	folderEntry := indexedTestEntry("folder", "folder", "root-drive", googleFolderMIMEType, 0)
	folderEntry.Object.CapabilitiesJSON = `{"canEdit":true,"canAddChildren":false,"canRemoveChildren":false}`
	nestedEntry := indexedTestEntry("nested-file", "nested.txt", "folder", "text/plain", 0)
	nestedEntry.Object.CapabilitiesJSON = `{"canEdit":true,"canTrash":true,"canRename":true,"canMoveItemWithinDrive":true}`
	page, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, page, []store.RemoteChange{
		{DriveFileID: "file-0000", Entry: &entry},
		{DriveFileID: "folder", Entry: &folderEntry},
		{DriveFileID: "nested-file", Entry: &nestedEntry},
	}, "", "move-live"); err != nil {
		t.Fatal(err)
	}
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, nil, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
	folderInode, errno := root.Lookup(ctx, "folder", &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	folder := folderInode.Operations().(*IndexedNode)
	if _, _, _, errno := folder.Create(ctx, "blocked.txt", uint32(os.O_RDWR), 0o644, &fuse.EntryOut{}); errno != syscall.EACCES {
		t.Fatalf("Create without canAddChildren errno = %v, want EACCES", errno)
	}
	if errno := folder.Unlink(ctx, "nested.txt"); errno != syscall.EACCES {
		t.Fatalf("Unlink without parent canRemoveChildren errno = %v, want EACCES", errno)
	}
	if errno := folder.Rename(ctx, "nested.txt", root, "nested-moved.txt", 0); errno != syscall.EACCES {
		t.Fatalf("cross-parent rename without parent canRemoveChildren errno = %v, want EACCES", errno)
	}
	if errno := root.Unlink(ctx, "file-0000.txt"); errno != syscall.EACCES {
		t.Fatalf("Unlink without canTrash errno = %v, want EACCES", errno)
	}
	occupied, _ := createFUSEFile(t, ctx, root, metadata, "occupied.txt", "local")
	_ = occupied
	if errno := root.Rename(ctx, "file-0000.txt", root, "occupied.txt", uint32(unix.RENAME_NOREPLACE)); errno != syscall.EEXIST {
		t.Fatalf("RENAME_NOREPLACE errno = %v, want EEXIST", errno)
	}
	destinationInode, errno := root.Mkdir(ctx, "destination", 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	destination := destinationInode.Operations().(*IndexedNode)
	if errno := root.Rename(ctx, "file-0000.txt", destination, "renamed-remote.txt", 0); errno != 0 {
		t.Fatalf("remote rename errno = %v", errno)
	}
	object, err := metadata.ObjectByID(ctx, "drive:file-0000")
	if err != nil || object.DriveFileID == nil || *object.DriveFileID != "file-0000" {
		t.Fatalf("remote rename lost Drive ID: object=%#v err=%v", object, err)
	}
	destinationObject, err := metadata.ObjectByInode(ctx, domain.Inode(destinationInode.StableAttr().Ino))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.DirectoryEntry(ctx, destinationObject.ID, "renamed-remote.txt"); err != nil {
		t.Fatal(err)
	}
	operations, err := metadata.OperationsForObject(ctx, object.ID)
	if err != nil || len(operations) != 1 || operations[0].Kind != domain.OperationRename {
		t.Fatalf("remote move operations = %#v err=%v", operations, err)
	}
	dependencies, err := metadata.OperationDependencies(ctx, operations[0].ID)
	destinationOperations, opErr := metadata.OperationsForObject(ctx, destinationObject.ID)
	if err != nil || opErr != nil || len(destinationOperations) != 1 || !containsOperationID(dependencies, destinationOperations[0].ID) {
		t.Fatalf("remote move dependencies = %v err=%v; destination ops=%#v err=%v", dependencies, err, destinationOperations, opErr)
	}
	if errno := destination.Unlink(ctx, "renamed-remote.txt"); errno != syscall.EACCES {
		t.Fatalf("Unlink after move without canTrash errno = %v, want EACCES", errno)
	}
}

func createFUSEFile(t *testing.T, ctx context.Context, parent *IndexedNode, metadata *store.Store, name, contents string) (domain.Object, *IndexedNode) {
	t.Helper()
	inode, handle, _, errno := parent.Create(ctx, name, uint32(os.O_RDWR), 0o644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("Create(%s) errno = %v", name, errno)
	}
	node := inode.Operations().(*IndexedNode)
	if len(contents) > 0 {
		if count, errno := node.Write(ctx, handle, []byte(contents), 0); errno != 0 || count != uint32(len(contents)) {
			t.Fatalf("Write(%s) = %d, %v", name, count, errno)
		}
	}
	if errno := node.Fsync(ctx, handle, 0); errno != 0 {
		t.Fatalf("Fsync(%s) errno = %v", name, errno)
	}
	object, err := metadata.ObjectByInode(ctx, domain.Inode(inode.StableAttr().Ino))
	if err != nil {
		t.Fatal(err)
	}
	if errno := node.Release(ctx, handle); errno != 0 {
		t.Fatalf("Release(%s) errno = %v", name, errno)
	}
	return object, node
}

func containsOperationID(ids []domain.OperationID, wanted domain.OperationID) bool {
	for _, id := range ids {
		if id == wanted {
			return true
		}
	}
	return false
}

func TestFsyncSurvivesImmediateSIGKILL(t *testing.T) {
	if os.Getenv("GDL_FSYNC_CRASH_HELPER") == "1" {
		metadata, err := store.Open(os.Getenv("GDL_FSYNC_DB"))
		if err != nil {
			fmt.Fprintln(os.Stdout, "child metadata error:", err)
			os.Exit(2)
		}
		content, err := store.NewContentStore(metadata, os.Getenv("GDL_FSYNC_CONTENT"))
		if err != nil {
			fmt.Fprintln(os.Stdout, "child content error:", err)
			os.Exit(2)
		}
		root, err := NewIndexedRootWithWriter(metadata, nil, content)
		if err != nil {
			fmt.Fprintln(os.Stdout, "child root error:", err)
			os.Exit(2)
		}
		_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})
		inode, handle, _, errno := root.Create(context.Background(), "crash-safe.txt", uint32(os.O_RDWR), 0o644, &fuse.EntryOut{})
		if errno != 0 {
			fmt.Fprintln(os.Stdout, "child create error:", errno)
			os.Exit(2)
		}
		node := inode.Operations().(*IndexedNode)
		if _, errno := node.Write(context.Background(), handle, []byte("fsync survived SIGKILL"), 0); errno != 0 {
			fmt.Fprintln(os.Stdout, "child write error:", errno)
			os.Exit(2)
		}
		if errno := node.Fsync(context.Background(), handle, 0); errno != 0 {
			fmt.Fprintln(os.Stdout, "child fsync error:", errno)
			os.Exit(2)
		}
		fmt.Fprintln(os.Stdout, "FSYNC_DONE")
		select {}
	}

	metadata := openIndexedTestStore(t, 0)
	databasePath := metadata.Path()
	contentPath := filepath.Join(t.TempDir(), "content")
	if _, err := store.NewContentStore(metadata, contentPath); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestFsyncSurvivesImmediateSIGKILL$")
	command.Env = append(os.Environ(), "GDL_FSYNC_CRASH_HELPER=1", "GDL_FSYNC_DB="+databasePath, "GDL_FSYNC_CONTENT="+contentPath)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "FSYNC_DONE\n" {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("child did not confirm fsync: line=%q err=%v", line, err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("child process exited successfully after SIGKILL")
	}

	metadata, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	content, err := store.NewContentStore(metadata, contentPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := content.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	entry, err := metadata.DirectoryEntry(context.Background(), domain.RootObjectID, "crash-safe.txt")
	if err != nil {
		t.Fatal(err)
	}
	assertPendingContent(t, context.Background(), metadata, content, entry.ObjectID, "fsync survived SIGKILL", 1)
}

func assertPendingContent(t *testing.T, ctx context.Context, metadata *store.Store, content *store.ContentStore, objectID domain.ObjectID, expected string, operations int) {
	t.Helper()
	count, err := metadata.OperationCount(ctx, objectID)
	if err != nil || count != operations {
		t.Fatalf("operation count = %d, err=%v; want %d", count, err, operations)
	}
	queued, err := metadata.OperationsForObject(ctx, objectID)
	if err != nil || len(queued) != operations {
		t.Fatalf("queued operation records = %#v err=%v; want %d", queued, err, operations)
	}
	for _, operation := range queued {
		if operation.State != domain.OperationQueued {
			t.Fatalf("operation %s state = %q, want queued", operation.ID, operation.State)
		}
	}
	current, err := content.CurrentContentVersion(ctx, objectID)
	if err != nil || current.Kind != domain.ContentPending || !current.Complete || !current.Current {
		t.Fatalf("current pending version = %#v, err=%v", current, err)
	}
	file, _, err := content.OpenContentVersion(ctx, current.ID)
	if err != nil {
		t.Fatal(err)
	}
	actual, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(actual, []byte(expected)) {
		t.Fatalf("pending bytes = %q, readErr=%v closeErr=%v; want %q", actual, readErr, closeErr, expected)
	}
	object, err := metadata.ObjectByID(ctx, objectID)
	if err != nil || object.Size != int64(len(expected)) {
		t.Fatalf("object size = %d, err=%v; want %d", object.Size, err, len(expected))
	}
}

func TestFUSEEditLoadsUpdatedRemoteContentAfterLocalCreateWasEditedRemotely(t *testing.T) {
	ctx := context.Background()
	metadata := openIndexedTestStore(t, 0)
	defer metadata.Close()
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	drive := &indexedReadDrive{}
	reader, err := contentcache.New(drive, metadata, content, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	root, err := NewIndexedRootWithWriter(metadata, reader, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs.NewNodeFS(root, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: uint64(store.RootInode)}})

	inode, handle, _, errno := root.Create(ctx, "synced.txt", uint32(os.O_RDWR), 0o644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("Create errno = %v", errno)
	}
	node := inode.Operations().(*IndexedNode)
	if written, errno := node.Write(ctx, handle, []byte("local v1"), 0); errno != 0 || written != 8 {
		t.Fatalf("Write = %d, %v", written, errno)
	}
	if errno := node.Release(ctx, handle); errno != 0 {
		t.Fatalf("Release errno = %v", errno)
	}

	claimed, found, err := metadata.ClaimNextOperation(ctx, time.Now())
	if err != nil || !found {
		t.Fatalf("ClaimNextOperation = (%#v, %v, %v)", claimed, found, err)
	}
	if err := metadata.CommitRemoteOperation(ctx, claimed.ID, store.RemoteCommit{
		DriveFileID: "drive-fuse-1", MIMEType: "text/plain", Size: 8, DriveVersion: "v1",
		CapabilitiesJSON: `{"canEdit":true,"canDownload":true}`,
	}); err != nil {
		t.Fatal(err)
	}

	// Another computer updates the file on Drive to v2.
	remoteV2 := []byte("remote v2")
	drive.file = driveapi.File{
		ID: "drive-fuse-1", MIMEType: "text/plain", Size: int64(len(remoteV2)), Version: "v2",
		MD5Checksum:  checksumString(remoteV2),
		Capabilities: driveapi.Capabilities{CanDownload: true, CanEdit: true},
	}
	drive.payload = remoteV2
	page, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remoteEntry := indexedTestEntry("drive-fuse-1", "synced.txt", "root-drive", "text/plain", int64(len(remoteV2)))
	remoteEntry.Object.DriveVersion = "v2"
	remoteEntry.Object.CapabilitiesJSON = `{"canEdit":true,"canDownload":true}`
	if err := metadata.CommitChangePage(ctx, page, []store.RemoteChange{{DriveFileID: "drive-fuse-1", Entry: &remoteEntry}}, "", "live-v2"); err != nil {
		t.Fatal(err)
	}

	editHandle, _, errno := node.Open(ctx, uint32(os.O_RDWR))
	if errno != 0 {
		t.Fatalf("Open(O_RDWR) after remote update errno = %v", errno)
	}
	defer node.Release(ctx, editHandle)
	readRes, errno := node.Read(ctx, editHandle, make([]byte, len(remoteV2)), 0)
	if errno != 0 {
		t.Fatalf("Read errno = %v", errno)
	}
	got, _ := readRes.Bytes(make([]byte, len(remoteV2)))
	if string(got) != string(remoteV2) {
		t.Fatalf("FUSE O_RDWR read = %q, want %q", got, remoteV2)
	}
}
