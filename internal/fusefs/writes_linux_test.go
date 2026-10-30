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

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

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
