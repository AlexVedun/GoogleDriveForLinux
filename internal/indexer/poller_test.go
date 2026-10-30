package indexer

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

func TestPollerResumesCommittedPagesAndReindexesWithoutLosingQueuedWrite(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.db")
	metadata, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if metadata != nil {
			_ = metadata.Close()
		}
	}()
	initial := remoteFile("queued-file", "original.txt", "text/plain", "root-drive", 4)
	initial.Version = "1"
	remoteUpdate := remoteFile("queued-file", "renamed-remotely.txt", "text/plain", "root-drive", 900)
	remoteUpdate.Version = "2"
	remoteUpdate.Capabilities.CanEdit = true
	drive := &fakeDrive{
		root:          driveapi.File{ID: "root-drive", Name: "My Drive", MIMEType: driveFolderMIMEType},
		filesByParent: map[string][]driveapi.File{"root-drive": {initial}},
		changePages: map[string]driveapi.ChangeList{
			"snapshot-token": {NewStartPageToken: "live-1"},
			"live-1": {
				Changes:       []driveapi.Change{{FileID: "queued-file", File: &remoteUpdate}},
				NextPageToken: "poll-page-2",
			},
			"poll-page-2": {Changes: []driveapi.Change{{FileID: "queued-file", Removed: true}}, NewStartPageToken: "live-2"},
		},
		changeFailOnce: map[string]error{"poll-page-2": errors.New("injected interruption after first page commit")},
	}
	indexer, err := New(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := indexer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	contentRoot := filepath.Join(t.TempDir(), "content")
	content, err := store.NewContentStore(metadata, contentRoot)
	if err != nil {
		t.Fatal(err)
	}
	const localRevision = "local revision that must survive remote changes"
	write, err := content.WritePending(ctx, store.WriteRequest{
		OperationID: "op-local-pending", ObjectID: "drive:queued-file", Kind: domain.OperationWrite,
		IdempotencyKey: "queued-local-revision", BaseDriveVersion: "1", ExpectedSize: int64(len(localRevision)),
	}, strings.NewReader(localRevision))
	if err != nil {
		t.Fatal(err)
	}
	itemBefore, err := metadata.ObjectByID(ctx, "drive:queued-file")
	if err != nil {
		t.Fatal(err)
	}
	poller, err := NewPoller(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.Poll(ctx); err == nil {
		t.Fatal("poll unexpectedly passed injected interruption")
	}
	status, err := metadata.InitialIndexStatus(ctx)
	if err != nil || status.ChangeToken != "live-1" || status.CatchupPageToken != "poll-page-2" {
		t.Fatalf("interrupted poll cursor = %#v, err = %v", status, err)
	}
	shadow, err := metadata.RemoteShadow(ctx, "drive:queued-file")
	if err != nil || shadow.RemoteName != "renamed-remotely.txt" || shadow.DriveVersion != "2" || !shadow.Confirmed {
		t.Fatalf("first page shadow = %#v, err = %v", shadow, err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	metadata = nil
	metadata, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := NewPoller(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = metadata.InitialIndexStatus(ctx)
	if err != nil || status.ChangeToken != "live-2" || status.CatchupPageToken != "" {
		t.Fatalf("resumed poll cursor = %#v, err = %v", status, err)
	}
	itemAfter, err := metadata.ObjectByID(ctx, "drive:queued-file")
	if err != nil || itemAfter.Deleted || itemAfter.Inode != itemBefore.Inode || itemAfter.Size != itemBefore.Size {
		t.Fatalf("remote removal discarded queued object: before=%#v after=%#v err=%v", itemBefore, itemAfter, err)
	}
	shadow, err = metadata.RemoteShadow(ctx, "drive:queued-file")
	if err != nil || !shadow.Removed || !shadow.Confirmed {
		t.Fatalf("remote removal shadow = %#v, err = %v", shadow, err)
	}

	// Force an invalid saved start token, then make the remote snapshot omit
	// the object. The local queued copy must remain readable and visible.
	drive.mu.Lock()
	drive.changeFailOnce["live-2"] = &driveapi.APIError{StatusCode: 400, Kind: driveapi.FailureInvalidPageToken}
	drive.filesByParent["root-drive"] = nil
	drive.changePages["snapshot-token"] = driveapi.ChangeList{NewStartPageToken: "reindexed-live"}
	drive.mu.Unlock()
	if err := resumed.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = metadata.InitialIndexStatus(ctx)
	if err != nil || status.State != "ready" || status.ChangeToken != "reindexed-live" || status.CatchupPageToken != "" {
		t.Fatalf("reindex status = %#v, err = %v", status, err)
	}
	itemAfter, err = metadata.ObjectByID(ctx, "drive:queued-file")
	if err != nil || itemAfter.Deleted || itemAfter.Inode != itemBefore.Inode || itemAfter.Size != itemBefore.Size {
		t.Fatalf("reindex discarded queued object: %#v, err = %v", itemAfter, err)
	}
	children, err := metadata.Children(ctx, domain.RootObjectID)
	if err != nil || len(children) != 1 || children[0].ObjectID != "drive:queued-file" {
		t.Fatalf("queued object disappeared from namespace: %#v, err = %v", children, err)
	}
	shadow, err = metadata.RemoteShadow(ctx, "drive:queued-file")
	if err != nil || !shadow.Removed || !shadow.Confirmed {
		t.Fatalf("confirmed reindex removal shadow = %#v, err = %v", shadow, err)
	}
	reopenedContent, err := store.NewContentStore(metadata, contentRoot)
	if err != nil {
		t.Fatal(err)
	}
	file, _, err := reopenedContent.OpenContentVersion(ctx, *write.Operation.ContentVersionID)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != localRevision {
		t.Fatalf("local content after reindex = %q, want %q", buffer, localRevision)
	}
}

func TestPollerIndexesNewFoldersBeforeAdvancingToReady(t *testing.T) {
	ctx := context.Background()
	metadata, err := store.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	folder := remoteFile("new-folder", "new folder", driveFolderMIMEType, "root-drive", 0)
	child := remoteFile("nested-file", "nested.txt", "text/plain", "new-folder", 12)
	drive := &fakeDrive{
		root:          driveapi.File{ID: "root-drive", Name: "My Drive", MIMEType: driveFolderMIMEType},
		filesByParent: map[string][]driveapi.File{"root-drive": nil, "new-folder": {child}},
		changePages: map[string]driveapi.ChangeList{
			"snapshot-token": {NewStartPageToken: "live-1"},
			"live-1":         {Changes: []driveapi.Change{{FileID: "new-folder", File: &folder}}, NewStartPageToken: "live-2"},
			"live-2":         {NewStartPageToken: "live-3"},
		},
	}
	indexer, err := New(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := indexer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	poller, err := NewPoller(drive, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := poller.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := metadata.InitialIndexStatus(ctx)
	if err != nil || status.State != "ready" || status.ChangeToken != "live-3" {
		t.Fatalf("index status after new-folder scan = %#v, err = %v", status, err)
	}
	if drive.changeCalls != 3 {
		t.Fatalf("change pages read = %d, want initial plus pre/post-scan catch-up", drive.changeCalls)
	}
	if _, err := metadata.DirectoryEntry(ctx, "drive:new-folder", "nested.txt"); err != nil {
		t.Fatalf("new folder contents were not indexed before ready: %v", err)
	}
}
