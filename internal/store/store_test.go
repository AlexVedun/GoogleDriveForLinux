package store

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
)

func TestWriteTransactionReservesWriterBeforeReading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err := first.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT index_state FROM sync_state WHERE singleton = 1`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	otherDone := make(chan error, 1)
	go func() {
		close(started)
		_, err := second.db.ExecContext(ctx, `UPDATE sync_state SET updated_at = ? WHERE singleton = 1`, formatTime(time.Now()))
		otherDone <- err
	}()
	<-started
	select {
	case err := <-otherDone:
		t.Fatalf("concurrent writer finished before transaction commit: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET updated_at = ? WHERE singleton = 1`, formatTime(time.Now())); err != nil {
		t.Fatalf("read transaction could not write after another writer waited: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-otherDone; err != nil {
		t.Fatalf("concurrent writer did not resume: %v", err)
	}
}

func TestSchemaMigrationAndObjectIdentitySurviveReopen(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "state", "metadata.db")
	store, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	version, err := store.SchemaVersion(ctx)
	if err != nil || version != 5 {
		t.Fatalf("schema version = %d, err = %v; want 5, nil", version, err)
	}
	root, err := store.ObjectByID(ctx, domain.RootObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if root.Inode != RootInode {
		t.Fatalf("root inode = %d, want %d", root.Inode, RootInode)
	}

	driveID := domain.DriveFileID("drive-file-123")
	created, err := store.CreateObject(ctx, domain.Object{
		ID:          "object-123",
		DriveFileID: &driveID,
		MIMEType:    "text/plain",
		Size:        27,
	}, domain.DirectoryEntry{
		ParentID:    domain.RootObjectID,
		ObjectID:    "object-123",
		RemoteName:  "Quarterly report.txt",
		DisplayName: "Quarterly report (a1b2).txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Inode <= RootInode {
		t.Fatalf("created inode = %d, want greater than root inode", created.Inode)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reopened, err := store.ObjectByID(ctx, "object-123")
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Inode != created.Inode || reopened.DriveFileID == nil || *reopened.DriveFileID != driveID {
		t.Fatalf("reopened object identity changed: %#v", reopened)
	}
	byInode, err := store.ObjectByInode(ctx, created.Inode)
	if err != nil || byInode.ID != created.ID {
		t.Fatalf("lookup by inode returned %#v, err = %v", byInode, err)
	}
	entry, err := store.DirectoryEntry(ctx, domain.RootObjectID, "Quarterly report (a1b2).txt")
	if err != nil {
		t.Fatal(err)
	}
	if entry.RemoteName != "Quarterly report.txt" || entry.DisplayName != "Quarterly report (a1b2).txt" {
		t.Fatalf("reopened directory entry changed: %#v", entry)
	}
	info, err := os.Stat(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("database permissions = %04o, want 0600", got)
	}
}

func TestExistingVersionOneDatabaseUpgradesToVersionFiveAndPreservesFolderPins(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	script, err := migrationFiles.ReadFile("migrations/001_initial.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, applied_at TEXT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if err := applyMigration(ctx, legacy, 1, checksumMigration(script), string(script)); err != nil {
		t.Fatal(err)
	}
	now := formatTime(time.Now())
	if _, err := legacy.ExecContext(ctx, `INSERT INTO inode_sequence(inode) VALUES (?)`, RootInode); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `INSERT INTO objects (
		object_id, drive_file_id, inode, mime_type, size_bytes, capabilities_json,
		is_deleted, created_at, updated_at
	) VALUES (?, NULL, ?, ?, 0, '{}', 0, ?, ?)`, domain.RootObjectID, RootInode, rootMIMEType, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `INSERT INTO sync_state(singleton, change_token, index_state, updated_at)
		VALUES (1, '', 'empty', ?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `INSERT INTO inode_sequence DEFAULT VALUES`); err != nil {
		t.Fatal(err)
	}
	var folderInodeID int64
	if err := legacy.QueryRowContext(ctx, `SELECT MAX(inode) FROM inode_sequence`).Scan(&folderInodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `INSERT INTO objects (
		object_id, drive_file_id, inode, mime_type, size_bytes, capabilities_json, is_deleted, created_at, updated_at
	) VALUES ('legacy-folder', 'legacy-folder-drive-id', ?, ?, 0, '{}', 0, ?, ?)`, folderInodeID, rootMIMEType, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `INSERT INTO directory_entries(parent_object_id, object_id, remote_name, display_name)
		VALUES (?, 'legacy-folder', 'folder', 'folder')`, domain.RootObjectID); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `INSERT INTO offline_pins(object_id, pinned_at) VALUES ('legacy-folder', ?)`, now); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	version, err := upgraded.SchemaVersion(ctx)
	if err != nil || version != 5 {
		t.Fatalf("upgraded schema version = %d, err = %v", version, err)
	}
	pins, err := upgraded.OfflinePins(ctx)
	if err != nil || len(pins) != 1 || !pins[0].Recursive {
		t.Fatalf("upgraded folder pin = %#v, err = %v; want recursive", pins, err)
	}
	root, err := upgraded.ObjectByID(ctx, domain.RootObjectID)
	if err != nil || root.Inode != RootInode {
		t.Fatalf("upgraded root = %#v, err = %v", root, err)
	}
	status, err := upgraded.InitialIndexStatus(ctx)
	if err != nil || status.State != "empty" || status.Phase != "none" {
		t.Fatalf("upgraded initial index status = %#v, err = %v", status, err)
	}
}

func TestInitialIndexPageBatchAndProgressCommitAtomically(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err := metadata.BeginInitialIndex(ctx, "snapshot-token", "root-drive"); err != nil {
		t.Fatal(err)
	}
	first := indexedRemoteEntry("first", "first.txt", "root-drive")
	invalid := indexedRemoteEntry("invalid", "invalid.txt", "root-drive")
	invalid.Object.DriveFileID = nil
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "next-page", []RemoteEntry{first, invalid}); err == nil {
		t.Fatal("committed an invalid remote entry batch")
	}
	if _, err := metadata.ObjectByID(ctx, first.Object.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed page partially inserted first object: %v", err)
	}
	folders, err := metadata.InitialIndexFolders(ctx)
	if err != nil || len(folders) != 1 || folders[0].PageToken != "" {
		t.Fatalf("failed page advanced the cursor: folders=%#v err=%v", folders, err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "next-page", []RemoteEntry{first}); err != nil {
		t.Fatal(err)
	}
	folders, err = metadata.InitialIndexFolders(ctx)
	if err != nil || len(folders) != 1 || folders[0].PageToken != "next-page" {
		t.Fatalf("successful page cursor = %#v, err = %v", folders, err)
	}
}

func TestInitialCatchupChangesAndCursorCommitAtomically(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err := metadata.BeginInitialIndex(ctx, "snapshot-token", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", nil); err != nil {
		t.Fatal(err)
	}
	first := indexedRemoteEntry("first", "first.txt", "root-drive")
	invalid := RemoteChange{DriveFileID: ""}
	if err := metadata.CommitInitialChanges(ctx, []RemoteChange{{DriveFileID: "first", Entry: &first}, invalid}, "next-change", ""); err == nil {
		t.Fatal("committed an invalid initial change page")
	}
	if _, err := metadata.ObjectByID(ctx, first.Object.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed change page partially applied first object: %v", err)
	}
	status, err := metadata.InitialIndexStatus(ctx)
	if err != nil || status.Phase != "catching-up" || status.CatchupPageToken != "snapshot-token" {
		t.Fatalf("failed change page advanced cursor: status=%#v err=%v", status, err)
	}
	if err := metadata.CommitInitialChanges(ctx, []RemoteChange{{DriveFileID: "first", Entry: &first}}, "next-change", ""); err != nil {
		t.Fatal(err)
	}
	status, err = metadata.InitialIndexStatus(ctx)
	if err != nil || status.CatchupPageToken != "next-change" || status.State != "indexing" {
		t.Fatalf("successful change page cursor = %#v, err = %v", status, err)
	}
}

func indexedRemoteEntry(id, name, parent string) RemoteEntry {
	driveID := domain.DriveFileID(id)
	return RemoteEntry{
		Object: domain.Object{
			ID: domain.ObjectID("drive:" + id), DriveFileID: &driveID,
			MIMEType: "text/plain", CapabilitiesJSON: "{}",
		},
		RemoteName: name, ParentDriveFileID: parent,
	}
}

func TestIncrementalChangePagesCommitCursorAtomicallyAndRejectReplay(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	folder := indexedRemoteEntry("folder", "destination", "root-drive")
	folder.Object.MIMEType = rootMIMEType
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{indexedRemoteEntry("existing", "before.txt", "root-drive"), folder}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "folder", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil || pageToken != "live-1" {
		t.Fatalf("first poll token = %q, err = %v", pageToken, err)
	}
	first := indexedRemoteEntry("new", "new.txt", "root-drive")
	moved := indexedRemoteEntry("existing", "moved.txt", "folder")
	moved.Object.DriveVersion = "2"
	moved.Object.CapabilitiesJSON = `{"canEdit":true}`
	invalid := indexedRemoteEntry("invalid", "invalid.txt", "root-drive")
	invalid.Object.DriveFileID = nil
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "new", Entry: &first}, {DriveFileID: "existing", Entry: &moved}, {DriveFileID: "invalid", Entry: &invalid}}, "page-2", ""); err == nil {
		t.Fatal("committed invalid incremental change page")
	}
	if _, err := metadata.ObjectByID(ctx, first.Object.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed page left a partial object: %v", err)
	}
	status, err := metadata.InitialIndexStatus(ctx)
	if err != nil || status.ChangeToken != "live-1" || status.CatchupPageToken != "live-1" {
		t.Fatalf("failed page advanced cursor: %#v, err = %v", status, err)
	}
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "new", Entry: &first}, {DriveFileID: "existing", Entry: &moved}}, "page-2", ""); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "new", Entry: &first}}, "page-2", ""); err == nil {
		t.Fatal("replayed an already committed change page")
	}
	status, err = metadata.InitialIndexStatus(ctx)
	if err != nil || status.ChangeToken != "live-1" || status.CatchupPageToken != "page-2" {
		t.Fatalf("intermediate page cursor = %#v, err = %v", status, err)
	}
	if err := metadata.CommitChangePage(ctx, "page-2", []RemoteChange{{DriveFileID: "new", Removed: true}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	status, err = metadata.InitialIndexStatus(ctx)
	if err != nil || status.ChangeToken != "live-2" || status.CatchupPageToken != "" || status.State != "ready" {
		t.Fatalf("completed poll cursor = %#v, err = %v", status, err)
	}
	deleted, err := metadata.ObjectByID(ctx, "drive:new")
	if err != nil || !deleted.Deleted {
		t.Fatalf("deleted entry = %#v, err = %v", deleted, err)
	}
	if _, err := metadata.DirectoryEntry(ctx, "drive:folder", "moved.txt"); err != nil {
		t.Fatalf("moved entry missing at new parent: %v", err)
	}
	if _, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "before.txt"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("old moved path remains visible: %v", err)
	}
	updated, err := metadata.ObjectByID(ctx, "drive:existing")
	if err != nil || updated.DriveVersion != "2" || !strings.Contains(updated.CapabilitiesJSON, "canEdit") {
		t.Fatalf("moved remote metadata = %#v, err = %v", updated, err)
	}
	pageToken, err = metadata.BeginChangePoll(ctx)
	if err != nil || pageToken != "live-2" {
		t.Fatalf("empty poll token = %q, err = %v", pageToken, err)
	}
	if err := metadata.CommitChangePage(ctx, pageToken, nil, "", "live-2"); err != nil {
		t.Fatalf("commit empty page with unchanged start token: %v", err)
	}
}

func TestRemoteChangeForksQueuedLocalWriteAndKeepsRemoteVersion(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	entry := indexedRemoteEntry("queued", "original.txt", "root-drive")
	entry.Object.Size = 4
	entry.Object.DriveVersion = "1"
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	content, err := NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	const localBytes = "unsent local revision"
	write, err := content.WritePending(ctx, WriteRequest{
		OperationID: "op-queued-write", ObjectID: entry.Object.ID, Kind: domain.OperationWrite,
		IdempotencyKey: "queued-write", BaseDriveVersion: "1", ExpectedSize: int64(len(localBytes)),
	}, strings.NewReader(localBytes))
	if err != nil || write.Operation.State != domain.OperationQueued {
		t.Fatalf("queued local write = %#v, err = %v", write, err)
	}
	before, err := metadata.ObjectByID(ctx, entry.Object.ID)
	if err != nil {
		t.Fatal(err)
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := indexedRemoteEntry("queued", "renamed remotely.txt", "root-drive")
	changed.Object.Size = 800
	changed.Object.DriveVersion = "2"
	changed.Object.CapabilitiesJSON = `{"canEdit":true}`
	changed.Object.WebViewLink = "https://drive.google.com/file/d/queued"
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "queued", Entry: &changed}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	after, err := metadata.ObjectByID(ctx, entry.Object.ID)
	if err != nil || after.Size != 800 || after.DriveVersion != "2" || after.Deleted {
		t.Fatalf("remote object was not updated to its confirmed version: before=%#v after=%#v err=%v", before, after, err)
	}
	if _, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "renamed remotely.txt"); err != nil {
		t.Fatalf("remote path is not visible beside the preserved local copy: %v", err)
	}
	if _, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "original.txt"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale local path remains beside the remote version: %v", err)
	}
	operations, err := metadata.OperationsForObject(ctx, entry.Object.ID)
	if err != nil || len(operations) != 1 || operations[0].State != domain.OperationConflict {
		t.Fatalf("conflicted write operations = %#v, err = %v", operations, err)
	}
	children, err := metadata.Children(ctx, domain.RootObjectID)
	if err != nil || len(children) != 2 {
		t.Fatalf("remote and local conflict entries = %#v, err = %v", children, err)
	}
	var conflictObjectID domain.ObjectID
	var conflictName string
	for _, child := range children {
		if child.ObjectID != entry.Object.ID {
			conflictObjectID, conflictName = child.ObjectID, child.DisplayName
		}
	}
	if conflictObjectID == "" || !strings.Contains(conflictName, "local conflict") {
		t.Fatalf("local conflict copy entry = %q, object=%q", conflictName, conflictObjectID)
	}
	version, err := content.CurrentContentVersion(ctx, conflictObjectID)
	if err != nil || version.Size != int64(len(localBytes)) {
		t.Fatalf("local conflict content version = %#v, err = %v", version, err)
	}
	file, _, err := content.OpenContentVersion(ctx, version.ID)
	if err != nil {
		t.Fatal(err)
	}
	preserved, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || string(preserved) != localBytes {
		t.Fatalf("local conflict bytes = %q, readErr=%v closeErr=%v", preserved, readErr, closeErr)
	}
	if _, err := metadata.RemoteShadow(ctx, entry.Object.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("confirmed write conflict still has an unresolved shadow: %v", err)
	}
	conflicts, err := metadata.Conflicts(ctx)
	if err != nil || len(conflicts) != 1 || conflicts[0].LocalCopyObjectID != conflictObjectID ||
		conflicts[0].LocalCopyName != conflictName || conflicts[0].RemoteName != "renamed remotely.txt" ||
		conflicts[0].RemoteVersion != "2" {
		t.Fatalf("write conflict record = %#v, err = %v", conflicts, err)
	}
	pageToken, err = metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "queued", Removed: true}}, "", "live-3"); err != nil {
		t.Fatal(err)
	}
	after, err = metadata.ObjectByID(ctx, entry.Object.ID)
	if err != nil || !after.Deleted || after.Size != 800 {
		t.Fatalf("remote removal did not remove only the remote object: %#v, err = %v", after, err)
	}
	children, err = metadata.Children(ctx, domain.RootObjectID)
	if err != nil || len(children) != 1 || children[0].ObjectID != conflictObjectID {
		t.Fatalf("remote removal discarded local conflict copy: %#v, err = %v", children, err)
	}
	if err := metadata.ResolveConflict(ctx, conflicts[0].EventID, "keep-remote"); err != nil {
		t.Fatal(err)
	}
	conflicts, err = metadata.Conflicts(ctx)
	if err != nil || len(conflicts) != 0 {
		t.Fatalf("resolved conflict remains visible: %#v, err = %v", conflicts, err)
	}
	copyObject, err := metadata.ObjectByID(ctx, conflictObjectID)
	if err != nil || !copyObject.Deleted {
		t.Fatalf("keep-remote did not queue removal of the local conflict copy: %#v, err = %v", copyObject, err)
	}
}

func TestRemoteRenameConflictCanRetryLocalIntentAgainstLatestVersion(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	entry := indexedRemoteEntry("namespace-conflict", "old.txt", "root-drive")
	entry.Object.DriveVersion = "1"
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.QueueRename(ctx, domain.RootObjectID, "old.txt", domain.RootObjectID, "local-name.txt", false); err != nil {
		t.Fatal(err)
	}
	operations, err := metadata.OperationsForObject(ctx, entry.Object.ID)
	if err != nil || len(operations) != 1 || operations[0].Kind != domain.OperationRename {
		t.Fatalf("local rename operation = %#v, err = %v", operations, err)
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := indexedRemoteEntry("namespace-conflict", "remote-name.txt", "root-drive")
	changed.Object.DriveVersion = "2"
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "namespace-conflict", Entry: &changed}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	operations, err = metadata.OperationsForObject(ctx, entry.Object.ID)
	if err != nil || len(operations) != 1 || operations[0].State != domain.OperationConflict {
		t.Fatalf("remote edit did not stop local rename: %#v, err = %v", operations, err)
	}
	conflicts, err := metadata.Conflicts(ctx)
	if err != nil || len(conflicts) != 1 || conflicts[0].LocalOperationKind != domain.OperationRename ||
		conflicts[0].LocalIntent != "rename or move to local-name.txt" || conflicts[0].RemoteName != "remote-name.txt" {
		t.Fatalf("namespace conflict record = %#v, err = %v", conflicts, err)
	}
	if err := metadata.ResolveConflict(ctx, conflicts[0].EventID, "keep-local"); err != nil {
		t.Fatal(err)
	}
	operation, err := metadata.OperationByID(ctx, operations[0].ID)
	if err != nil || operation.State != domain.OperationQueued || operation.BaseDriveVersion != "2" {
		t.Fatalf("selected local intent was not rebased on latest Drive version: %#v, err = %v", operation, err)
	}
	conflicts, err = metadata.Conflicts(ctx)
	if err != nil || len(conflicts) != 0 {
		t.Fatalf("resolved namespace conflict remains visible: %#v, err = %v", conflicts, err)
	}
}

func TestBenignVersionBumpDuringPendingRenameDoesNotCreateConflict(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	modified := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	entry := indexedRemoteEntry("atomic-file", ".goutputstream-5US5X3", "root-drive")
	entry.Object.Size = 128
	entry.Object.DriveVersion = "1"
	entry.Object.ModifiedAt = modified
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.QueueRename(ctx, domain.RootObjectID, ".goutputstream-5US5X3", domain.RootObjectID, "zshrc", false); err != nil {
		t.Fatal(err)
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bumped := indexedRemoteEntry("atomic-file", ".goutputstream-5US5X3", "root-drive")
	bumped.Object.Size = 128
	bumped.Object.DriveVersion = "2"
	bumped.Object.ModifiedAt = modified
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "atomic-file", Entry: &bumped}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	operations, err := metadata.OperationsForObject(ctx, entry.Object.ID)
	if err != nil || len(operations) != 1 || operations[0].State != domain.OperationQueued {
		t.Fatalf("benign version bump altered rename operation: %#v, err = %v", operations, err)
	}
	conflicts, err := metadata.Conflicts(ctx)
	if err != nil || len(conflicts) != 0 {
		t.Fatalf("benign version bump created conflict: %#v, err = %v", conflicts, err)
	}
	if _, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "zshrc"); err != nil {
		t.Fatalf("local renamed entry was lost: %v", err)
	}
	shadow, err := metadata.RemoteShadow(ctx, entry.Object.ID)
	if err != nil || !shadow.Confirmed || shadow.DriveVersion != "2" {
		t.Fatalf("remote shadow was not updated to version 2: %#v, err = %v", shadow, err)
	}
}

func TestRemoteModifiedTimeChangeDuringPendingRenameStillMarksConflict(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	modified := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	entry := indexedRemoteEntry("edited-file", "notes.txt", "root-drive")
	entry.Object.Size = 128
	entry.Object.DriveVersion = "1"
	entry.Object.ModifiedAt = modified
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.QueueRename(ctx, domain.RootObjectID, "notes.txt", domain.RootObjectID, "notes-local.txt", false); err != nil {
		t.Fatal(err)
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	edited := indexedRemoteEntry("edited-file", "notes.txt", "root-drive")
	edited.Object.Size = 128
	edited.Object.DriveVersion = "2"
	edited.Object.ModifiedAt = modified.Add(time.Minute)
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "edited-file", Entry: &edited}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	operations, err := metadata.OperationsForObject(ctx, entry.Object.ID)
	if err != nil || len(operations) != 1 || operations[0].State != domain.OperationConflict {
		t.Fatalf("remote content edit did not mark conflict: %#v, err = %v", operations, err)
	}
	conflicts, err := metadata.Conflicts(ctx)
	if err != nil || len(conflicts) != 1 || conflicts[0].Code != remoteNamespaceConflictCode {
		t.Fatalf("conflicts = %#v, err = %v", conflicts, err)
	}
}

func TestOwnOperationMarkerAvoidsFalseRemoteWriteConflict(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	entry := indexedRemoteEntry("marked-write", "draft.txt", "root-drive")
	entry.Object.DriveVersion = "1"
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	content, err := NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	write, err := content.WritePending(ctx, WriteRequest{OperationID: "op-marked-write", ObjectID: entry.Object.ID,
		Kind: domain.OperationWrite, IdempotencyKey: "marked-write", BaseDriveVersion: "1", ExpectedSize: 4}, strings.NewReader("edit"))
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.SetOperationState(ctx, write.Operation.ID, domain.OperationUploading, "", "", time.Time{}); err != nil {
		t.Fatal(err)
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := indexedRemoteEntry("marked-write", "draft.txt", "root-drive")
	changed.Object.DriveVersion = "2"
	changed.AppProperties = map[string]string{domain.DriveOperationProperty: string(write.Operation.ID)}
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "marked-write", Entry: &changed}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	operation, err := metadata.OperationByID(ctx, write.Operation.ID)
	if err != nil || operation.State != domain.OperationUploading {
		t.Fatalf("own marked change became a conflict: %#v, err = %v", operation, err)
	}
	conflicts, err := metadata.Conflicts(ctx)
	if err != nil || len(conflicts) != 0 {
		t.Fatalf("own marked change created a conflict: %#v, err = %v", conflicts, err)
	}
	shadow, err := metadata.RemoteShadow(ctx, entry.Object.ID)
	if err != nil || !shadow.Confirmed || shadow.DriveVersion != "2" {
		t.Fatalf("own marked change was not retained for post-commit reconciliation: %#v, err = %v", shadow, err)
	}
}

func TestReindexRetainsAncestorsOfQueuedLocalObjects(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	folder := indexedRemoteEntry("folder", "working", "root-drive")
	folder.Object.MIMEType = rootMIMEType
	child := indexedRemoteEntry("child", "draft.txt", "folder")
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", []RemoteEntry{folder}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "folder", "", []RemoteEntry{child}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-1"); err != nil {
		t.Fatal(err)
	}
	content, err := NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := content.WritePending(ctx, WriteRequest{
		OperationID: "op-nested-pending", ObjectID: child.Object.ID, Kind: domain.OperationWrite,
		IdempotencyKey: "nested-pending", ExpectedSize: 5,
	}, strings.NewReader("draft")); err != nil {
		t.Fatal(err)
	}
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "folder", Removed: true}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	rootChildren, err := metadata.Children(ctx, domain.RootObjectID)
	if err != nil || len(rootChildren) != 1 || rootChildren[0].ObjectID != folder.Object.ID {
		t.Fatalf("remote folder removal hid queued descendant: %#v, err = %v", rootChildren, err)
	}
	if err := metadata.BeginReindex(ctx, "snapshot-2", "root-drive"); err != nil {
		t.Fatal(err)
	}
	root, err := metadata.ObjectByID(ctx, domain.RootObjectID)
	if err != nil || root.Deleted {
		t.Fatalf("reindex marked namespace root deleted: %#v, err = %v", root, err)
	}
	rootChildren, err = metadata.Children(ctx, domain.RootObjectID)
	if err != nil || len(rootChildren) != 1 || rootChildren[0].ObjectID != folder.Object.ID {
		t.Fatalf("reindex hid ancestor folder: %#v, err = %v", rootChildren, err)
	}
	folderChildren, err := metadata.Children(ctx, folder.Object.ID)
	if err != nil || len(folderChildren) != 1 || folderChildren[0].ObjectID != child.Object.ID {
		t.Fatalf("reindex hid queued local child: %#v, err = %v", folderChildren, err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live-3"); err != nil {
		t.Fatal(err)
	}
	shadow, err := metadata.RemoteShadow(ctx, folder.Object.ID)
	if err != nil || !shadow.Removed || !shadow.Confirmed {
		t.Fatalf("missing ancestor shadow = %#v, err = %v", shadow, err)
	}
}

func TestDuplicateDriveNamesUseDistinctPersistedDisplayNames(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.db")
	store, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()

	entries := []domain.DirectoryEntry{
		{ParentID: domain.RootObjectID, ObjectID: "first", RemoteName: "Plan.docx", DisplayName: "Plan (a1b2).docx"},
		{ParentID: domain.RootObjectID, ObjectID: "second", RemoteName: "Plan.docx", DisplayName: "Plan (c3d4).docx"},
	}
	for _, entry := range entries {
		if _, err := store.CreateObject(ctx, domain.Object{ID: entry.ObjectID, MIMEType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}, entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range entries {
		got, err := store.DirectoryEntry(ctx, domain.RootObjectID, expected.DisplayName)
		if err != nil {
			t.Fatal(err)
		}
		if got != expected {
			t.Fatalf("directory entry = %#v, want %#v", got, expected)
		}
	}
}

func TestObjectInsertIsAtomicAndConstraintsAreEnforced(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	driveID := domain.DriveFileID("drive-unique")
	first := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "first", RemoteName: "same", DisplayName: "same (a1).txt"}
	if _, err := store.CreateObject(ctx, domain.Object{ID: first.ObjectID, DriveFileID: &driveID, MIMEType: "text/plain"}, first); err != nil {
		t.Fatal(err)
	}

	duplicateID := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "duplicate-drive-id", RemoteName: "other", DisplayName: "other.txt"}
	if _, err := store.CreateObject(ctx, domain.Object{ID: duplicateID.ObjectID, DriveFileID: &driveID, MIMEType: "text/plain"}, duplicateID); err == nil {
		t.Fatal("duplicate Drive file ID was accepted")
	}
	if _, err := store.ObjectByID(ctx, duplicateID.ObjectID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed insert left a partial object behind: %v", err)
	}

	duplicateName := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "duplicate-display-name", RemoteName: "different", DisplayName: first.DisplayName}
	if _, err := store.CreateObject(ctx, domain.Object{ID: duplicateName.ObjectID, MIMEType: "text/plain"}, duplicateName); err == nil {
		t.Fatal("duplicate visible name in one parent was accepted")
	}
	if _, err := store.ObjectByID(ctx, duplicateName.ObjectID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed directory insert left an object behind: %v", err)
	}

	badParent := domain.DirectoryEntry{ParentID: "missing-parent", ObjectID: "orphan", RemoteName: "orphan", DisplayName: "orphan"}
	if _, err := store.CreateObject(ctx, domain.Object{ID: badParent.ObjectID, MIMEType: "text/plain"}, badParent); err == nil {
		t.Fatal("missing parent was accepted")
	}
	if _, err := store.ObjectByID(ctx, badParent.ObjectID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("foreign-key failure left an object behind: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO objects (
		object_id, inode, mime_type, size_bytes, capabilities_json, is_deleted, created_at, updated_at
	) VALUES ('duplicate-inode', ?, 'text/plain', 0, '{}', 0, ?, ?)`, RootInode, now, now); err == nil {
		t.Fatal("duplicate inode was accepted")
	}

	var foreignKeys int
	if err := store.db.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign_keys pragma = %d, err = %v; want 1, nil", foreignKeys, err)
	}
}

func TestInodeSequenceDoesNotReuseDeletedObject(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	firstEntry := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "first", RemoteName: "first", DisplayName: "first"}
	first, err := store.CreateObject(ctx, domain.Object{ID: firstEntry.ObjectID, MIMEType: "text/plain"}, firstEntry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM directory_entries WHERE object_id = ?`, string(first.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM objects WHERE object_id = ?`, string(first.ID)); err != nil {
		t.Fatal(err)
	}
	secondEntry := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "second", RemoteName: "second", DisplayName: "second"}
	second, err := store.CreateObject(ctx, domain.Object{ID: secondEntry.ObjectID, MIMEType: "text/plain"}, secondEntry)
	if err != nil {
		t.Fatal(err)
	}
	if second.Inode <= first.Inode {
		t.Fatalf("inode was reused: deleted=%d next=%d", first.Inode, second.Inode)
	}
}

func TestOperationIDsAndIdempotencyKeysAreUnique(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entry := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "object", RemoteName: "file", DisplayName: "file"}
	if _, err := store.CreateObject(ctx, domain.Object{ID: entry.ObjectID, MIMEType: "text/plain"}, entry); err != nil {
		t.Fatal(err)
	}
	insert := func(id, idempotency string) error {
		_, err := store.db.ExecContext(ctx, `INSERT INTO operations (
			operation_id, object_id, kind, state, idempotency_key, payload_json, created_at, updated_at
		) VALUES (?, ?, 'write', 'queued', ?, '{}', ?, ?)`, id, "object", idempotency,
			time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
		return err
	}
	if err := insert("op-1", "idem-1"); err != nil {
		t.Fatal(err)
	}
	if err := insert("op-2", "idem-1"); err == nil {
		t.Fatal("duplicate idempotency key was accepted")
	}
	if err := insert("op-1", "idem-2"); err == nil {
		t.Fatal("duplicate operation ID was accepted")
	}
}

func TestAppliedMigrationChecksumCannotChange(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.db")
	store, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE schema_migrations SET checksum = 'changed' WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(databasePath); !errors.Is(err, ErrChangedMigration) {
		t.Fatalf("reopen error = %v, want ErrChangedMigration", err)
	}
}

func TestDisplayNamesRejectUnsafePathComponents(t *testing.T) {
	for _, name := range []string{"", ".", "..", "folder/file", "bad\x00name"} {
		if err := validateEntry(domain.DirectoryEntry{
			ParentID: domain.RootObjectID, ObjectID: "file", RemoteName: name, DisplayName: name,
		}); err == nil {
			t.Errorf("validateEntry accepted display name %q", name)
		}
	}
}

func TestRemoteUpdateRetiresSupersededCommittedLocalContent(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
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
	content, err := NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}

	localObj, err := metadata.CreateLocalFile(ctx, domain.RootObjectID, "note.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	written, err := content.WritePending(ctx, WriteRequest{
		ObjectID: localObj.ID, Kind: domain.OperationCreate,
		IdempotencyKey: "create-note", ExpectedSize: 8,
	}, strings.NewReader("local v1"))
	if err != nil {
		t.Fatal(err)
	}
	// Uncommitted local write (drive_version == "") is returned.
	if cv, ok, err := content.CurrentLocalContentVersion(ctx, localObj); err != nil || !ok || cv.ID != written.ContentVersion.ID {
		t.Fatalf("uncommitted CurrentLocalContentVersion = (%#v, %v, %v)", cv, ok, err)
	}

	claimed, found, err := metadata.ClaimNextOperation(ctx, time.Now())
	if err != nil || !found || claimed.ID != written.Operation.ID {
		t.Fatalf("ClaimNextOperation = (%#v, %v, %v)", claimed, found, err)
	}
	if err := metadata.CommitRemoteOperation(ctx, claimed.ID, RemoteCommit{
		DriveFileID: "drive-note-1", MIMEType: "text/plain", Size: 8, DriveVersion: "v1", CapabilitiesJSON: `{"canEdit":true}`,
	}); err != nil {
		t.Fatal(err)
	}
	createdObj, err := metadata.ObjectByID(ctx, localObj.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cv, ok, err := content.CurrentLocalContentVersion(ctx, createdObj); err != nil || !ok || cv.DriveVersion != "v1" {
		t.Fatalf("committed CurrentLocalContentVersion = (%#v, %v, %v)", cv, ok, err)
	}

	// Own rename advances drive_version on the committed local content version.
	if _, err := metadata.QueueRename(ctx, domain.RootObjectID, "note.txt", domain.RootObjectID, "note-renamed.txt", false); err != nil {
		t.Fatal(err)
	}
	renameOp, found, err := metadata.ClaimNextOperation(ctx, time.Now())
	if err != nil || !found || renameOp.Kind != domain.OperationRename {
		t.Fatalf("ClaimNextOperation(rename) = (%#v, %v, %v)", renameOp, found, err)
	}
	if err := metadata.CommitRemoteOperation(ctx, renameOp.ID, RemoteCommit{
		DriveFileID: "drive-note-1", MIMEType: "text/plain", Size: 8, DriveVersion: "v2", CapabilitiesJSON: `{"canEdit":true}`,
	}); err != nil {
		t.Fatal(err)
	}
	renamedObj, err := metadata.ObjectByID(ctx, localObj.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cv, ok, err := content.CurrentLocalContentVersion(ctx, renamedObj); err != nil || !ok || cv.DriveVersion != "v2" {
		t.Fatalf("renamed CurrentLocalContentVersion = (%#v, %v, %v)", cv, ok, err)
	}

	// Remote edit from another computer bumps DriveVersion to v3 and retires local pending content.
	pageToken, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remoteEdit := indexedRemoteEntry("drive-note-1", "note-renamed.txt", "root-drive")
	remoteEdit.Object.Size = 18
	remoteEdit.Object.DriveVersion = "v3"
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "drive-note-1", Entry: &remoteEdit}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	updatedObj, err := metadata.ObjectByID(ctx, localObj.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cv, ok, err := content.CurrentLocalContentVersion(ctx, updatedObj); err != nil || ok {
		t.Fatalf("stale local content was still active after remote edit: (%#v, %v, %v)", cv, ok, err)
	}
	if _, err := content.CurrentContentVersion(ctx, localObj.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("CurrentContentVersion after remote edit = %v, want sql.ErrNoRows", err)
	}
}
