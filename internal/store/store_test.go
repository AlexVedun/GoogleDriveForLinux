package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
)

func TestSchemaMigrationAndObjectIdentitySurviveReopen(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "state", "metadata.db")
	store, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	version, err := store.SchemaVersion(ctx)
	if err != nil || version != 4 {
		t.Fatalf("schema version = %d, err = %v; want 4, nil", version, err)
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

func TestExistingVersionOneDatabaseUpgradesToVersionThree(t *testing.T) {
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
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	version, err := upgraded.SchemaVersion(ctx)
	if err != nil || version != 4 {
		t.Fatalf("upgraded schema version = %d, err = %v", version, err)
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

func TestRemoteChangeShadowPreservesQueuedLocalWriteAndPath(t *testing.T) {
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
	if err != nil || after.Size != before.Size || after.DriveVersion != before.DriveVersion || after.Deleted {
		t.Fatalf("remote update overwrote queued local object: before=%#v after=%#v err=%v", before, after, err)
	}
	if _, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "original.txt"); err != nil {
		t.Fatalf("local path changed before queued rename was sent: %v", err)
	}
	shadow, err := metadata.RemoteShadow(ctx, entry.Object.ID)
	if err != nil || shadow.RemoteName != "renamed remotely.txt" || shadow.Size != 800 || shadow.DriveVersion != "2" || !shadow.Confirmed || shadow.Removed || !strings.Contains(shadow.CapabilitiesJSON, "canEdit") {
		t.Fatalf("remote update shadow = %#v, err = %v", shadow, err)
	}
	pageToken, err = metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, pageToken, []RemoteChange{{DriveFileID: "queued", Removed: true}}, "", "live-3"); err != nil {
		t.Fatal(err)
	}
	after, err = metadata.ObjectByID(ctx, entry.Object.ID)
	if err != nil || after.Deleted || after.Size != before.Size {
		t.Fatalf("remote removal discarded queued local object: %#v, err = %v", after, err)
	}
	shadow, err = metadata.RemoteShadow(ctx, entry.Object.ID)
	if err != nil || !shadow.Removed || !shadow.Confirmed {
		t.Fatalf("remote removal shadow = %#v, err = %v", shadow, err)
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
