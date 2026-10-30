package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"google-drive-linux/internal/domain"
)

func TestGeneratedDriveIDKeepsLocalObjectIdentityDuringChangeIndexing(t *testing.T) {
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
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live"); err != nil {
		t.Fatal(err)
	}
	folder, _, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "reserved-folder")
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.ReserveDriveFileID(ctx, folder.ID, "generated-folder-id"); err != nil {
		t.Fatal(err)
	}
	page, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	driveID := domain.DriveFileID("generated-folder-id")
	entry := RemoteEntry{Object: domain.Object{
		ID: "drive:generated-folder-id", DriveFileID: &driveID, MIMEType: rootMIMEType, CapabilitiesJSON: "{}",
	}, RemoteName: "reserved-folder", ParentDriveFileID: "root-drive"}
	if err := metadata.CommitChangePage(ctx, page, []RemoteChange{{DriveFileID: string(driveID), Entry: &entry}}, "", "live-2"); err != nil {
		t.Fatal(err)
	}
	updated, err := metadata.ObjectByID(ctx, folder.ID)
	if err != nil || updated.DriveFileID == nil || *updated.DriveFileID != driveID || updated.Inode != folder.Inode {
		t.Fatalf("pre-generated folder identity changed: object=%#v err=%v", updated, err)
	}
	if _, err := metadata.ObjectByID(ctx, entry.Object.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("indexer inserted a duplicate Drive object after pre-generated ID: err=%v", err)
	}
	shadow, err := metadata.RemoteShadow(ctx, folder.ID)
	if err != nil || shadow.DriveFileID != string(driveID) || shadow.RemoteName != "reserved-folder" {
		t.Fatalf("pending-object remote shadow = %#v err=%v", shadow, err)
	}
}
