package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"google-drive-linux/internal/domain"
)

func TestNamespaceOperationsQueueDependenciesAndPreserveLocalIdentity(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	databasePath := metadata.Path()
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live"); err != nil {
		t.Fatal(err)
	}
	content, err := NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}

	parent, parentCreate, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "parent")
	if err != nil {
		t.Fatal(err)
	}
	child, childCreate, err := metadata.CreateLocalFolder(ctx, parent.ID, "child")
	if err != nil {
		t.Fatal(err)
	}
	dependencies, err := metadata.OperationDependencies(ctx, childCreate.ID)
	if err != nil || !containsOperation(dependencies, parentCreate.ID) {
		t.Fatalf("child mkdir dependencies = %v, err=%v; want parent %s", dependencies, err, parentCreate.ID)
	}

	file, err := metadata.CreateLocalFile(ctx, child.ID, "draft.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	written, err := content.WritePending(ctx, WriteRequest{ObjectID: file.ID, Kind: domain.OperationCreate,
		IdempotencyKey: "namespace-test-create", ExpectedSize: 4}, strings.NewReader("data"))
	if err != nil {
		t.Fatal(err)
	}
	dependencies, err = metadata.OperationDependencies(ctx, written.Operation.ID)
	if err != nil || !containsOperation(dependencies, childCreate.ID) {
		t.Fatalf("file create dependencies = %v, err=%v; want parent %s", dependencies, err, childCreate.ID)
	}
	if _, err := metadata.QueueRename(ctx, child.ID, "draft.txt", domain.RootObjectID, "renamed.txt", false); err != nil {
		t.Fatal(err)
	}
	entry, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "renamed.txt")
	if err != nil || entry.ObjectID != file.ID {
		t.Fatalf("moved entry = %#v, err=%v", entry, err)
	}
	object, err := metadata.ObjectByID(ctx, file.ID)
	if err != nil || object.DriveFileID != nil {
		t.Fatalf("local move changed remote identity: object=%#v err=%v", object, err)
	}
	operations, err := metadata.OperationsForObject(ctx, file.ID)
	if err != nil || len(operations) != 2 || operations[1].Kind != domain.OperationRename {
		t.Fatalf("file operations = %#v err=%v; want create then rename", operations, err)
	}
	dependencies, err = metadata.OperationDependencies(ctx, operations[1].ID)
	if err != nil || !containsOperation(dependencies, written.Operation.ID) {
		t.Fatalf("rename dependencies = %v err=%v; want create %s", dependencies, err, written.Operation.ID)
	}

	if _, err := metadata.QueueTrash(ctx, domain.RootObjectID, "renamed.txt", false); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.DirectoryEntry(ctx, domain.RootObjectID, "renamed.txt"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("trashed file remains visible")
	}
	operations, err = metadata.OperationsForObject(ctx, file.ID)
	if err != nil || len(operations) != 3 || operations[2].Kind != domain.OperationTrash {
		t.Fatalf("file operations after trash = %#v err=%v", operations, err)
	}
	dependencies, err = metadata.OperationDependencies(ctx, operations[2].ID)
	if err != nil || !containsOperation(dependencies, operations[1].ID) {
		t.Fatalf("trash dependencies = %v err=%v; want rename %s", dependencies, err, operations[1].ID)
	}

	if _, err := metadata.QueueTrash(ctx, domain.RootObjectID, "parent", true); !errors.Is(err, ErrDirectoryNotEmpty) {
		t.Fatalf("rmdir non-empty parent error = %v; want ErrDirectoryNotEmpty", err)
	}
	if _, err := metadata.QueueTrash(ctx, parent.ID, "child", true); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.QueueTrash(ctx, domain.RootObjectID, "parent", true); err != nil {
		t.Fatal(err)
	}
	parentOperations, err := metadata.OperationsForObject(ctx, parent.ID)
	if err != nil || len(parentOperations) != 2 || parentOperations[1].Kind != domain.OperationTrash {
		t.Fatalf("parent operations = %#v err=%v", parentOperations, err)
	}
	parentDependencies, err := metadata.OperationDependencies(ctx, parentOperations[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	childOperations, err := metadata.OperationsForObject(ctx, child.ID)
	if err != nil || len(childOperations) != 2 || childOperations[1].Kind != domain.OperationTrash {
		t.Fatalf("child operations = %#v err=%v", childOperations, err)
	}
	if err != nil || !containsOperation(parentDependencies, childOperations[1].ID) {
		t.Fatalf("parent trash dependencies = %v err=%v; want child trash %s", parentDependencies, err, childOperations[1].ID)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	metadata, err = Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	operations, err = metadata.OperationsForObject(ctx, file.ID)
	if err != nil || len(operations) != 3 || operations[2].Kind != domain.OperationTrash {
		t.Fatalf("file operations after restart = %#v err=%v", operations, err)
	}
	parentOperations, err = metadata.OperationsForObject(ctx, parent.ID)
	if err != nil || len(parentOperations) != 2 || parentOperations[1].Kind != domain.OperationTrash {
		t.Fatalf("parent operations after restart = %#v err=%v", parentOperations, err)
	}
	parentDependencies, err = metadata.OperationDependencies(ctx, parentOperations[1].ID)
	if err != nil || !containsOperation(parentDependencies, childOperations[1].ID) {
		t.Fatalf("parent dependencies after restart = %v err=%v", parentDependencies, err)
	}
}

func TestQueueRenameRejectsFolderCyclesAndNonemptyReplacement(t *testing.T) {
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
	parent, _, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "parent")
	if err != nil {
		t.Fatal(err)
	}
	child, _, err := metadata.CreateLocalFolder(ctx, parent.ID, "child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.QueueRename(ctx, domain.RootObjectID, "parent", child.ID, "cycle", false); !errors.Is(err, ErrDirectoryCycle) {
		t.Fatalf("cycle rename error = %v; want ErrDirectoryCycle", err)
	}
	destination, _, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "destination")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := metadata.CreateLocalFolder(ctx, destination.ID, "occupied"); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.QueueRename(ctx, domain.RootObjectID, "parent", domain.RootObjectID, "destination", false); !errors.Is(err, ErrDirectoryNotEmpty) {
		t.Fatalf("nonempty replacement error = %v; want ErrDirectoryNotEmpty", err)
	}
}

func containsOperation(ids []domain.OperationID, expected domain.OperationID) bool {
	for _, id := range ids {
		if id == expected {
			return true
		}
	}
	return false
}
