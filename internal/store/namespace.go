package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"

	"google-drive-linux/internal/domain"
)

var (
	ErrDirectoryNotEmpty = errors.New("directory is not empty")
	ErrNotDirectory      = errors.New("object is not a directory")
	ErrIsDirectory       = errors.New("object is a directory")
	ErrDirectoryCycle    = errors.New("directory cannot be moved into itself or a descendant")
)

type namespacePayload struct {
	ParentObjectID    domain.ObjectID `json:"parent_object_id,omitempty"`
	Name              string          `json:"name,omitempty"`
	OldParentObjectID domain.ObjectID `json:"old_parent_object_id,omitempty"`
	OldName           string          `json:"old_name,omitempty"`
	NewParentObjectID domain.ObjectID `json:"new_parent_object_id,omitempty"`
	NewName           string          `json:"new_name,omitempty"`
	ReplacedObjectID  domain.ObjectID `json:"replaced_object_id,omitempty"`
}

// CreateLocalFolder creates a local directory and its queued mkdir operation atomically.
func (s *Store) CreateLocalFolder(ctx context.Context, parentID domain.ObjectID, name string) (domain.Object, domain.Operation, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return domain.Object{}, domain.Operation{}, errors.New("directory name must be a valid POSIX path component")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Object{}, domain.Operation{}, fmt.Errorf("begin local folder creation: %w", err)
	}
	defer tx.Rollback()
	parent, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(parentID)))
	if err != nil {
		return domain.Object{}, domain.Operation{}, fmt.Errorf("read local folder parent: %w", err)
	}
	if parent.Deleted {
		return domain.Object{}, domain.Operation{}, sql.ErrNoRows
	}
	if parent.MIMEType != rootMIMEType {
		return domain.Object{}, domain.Operation{}, ErrNotDirectory
	}
	var duplicate int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM directory_entries WHERE parent_object_id = ? AND display_name = ?)`, string(parentID), name).Scan(&duplicate); err != nil {
		return domain.Object{}, domain.Operation{}, fmt.Errorf("check local folder name: %w", err)
	}
	if duplicate != 0 {
		return domain.Object{}, domain.Operation{}, ErrDirectoryEntryExists
	}
	object, err := insertLocalObjectTx(ctx, tx, parentID, name, rootMIMEType)
	if err != nil {
		return domain.Object{}, domain.Operation{}, err
	}
	dependencies, err := pendingOperationsTx(ctx, tx, parentID, "")
	if err != nil {
		return domain.Object{}, domain.Operation{}, err
	}
	nameDependencies, err := pendingTrashAtPathTx(ctx, tx, parentID, name)
	if err != nil {
		return domain.Object{}, domain.Operation{}, err
	}
	dependencies = uniqueOperationIDs(append(dependencies, nameDependencies...))
	if parent.DriveFileID == nil && len(dependencies) == 0 {
		return domain.Object{}, domain.Operation{}, errors.New("local folder parent has no queued creation operation")
	}
	payload, _ := json.Marshal(namespacePayload{ParentObjectID: parentID, Name: name})
	operation, err := insertQueuedOperationTx(ctx, tx, object.ID, domain.OperationMkdir, "", string(payload), dependencies)
	if err != nil {
		return domain.Object{}, domain.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Object{}, domain.Operation{}, fmt.Errorf("commit local folder creation: %w", err)
	}
	return object, operation, nil
}

// QueueRename changes the local namespace and queues the corresponding remote move atomically.
// Replacing an existing name queues its trash operation before the rename.
func (s *Store) QueueRename(ctx context.Context, oldParentID domain.ObjectID, oldName string, newParentID domain.ObjectID, newName string, noReplace bool) (domain.Object, error) {
	if oldName == "" || newName == "" || oldName == "." || oldName == ".." || newName == "." || newName == ".." || strings.ContainsAny(oldName+newName, "/\x00") {
		return domain.Object{}, errors.New("rename names must be valid POSIX path components")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Object{}, fmt.Errorf("begin local rename: %w", err)
	}
	defer tx.Rollback()
	sourceEntry, err := directoryEntryTx(ctx, tx, oldParentID, oldName)
	if err != nil {
		return domain.Object{}, err
	}
	oldParent, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(oldParentID)))
	if err != nil {
		return domain.Object{}, fmt.Errorf("read local rename source parent: %w", err)
	}
	if oldParent.MIMEType != rootMIMEType {
		return domain.Object{}, ErrNotDirectory
	}
	source, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(sourceEntry.ObjectID)))
	if err != nil {
		return domain.Object{}, fmt.Errorf("read rename source: %w", err)
	}
	newParent, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(newParentID)))
	if err != nil {
		return domain.Object{}, fmt.Errorf("read rename target parent: %w", err)
	}
	if newParent.Deleted {
		return domain.Object{}, sql.ErrNoRows
	}
	if newParent.MIMEType != rootMIMEType {
		return domain.Object{}, ErrNotDirectory
	}
	if source.MIMEType == rootMIMEType {
		var descendant int
		if err := tx.QueryRowContext(ctx, `WITH RECURSIVE descendants(object_id) AS (
			SELECT object_id FROM directory_entries WHERE parent_object_id = ?
			UNION ALL SELECT de.object_id FROM directory_entries de JOIN descendants d ON de.parent_object_id = d.object_id
		) SELECT EXISTS(SELECT 1 FROM descendants WHERE object_id = ?)`, string(source.ID), string(newParentID)).Scan(&descendant); err != nil {
			return domain.Object{}, fmt.Errorf("check folder move cycle: %w", err)
		}
		if descendant != 0 || source.ID == newParentID {
			return domain.Object{}, ErrDirectoryCycle
		}
	}
	var destinationEntry domain.DirectoryEntry
	var destinationObject domain.Object
	destinationEntry, destinationErr := directoryEntryTx(ctx, tx, newParentID, newName)
	if destinationErr == nil {
		if destinationEntry.ObjectID == source.ID {
			if oldParentID == newParentID && oldName == newName {
				if err := tx.Rollback(); err != nil {
					return domain.Object{}, err
				}
				return source, nil
			}
			return domain.Object{}, errors.New("rename target resolves to the source object")
		}
		if noReplace {
			return domain.Object{}, ErrDirectoryEntryExists
		}
		destinationObject, err = scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(destinationEntry.ObjectID)))
		if err != nil {
			return domain.Object{}, fmt.Errorf("read replaced rename destination: %w", err)
		}
		if source.MIMEType == rootMIMEType && destinationObject.MIMEType != rootMIMEType {
			return domain.Object{}, ErrNotDirectory
		}
		if source.MIMEType != rootMIMEType && destinationObject.MIMEType == rootMIMEType {
			return domain.Object{}, ErrIsDirectory
		}
		if destinationObject.MIMEType == rootMIMEType {
			var children int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM directory_entries WHERE parent_object_id = ?`, string(destinationObject.ID)).Scan(&children); err != nil {
				return domain.Object{}, err
			}
			if children != 0 {
				return domain.Object{}, ErrDirectoryNotEmpty
			}
		}
	} else if !errors.Is(destinationErr, sql.ErrNoRows) {
		return domain.Object{}, destinationErr
	}
	if source.MIMEType == rootMIMEType && source.ID == newParentID {
		return domain.Object{}, ErrDirectoryCycle
	}

	var replacementOperation domain.Operation
	if destinationErr == nil {
		replacementOperation, err = queueTrashTx(ctx, tx, destinationObject, destinationEntry)
		if err != nil {
			return domain.Object{}, err
		}
	}
	dependencies, err := pendingOperationsTx(ctx, tx, source.ID, "")
	if err != nil {
		return domain.Object{}, err
	}
	parentDependencies, err := pendingOperationsTx(ctx, tx, newParentID, "")
	if err != nil {
		return domain.Object{}, err
	}
	dependencies = append(dependencies, parentDependencies...)
	nameDependencies, err := pendingTrashAtPathTx(ctx, tx, newParentID, newName)
	if err != nil {
		return domain.Object{}, err
	}
	dependencies = append(dependencies, nameDependencies...)
	if replacementOperation.ID != "" {
		dependencies = append(dependencies, replacementOperation.ID)
	}
	dependencies = uniqueOperationIDs(dependencies)
	// If a new local file has never been flushed, its create operation will use the
	// final directory entry directly; queueing a rename before its create would invert order.
	sourceOperations, err := pendingOperationsTx(ctx, tx, source.ID, "")
	if err != nil {
		return domain.Object{}, err
	}
	if source.DriveFileID != nil || len(sourceOperations) > 0 {
		payload, _ := json.Marshal(namespacePayload{
			OldParentObjectID: oldParentID, OldName: sourceEntry.RemoteName,
			NewParentObjectID: newParentID, NewName: newName,
			ReplacedObjectID: destinationObject.ID,
		})
		if _, err := insertQueuedOperationTx(ctx, tx, source.ID, domain.OperationRename, source.DriveVersion, string(payload), dependencies); err != nil {
			return domain.Object{}, err
		}
	}
	if destinationErr == nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM directory_entries WHERE object_id = ?`, string(destinationObject.ID)); err != nil {
			return domain.Object{}, fmt.Errorf("hide replaced rename destination: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET is_deleted = 1, updated_at = ? WHERE object_id = ?`, formatTime(time.Now()), string(destinationObject.ID)); err != nil {
			return domain.Object{}, fmt.Errorf("mark replaced rename destination: %w", err)
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE directory_entries SET parent_object_id = ?, remote_name = ?, display_name = ? WHERE object_id = ?`,
		string(newParentID), newName, newName, string(source.ID))
	if err != nil {
		var sqliteErr sqlite3.Error
		if errors.As(err, &sqliteErr) && sqliteErr.ExtendedCode == sqlite3.ErrConstraintPrimaryKey {
			return domain.Object{}, ErrDirectoryEntryExists
		}
		return domain.Object{}, fmt.Errorf("move local directory entry: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return domain.Object{}, fmt.Errorf("move local directory entry affected %d rows (err=%v)", affected, err)
	}
	updatedAt := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET updated_at = ? WHERE object_id = ?`, formatTime(updatedAt), string(source.ID)); err != nil {
		return domain.Object{}, fmt.Errorf("update renamed object timestamp: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Object{}, fmt.Errorf("commit local rename: %w", err)
	}
	source.UpdatedAt = updatedAt
	return source, nil
}

// QueueTrash removes a file or empty folder from the visible tree and queues Drive trash.
func (s *Store) QueueTrash(ctx context.Context, parentID domain.ObjectID, name string, directory bool) (domain.Object, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Object{}, fmt.Errorf("begin local trash operation: %w", err)
	}
	defer tx.Rollback()
	parent, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(parentID)))
	if err != nil {
		return domain.Object{}, fmt.Errorf("read local trash parent: %w", err)
	}
	if parent.MIMEType != rootMIMEType {
		return domain.Object{}, ErrNotDirectory
	}
	entry, err := directoryEntryTx(ctx, tx, parentID, name)
	if err != nil {
		return domain.Object{}, err
	}
	object, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(entry.ObjectID)))
	if err != nil {
		return domain.Object{}, err
	}
	if directory != (object.MIMEType == rootMIMEType) {
		if directory {
			return domain.Object{}, ErrNotDirectory
		}
		return domain.Object{}, ErrIsDirectory
	}
	if directory {
		var children int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM directory_entries WHERE parent_object_id = ?`, string(object.ID)).Scan(&children); err != nil {
			return domain.Object{}, fmt.Errorf("check folder before trash: %w", err)
		}
		if children != 0 {
			return domain.Object{}, ErrDirectoryNotEmpty
		}
	}
	if _, err := queueTrashTx(ctx, tx, object, entry); err != nil {
		return domain.Object{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Object{}, fmt.Errorf("commit local trash operation: %w", err)
	}
	object.Deleted = true
	return object, nil
}

func queueTrashTx(ctx context.Context, tx *sql.Tx, object domain.Object, entry domain.DirectoryEntry) (domain.Operation, error) {
	dependencies, err := pendingOperationsTx(ctx, tx, object.ID, "")
	if err != nil {
		return domain.Operation{}, err
	}
	if object.MIMEType == rootMIMEType {
		children, err := pendingTrashChildrenTx(ctx, tx, object.ID)
		if err != nil {
			return domain.Operation{}, err
		}
		dependencies = append(dependencies, children...)
	}
	payload, _ := json.Marshal(namespacePayload{ParentObjectID: entry.ParentID, Name: entry.RemoteName})
	operation, err := insertQueuedOperationTx(ctx, tx, object.ID, domain.OperationTrash, object.DriveVersion, string(payload), dependencies)
	if err != nil {
		return domain.Operation{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM directory_entries WHERE object_id = ?`, string(object.ID)); err != nil {
		return domain.Operation{}, fmt.Errorf("hide locally trashed object: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET is_deleted = 1, updated_at = ? WHERE object_id = ?`, formatTime(time.Now()), string(object.ID)); err != nil {
		return domain.Operation{}, fmt.Errorf("mark locally trashed object: %w", err)
	}
	return operation, nil
}

func insertLocalObjectTx(ctx context.Context, tx *sql.Tx, parentID domain.ObjectID, name, mimeType string) (domain.Object, error) {
	token, err := randomContentToken()
	if err != nil {
		return domain.Object{}, fmt.Errorf("generate local object ID: %w", err)
	}
	objectID := domain.ObjectID("local:" + token)
	sequence, err := tx.ExecContext(ctx, `INSERT INTO inode_sequence DEFAULT VALUES`)
	if err != nil {
		return domain.Object{}, fmt.Errorf("allocate local object inode: %w", err)
	}
	inode, err := sequence.LastInsertId()
	if err != nil {
		return domain.Object{}, fmt.Errorf("read local object inode: %w", err)
	}
	now := time.Now().UTC()
	object := domain.Object{ID: objectID, Inode: domain.Inode(inode), MIMEType: mimeType,
		CapabilitiesJSON: `{"canEdit":true}`, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO objects (
		object_id, drive_file_id, inode, mime_type, size_bytes, drive_version, modified_at,
		web_view_link, capabilities_json, is_deleted, created_at, updated_at
	) VALUES (?, NULL, ?, ?, 0, NULL, NULL, NULL, ?, 0, ?, ?)`, string(object.ID), inode, object.MIMEType,
		object.CapabilitiesJSON, formatTime(now), formatTime(now)); err != nil {
		return domain.Object{}, fmt.Errorf("insert local object metadata: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO directory_entries (parent_object_id, object_id, remote_name, display_name)
		VALUES (?, ?, ?, ?)`, string(parentID), string(object.ID), name, name); err != nil {
		var sqliteErr sqlite3.Error
		if errors.As(err, &sqliteErr) && sqliteErr.ExtendedCode == sqlite3.ErrConstraintPrimaryKey {
			return domain.Object{}, ErrDirectoryEntryExists
		}
		return domain.Object{}, fmt.Errorf("insert local directory entry: %w", err)
	}
	return object, nil
}

func directoryEntryTx(ctx context.Context, tx *sql.Tx, parentID domain.ObjectID, name string) (domain.DirectoryEntry, error) {
	var parent, object string
	var entry domain.DirectoryEntry
	err := tx.QueryRowContext(ctx, `SELECT parent_object_id, object_id, remote_name, display_name
		FROM directory_entries WHERE parent_object_id = ? AND display_name = ?`, string(parentID), name).
		Scan(&parent, &object, &entry.RemoteName, &entry.DisplayName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.DirectoryEntry{}, err
		}
		return domain.DirectoryEntry{}, fmt.Errorf("read directory entry: %w", err)
	}
	entry.ParentID, entry.ObjectID = domain.ObjectID(parent), domain.ObjectID(object)
	return entry, nil
}

func pendingOperationsTx(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, exclude domain.OperationID) ([]domain.OperationID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT operation_id FROM operations WHERE object_id = ? AND state <> 'committed'
		AND operation_id <> ? ORDER BY created_at, operation_id`, string(objectID), string(exclude))
	if err != nil {
		return nil, fmt.Errorf("list pending object operations: %w", err)
	}
	defer rows.Close()
	var ids []domain.OperationID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("read pending operation ID: %w", err)
		}
		ids = append(ids, domain.OperationID(id))
	}
	return uniqueOperationIDs(ids), rows.Err()
}

func operationDependenciesForContentTx(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, operationID domain.OperationID) ([]domain.OperationID, error) {
	dependencies, err := pendingOperationsTx(ctx, tx, objectID, operationID)
	if err != nil {
		return nil, err
	}
	var parentID, name string
	err = tx.QueryRowContext(ctx, `SELECT parent_object_id, remote_name FROM directory_entries WHERE object_id = ?`, string(objectID)).Scan(&parentID, &name)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("read content operation parent: %w", err)
	}
	if err == nil {
		parentDependencies, err := pendingOperationsTx(ctx, tx, domain.ObjectID(parentID), operationID)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, parentDependencies...)
		nameDependencies, err := pendingTrashAtPathTx(ctx, tx, domain.ObjectID(parentID), name)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, nameDependencies...)
	}
	return uniqueOperationIDs(dependencies), nil
}

func pendingTrashAtPathTx(ctx context.Context, tx *sql.Tx, parentID domain.ObjectID, name string) ([]domain.OperationID, error) {
	if name == "" {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT operation_id FROM operations WHERE kind = 'trash' AND state <> 'committed'
		AND json_extract(payload_json, '$.parent_object_id') = ? AND json_extract(payload_json, '$.name') = ?
		ORDER BY created_at, operation_id`, string(parentID), name)
	if err != nil {
		return nil, fmt.Errorf("find pending trash at namespace path: %w", err)
	}
	defer rows.Close()
	var ids []domain.OperationID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, domain.OperationID(id))
	}
	return uniqueOperationIDs(ids), rows.Err()
}

func pendingTrashChildrenTx(ctx context.Context, tx *sql.Tx, parentID domain.ObjectID) ([]domain.OperationID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT operation_id FROM operations WHERE kind = 'trash' AND state <> 'committed'
		AND json_extract(payload_json, '$.parent_object_id') = ? ORDER BY created_at, operation_id`, string(parentID))
	if err != nil {
		return nil, fmt.Errorf("find pending child trash operations: %w", err)
	}
	defer rows.Close()
	var ids []domain.OperationID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, domain.OperationID(id))
	}
	return uniqueOperationIDs(ids), rows.Err()
}

func insertOperationDependenciesTx(ctx context.Context, tx *sql.Tx, operationID domain.OperationID, dependencies []domain.OperationID) error {
	for _, dependency := range uniqueOperationIDs(dependencies) {
		if dependency == "" || dependency == operationID {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO operation_dependencies (operation_id, dependency_id) VALUES (?, ?)`, string(operationID), string(dependency)); err != nil {
			return fmt.Errorf("record operation dependency: %w", err)
		}
	}
	return nil
}

func insertQueuedOperationTx(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, kind domain.OperationKind, baseVersion, payload string, dependencies []domain.OperationID) (domain.Operation, error) {
	if objectID == "" || (kind != domain.OperationMkdir && kind != domain.OperationRename && kind != domain.OperationTrash) {
		return domain.Operation{}, errors.New("invalid namespace operation")
	}
	if payload == "" {
		payload = "{}"
	}
	if !json.Valid([]byte(payload)) {
		return domain.Operation{}, errors.New("namespace operation payload must be valid JSON")
	}
	token, err := randomContentToken()
	if err != nil {
		return domain.Operation{}, fmt.Errorf("generate namespace operation ID: %w", err)
	}
	operationID := domain.OperationID("op-" + token)
	idempotencyKey := "namespace-op:" + token
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (
		operation_id, object_id, kind, state, idempotency_key, base_drive_version, content_version_id,
		payload_json, created_at, updated_at
	) VALUES (?, ?, ?, 'queued', ?, ?, NULL, ?, ?, ?)`, string(operationID), string(objectID), string(kind), idempotencyKey,
		baseVersion, payload, formatTime(now), formatTime(now)); err != nil {
		return domain.Operation{}, fmt.Errorf("queue namespace operation: %w", err)
	}
	for _, dependency := range uniqueOperationIDs(dependencies) {
		if dependency == "" || dependency == operationID {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO operation_dependencies (operation_id, dependency_id) VALUES (?, ?)`, string(operationID), string(dependency)); err != nil {
			return domain.Operation{}, fmt.Errorf("record namespace operation dependency: %w", err)
		}
	}
	return domain.Operation{ID: operationID, ObjectID: objectID, Kind: kind, State: domain.OperationQueued,
		IdempotencyKey: idempotencyKey, BaseDriveVersion: baseVersion, PayloadJSON: payload, CreatedAt: now, UpdatedAt: now}, nil
}

func uniqueOperationIDs(ids []domain.OperationID) []domain.OperationID {
	seen := make(map[domain.OperationID]struct{}, len(ids))
	result := make([]domain.OperationID, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func (s *Store) OperationDependencies(ctx context.Context, operationID domain.OperationID) ([]domain.OperationID, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT dependency_id FROM operation_dependencies WHERE operation_id = ? ORDER BY dependency_id`, string(operationID))
	if err != nil {
		return nil, fmt.Errorf("list operation dependencies: %w", err)
	}
	defer rows.Close()
	var ids []domain.OperationID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, domain.OperationID(id))
	}
	return ids, rows.Err()
}

func (s *Store) OperationsForObject(ctx context.Context, objectID domain.ObjectID) ([]domain.Operation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT operation_id, object_id, kind, state, idempotency_key, base_drive_version,
		content_version_id, payload_json, resumable_session_url, resumable_offset, attempt_count, next_attempt_at,
		last_error_code, last_error_message, created_at, updated_at FROM operations
		WHERE object_id = ? ORDER BY created_at, operation_id`, string(objectID))
	if err != nil {
		return nil, fmt.Errorf("list object operations: %w", err)
	}
	defer rows.Close()
	reader := &ContentStore{metadata: s}
	var operations []domain.Operation
	for rows.Next() {
		operation, err := reader.scanOperation(rows)
		if err != nil {
			return nil, fmt.Errorf("read object operation: %w", err)
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read object operations: %w", err)
	}
	return operations, nil
}
