package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google-drive-linux/internal/domain"
)

const (
	remoteWriteConflictCode     = "remote_write_conflict"
	remoteNamespaceConflictCode = "remote_namespace_conflict"
)

type ConflictCopy struct {
	OperationID       domain.OperationID `json:"operation_id"`
	RemoteObjectID    domain.ObjectID    `json:"remote_object_id"`
	LocalCopyObjectID domain.ObjectID    `json:"local_copy_object_id"`
	LocalCopyName     string             `json:"local_copy_name"`
	RemoteName        string             `json:"remote_name"`
	RemoteVersion     string             `json:"remote_version"`
}

type ConflictRecord struct {
	EventID            domain.EventID       `json:"event_id"`
	OperationID        domain.OperationID   `json:"operation_id"`
	ObjectID           domain.ObjectID      `json:"object_id"`
	Code               string               `json:"code"`
	LocalOperationKind domain.OperationKind `json:"local_operation_kind"`
	LocalIntent        string               `json:"local_intent"`
	LocalCopyObjectID  domain.ObjectID      `json:"local_copy_object_id"`
	LocalCopyName      string               `json:"local_copy_name"`
	RemoteName         string               `json:"remote_name"`
	RemoteVersion      string               `json:"remote_version"`
	CreatedAt          time.Time            `json:"created_at"`
}

func (s *Store) Conflicts(ctx context.Context) ([]ConflictRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT event_id, operation_id, object_id, code, details_json, created_at
		FROM events WHERE kind = 'conflict' AND resolved_at IS NULL ORDER BY created_at, event_id`)
	if err != nil {
		return nil, fmt.Errorf("list unresolved conflicts: %w", err)
	}
	defer rows.Close()
	var conflicts []ConflictRecord
	for rows.Next() {
		var item ConflictRecord
		var eventID, operationID, objectID, detailsJSON, createdAt string
		if err := rows.Scan(&eventID, &operationID, &objectID, &item.Code, &detailsJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("read unresolved conflict: %w", err)
		}
		var details map[string]string
		if err := json.Unmarshal([]byte(detailsJSON), &details); err != nil {
			return nil, fmt.Errorf("decode conflict details: %w", err)
		}
		item.EventID, item.OperationID, item.ObjectID = domain.EventID(eventID), domain.OperationID(operationID), domain.ObjectID(objectID)
		item.LocalOperationKind = domain.OperationKind(details["local_operation_kind"])
		item.LocalIntent = details["local_intent"]
		item.LocalCopyObjectID = domain.ObjectID(details["local_copy_object_id"])
		item.LocalCopyName, item.RemoteName, item.RemoteVersion = details["local_copy_name"], details["remote_name"], details["remote_version"]
		item.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse conflict creation time: %w", err)
		}
		conflicts = append(conflicts, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unresolved conflicts: %w", err)
	}
	return conflicts, nil
}

func (s *Store) ResolveConflict(ctx context.Context, eventID domain.EventID, action string) error {
	if eventID == "" || (action != "keep-both" && action != "keep-remote" && action != "keep-local") {
		return errors.New("conflict event and supported action are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin conflict resolution: %w", err)
	}
	defer tx.Rollback()
	var objectID, operationID, detailsJSON string
	err = tx.QueryRowContext(ctx, `SELECT object_id, operation_id, details_json FROM events
		WHERE event_id = ? AND kind = 'conflict' AND resolved_at IS NULL`, string(eventID)).
		Scan(&objectID, &operationID, &detailsJSON)
	if err != nil {
		return fmt.Errorf("find unresolved conflict: %w", err)
	}
	var details map[string]string
	if err := json.Unmarshal([]byte(detailsJSON), &details); err != nil {
		return fmt.Errorf("decode conflict resolution details: %w", err)
	}
	copyID := domain.ObjectID(details["local_copy_object_id"])
	kind := domain.OperationKind(details["local_operation_kind"])
	if kind == "" {
		operation, err := (&ContentStore{metadata: s}).scanOperation(tx.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
			idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
			resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
			FROM operations WHERE operation_id = ?`, operationID))
		if err != nil {
			return fmt.Errorf("read conflict operation: %w", err)
		}
		kind = operation.Kind
	}
	if kind == domain.OperationWrite && action == "keep-local" {
		return errors.New("write conflict actions are keep-both or keep-remote")
	}
	if (kind == domain.OperationRename || kind == domain.OperationTrash) && action == "keep-both" {
		return errors.New("namespace conflict actions are keep-local or keep-remote")
	}
	if action == "keep-local" {
		var baseVersion string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(drive_version, '') FROM objects WHERE object_id = ?`, objectID).Scan(&baseVersion); err != nil {
			return fmt.Errorf("read current remote version for local conflict intent: %w", err)
		}
		if baseVersion == "" {
			return errors.New("cannot retry local namespace intent without a current Drive version")
		}
		result, err := tx.ExecContext(ctx, `UPDATE operations SET state = 'queued', base_drive_version = ?,
			last_error_code = '', last_error_message = '', next_attempt_at = NULL, resumable_session_url = '',
			resumable_offset = 0, updated_at = ? WHERE operation_id = ? AND state = 'conflict'
			AND kind IN ('rename', 'trash')`, baseVersion, formatTime(time.Now()), operationID)
		if err != nil {
			return fmt.Errorf("requeue selected local namespace intent: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("requeue local namespace intent affected %d rows (err=%v)", affected, err)
		}
	}
	if action == "keep-remote" && copyID != "" {
		var parentRaw, name string
		err := tx.QueryRowContext(ctx, `SELECT parent_object_id, display_name FROM directory_entries WHERE object_id = ?`, string(copyID)).
			Scan(&parentRaw, &name)
		if err == nil {
			parentID := domain.ObjectID(parentRaw)
			object, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(copyID)))
			if err != nil {
				return fmt.Errorf("read local conflict copy: %w", err)
			}
			if _, err := queueTrashTx(ctx, tx, object, domain.DirectoryEntry{
				ParentID: parentID, ObjectID: copyID, RemoteName: name, DisplayName: name,
			}); err != nil {
				return fmt.Errorf("discard local conflict copy: %w", err)
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("find local conflict copy: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET resolved_at = ? WHERE event_id = ? AND resolved_at IS NULL`,
		formatTime(time.Now()), string(eventID)); err != nil {
		return fmt.Errorf("resolve conflict event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit conflict resolution: %w", err)
	}
	return nil
}

func (s *Store) PreserveWriteConflict(ctx context.Context, operationID domain.OperationID, remote RemoteEntry) (ConflictCopy, error) {
	if ctx == nil || operationID == "" || remote.Object.DriveFileID == nil ||
		*remote.Object.DriveFileID == "" || remote.RemoteName == "" || remote.Object.DriveVersion == "" {
		return ConflictCopy{}, errors.New("operation and complete remote file metadata are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ConflictCopy{}, fmt.Errorf("begin remote write-conflict preservation: %w", err)
	}
	defer tx.Rollback()
	result, err := preserveWriteConflictTx(ctx, tx, remote.Object.ID, operationID, remote)
	if err != nil {
		return ConflictCopy{}, err
	}
	if err := tx.Commit(); err != nil {
		return ConflictCopy{}, fmt.Errorf("commit remote write-conflict preservation: %w", err)
	}
	return result, nil
}

func firstActiveOperation(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID) (domain.OperationID, domain.OperationKind, error) {
	var id, kind string
	err := tx.QueryRowContext(ctx, `SELECT operation_id, kind FROM operations WHERE object_id = ?
		AND state IN ('prepared', 'queued', 'uploading', 'retryable-error', 'needs-reconcile')
		ORDER BY created_at, operation_id LIMIT 1`, string(objectID)).Scan(&id, &kind)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("find active operation for remote conflict: %w", err)
	}
	return domain.OperationID(id), domain.OperationKind(kind), nil
}

func hasLocalOperationMarker(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, marker string) (bool, error) {
	if marker == "" {
		return false, nil
	}
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM operations WHERE object_id = ? AND operation_id = ?
		AND state IN ('prepared', 'queued', 'uploading', 'retryable-error', 'needs-reconcile') LIMIT 1`,
		string(objectID), marker).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("match remote app operation marker: %w", err)
	}
	return true, nil
}

func preserveWriteConflictTx(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, operationID domain.OperationID,
	remote RemoteEntry) (ConflictCopy, error) {
	if existing, found, err := existingWriteConflict(ctx, tx, operationID); err != nil {
		return ConflictCopy{}, err
	} else if found {
		return existing, nil
	}
	operation, err := (&ContentStore{metadata: nil}).scanOperation(tx.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations WHERE operation_id = ?`, string(operationID)))
	if err != nil {
		return ConflictCopy{}, fmt.Errorf("read operation for remote write conflict: %w", err)
	}
	if operation.ObjectID != objectID || operation.Kind != domain.OperationWrite ||
		operation.State == domain.OperationCommitted || operation.State == domain.OperationConflict {
		return ConflictCopy{}, errors.New("remote conflict does not target an active local write")
	}
	object, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(objectID)))
	if err != nil {
		return ConflictCopy{}, fmt.Errorf("read local object for remote write conflict: %w", err)
	}
	if object.DriveFileID == nil || remote.Object.DriveFileID == nil ||
		*object.DriveFileID != *remote.Object.DriveFileID || operation.ContentVersionID == nil {
		return ConflictCopy{}, errors.New("remote conflict does not match the local object's Drive ID and content version")
	}

	var currentVersionID, localPath, checksum string
	var size int64
	var complete int
	err = tx.QueryRowContext(ctx, `SELECT version_id, local_path, size_bytes, COALESCE(sha256, ''), is_complete
		FROM content_versions WHERE object_id = ? AND is_current = 1 AND kind = 'pending'`,
		string(objectID)).Scan(&currentVersionID, &localPath, &size, &checksum, &complete)
	if err != nil {
		return ConflictCopy{}, fmt.Errorf("read current local content for conflict copy: %w", err)
	}
	if complete != 1 || checksum == "" {
		return ConflictCopy{}, errors.New("local conflict content is incomplete")
	}

	var parentRaw, localName string
	err = tx.QueryRowContext(ctx, `SELECT parent_object_id, remote_name FROM directory_entries WHERE object_id = ?`,
		string(objectID)).Scan(&parentRaw, &localName)
	if errors.Is(err, sql.ErrNoRows) {
		parentRaw, localName, err = lastLocalNamespacePath(ctx, tx, objectID)
	}
	if err != nil || parentRaw == "" || localName == "" {
		return ConflictCopy{}, errors.New("local conflict path is unavailable")
	}
	parentID := domain.ObjectID(parentRaw)
	copyName, err := uniqueConflictCopyName(ctx, tx, parentID, localName, operationID)
	if err != nil {
		return ConflictCopy{}, err
	}
	copyObject, err := insertLocalObjectTx(ctx, tx, parentID, copyName, object.MIMEType)
	if err != nil {
		return ConflictCopy{}, fmt.Errorf("create local conflict copy: %w", err)
	}
	now := formatTime(time.Now())
	result, err := tx.ExecContext(ctx, `UPDATE content_versions SET object_id = ? WHERE object_id = ? AND version_id = ?
		AND is_current = 1 AND kind = 'pending'`, string(copyObject.ID), string(objectID), currentVersionID)
	if err != nil {
		return ConflictCopy{}, fmt.Errorf("move local content version to conflict copy: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ConflictCopy{}, fmt.Errorf("move local conflict version affected %d rows (err=%v)", affected, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET size_bytes = ?, updated_at = ? WHERE object_id = ?`,
		size, now, string(copyObject.ID)); err != nil {
		return ConflictCopy{}, fmt.Errorf("update local conflict-copy size: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET state = 'conflict', last_error_code = 'remote_version_changed',
		last_error_message = 'Drive changed while a local write was pending', next_attempt_at = NULL, updated_at = ?
		WHERE object_id = ? AND state IN ('prepared', 'queued', 'uploading', 'retryable-error', 'needs-reconcile')`,
		now, string(objectID)); err != nil {
		return ConflictCopy{}, fmt.Errorf("mark local operations conflicted: %w", err)
	}

	remoteParentID, parentErr := objectIDForParent(ctx, tx, remote.ParentDriveFileID)
	if parentErr != nil && !errors.Is(parentErr, sql.ErrNoRows) {
		return ConflictCopy{}, parentErr
	}
	var modifiedAt any
	if !remote.Object.ModifiedAt.IsZero() {
		modifiedAt = nullableTime(remote.Object.ModifiedAt)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET mime_type = ?, size_bytes = ?, drive_version = ?,
		modified_at = COALESCE(?, modified_at), web_view_link = ?, capabilities_json = ?, is_deleted = 0, updated_at = ?
		WHERE object_id = ?`, remote.Object.MIMEType, remote.Object.Size, remote.Object.DriveVersion,
		modifiedAt, remote.Object.WebViewLink, remote.Object.CapabilitiesJSON, now, string(objectID)); err != nil {
		return ConflictCopy{}, fmt.Errorf("apply remote version to conflicted object: %w", err)
	}
	if parentErr == nil {
		if err := upsertDirectoryEntry(ctx, tx, remoteParentID, objectID, string(*remote.Object.DriveFileID), remote.RemoteName); err != nil {
			return ConflictCopy{}, fmt.Errorf("apply remote conflict path: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM remote_change_shadow WHERE object_id = ?`, string(objectID)); err != nil {
		return ConflictCopy{}, fmt.Errorf("clear remote conflict shadow: %w", err)
	}

	operationToken, err := randomContentToken()
	if err != nil {
		return ConflictCopy{}, fmt.Errorf("generate conflict upload operation ID: %w", err)
	}
	copyOperationID := domain.OperationID("op-" + operationToken)
	payload, _ := json.Marshal(namespacePayload{ParentObjectID: parentID, Name: copyName})
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (
		operation_id, object_id, kind, state, idempotency_key, base_drive_version, content_version_id,
		payload_json, created_at, updated_at
	) VALUES (?, ?, 'create', 'queued', ?, '', ?, ?, ?, ?)`, string(copyOperationID), string(copyObject.ID),
		"conflict-copy:"+string(operationID), currentVersionID, string(payload), now, now); err != nil {
		return ConflictCopy{}, fmt.Errorf("queue local conflict copy: %w", err)
	}
	dependencies, err := pendingOperationsTx(ctx, tx, parentID, copyOperationID)
	if err != nil {
		return ConflictCopy{}, err
	}
	if err := insertOperationDependenciesTx(ctx, tx, copyOperationID, dependencies); err != nil {
		return ConflictCopy{}, err
	}
	copy := ConflictCopy{OperationID: operationID, RemoteObjectID: objectID, LocalCopyObjectID: copyObject.ID,
		LocalCopyName: copyName, RemoteName: remote.RemoteName, RemoteVersion: remote.Object.DriveVersion}
	details, _ := json.Marshal(map[string]string{
		"local_operation_kind": string(domain.OperationWrite), "local_intent": "write",
		"local_copy_object_id": string(copy.LocalCopyObjectID), "local_copy_name": copy.LocalCopyName,
		"remote_name": copy.RemoteName, "remote_version": copy.RemoteVersion,
	})
	if err := insertConflictEventTx(ctx, tx, objectID, operationID, remoteWriteConflictCode, string(details)); err != nil {
		return ConflictCopy{}, err
	}
	return copy, nil
}

func isBenignNamespaceChangeTx(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, operationID domain.OperationID,
	kind domain.OperationKind, remoteParentID domain.ObjectID, remote RemoteEntry) (bool, error) {
	if remote.Object.Deleted {
		return false, nil
	}
	var payloadJSON string
	if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM operations WHERE operation_id = ?`, string(operationID)).Scan(&payloadJSON); err != nil {
		return false, fmt.Errorf("read namespace operation payload: %w", err)
	}
	var payload namespacePayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return false, nil
	}
	var expectedParentID domain.ObjectID
	var expectedName string
	switch kind {
	case domain.OperationRename:
		expectedParentID, expectedName = payload.OldParentObjectID, payload.OldName
	case domain.OperationTrash:
		expectedParentID, expectedName = payload.ParentObjectID, payload.Name
	default:
		return false, nil
	}
	if expectedParentID == "" || expectedName == "" || remoteParentID != expectedParentID || remote.RemoteName != expectedName {
		return false, nil
	}
	localObject, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(objectID)))
	if err != nil {
		return false, fmt.Errorf("read local object for namespace conflict check: %w", err)
	}
	if remote.Object.MIMEType != localObject.MIMEType ||
		localObject.ModifiedAt.IsZero() || remote.Object.ModifiedAt.IsZero() ||
		!remote.Object.ModifiedAt.Equal(localObject.ModifiedAt) {
		return false, nil
	}
	if remote.Object.Size != localObject.Size {
		var hasPendingWrite int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE object_id = ? AND kind = 'write'
			AND state IN ('prepared', 'queued', 'uploading', 'retryable-error', 'needs-reconcile'))`, string(objectID)).Scan(&hasPendingWrite); err != nil {
			return false, fmt.Errorf("check pending write during namespace conflict check: %w", err)
		}
		if hasPendingWrite == 0 {
			return false, nil
		}
	}
	return true, nil
}

func markRemoteNamespaceConflictTx(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID,
	operationID domain.OperationID, remote RemoteEntry) error {
	operation, err := (&ContentStore{metadata: nil}).scanOperation(tx.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations WHERE operation_id = ?`, string(operationID)))
	if err != nil {
		return fmt.Errorf("read operation for remote namespace conflict: %w", err)
	}
	details, _ := json.Marshal(map[string]string{
		"local_operation": operation.PayloadJSON, "local_operation_kind": string(operation.Kind),
		"local_intent": describeNamespaceIntent(operation.Kind, operation.PayloadJSON),
		"remote_name":  remote.RemoteName, "remote_version": remote.Object.DriveVersion,
	})
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET state = 'conflict', last_error_code = 'remote_version_changed',
		last_error_message = 'Drive changed while a local namespace operation was pending', next_attempt_at = NULL,
		updated_at = ? WHERE object_id = ? AND state IN ('prepared', 'queued', 'uploading', 'retryable-error', 'needs-reconcile')`,
		formatTime(time.Now()), string(objectID)); err != nil {
		return fmt.Errorf("mark remote namespace conflict: %w", err)
	}
	return insertConflictEventTx(ctx, tx, objectID, operationID, remoteNamespaceConflictCode, string(details))
}

func describeNamespaceIntent(kind domain.OperationKind, payload string) string {
	if kind == domain.OperationTrash {
		return "move to Drive trash"
	}
	var value namespacePayload
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		return string(kind)
	}
	name := value.NewName
	if name == "" {
		name = value.Name
	}
	if name == "" {
		return string(kind)
	}
	return "rename or move to " + name
}

func (s *Store) PreserveNamespaceConflict(ctx context.Context, operationID domain.OperationID, remote RemoteEntry) error {
	if ctx == nil || operationID == "" || remote.Object.DriveFileID == nil ||
		*remote.Object.DriveFileID == "" || remote.RemoteName == "" || remote.Object.DriveVersion == "" {
		return errors.New("operation and complete remote file metadata are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin remote namespace-conflict preservation: %w", err)
	}
	defer tx.Rollback()
	operation, err := (&ContentStore{metadata: s}).scanOperation(tx.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations WHERE operation_id = ?`, string(operationID)))
	if err != nil {
		return fmt.Errorf("read operation for namespace conflict: %w", err)
	}
	if operation.Kind != domain.OperationRename && operation.Kind != domain.OperationTrash {
		return errors.New("namespace conflict does not target a rename or trash operation")
	}
	if err := markRemoteNamespaceConflictTx(ctx, tx, operation.ObjectID, operationID, remote); err != nil {
		return err
	}
	if remote.Object.DriveFileID == nil {
		return errors.New("remote namespace conflict has no Drive ID")
	}
	var modifiedAt any
	if !remote.Object.ModifiedAt.IsZero() {
		modifiedAt = nullableTime(remote.Object.ModifiedAt)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET mime_type = ?, size_bytes = ?, drive_version = ?,
		modified_at = COALESCE(?, modified_at), web_view_link = ?, capabilities_json = ?, is_deleted = 0, updated_at = ?
		WHERE object_id = ?`, remote.Object.MIMEType, remote.Object.Size, remote.Object.DriveVersion, modifiedAt,
		remote.Object.WebViewLink, remote.Object.CapabilitiesJSON, formatTime(time.Now()), string(operation.ObjectID)); err != nil {
		return fmt.Errorf("apply remote version after namespace conflict: %w", err)
	}
	if remote.Object.DriveVersion != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE content_versions SET is_current = 0
			WHERE object_id = ? AND is_current = 1 AND kind = 'pending'
			AND COALESCE(drive_version, '') <> '' AND drive_version <> ?`,
			string(operation.ObjectID), remote.Object.DriveVersion); err != nil {
			return fmt.Errorf("retire stale committed local content after namespace conflict: %w", err)
		}
	}
	parentID, parentErr := objectIDForParent(ctx, tx, remote.ParentDriveFileID)
	if parentErr != nil && !errors.Is(parentErr, sql.ErrNoRows) {
		return parentErr
	}
	if parentErr == nil {
		if err := upsertDirectoryEntry(ctx, tx, parentID, operation.ObjectID, string(*remote.Object.DriveFileID), remote.RemoteName); err != nil {
			return fmt.Errorf("apply remote path after namespace conflict: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM remote_change_shadow WHERE object_id = ?`, string(operation.ObjectID)); err != nil {
		return fmt.Errorf("clear remote namespace-conflict shadow: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit remote namespace-conflict preservation: %w", err)
	}
	return nil
}

func insertConflictEventTx(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, operationID domain.OperationID,
	code, details string) error {
	token, err := randomContentToken()
	if err != nil {
		return fmt.Errorf("generate conflict event ID: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO events (event_id, object_id, operation_id, kind, code, details_json, created_at)
		VALUES (?, ?, ?, 'conflict', ?, ?, ?)`, "evt-"+token, string(objectID), string(operationID), code, details, formatTime(time.Now()))
	if err != nil {
		return fmt.Errorf("record conflict event: %w", err)
	}
	return nil
}

func existingWriteConflict(ctx context.Context, tx *sql.Tx, operationID domain.OperationID) (ConflictCopy, bool, error) {
	var objectID, detailsJSON string
	err := tx.QueryRowContext(ctx, `SELECT object_id, details_json FROM events WHERE operation_id = ?
		AND kind = 'conflict' AND code = ? AND resolved_at IS NULL ORDER BY created_at LIMIT 1`,
		string(operationID), remoteWriteConflictCode).Scan(&objectID, &detailsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return ConflictCopy{}, false, nil
	}
	if err != nil {
		return ConflictCopy{}, false, fmt.Errorf("find prior write-conflict event: %w", err)
	}
	var details struct {
		LocalCopyObjectID domain.ObjectID `json:"local_copy_object_id"`
		LocalCopyName     string          `json:"local_copy_name"`
		RemoteName        string          `json:"remote_name"`
		RemoteVersion     string          `json:"remote_version"`
	}
	if err := json.Unmarshal([]byte(detailsJSON), &details); err != nil {
		return ConflictCopy{}, false, fmt.Errorf("decode prior write-conflict event: %w", err)
	}
	return ConflictCopy{OperationID: operationID, RemoteObjectID: domain.ObjectID(objectID),
		LocalCopyObjectID: details.LocalCopyObjectID, LocalCopyName: details.LocalCopyName,
		RemoteName: details.RemoteName, RemoteVersion: details.RemoteVersion}, true, nil
}

func uniqueConflictCopyName(ctx context.Context, tx *sql.Tx, parentID domain.ObjectID, originalName string,
	operationID domain.OperationID) (string, error) {
	token := string(operationID)
	if len(token) > 8 {
		token = token[len(token)-8:]
	}
	base := safeDisplayName(originalName + " (local conflict " + token + ")")
	for suffix := 1; suffix < 10000; suffix++ {
		name := base
		if suffix > 1 {
			name = fmt.Sprintf("%s %d", base, suffix)
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM directory_entries
			WHERE parent_object_id = ? AND display_name = ?)`, string(parentID), name).Scan(&exists); err != nil {
			return "", fmt.Errorf("check local conflict-copy name: %w", err)
		}
		if exists == 0 {
			return name, nil
		}
	}
	return "", errors.New("cannot allocate a unique local conflict-copy name")
}

func lastLocalNamespacePath(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID) (string, string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT kind, payload_json FROM operations WHERE object_id = ?
		AND kind IN ('rename', 'trash') ORDER BY created_at DESC, operation_id DESC`, string(objectID))
	if err != nil {
		return "", "", fmt.Errorf("find last local path for conflict copy: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind, payloadJSON string
		if err := rows.Scan(&kind, &payloadJSON); err != nil {
			return "", "", err
		}
		var payload namespacePayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			continue
		}
		if kind == string(domain.OperationRename) && payload.NewParentObjectID != "" && payload.NewName != "" {
			return string(payload.NewParentObjectID), payload.NewName, nil
		}
		if payload.ParentObjectID != "" && payload.Name != "" {
			return string(payload.ParentObjectID), payload.Name, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", fmt.Errorf("read last local path for conflict copy: %w", err)
	}
	return "", "", sql.ErrNoRows
}
