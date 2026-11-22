package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"google-drive-linux/internal/domain"
)

// RemoteCommit is the verified Drive metadata returned after an operation is
// reconciled. The caller must verify identity and content before committing it.
type RemoteCommit struct {
	DriveFileID      string
	MIMEType         string
	Size             int64
	DriveVersion     string
	ModifiedAt       time.Time
	WebViewLink      string
	CapabilitiesJSON string
}

// ClaimNextOperation atomically transitions one eligible queue record to
// uploading. Already-uploading work is returned for resumable recovery.
func (s *Store) ClaimNextOperation(ctx context.Context, now time.Time) (domain.Operation, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Operation{}, false, fmt.Errorf("begin queue claim: %w", err)
	}
	defer tx.Rollback()

	// Dependents of terminally failed work cannot make progress. Preserve their
	// local bytes and make the blocked reason visible in the queue.
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET state = 'permanent-error',
		last_error_code = 'dependency_failed', last_error_message = 'A prerequisite operation did not complete',
		next_attempt_at = NULL, updated_at = ?
		WHERE state IN ('queued', 'retryable-error') AND EXISTS (
			SELECT 1 FROM operation_dependencies d JOIN operations prerequisite ON prerequisite.operation_id = d.dependency_id
			WHERE d.operation_id = operations.operation_id AND prerequisite.state IN ('conflict', 'permanent-error')
		)`, formatTime(now)); err != nil {
		return domain.Operation{}, false, fmt.Errorf("mark blocked queue operations: %w", err)
	}

	reader := &ContentStore{metadata: s}
	operation, err := reader.scanOperation(tx.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations op
		WHERE (op.state = 'uploading' OR (op.state IN ('queued', 'retryable-error', 'needs-reconcile')
			AND (op.next_attempt_at IS NULL OR op.next_attempt_at <= ?)))
		AND NOT EXISTS (
			SELECT 1 FROM operation_dependencies d JOIN operations prerequisite ON prerequisite.operation_id = d.dependency_id
			WHERE d.operation_id = op.operation_id AND prerequisite.state <> 'committed'
		)
		ORDER BY CASE op.state WHEN 'uploading' THEN 0 ELSE 1 END, op.created_at, op.operation_id LIMIT 1`, formatTime(now)))
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return domain.Operation{}, false, fmt.Errorf("commit empty queue claim: %w", err)
		}
		return domain.Operation{}, false, nil
	}
	if err != nil {
		return domain.Operation{}, false, fmt.Errorf("read next queue operation: %w", err)
	}
	if operation.State != domain.OperationUploading {
		result, err := tx.ExecContext(ctx, `UPDATE operations SET state = 'uploading', attempt_count = attempt_count + 1,
			next_attempt_at = NULL, last_error_code = '', last_error_message = '', updated_at = ?
			WHERE operation_id = ? AND state = ?`, formatTime(now), string(operation.ID), string(operation.State))
		if err != nil {
			return domain.Operation{}, false, fmt.Errorf("claim queue operation: %w", err)
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return domain.Operation{}, false, fmt.Errorf("claim queue operation affected %d rows (err=%v)", count, err)
		}
		operation.State = domain.OperationUploading
		operation.AttemptCount++
		operation.NextAttemptAt = time.Time{}
		operation.LastErrorCode = ""
		operation.LastErrorMessage = ""
		operation.UpdatedAt = now.UTC()
	}
	if err := tx.Commit(); err != nil {
		return domain.Operation{}, false, fmt.Errorf("commit queue claim: %w", err)
	}
	return operation, true, nil
}

func (s *Store) OperationByID(ctx context.Context, operationID domain.OperationID) (domain.Operation, error) {
	return (&ContentStore{metadata: s}).scanOperation(s.db.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations WHERE operation_id = ?`, string(operationID)))
}

// ListOperations returns queue rows in stable creation order. Callers should
// expose only status fields; payloads and resumable URLs may contain private data.
func (s *Store) ListOperations(ctx context.Context) ([]domain.Operation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations ORDER BY created_at, operation_id`)
	if err != nil {
		return nil, fmt.Errorf("list queued operations: %w", err)
	}
	defer rows.Close()
	operations := make([]domain.Operation, 0)
	for rows.Next() {
		operation, err := (&ContentStore{metadata: s}).scanOperation(rows)
		if err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read queued operations: %w", err)
	}
	return operations, nil
}

func (s *Store) EntryForObject(ctx context.Context, objectID domain.ObjectID) (domain.DirectoryEntry, error) {
	var entry domain.DirectoryEntry
	var parentID, foundObjectID string
	err := s.db.QueryRowContext(ctx, `SELECT parent_object_id, object_id, remote_name, display_name
		FROM directory_entries WHERE object_id = ?`, string(objectID)).Scan(
		&parentID, &foundObjectID, &entry.RemoteName, &entry.DisplayName)
	if err != nil {
		return domain.DirectoryEntry{}, err
	}
	entry.ParentID, entry.ObjectID = domain.ObjectID(parentID), domain.ObjectID(foundObjectID)
	return entry, nil
}

func (s *Store) ReserveDriveFileID(ctx context.Context, objectID domain.ObjectID, driveFileID string) error {
	if driveFileID == "" {
		return errors.New("Drive file ID must not be empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Drive ID reservation: %w", err)
	}
	defer tx.Rollback()
	var existing sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT drive_file_id FROM objects WHERE object_id = ?`, string(objectID)).Scan(&existing); err != nil {
		return fmt.Errorf("read object before Drive ID reservation: %w", err)
	}
	if existing.Valid && existing.String != driveFileID {
		return errors.New("object already has a different Drive file ID")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET drive_file_id = ?, updated_at = ? WHERE object_id = ?`,
		driveFileID, formatTime(time.Now()), string(objectID)); err != nil {
		return fmt.Errorf("reserve Drive file ID: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Drive ID reservation: %w", err)
	}
	return nil
}

func (s *Store) SaveResumableSession(ctx context.Context, operationID domain.OperationID, sessionURL string, offset int64) error {
	if sessionURL == "" || offset < 0 {
		return errors.New("resumable session URL and non-negative offset are required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE operations SET resumable_session_url = ?, resumable_offset = ?, updated_at = ?
		WHERE operation_id = ? AND state = 'uploading'`, sessionURL, offset, formatTime(time.Now()), string(operationID))
	if err != nil {
		return fmt.Errorf("persist resumable session: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("persist resumable session affected %d rows (err=%v)", count, err)
	}
	return nil
}

func (s *Store) SaveResumableOffset(ctx context.Context, operationID domain.OperationID, offset int64) error {
	if offset < 0 {
		return errors.New("resumable offset must not be negative")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE operations SET resumable_offset = ?, updated_at = ?
		WHERE operation_id = ? AND state = 'uploading'`, offset, formatTime(time.Now()), string(operationID))
	if err != nil {
		return fmt.Errorf("persist resumable offset: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("persist resumable offset affected %d rows (err=%v)", count, err)
	}
	return nil
}

func (s *Store) ResetResumableSession(ctx context.Context, operationID domain.OperationID) error {
	result, err := s.db.ExecContext(ctx, `UPDATE operations SET resumable_session_url = '', resumable_offset = 0, updated_at = ?
		WHERE operation_id = ? AND state = 'uploading'`, formatTime(time.Now()), string(operationID))
	if err != nil {
		return fmt.Errorf("clear expired resumable session: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("clear expired resumable session affected %d rows (err=%v)", count, err)
	}
	return nil
}

func (s *Store) SetOperationState(ctx context.Context, operationID domain.OperationID, state domain.OperationState, code, message string, retryAt time.Time) error {
	switch state {
	case domain.OperationQueued, domain.OperationUploading, domain.OperationCommitted, domain.OperationConflict,
		domain.OperationRetryableError, domain.OperationPermanentError, domain.OperationNeedsReconcile:
	default:
		return fmt.Errorf("unsupported queue operation state %q", state)
	}
	var nextAttempt any
	if !retryAt.IsZero() {
		nextAttempt = formatTime(retryAt)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE operations SET state = ?, last_error_code = ?, last_error_message = ?,
		next_attempt_at = ?, updated_at = ? WHERE operation_id = ?`, string(state), code, message, nextAttempt,
		formatTime(time.Now()), string(operationID))
	if err != nil {
		return fmt.Errorf("update queue operation state: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("update queue operation state affected %d rows (err=%v)", count, err)
	}
	return nil
}

func (s *Store) CommitRemoteOperation(ctx context.Context, operationID domain.OperationID, remote RemoteCommit) error {
	if remote.DriveFileID == "" || remote.MIMEType == "" || remote.DriveVersion == "" || remote.Size < 0 {
		return errors.New("verified remote metadata is incomplete")
	}
	if remote.CapabilitiesJSON == "" {
		remote.CapabilitiesJSON = "{}"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin remote queue commit: %w", err)
	}
	defer tx.Rollback()
	op, err := (&ContentStore{metadata: s}).scanOperation(tx.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations WHERE operation_id = ?`, string(operationID)))
	if err != nil {
		return fmt.Errorf("read operation for remote commit: %w", err)
	}
	if op.State == domain.OperationCommitted {
		return tx.Commit()
	}
	if op.State != domain.OperationUploading {
		return fmt.Errorf("cannot commit operation %q from state %q", operationID, op.State)
	}
	var currentDriveID sql.NullString
	var currentVersion sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT drive_file_id FROM objects WHERE object_id = ?`, string(op.ObjectID)).Scan(&currentDriveID); err != nil {
		return fmt.Errorf("read object before remote commit: %w", err)
	}
	if currentDriveID.Valid && currentDriveID.String != remote.DriveFileID {
		return errors.New("verified Drive file ID does not match local object")
	}
	if op.ContentVersionID != nil {
		if err := tx.QueryRowContext(ctx, `SELECT version_id FROM content_versions WHERE object_id = ? AND is_current = 1`,
			string(op.ObjectID)).Scan(&currentVersion); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read current local content before remote commit: %w", err)
		}
	}
	updateSize := op.ContentVersionID == nil || (currentVersion.Valid && currentVersion.String == string(*op.ContentVersionID))
	now := time.Now().UTC()
	var modifiedAt any
	if !remote.ModifiedAt.IsZero() {
		modifiedAt = nullableTime(remote.ModifiedAt)
	}
	query := `UPDATE objects SET drive_file_id = ?, mime_type = ?, size_bytes = CASE WHEN ? THEN ? ELSE size_bytes END,
		drive_version = COALESCE(NULLIF(?, ''), drive_version), modified_at = COALESCE(?, modified_at),
		web_view_link = CASE WHEN ? <> '' THEN ? ELSE web_view_link END, capabilities_json = ?, updated_at = ?
		WHERE object_id = ?`
	if _, err := tx.ExecContext(ctx, query, remote.DriveFileID, remote.MIMEType, updateSize, remote.Size,
		remote.DriveVersion, modifiedAt, remote.WebViewLink, remote.WebViewLink, remote.CapabilitiesJSON,
		formatTime(now), string(op.ObjectID)); err != nil {
		return fmt.Errorf("update object from verified Drive metadata: %w", err)
	}
	if op.ContentVersionID != nil && remote.DriveVersion != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE content_versions SET drive_version = ? WHERE version_id = ? AND object_id = ?`,
			remote.DriveVersion, string(*op.ContentVersionID), string(op.ObjectID)); err != nil {
			return fmt.Errorf("record uploaded content Drive version: %w", err)
		}
	}
	if remote.DriveVersion != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET base_drive_version = ?, updated_at = ?
			WHERE object_id = ? AND operation_id <> ? AND kind IN ('write', 'rename', 'trash')
			AND state IN ('prepared', 'queued', 'retryable-error', 'needs-reconcile')`,
			remote.DriveVersion, formatTime(now), string(op.ObjectID), string(operationID)); err != nil {
			return fmt.Errorf("advance dependent operation base version: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET state = 'committed', resumable_session_url = '',
		resumable_offset = 0, next_attempt_at = NULL, last_error_code = '', last_error_message = ?, updated_at = ?
		WHERE operation_id = ? AND state = 'uploading'`, "", formatTime(now), string(operationID)); err != nil {
		return fmt.Errorf("mark remote operation committed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit verified Drive operation: %w", err)
	}
	return nil
}

func (s *Store) CommitSkippedOperation(ctx context.Context, operationID domain.OperationID) error {
	result, err := s.db.ExecContext(ctx, `UPDATE operations SET state = 'committed', resumable_session_url = '',
		resumable_offset = 0, next_attempt_at = NULL, last_error_code = '', last_error_message = '', updated_at = ?
		WHERE operation_id = ? AND state = 'uploading'`, formatTime(time.Now()), string(operationID))
	if err != nil {
		return fmt.Errorf("commit skipped local-only operation: %w", err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("commit skipped operation affected %d rows (err=%v)", count, err)
	}
	return nil
}

func (s *Store) RetryAuthorizationErrors(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE operations SET next_attempt_at = NULL, updated_at = ?
		WHERE state = 'retryable-error' AND last_error_code = 'authorization_required'`, formatTime(time.Now())); err != nil {
		return fmt.Errorf("clear authorization retry backoff: %w", err)
	}
	return nil
}
