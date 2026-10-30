package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"google-drive-linux/internal/domain"
)

func (s *Store) FindCachedContentVersion(ctx context.Context, objectID domain.ObjectID, driveVersion string) (domain.ContentVersion, error) {
	return scanContentVersion(s.db.QueryRowContext(ctx, `SELECT version_id, object_id, kind, local_path,
		size_bytes, COALESCE(sha256, ''), COALESCE(drive_version, ''), is_complete, is_current, created_at
		FROM content_versions WHERE object_id = ? AND drive_version = ? AND kind = 'cached' AND is_complete = 1`,
		string(objectID), driveVersion))
}

func (s *Store) CachedContentBytes(ctx context.Context) (int64, error) {
	var size int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM content_versions
		WHERE kind = 'cached' AND is_complete = 1`).Scan(&size); err != nil {
		return 0, fmt.Errorf("read cached content size: %w", err)
	}
	return size, nil
}

func (s *Store) EvictableCachedVersions(ctx context.Context) ([]domain.ContentVersion, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT c.version_id, c.object_id, c.kind, c.local_path,
		c.size_bytes, COALESCE(c.sha256, ''), COALESCE(c.drive_version, ''), c.is_complete, c.is_current, c.created_at
		FROM content_versions c JOIN objects o ON o.object_id = c.object_id
		WHERE c.kind = 'cached' AND c.is_complete = 1
		AND NOT EXISTS (
			WITH RECURSIVE ancestors(object_id) AS (
				SELECT c.object_id UNION SELECT de.parent_object_id FROM directory_entries de
				JOIN ancestors a ON de.object_id = a.object_id
			)
			SELECT 1 FROM offline_pins p JOIN ancestors a ON a.object_id = p.object_id
			WHERE p.object_id = c.object_id OR p.recursive = 1
		)
		AND NOT EXISTS (SELECT 1 FROM operations op WHERE op.object_id = c.object_id AND op.state <> 'committed')
		ORDER BY c.created_at, c.version_id`)
	if err != nil {
		return nil, fmt.Errorf("list evictable cached content: %w", err)
	}
	defer rows.Close()
	versions := make([]domain.ContentVersion, 0)
	for rows.Next() {
		version, err := scanContentVersion(rows)
		if err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read evictable cached content: %w", err)
	}
	return versions, nil
}

func (s *Store) RegisterCachedContentVersion(ctx context.Context, version domain.ContentVersion) error {
	if !contentIDPattern.MatchString(string(version.ID)) || version.ObjectID == "" || version.DriveVersion == "" ||
		version.Size < 0 || !validSHA256(version.SHA256) || !version.Complete {
		return errors.New("cached content metadata is incomplete")
	}
	if version.CreatedAt.IsZero() {
		version.CreatedAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cached content transaction: %w", err)
	}
	defer tx.Rollback()
	var currentVersion string
	var driveVersion sql.NullString
	var deleted int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(drive_version, ''), is_deleted FROM objects WHERE object_id = ?`,
		string(version.ObjectID)).Scan(&driveVersion, &deleted); err != nil {
		return fmt.Errorf("check object before caching content: %w", err)
	}
	currentVersion = driveVersion.String
	if deleted != 0 || currentVersion != version.DriveVersion {
		return ErrCachedObjectChanged
	}
	var dirty int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations WHERE object_id = ? AND state <> 'committed'`,
		string(version.ObjectID)).Scan(&dirty); err != nil {
		return fmt.Errorf("check local changes before caching content: %w", err)
	}
	if dirty != 0 {
		return ErrCachedObjectDirty
	}
	expectedEvictable := filepath.ToSlash(filepath.Join("cache", "evictable", string(version.ID)+".blob"))
	expectedPinned := filepath.ToSlash(filepath.Join("cache", "pinned", string(version.ID)+".blob"))
	if version.LocalPath != expectedEvictable && version.LocalPath != expectedPinned {
		return errors.New("cached content local path is invalid")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO content_versions (
		version_id, object_id, kind, local_path, size_bytes, sha256, drive_version, is_complete, is_current, created_at
	) VALUES (?, ?, 'cached', ?, ?, ?, ?, 1, 0, ?)`,
		string(version.ID), string(version.ObjectID), version.LocalPath, version.Size, version.SHA256,
		version.DriveVersion, formatTime(version.CreatedAt))
	if err != nil {
		return fmt.Errorf("record cached content version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cached content version: %w", err)
	}
	return nil
}

func (s *Store) DeleteEvictableCachedContent(ctx context.Context, versionID domain.ContentVersionID) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin cached content eviction transaction: %w", err)
	}
	defer tx.Rollback()
	var objectID, localPath string
	if err := tx.QueryRowContext(ctx, `SELECT object_id, local_path FROM content_versions
		WHERE version_id = ? AND kind = 'cached' AND is_complete = 1`, string(versionID)).Scan(&objectID, &localPath); err != nil {
		return "", fmt.Errorf("read cached content for eviction: %w", err)
	}
	var protected int
	if err := tx.QueryRowContext(ctx, `WITH RECURSIVE ancestors(object_id) AS (
		SELECT ? UNION SELECT de.parent_object_id FROM directory_entries de
		JOIN ancestors a ON de.object_id = a.object_id
	)
	SELECT EXISTS (
		SELECT 1 FROM offline_pins p JOIN ancestors a ON a.object_id = p.object_id
		WHERE p.object_id = ? OR p.recursive = 1
	) OR EXISTS (SELECT 1 FROM operations WHERE object_id = ? AND state <> 'committed')`,
		objectID, objectID, objectID).Scan(&protected); err != nil {
		return "", fmt.Errorf("check cached content eviction protection: %w", err)
	}
	if protected != 0 {
		return "", ErrContentNotEvictable
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM content_versions WHERE version_id = ?`, string(versionID)); err != nil {
		return "", fmt.Errorf("remove cached content metadata: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit cached content eviction: %w", err)
	}
	return localPath, nil
}

func (s *Store) DeleteCachedContentVersion(ctx context.Context, versionID domain.ContentVersionID) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin corrupt cache cleanup transaction: %w", err)
	}
	defer tx.Rollback()
	var localPath string
	if err := tx.QueryRowContext(ctx, `SELECT local_path FROM content_versions WHERE version_id = ? AND kind = 'cached'`,
		string(versionID)).Scan(&localPath); err != nil {
		return "", fmt.Errorf("read corrupt cached content metadata: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM content_versions WHERE version_id = ?`, string(versionID)); err != nil {
		return "", fmt.Errorf("remove corrupt cached content metadata: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit corrupt cache cleanup: %w", err)
	}
	return localPath, nil
}
