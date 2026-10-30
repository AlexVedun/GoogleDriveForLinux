package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google-drive-linux/internal/domain"
)

type OfflinePin struct {
	ObjectID  domain.ObjectID
	Recursive bool
	PinnedAt  time.Time
}

// SetOfflinePin stores a direct pin. Recursive pins apply to the selected folder
// and every current or future descendant reached through the indexed namespace.
func (s *Store) SetOfflinePin(ctx context.Context, objectID domain.ObjectID, recursive bool) error {
	if ctx == nil || objectID == "" {
		return errors.New("context and object ID are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin offline pin update: %w", err)
	}
	defer tx.Rollback()
	object, err := scanObject(tx.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(objectID)))
	if err != nil {
		return fmt.Errorf("read object for offline pin: %w", err)
	}
	if object.Deleted {
		return errors.New("cannot pin a deleted object")
	}
	isFolder := object.MIMEType == rootMIMEType
	if recursive && !isFolder {
		return errors.New("recursive offline pins require a folder")
	}
	if isFolder && !recursive {
		return errors.New("folder offline pins must include descendants")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO offline_pins(object_id, pinned_at, recursive)
		VALUES (?, ?, ?) ON CONFLICT(object_id) DO UPDATE SET pinned_at = excluded.pinned_at, recursive = excluded.recursive`,
		string(objectID), formatTime(time.Now()), boolInt(recursive)); err != nil {
		return fmt.Errorf("store offline pin: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit offline pin: %w", err)
	}
	return nil
}

func (s *Store) RemoveOfflinePin(ctx context.Context, objectID domain.ObjectID) error {
	if ctx == nil || objectID == "" {
		return errors.New("context and object ID are required")
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM offline_pins WHERE object_id = ?`, string(objectID)); err != nil {
		return fmt.Errorf("remove offline pin: %w", err)
	}
	return nil
}

func (s *Store) OfflinePins(ctx context.Context) ([]OfflinePin, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT object_id, recursive, pinned_at FROM offline_pins ORDER BY pinned_at, object_id`)
	if err != nil {
		return nil, fmt.Errorf("list offline pins: %w", err)
	}
	defer rows.Close()
	pins := make([]OfflinePin, 0)
	for rows.Next() {
		var pin OfflinePin
		var objectID, pinnedAt string
		var recursive int
		if err := rows.Scan(&objectID, &recursive, &pinnedAt); err != nil {
			return nil, fmt.Errorf("read offline pin: %w", err)
		}
		pin.ObjectID, pin.Recursive = domain.ObjectID(objectID), recursive != 0
		pin.PinnedAt, err = time.Parse(time.RFC3339Nano, pinnedAt)
		if err != nil {
			return nil, fmt.Errorf("parse offline pin timestamp: %w", err)
		}
		pins = append(pins, pin)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read offline pins: %w", err)
	}
	return pins, nil
}

func (s *Store) IsOfflinePinned(ctx context.Context, objectID domain.ObjectID) (bool, error) {
	if objectID == "" {
		return false, errors.New("object ID is required")
	}
	var pinned int
	err := s.db.QueryRowContext(ctx, `WITH RECURSIVE ancestors(object_id) AS (
		SELECT ? UNION SELECT de.parent_object_id FROM directory_entries de
		JOIN ancestors a ON de.object_id = a.object_id
	)
	SELECT EXISTS (
		SELECT 1 FROM offline_pins p JOIN ancestors a ON a.object_id = p.object_id
		WHERE p.object_id = ? OR p.recursive = 1
	)`, string(objectID), string(objectID)).Scan(&pinned)
	if err != nil {
		return false, fmt.Errorf("check effective offline pin: %w", err)
	}
	return pinned != 0, nil
}

// OfflinePinnedFiles lists indexed Drive blobs covered by a direct file pin or
// a recursive folder pin. Folder metadata is intentionally omitted.
func (s *Store) OfflinePinnedFiles(ctx context.Context) ([]domain.Object, error) {
	rows, err := s.db.QueryContext(ctx, `WITH RECURSIVE recursive_pin_tree(root_id, object_id) AS (
		SELECT object_id, object_id FROM offline_pins WHERE recursive = 1
		UNION
		SELECT tree.root_id, de.object_id FROM directory_entries de
		JOIN recursive_pin_tree tree ON de.parent_object_id = tree.object_id
	), effective(object_id) AS (
		SELECT object_id FROM offline_pins UNION SELECT object_id FROM recursive_pin_tree
	)
	`+objectSelect+` WHERE object_id IN (SELECT object_id FROM effective)
		AND is_deleted = 0 AND drive_file_id IS NOT NULL AND mime_type <> ? ORDER BY object_id`, rootMIMEType)
	if err != nil {
		return nil, fmt.Errorf("list indexed files covered by offline pins: %w", err)
	}
	defer rows.Close()
	objects := make([]domain.Object, 0)
	for rows.Next() {
		object, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read files covered by offline pins: %w", err)
	}
	return objects, nil
}

func (s *Store) ProtectedContentBytes(ctx context.Context) (int64, error) {
	var size int64
	err := s.db.QueryRowContext(ctx, `WITH RECURSIVE recursive_pin_tree(root_id, object_id) AS (
		SELECT object_id, object_id FROM offline_pins WHERE recursive = 1
		UNION
		SELECT tree.root_id, de.object_id FROM directory_entries de
		JOIN recursive_pin_tree tree ON de.parent_object_id = tree.object_id
	), effective(object_id) AS (
		SELECT object_id FROM offline_pins UNION SELECT object_id FROM recursive_pin_tree
	)
	SELECT COALESCE(SUM(cv.size_bytes), 0) FROM content_versions cv
	WHERE cv.is_complete = 1 AND (cv.kind = 'pending' OR (cv.kind = 'cached' AND cv.object_id IN (SELECT object_id FROM effective)))`).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("count pinned and pending content bytes: %w", err)
	}
	return size, nil
}

func (s *Store) ProtectedContentOverage(ctx context.Context, cacheLimit int64) (int64, error) {
	if cacheLimit < 0 {
		return 0, errors.New("cache limit must not be negative")
	}
	protected, err := s.ProtectedContentBytes(ctx)
	if err != nil {
		return 0, err
	}
	if protected <= cacheLimit {
		return 0, nil
	}
	return protected - cacheLimit, nil
}
