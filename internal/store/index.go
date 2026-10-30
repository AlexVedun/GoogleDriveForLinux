package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"google-drive-linux/internal/domain"
)

type InitialIndexStatus struct {
	State            string
	Phase            string
	ChangeToken      string
	SnapshotToken    string
	CatchupPageToken string
}

type InitialIndexFolder struct {
	DriveFileID    string
	ObjectID       domain.ObjectID
	ParentObjectID domain.ObjectID
	PageToken      string
}

type RemoteEntry struct {
	Object            domain.Object
	RemoteName        string
	ParentDriveFileID string
}

type RemoteChange struct {
	DriveFileID string
	Removed     bool
	Entry       *RemoteEntry
}

func (s *Store) InitialIndexStatus(ctx context.Context) (InitialIndexStatus, error) {
	var status InitialIndexStatus
	err := s.db.QueryRowContext(ctx, `SELECT index_state, index_phase, change_token, snapshot_token, catchup_page_token
		FROM sync_state WHERE singleton = 1`).Scan(
		&status.State, &status.Phase, &status.ChangeToken, &status.SnapshotToken, &status.CatchupPageToken)
	if err != nil {
		return InitialIndexStatus{}, fmt.Errorf("read initial index status: %w", err)
	}
	return status, nil
}

func (s *Store) BeginInitialIndex(ctx context.Context, snapshotToken, rootDriveFileID string) error {
	if snapshotToken == "" || rootDriveFileID == "" {
		return errors.New("initial index requires a snapshot token and root Drive ID")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin initial index: %w", err)
	}
	defer tx.Rollback()
	now := formatTime(time.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET drive_file_id = ?, updated_at = ? WHERE object_id = ?`,
		rootDriveFileID, now, string(domain.RootObjectID)); err != nil {
		return fmt.Errorf("record My Drive root ID: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM initial_index_work`); err != nil {
		return fmt.Errorf("reset initial index work: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM initial_index_seen_folders`); err != nil {
		return fmt.Errorf("reset initial index folder journal: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO initial_index_work (
		folder_drive_file_id, object_id, parent_object_id, page_token, created_at, updated_at
	) VALUES (?, ?, ?, '', ?, ?)`, rootDriveFileID, string(domain.RootObjectID), string(domain.RootObjectID), now, now); err != nil {
		return fmt.Errorf("queue My Drive root: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET change_token = '', index_state = 'indexing',
		index_phase = 'scanning', snapshot_token = ?, catchup_page_token = '', updated_at = ? WHERE singleton = 1`,
		snapshotToken, now); err != nil {
		return fmt.Errorf("record initial index snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit initial index start: %w", err)
	}
	return nil
}

func (s *Store) InitialIndexFolders(ctx context.Context) ([]InitialIndexFolder, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT folder_drive_file_id, object_id, parent_object_id, page_token
		FROM initial_index_work ORDER BY created_at, folder_drive_file_id`)
	if err != nil {
		return nil, fmt.Errorf("list initial index folders: %w", err)
	}
	defer rows.Close()
	var folders []InitialIndexFolder
	for rows.Next() {
		var folder InitialIndexFolder
		var objectID, parentObjectID string
		if err := rows.Scan(&folder.DriveFileID, &objectID, &parentObjectID, &folder.PageToken); err != nil {
			return nil, fmt.Errorf("read initial index folder: %w", err)
		}
		folder.ObjectID = domain.ObjectID(objectID)
		folder.ParentObjectID = domain.ObjectID(parentObjectID)
		folders = append(folders, folder)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read initial index folders: %w", err)
	}
	return folders, nil
}

func (s *Store) ResetInitialIndexFolder(ctx context.Context, folderDriveFileID string) error {
	if folderDriveFileID == "" {
		return errors.New("initial index folder ID must not be empty")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE initial_index_work SET page_token = '', updated_at = ?
		WHERE folder_drive_file_id = ?`, formatTime(time.Now()), folderDriveFileID)
	if err != nil {
		return fmt.Errorf("reset initial index folder page: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("check initial index folder reset: %w", err)
	} else if affected != 1 {
		return errors.New("initial index folder is not pending")
	}
	return nil
}

func (s *Store) CommitInitialIndexPage(ctx context.Context, folderDriveFileID, nextPageToken string, entries []RemoteEntry) error {
	if folderDriveFileID == "" {
		return errors.New("initial index folder ID must not be empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin initial index page: %w", err)
	}
	defer tx.Rollback()
	var folder InitialIndexFolder
	var objectID, parentObjectID string
	if err := tx.QueryRowContext(ctx, `SELECT folder_drive_file_id, object_id, parent_object_id, page_token
		FROM initial_index_work WHERE folder_drive_file_id = ?`, folderDriveFileID).Scan(
		&folder.DriveFileID, &objectID, &parentObjectID, &folder.PageToken); err != nil {
		return fmt.Errorf("find initial index folder: %w", err)
	}
	folder.ObjectID = domain.ObjectID(objectID)
	folder.ParentObjectID = domain.ObjectID(parentObjectID)
	if folder.PageToken != "" && folder.PageToken == nextPageToken {
		return errors.New("Drive repeated an initial index page token")
	}
	for _, entry := range entries {
		if entry.ParentDriveFileID != "" && entry.ParentDriveFileID != folderDriveFileID {
			return errors.New("initial index entry belongs to a different parent folder")
		}
		if err := upsertRemoteEntry(ctx, tx, folder.ObjectID, entry); err != nil {
			return err
		}
		if entry.Object.MIMEType == rootMIMEType && entry.Object.DriveFileID != nil {
			if err := queueUnseenFolder(ctx, tx, string(*entry.Object.DriveFileID), entry.Object.ID, folder.ObjectID); err != nil {
				return fmt.Errorf("queue indexed Drive folder: %w", err)
			}
		}
	}
	now := formatTime(time.Now())
	if nextPageToken == "" {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO initial_index_seen_folders(folder_drive_file_id, scanned_at) VALUES (?, ?)`, folderDriveFileID, now); err != nil {
			return fmt.Errorf("record scanned Drive folder: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM initial_index_work WHERE folder_drive_file_id = ?`, folderDriveFileID); err != nil {
			return fmt.Errorf("finish initial index folder: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE initial_index_work SET page_token = ?, updated_at = ? WHERE folder_drive_file_id = ?`,
			nextPageToken, now, folderDriveFileID); err != nil {
			return fmt.Errorf("advance initial index page: %w", err)
		}
	}
	var pendingFolders int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM initial_index_work`).Scan(&pendingFolders); err != nil {
		return fmt.Errorf("count pending initial index folders: %w", err)
	}
	if pendingFolders == 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET index_phase = 'catching-up',
			catchup_page_token = snapshot_token, updated_at = ? WHERE singleton = 1 AND index_state = 'indexing'`, now); err != nil {
			return fmt.Errorf("start initial index catch-up: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit initial index page: %w", err)
	}
	return nil
}

func (s *Store) CommitInitialChanges(ctx context.Context, changes []RemoteChange, nextPageToken, newStartPageToken string) error {
	if nextPageToken == "" && newStartPageToken == "" {
		return errors.New("last Drive change page must provide a new start page token")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin initial Drive change page: %w", err)
	}
	defer tx.Rollback()
	var status InitialIndexStatus
	if err := tx.QueryRowContext(ctx, `SELECT index_state, index_phase, change_token, snapshot_token, catchup_page_token
		FROM sync_state WHERE singleton = 1`).Scan(
		&status.State, &status.Phase, &status.ChangeToken, &status.SnapshotToken, &status.CatchupPageToken); err != nil {
		return fmt.Errorf("read initial index catch-up state: %w", err)
	}
	if status.State != "indexing" || status.Phase != "catching-up" {
		return errors.New("Drive change page arrived outside initial index catch-up")
	}
	if nextPageToken != "" && nextPageToken == status.CatchupPageToken {
		return errors.New("Drive repeated an initial change page token")
	}
	for _, change := range changes {
		if change.DriveFileID == "" {
			return errors.New("Drive change has an empty file ID")
		}
		if change.Removed || change.Entry == nil || change.Entry.Object.Deleted {
			if err := markRemoteObjectDeleted(ctx, tx, change.DriveFileID); err != nil {
				return err
			}
			continue
		}
		parentObjectID, err := objectIDForParent(ctx, tx, change.Entry.ParentDriveFileID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if err := markRemoteObjectDeleted(ctx, tx, change.DriveFileID); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if err := upsertRemoteEntry(ctx, tx, parentObjectID, *change.Entry); err != nil {
			return err
		}
		if change.Entry.Object.MIMEType == rootMIMEType && change.Entry.Object.DriveFileID != nil {
			if err := queueUnseenFolder(ctx, tx, string(*change.Entry.Object.DriveFileID), change.Entry.Object.ID, parentObjectID); err != nil {
				return fmt.Errorf("queue newly discovered Drive folder: %w", err)
			}
		}
	}
	now := formatTime(time.Now())
	if nextPageToken != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET catchup_page_token = ?, updated_at = ? WHERE singleton = 1`, nextPageToken, now); err != nil {
			return fmt.Errorf("advance initial catch-up page: %w", err)
		}
	} else {
		var pendingFolders int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM initial_index_work`).Scan(&pendingFolders); err != nil {
			return fmt.Errorf("count folders discovered during catch-up: %w", err)
		}
		if pendingFolders != 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET index_phase = 'scanning', snapshot_token = ?,
				catchup_page_token = ?, updated_at = ? WHERE singleton = 1`, newStartPageToken, newStartPageToken, now); err != nil {
				return fmt.Errorf("schedule folders discovered during catch-up: %w", err)
			}
		} else if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET change_token = ?, index_state = 'ready', index_phase = 'ready',
			snapshot_token = '', catchup_page_token = '', updated_at = ? WHERE singleton = 1`, newStartPageToken, now); err != nil {
			return fmt.Errorf("finish initial index catch-up: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit initial Drive change page: %w", err)
	}
	return nil
}

func upsertRemoteEntry(ctx context.Context, tx *sql.Tx, parentObjectID domain.ObjectID, entry RemoteEntry) error {
	object := entry.Object
	if err := validateObject(object); err != nil {
		return fmt.Errorf("invalid indexed Drive object: %w", err)
	}
	if object.DriveFileID == nil || *object.DriveFileID == "" {
		return errors.New("indexed Drive object must have a Drive file ID")
	}
	if entry.RemoteName == "" {
		return errors.New("indexed Drive object must have a name")
	}
	if object.CreatedAt.IsZero() {
		object.CreatedAt = time.Now().UTC()
	}
	if object.UpdatedAt.IsZero() {
		object.UpdatedAt = object.CreatedAt
	}
	if object.CapabilitiesJSON == "" {
		object.CapabilitiesJSON = "{}"
	}
	if !json.Valid([]byte(object.CapabilitiesJSON)) {
		return errors.New("Drive capabilities must be valid JSON")
	}
	var inode uint64
	err := tx.QueryRowContext(ctx, `SELECT inode FROM objects WHERE object_id = ?`, string(object.ID)).Scan(&inode)
	if errors.Is(err, sql.ErrNoRows) {
		sequence, insertErr := tx.ExecContext(ctx, `INSERT INTO inode_sequence DEFAULT VALUES`)
		if insertErr != nil {
			return fmt.Errorf("allocate indexed Drive inode: %w", insertErr)
		}
		lastID, insertErr := sequence.LastInsertId()
		if insertErr != nil {
			return fmt.Errorf("read indexed Drive inode: %w", insertErr)
		}
		inode = uint64(lastID)
		_, err = tx.ExecContext(ctx, `INSERT INTO objects (
			object_id, drive_file_id, inode, mime_type, size_bytes, drive_version, modified_at,
			web_view_link, capabilities_json, is_deleted, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`, string(object.ID), string(*object.DriveFileID), inode,
			object.MIMEType, object.Size, nullableString(object.DriveVersion), nullableTime(object.ModifiedAt),
			nullableString(object.WebViewLink), object.CapabilitiesJSON, formatTime(object.CreatedAt), formatTime(object.UpdatedAt))
		if err != nil {
			return fmt.Errorf("insert indexed Drive object: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("find indexed Drive object: %w", err)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE objects SET drive_file_id = ?, mime_type = ?, size_bytes = ?, drive_version = ?,
			modified_at = ?, web_view_link = ?, capabilities_json = ?, is_deleted = 0, updated_at = ? WHERE object_id = ?`,
			string(*object.DriveFileID), object.MIMEType, object.Size, nullableString(object.DriveVersion), nullableTime(object.ModifiedAt),
			nullableString(object.WebViewLink), object.CapabilitiesJSON, formatTime(object.UpdatedAt), string(object.ID))
		if err != nil {
			return fmt.Errorf("update indexed Drive object: %w", err)
		}
	}
	return upsertDirectoryEntry(ctx, tx, parentObjectID, object.ID, string(*object.DriveFileID), entry.RemoteName)
}

type sibling struct {
	objectID domain.ObjectID
	driveID  string
	name     string
}

func upsertDirectoryEntry(ctx context.Context, tx *sql.Tx, parentID, objectID domain.ObjectID, driveID, remoteName string) error {
	rows, err := tx.QueryContext(ctx, `SELECT de.object_id, o.drive_file_id, de.remote_name
		FROM directory_entries de JOIN objects o ON o.object_id = de.object_id
		WHERE de.parent_object_id = ? AND de.remote_name = ? AND de.object_id <> ?`,
		string(parentID), remoteName, string(objectID))
	if err != nil {
		return fmt.Errorf("find duplicate Drive names: %w", err)
	}
	var duplicates []sibling
	for rows.Next() {
		var item sibling
		var id, driveIDValue string
		if err := rows.Scan(&id, &driveIDValue, &item.name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read duplicate Drive name: %w", err)
		}
		item.objectID, item.driveID = domain.ObjectID(id), driveIDValue
		duplicates = append(duplicates, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read duplicate Drive names: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close duplicate Drive names: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM directory_entries WHERE object_id = ?`, string(objectID)); err != nil {
		return fmt.Errorf("remove prior Drive path: %w", err)
	}
	base := safeDisplayName(remoteName)
	if len(duplicates) != 0 {
		for _, item := range duplicates {
			if _, err := tx.ExecContext(ctx, `DELETE FROM directory_entries WHERE object_id = ?`, string(item.objectID)); err != nil {
				return fmt.Errorf("replace duplicate Drive path: %w", err)
			}
		}
		duplicates = append(duplicates, sibling{objectID: objectID, driveID: driveID, name: remoteName})
		for _, item := range duplicates {
			display, err := uniqueSuffixedName(ctx, tx, parentID, base, item.driveID)
			if err != nil {
				return err
			}
			if err := insertDirectoryEntry(ctx, tx, parentID, item.objectID, item.name, display); err != nil {
				return err
			}
		}
		return nil
	}
	var collision string
	err = tx.QueryRowContext(ctx, `SELECT object_id FROM directory_entries WHERE parent_object_id = ? AND display_name = ?`, string(parentID), base).Scan(&collision)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check Drive display name collision: %w", err)
	}
	display := base
	if err == nil {
		display, err = uniqueSuffixedName(ctx, tx, parentID, base, driveID)
		if err != nil {
			return err
		}
	}
	return insertDirectoryEntry(ctx, tx, parentID, objectID, remoteName, display)
}

func insertDirectoryEntry(ctx context.Context, tx *sql.Tx, parentID, objectID domain.ObjectID, remoteName, displayName string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO directory_entries(parent_object_id, object_id, remote_name, display_name)
		VALUES (?, ?, ?, ?)`, string(parentID), string(objectID), remoteName, displayName); err != nil {
		return fmt.Errorf("insert indexed Drive path: %w", err)
	}
	return nil
}

func uniqueSuffixedName(ctx context.Context, tx *sql.Tx, parentID domain.ObjectID, base, driveID string) (string, error) {
	checksum := sha256.Sum256([]byte(driveID))
	digest := hex.EncodeToString(checksum[:])
	for length := 8; length <= len(digest); length += 4 {
		if length > len(digest) {
			length = len(digest)
		}
		candidate := addNameSuffix(base, digest[:length])
		available, err := displayNameAvailable(ctx, tx, parentID, candidate)
		if err != nil {
			return "", err
		}
		if available {
			return candidate, nil
		}
	}
	for suffix := 1; ; suffix++ {
		candidate := addNameSuffix(base, fmt.Sprintf("%s-%d", driveID, suffix))
		available, err := displayNameAvailable(ctx, tx, parentID, candidate)
		if err != nil {
			return "", err
		}
		if available {
			return candidate, nil
		}
	}
}

func displayNameAvailable(ctx context.Context, tx *sql.Tx, parentID domain.ObjectID, displayName string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM directory_entries WHERE parent_object_id = ? AND display_name = ? LIMIT 1`,
		string(parentID), displayName).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("check suffixed Drive name: %w", err)
	}
	return false, nil
}

func addNameSuffix(name, suffix string) string {
	position := strings.LastIndex(name, ".")
	if position <= 0 {
		return name + " (" + suffix + ")"
	}
	return name[:position] + " (" + suffix + ")" + name[position:]
}

func safeDisplayName(name string) string {
	name = strings.ReplaceAll(name, "/", "／")
	name = strings.ReplaceAll(name, "\x00", "�")
	switch name {
	case "":
		return "unnamed"
	case ".":
		return "．"
	case "..":
		return "．．"
	default:
		return name
	}
}

func objectIDForParent(ctx context.Context, tx *sql.Tx, driveFileID string) (domain.ObjectID, error) {
	if driveFileID == "" {
		return "", sql.ErrNoRows
	}
	var objectID string
	err := tx.QueryRowContext(ctx, `SELECT object_id FROM objects WHERE drive_file_id = ? AND is_deleted = 0`, driveFileID).Scan(&objectID)
	if err != nil {
		return "", err
	}
	return domain.ObjectID(objectID), nil
}

func markRemoteObjectDeleted(ctx context.Context, tx *sql.Tx, driveFileID string) error {
	now := formatTime(time.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET is_deleted = 1, updated_at = ? WHERE drive_file_id = ?`, now, driveFileID); err != nil {
		return fmt.Errorf("mark remote Drive object deleted: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM directory_entries WHERE object_id IN (
		SELECT object_id FROM objects WHERE drive_file_id = ?
	)`, driveFileID); err != nil {
		return fmt.Errorf("remove deleted Drive path: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM initial_index_work WHERE folder_drive_file_id = ?`, driveFileID); err != nil {
		return fmt.Errorf("remove deleted Drive folder from index work: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM initial_index_seen_folders WHERE folder_drive_file_id = ?`, driveFileID); err != nil {
		return fmt.Errorf("remove deleted Drive folder from index journal: %w", err)
	}
	return nil
}

func queueUnseenFolder(ctx context.Context, tx *sql.Tx, driveFileID string, objectID, parentObjectID domain.ObjectID) error {
	now := formatTime(time.Now())
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO initial_index_work (
		folder_drive_file_id, object_id, parent_object_id, page_token, created_at, updated_at
	) SELECT ?, ?, ?, '', ?, ? WHERE NOT EXISTS (
		SELECT 1 FROM initial_index_seen_folders WHERE folder_drive_file_id = ?
	)`, driveFileID, string(objectID), string(parentObjectID), now, now, driveFileID)
	return err
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}
