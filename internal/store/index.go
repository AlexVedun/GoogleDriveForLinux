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

type RemoteChangeShadow struct {
	ObjectID          domain.ObjectID
	DriveFileID       string
	ParentDriveFileID string
	RemoteName        string
	MIMEType          string
	Size              int64
	DriveVersion      string
	ModifiedAt        time.Time
	WebViewLink       string
	CapabilitiesJSON  string
	Removed           bool
	Confirmed         bool
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
		if err := upsertRemoteEntry(ctx, tx, folder.ObjectID, entry, false); err != nil {
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
	if err := applyRemoteChanges(ctx, tx, changes, false); err != nil {
		return err
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
		} else {
			if _, err := tx.ExecContext(ctx, `UPDATE remote_change_shadow SET is_confirmed = 1, observed_at = ?`, now); err != nil {
				return fmt.Errorf("confirm local remote-change shadows: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit initial Drive change page: %w", err)
	}
	return nil
}

// BeginReindex invalidates only remote-derived namespace state. Local objects
// with uncommitted operations stay visible and receive a provisional shadow
// until the new remote snapshot confirms their state.
func (s *Store) BeginReindex(ctx context.Context, snapshotToken, rootDriveFileID string) error {
	if snapshotToken == "" || rootDriveFileID == "" {
		return errors.New("reindex requires a snapshot token and root Drive ID")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Drive reindex: %w", err)
	}
	defer tx.Rollback()
	now := formatTime(time.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE remote_change_shadow SET is_removed = 1, is_confirmed = 0, observed_at = ?
		WHERE object_id IN (SELECT o.object_id FROM objects o WHERE o.drive_file_id IS NOT NULL
		AND EXISTS (SELECT 1 FROM operations op WHERE op.object_id = o.object_id AND op.state <> 'committed'))`, now); err != nil {
		return fmt.Errorf("mark pending objects provisional during reindex: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `WITH RECURSIVE protected(object_id) AS (
		SELECT object_id FROM operations WHERE state <> 'committed'
		UNION SELECT de.parent_object_id FROM directory_entries de JOIN protected p ON de.object_id = p.object_id
	)
	DELETE FROM remote_change_shadow WHERE NOT EXISTS (
		SELECT 1 FROM protected WHERE protected.object_id = remote_change_shadow.object_id)`); err != nil {
		return fmt.Errorf("clear obsolete remote-change shadows during reindex: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `WITH RECURSIVE protected(object_id) AS (
		SELECT object_id FROM operations WHERE state <> 'committed'
		UNION SELECT de.parent_object_id FROM directory_entries de JOIN protected p ON de.object_id = p.object_id
	)
	INSERT INTO remote_change_shadow (
		object_id, drive_file_id, parent_drive_file_id, remote_name, mime_type, size_bytes, drive_version,
		modified_at, web_view_link, capabilities_json, is_removed, is_confirmed, observed_at
	) SELECT o.object_id, o.drive_file_id,
		COALESCE((SELECT p.drive_file_id FROM directory_entries de JOIN objects p ON p.object_id = de.parent_object_id
			WHERE de.object_id = o.object_id LIMIT 1), ''),
		COALESCE((SELECT de.remote_name FROM directory_entries de WHERE de.object_id = o.object_id LIMIT 1), ''),
		o.mime_type, o.size_bytes, COALESCE(o.drive_version, ''), o.modified_at, COALESCE(o.web_view_link, ''),
		o.capabilities_json, 1, 0, ?
	FROM objects o WHERE o.drive_file_id IS NOT NULL AND o.object_id <> ?
	AND o.object_id IN (SELECT object_id FROM protected)
	ON CONFLICT(object_id) DO UPDATE SET is_removed = 1, is_confirmed = 0, observed_at = excluded.observed_at`, now, string(domain.RootObjectID)); err != nil {
		return fmt.Errorf("record pending objects before reindex: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `WITH RECURSIVE protected(object_id) AS (
		SELECT object_id FROM operations WHERE state <> 'committed'
		UNION SELECT de.parent_object_id FROM directory_entries de JOIN protected p ON de.object_id = p.object_id
	)
	UPDATE objects SET is_deleted = 1, updated_at = ? WHERE drive_file_id IS NOT NULL
	AND object_id <> ? AND NOT EXISTS (SELECT 1 FROM protected WHERE protected.object_id = objects.object_id)`, now, string(domain.RootObjectID)); err != nil {
		return fmt.Errorf("mark clean remote objects stale during reindex: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `WITH RECURSIVE protected(object_id) AS (
		SELECT object_id FROM operations WHERE state <> 'committed'
		UNION SELECT de.parent_object_id FROM directory_entries de JOIN protected p ON de.object_id = p.object_id
	)
	DELETE FROM directory_entries WHERE object_id IN (
		SELECT o.object_id FROM objects o WHERE o.drive_file_id IS NOT NULL AND o.object_id <> ?
		AND NOT EXISTS (SELECT 1 FROM protected WHERE protected.object_id = o.object_id))`, string(domain.RootObjectID)); err != nil {
		return fmt.Errorf("remove stale remote paths during reindex: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET drive_file_id = ?, updated_at = ? WHERE object_id = ?`, rootDriveFileID, now, string(domain.RootObjectID)); err != nil {
		return fmt.Errorf("record My Drive root ID during reindex: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM initial_index_work`); err != nil {
		return fmt.Errorf("reset reindex work: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM initial_index_seen_folders`); err != nil {
		return fmt.Errorf("reset reindex folder journal: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO initial_index_work (
		folder_drive_file_id, object_id, parent_object_id, page_token, created_at, updated_at
	) VALUES (?, ?, ?, '', ?, ?)`, rootDriveFileID, string(domain.RootObjectID), string(domain.RootObjectID), now, now); err != nil {
		return fmt.Errorf("queue My Drive root for reindex: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET change_token = '', index_state = 'indexing', index_phase = 'scanning',
		snapshot_token = ?, catchup_page_token = '', updated_at = ? WHERE singleton = 1`, snapshotToken, now); err != nil {
		return fmt.Errorf("record Drive reindex snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Drive reindex start: %w", err)
	}
	return nil
}

// BeginChangePoll persists the token of the next page to fetch. The committed
// change token remains unchanged until the final page succeeds.
func (s *Store) BeginChangePoll(ctx context.Context) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin Drive changes poll: %w", err)
	}
	defer tx.Rollback()
	var state, changeToken, pageToken string
	if err := tx.QueryRowContext(ctx, `SELECT index_state, change_token, catchup_page_token FROM sync_state WHERE singleton = 1`).Scan(&state, &changeToken, &pageToken); err != nil {
		return "", fmt.Errorf("read Drive changes poll state: %w", err)
	}
	if state != "ready" {
		return "", fmt.Errorf("cannot poll Drive changes while index state is %q", state)
	}
	if pageToken == "" {
		if changeToken == "" {
			return "", errors.New("cannot poll Drive changes without a committed token")
		}
		pageToken = changeToken
		if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET catchup_page_token = ?, updated_at = ? WHERE singleton = 1`, pageToken, formatTime(time.Now())); err != nil {
			return "", fmt.Errorf("persist Drive changes poll cursor: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit Drive changes poll start: %w", err)
	}
	return pageToken, nil
}

func (s *Store) CancelChangePoll(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sync_state SET catchup_page_token = '', updated_at = ?
		WHERE singleton = 1 AND index_state = 'ready'`, formatTime(time.Now()))
	if err != nil {
		return fmt.Errorf("clear invalid Drive changes cursor: %w", err)
	}
	return nil
}

func (s *Store) CommitChangePage(ctx context.Context, pageToken string, changes []RemoteChange, nextPageToken, newStartPageToken string) error {
	if pageToken == "" || (nextPageToken == "" && newStartPageToken == "") {
		return errors.New("Drive change page requires its cursor and a continuation or new start token")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin incremental Drive change page: %w", err)
	}
	defer tx.Rollback()
	var state, currentPageToken string
	if err := tx.QueryRowContext(ctx, `SELECT index_state, catchup_page_token FROM sync_state WHERE singleton = 1`).Scan(&state, &currentPageToken); err != nil {
		return fmt.Errorf("read incremental change cursor: %w", err)
	}
	if state != "ready" || currentPageToken != pageToken {
		return errors.New("Drive change page cursor is stale or index is not ready")
	}
	if nextPageToken != "" && nextPageToken == pageToken {
		return errors.New("Drive repeated an incremental change page token")
	}
	if err := applyRemoteChanges(ctx, tx, changes, true); err != nil {
		return err
	}
	now := formatTime(time.Now())
	if nextPageToken != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET catchup_page_token = ?, updated_at = ? WHERE singleton = 1`, nextPageToken, now); err != nil {
			return fmt.Errorf("advance incremental change cursor: %w", err)
		}
	} else {
		var pendingFolders int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM initial_index_work`).Scan(&pendingFolders); err != nil {
			return fmt.Errorf("count folders found by change poll: %w", err)
		}
		if pendingFolders == 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET change_token = ?, catchup_page_token = '', updated_at = ? WHERE singleton = 1`, newStartPageToken, now); err != nil {
				return fmt.Errorf("commit incremental Drive token: %w", err)
			}
		} else {
			if _, err := tx.ExecContext(ctx, `UPDATE sync_state SET index_state = 'indexing', index_phase = 'scanning', snapshot_token = ?,
				catchup_page_token = ?, updated_at = ? WHERE singleton = 1`, newStartPageToken, newStartPageToken, now); err != nil {
				return fmt.Errorf("schedule newly discovered folders: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit incremental Drive change page: %w", err)
	}
	return nil
}

func (s *Store) RemoteShadow(ctx context.Context, objectID domain.ObjectID) (RemoteChangeShadow, error) {
	var shadow RemoteChangeShadow
	var storedObjectID string
	var modifiedAt sql.NullString
	var removed, confirmed int
	err := s.db.QueryRowContext(ctx, `SELECT object_id, drive_file_id, parent_drive_file_id, remote_name, mime_type,
		size_bytes, drive_version, modified_at, web_view_link, capabilities_json, is_removed, is_confirmed
		FROM remote_change_shadow WHERE object_id = ?`, string(objectID)).Scan(
		&storedObjectID, &shadow.DriveFileID, &shadow.ParentDriveFileID, &shadow.RemoteName, &shadow.MIMEType,
		&shadow.Size, &shadow.DriveVersion, &modifiedAt, &shadow.WebViewLink, &shadow.CapabilitiesJSON, &removed, &confirmed)
	if err != nil {
		return RemoteChangeShadow{}, err
	}
	shadow.ObjectID = domain.ObjectID(storedObjectID)
	if modifiedAt.Valid {
		shadow.ModifiedAt, err = time.Parse(time.RFC3339Nano, modifiedAt.String)
		if err != nil {
			return RemoteChangeShadow{}, fmt.Errorf("parse remote shadow modified time: %w", err)
		}
	}
	shadow.Removed, shadow.Confirmed = removed != 0, confirmed != 0
	return shadow, nil
}

func applyRemoteChanges(ctx context.Context, tx *sql.Tx, changes []RemoteChange, confirmed bool) error {
	for _, change := range changes {
		if change.DriveFileID == "" {
			return errors.New("Drive change has an empty file ID")
		}
		if change.Removed || change.Entry == nil || change.Entry.Object.Deleted {
			if err := markRemoteObjectDeleted(ctx, tx, change.DriveFileID, confirmed); err != nil {
				return err
			}
			continue
		}
		parentObjectID, err := objectIDForParent(ctx, tx, change.Entry.ParentDriveFileID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if err := markRemoteObjectDeleted(ctx, tx, change.DriveFileID, confirmed); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if err := upsertRemoteEntry(ctx, tx, parentObjectID, *change.Entry, confirmed); err != nil {
			return err
		}
		if change.Entry.Object.MIMEType == rootMIMEType && change.Entry.Object.DriveFileID != nil {
			if err := queueUnseenFolder(ctx, tx, string(*change.Entry.Object.DriveFileID), change.Entry.Object.ID, parentObjectID); err != nil {
				return fmt.Errorf("queue newly discovered Drive folder: %w", err)
			}
		}
	}
	return nil
}

func upsertRemoteEntry(ctx context.Context, tx *sql.Tx, parentObjectID domain.ObjectID, entry RemoteEntry, confirmed bool) error {
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
	if err == nil {
		pending, pendingErr := hasPendingLocalOperation(ctx, tx, object.ID)
		if pendingErr != nil {
			return pendingErr
		}
		if pending {
			return writeRemoteShadow(ctx, tx, parentObjectID, entry, false, confirmed)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM remote_change_shadow WHERE object_id = ?`, string(object.ID)); err != nil {
			return fmt.Errorf("clear remote-change shadow for clean object: %w", err)
		}
	}
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

func markRemoteObjectDeleted(ctx context.Context, tx *sql.Tx, driveFileID string, confirmed bool) error {
	now := formatTime(time.Now())
	var objectID string
	err := tx.QueryRowContext(ctx, `SELECT object_id FROM objects WHERE drive_file_id = ?`, driveFileID).Scan(&objectID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("find removed remote object: %w", err)
	}
	if err == nil {
		pending, pendingErr := hasPendingLocalSubtree(ctx, tx, domain.ObjectID(objectID))
		if pendingErr != nil {
			return pendingErr
		}
		if pending {
			return writeRemovedRemoteShadow(ctx, tx, domain.ObjectID(objectID), driveFileID, confirmed)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM remote_change_shadow WHERE object_id = ?`, objectID); err != nil {
			return fmt.Errorf("clear remote-change shadow for removed clean object: %w", err)
		}
	}
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

func hasPendingLocalOperation(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM operations WHERE object_id = ? AND state <> 'committed' LIMIT 1`, string(objectID)).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check pending local Drive operation: %w", err)
	}
	return true, nil
}

func hasPendingLocalSubtree(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `WITH RECURSIVE subtree(object_id) AS (
		SELECT ? UNION SELECT de.object_id FROM directory_entries de JOIN subtree s ON de.parent_object_id = s.object_id
	)
	SELECT 1 FROM operations op JOIN subtree s ON s.object_id = op.object_id
	WHERE op.state <> 'committed' LIMIT 1`, string(objectID)).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check pending local subtree operation: %w", err)
	}
	return true, nil
}

func writeRemoteShadow(ctx context.Context, tx *sql.Tx, parentObjectID domain.ObjectID, entry RemoteEntry, removed, confirmed bool) error {
	object := entry.Object
	if object.DriveFileID == nil {
		return errors.New("remote shadow requires a Drive file ID")
	}
	parentDriveFileID := entry.ParentDriveFileID
	if parentDriveFileID == "" && parentObjectID != "" {
		err := tx.QueryRowContext(ctx, `SELECT drive_file_id FROM objects WHERE object_id = ?`, string(parentObjectID)).Scan(&parentDriveFileID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read parent Drive ID for remote shadow: %w", err)
		}
	}
	if object.CapabilitiesJSON == "" {
		object.CapabilitiesJSON = "{}"
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO remote_change_shadow (
		object_id, drive_file_id, parent_drive_file_id, remote_name, mime_type, size_bytes, drive_version,
		modified_at, web_view_link, capabilities_json, is_removed, is_confirmed, observed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(object_id) DO UPDATE SET drive_file_id = excluded.drive_file_id,
		parent_drive_file_id = excluded.parent_drive_file_id, remote_name = excluded.remote_name,
		mime_type = excluded.mime_type, size_bytes = excluded.size_bytes, drive_version = excluded.drive_version,
		modified_at = excluded.modified_at, web_view_link = excluded.web_view_link,
		capabilities_json = excluded.capabilities_json, is_removed = excluded.is_removed,
		is_confirmed = excluded.is_confirmed, observed_at = excluded.observed_at`,
		string(object.ID), string(*object.DriveFileID), parentDriveFileID, entry.RemoteName, object.MIMEType, object.Size,
		object.DriveVersion, nullableTime(object.ModifiedAt), object.WebViewLink, object.CapabilitiesJSON,
		boolInt(removed), boolInt(confirmed), formatTime(time.Now()))
	if err != nil {
		return fmt.Errorf("save remote-change shadow: %w", err)
	}
	return nil
}

func writeRemovedRemoteShadow(ctx context.Context, tx *sql.Tx, objectID domain.ObjectID, driveFileID string, confirmed bool) error {
	var parentDriveID, remoteName string
	err := tx.QueryRowContext(ctx, `SELECT p.drive_file_id, de.remote_name FROM directory_entries de
		JOIN objects p ON p.object_id = de.parent_object_id WHERE de.object_id = ? LIMIT 1`, string(objectID)).Scan(&parentDriveID, &remoteName)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read local path for remote removal shadow: %w", err)
	}
	var object domain.Object
	var version, modifiedAt, webViewLink sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT mime_type, size_bytes, drive_version, modified_at, web_view_link, capabilities_json
		FROM objects WHERE object_id = ?`, string(objectID)).Scan(&object.MIMEType, &object.Size, &version, &modifiedAt, &webViewLink, &object.CapabilitiesJSON); err != nil {
		return fmt.Errorf("read local object for removal shadow: %w", err)
	}
	object.ID = objectID
	driveID := domain.DriveFileID(driveFileID)
	object.DriveFileID = &driveID
	object.DriveVersion = version.String
	object.WebViewLink = webViewLink.String
	if modifiedAt.Valid {
		parsed, err := time.Parse(time.RFC3339Nano, modifiedAt.String)
		if err != nil {
			return fmt.Errorf("parse local object modified time for removal shadow: %w", err)
		}
		object.ModifiedAt = parsed
	}
	return writeRemoteShadow(ctx, tx, "", RemoteEntry{Object: object, RemoteName: remoteName, ParentDriveFileID: parentDriveID}, true, confirmed)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
