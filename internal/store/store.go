package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"

	"google-drive-linux/internal/domain"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	RootInode        domain.Inode = 1
	rootMIMEType                  = "application/vnd.google-apps.folder"
	sqliteDriverName              = "google-drive-sqlite3"
)

var sqliteDriverRegistration sync.Once

var (
	ErrNewerSchema      = errors.New("database schema is newer than this application")
	ErrChangedMigration = errors.New("applied database migration checksum changed")
)

type Store struct {
	db   *sql.DB
	path string
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path must not be empty")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	file, err := os.OpenFile(absolutePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create database file: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("restrict database file permissions: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close database file: %w", err)
	}

	sqliteDriverRegistration.Do(func() {
		sql.Register(sqliteDriverName, &sqlite3.SQLiteDriver{ConnectHook: func(connection *sqlite3.SQLiteConn) error {
			_, err := connection.Exec(`PRAGMA wal_autocheckpoint = 1000`, nil)
			return err
		}})
	})
	db, err := sql.Open(sqliteDriverName, dataSourceName(absolutePath))
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to SQLite database: %w", err)
	}
	if err := verifySQLiteDurability(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &Store{db: db, path: absolutePath}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.ensureRoot(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func dataSourceName(path string) string {
	uri := url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	query := url.Values{}
	query.Set("_busy_timeout", "5000")
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "WAL")
	query.Set("_synchronous", "FULL")
	uri.RawQuery = query.Encode()
	return uri.String()
}

func verifySQLiteDurability(ctx context.Context, db *sql.DB) error {
	connections := make([]*sql.Conn, 0, 4)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for i := 0; i < cap(connections); i++ {
		connection, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("open SQLite connection for durability check: %w", err)
		}
		connections = append(connections, connection)
	}
	for index, connection := range connections {
		if _, err := connection.ExecContext(ctx, `PRAGMA wal_autocheckpoint = 1000`); err != nil {
			return fmt.Errorf("set SQLite checkpoint threshold on connection %d: %w", index+1, err)
		}
		var journalMode string
		var synchronous int
		var foreignKeys int
		var busyTimeout int
		var checkpointPages int
		if err := connection.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journalMode); err != nil {
			return fmt.Errorf("read SQLite journal mode on connection %d: %w", index+1, err)
		}
		if err := connection.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
			return fmt.Errorf("read SQLite synchronous mode on connection %d: %w", index+1, err)
		}
		if err := connection.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
			return fmt.Errorf("read SQLite foreign-key mode on connection %d: %w", index+1, err)
		}
		if err := connection.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
			return fmt.Errorf("read SQLite busy timeout on connection %d: %w", index+1, err)
		}
		if err := connection.QueryRowContext(ctx, `PRAGMA wal_autocheckpoint`).Scan(&checkpointPages); err != nil {
			return fmt.Errorf("read SQLite checkpoint setting on connection %d: %w", index+1, err)
		}
		if !strings.EqualFold(journalMode, "wal") || synchronous != 2 || foreignKeys != 1 || busyTimeout != 5000 || checkpointPages != 1000 {
			return fmt.Errorf("SQLite connection %d has unsafe settings: journal=%q synchronous=%d foreign_keys=%d busy_timeout=%d wal_autocheckpoint=%d",
				index+1, journalMode, synchronous, foreignKeys, busyTimeout, checkpointPages)
		}
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var version int
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		checksum TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema migration table: %w", err)
	}

	known := []struct {
		version int
		name    string
	}{
		{version: 1, name: "migrations/001_initial.sql"},
		{version: 2, name: "migrations/002_initial_index.sql"},
		{version: 3, name: "migrations/003_change_shadow.sql"},
	}

	rows, err := s.db.QueryContext(ctx, `SELECT version, checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read applied migrations: %w", err)
	}
	applied := make(map[int]string)
	for rows.Next() {
		var version int
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read applied migrations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close migration rows: %w", err)
	}
	if len(applied) > len(known) {
		return ErrNewerSchema
	}

	for _, migration := range known {
		sqlBytes, err := migrationFiles.ReadFile(migration.name)
		if err != nil {
			return fmt.Errorf("read migration %d: %w", migration.version, err)
		}
		checksum := checksumMigration(sqlBytes)
		if existing, ok := applied[migration.version]; ok {
			if existing != checksum {
				return fmt.Errorf("migration %d: %w", migration.version, ErrChangedMigration)
			}
			continue
		}
		if migration.version != len(applied)+1 {
			return fmt.Errorf("missing migration %d before migration %d", len(applied)+1, migration.version)
		}
		if err := applyMigration(ctx, s.db, migration.version, checksum, string(sqlBytes)); err != nil {
			return err
		}
		applied[migration.version] = checksum
	}
	return nil
}

func (s *Store) ensureRoot(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin root metadata transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO inode_sequence(inode) VALUES (?)`, RootInode); err != nil {
		return fmt.Errorf("reserve root inode: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO objects (
		object_id, drive_file_id, inode, mime_type, size_bytes, capabilities_json,
		is_deleted, created_at, updated_at
	) VALUES (?, NULL, ?, ?, 0, '{}', 0, ?, ?)`, domain.RootObjectID, RootInode, rootMIMEType, now, now); err != nil {
		return fmt.Errorf("create root object: %w", err)
	}
	var rootInode domain.Inode
	if err := tx.QueryRowContext(ctx, `SELECT inode FROM objects WHERE object_id = ?`, domain.RootObjectID).Scan(&rootInode); err != nil {
		return fmt.Errorf("read root object: %w", err)
	}
	if rootInode != RootInode {
		return fmt.Errorf("root object has inode %d, expected %d", rootInode, RootInode)
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO sync_state(singleton, change_token, index_state, updated_at)
		VALUES (1, '', 'empty', ?)`, now); err != nil {
		return fmt.Errorf("initialize sync state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit root metadata: %w", err)
	}
	return nil
}

func (s *Store) CreateObject(ctx context.Context, object domain.Object, entry domain.DirectoryEntry) (domain.Object, error) {
	if err := validateObject(object); err != nil {
		return domain.Object{}, err
	}
	if err := validateEntry(entry); err != nil {
		return domain.Object{}, err
	}
	if entry.ObjectID != object.ID {
		return domain.Object{}, errors.New("directory entry object ID does not match object")
	}
	if object.ID == domain.RootObjectID {
		return domain.Object{}, errors.New("root object is managed by the store")
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Object{}, fmt.Errorf("begin object transaction: %w", err)
	}
	defer tx.Rollback()
	sequence, err := tx.ExecContext(ctx, `INSERT INTO inode_sequence DEFAULT VALUES`)
	if err != nil {
		return domain.Object{}, fmt.Errorf("allocate object inode: %w", err)
	}
	inode, err := sequence.LastInsertId()
	if err != nil {
		return domain.Object{}, fmt.Errorf("read allocated inode: %w", err)
	}
	object.Inode = domain.Inode(inode)

	var driveFileID any
	if object.DriveFileID != nil {
		driveFileID = string(*object.DriveFileID)
	}
	var modifiedAt any
	if !object.ModifiedAt.IsZero() {
		modifiedAt = object.ModifiedAt.UTC().Format(time.RFC3339Nano)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO objects (
		object_id, drive_file_id, inode, mime_type, size_bytes, drive_version,
		modified_at, web_view_link, capabilities_json, is_deleted, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(object.ID), driveFileID, inode, object.MIMEType, object.Size,
		nullableString(object.DriveVersion), modifiedAt, nullableString(object.WebViewLink),
		object.CapabilitiesJSON, object.Deleted, formatTime(object.CreatedAt), formatTime(object.UpdatedAt))
	if err != nil {
		return domain.Object{}, fmt.Errorf("insert object metadata: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO directory_entries (
		parent_object_id, object_id, remote_name, display_name
	) VALUES (?, ?, ?, ?)`, string(entry.ParentID), string(entry.ObjectID), entry.RemoteName, entry.DisplayName); err != nil {
		return domain.Object{}, fmt.Errorf("insert directory entry: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Object{}, fmt.Errorf("commit object metadata: %w", err)
	}
	return object, nil
}

func (s *Store) ObjectByID(ctx context.Context, id domain.ObjectID) (domain.Object, error) {
	return scanObject(s.db.QueryRowContext(ctx, objectSelect+` WHERE object_id = ?`, string(id)))
}

func (s *Store) ObjectByInode(ctx context.Context, inode domain.Inode) (domain.Object, error) {
	return scanObject(s.db.QueryRowContext(ctx, objectSelect+` WHERE inode = ?`, uint64(inode)))
}

func (s *Store) DirectoryEntry(ctx context.Context, parentID domain.ObjectID, displayName string) (domain.DirectoryEntry, error) {
	var entry domain.DirectoryEntry
	var parent string
	var object string
	err := s.db.QueryRowContext(ctx, `SELECT parent_object_id, object_id, remote_name, display_name
		FROM directory_entries WHERE parent_object_id = ? AND display_name = ?`, string(parentID), displayName).
		Scan(&parent, &object, &entry.RemoteName, &entry.DisplayName)
	if err != nil {
		return domain.DirectoryEntry{}, fmt.Errorf("lookup directory entry: %w", err)
	}
	entry.ParentID = domain.ObjectID(parent)
	entry.ObjectID = domain.ObjectID(object)
	return entry, nil
}

func (s *Store) Children(ctx context.Context, parentID domain.ObjectID) ([]domain.DirectoryEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT de.parent_object_id, de.object_id, de.remote_name, de.display_name
		FROM directory_entries de JOIN objects o ON o.object_id = de.object_id
		WHERE de.parent_object_id = ? AND o.is_deleted = 0 ORDER BY de.display_name`, string(parentID))
	if err != nil {
		return nil, fmt.Errorf("list directory children: %w", err)
	}
	defer rows.Close()
	var entries []domain.DirectoryEntry
	for rows.Next() {
		var entry domain.DirectoryEntry
		var parent, object string
		if err := rows.Scan(&parent, &object, &entry.RemoteName, &entry.DisplayName); err != nil {
			return nil, fmt.Errorf("read directory child: %w", err)
		}
		entry.ParentID = domain.ObjectID(parent)
		entry.ObjectID = domain.ObjectID(object)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read directory children: %w", err)
	}
	return entries, nil
}

type ChildMetadata struct {
	Entry    domain.DirectoryEntry
	Inode    domain.Inode
	MIMEType string
}

// ChildrenWithMetadata returns one stable snapshot of visible child names,
// inode numbers, and MIME types for FUSE directory enumeration.
func (s *Store) ChildrenWithMetadata(ctx context.Context, parentID domain.ObjectID) ([]ChildMetadata, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT de.parent_object_id, de.object_id, de.remote_name, de.display_name,
		o.inode, o.mime_type FROM directory_entries de JOIN objects o ON o.object_id = de.object_id
		WHERE de.parent_object_id = ? AND o.is_deleted = 0 ORDER BY de.display_name`, string(parentID))
	if err != nil {
		return nil, fmt.Errorf("list directory child metadata: %w", err)
	}
	defer rows.Close()
	var entries []ChildMetadata
	for rows.Next() {
		var entry ChildMetadata
		var parentID, objectID string
		var inode uint64
		if err := rows.Scan(&parentID, &objectID, &entry.Entry.RemoteName, &entry.Entry.DisplayName, &inode, &entry.MIMEType); err != nil {
			return nil, fmt.Errorf("read directory child metadata: %w", err)
		}
		entry.Entry.ParentID = domain.ObjectID(parentID)
		entry.Entry.ObjectID = domain.ObjectID(objectID)
		entry.Inode = domain.Inode(inode)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read directory child metadata: %w", err)
	}
	return entries, nil
}

const objectSelect = `SELECT object_id, drive_file_id, inode, mime_type, size_bytes,
	drive_version, modified_at, web_view_link, capabilities_json, is_deleted, created_at, updated_at
	FROM objects`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanObject(row rowScanner) (domain.Object, error) {
	var object domain.Object
	var id string
	var driveFileID sql.NullString
	var inode uint64
	var driveVersion sql.NullString
	var modifiedAt sql.NullString
	var webViewLink sql.NullString
	var deleted int
	var createdAt string
	var updatedAt string
	err := row.Scan(
		&id, &driveFileID, &inode, &object.MIMEType, &object.Size,
		&driveVersion, &modifiedAt, &webViewLink, &object.CapabilitiesJSON,
		&deleted, &createdAt, &updatedAt,
	)
	if err != nil {
		return domain.Object{}, fmt.Errorf("read object metadata: %w", err)
	}
	object.ID = domain.ObjectID(id)
	object.Inode = domain.Inode(inode)
	if driveFileID.Valid {
		value := domain.DriveFileID(driveFileID.String)
		object.DriveFileID = &value
	}
	object.DriveVersion = driveVersion.String
	object.WebViewLink = webViewLink.String
	object.Deleted = deleted != 0
	if modifiedAt.Valid {
		object.ModifiedAt, err = time.Parse(time.RFC3339Nano, modifiedAt.String)
		if err != nil {
			return domain.Object{}, fmt.Errorf("parse object modification time: %w", err)
		}
	}
	if object.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return domain.Object{}, fmt.Errorf("parse object creation time: %w", err)
	}
	if object.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt); err != nil {
		return domain.Object{}, fmt.Errorf("parse object update time: %w", err)
	}
	return object, nil
}

func validateObject(object domain.Object) error {
	if object.ID == "" {
		return errors.New("object ID must not be empty")
	}
	if object.DriveFileID != nil && *object.DriveFileID == "" {
		return errors.New("Drive file ID must not be empty")
	}
	if strings.TrimSpace(object.MIMEType) == "" {
		return errors.New("object MIME type must not be empty")
	}
	if object.Size < 0 {
		return errors.New("object size must not be negative")
	}
	return nil
}

func validateEntry(entry domain.DirectoryEntry) error {
	if entry.ParentID == "" {
		return errors.New("directory entry parent ID must not be empty")
	}
	if entry.ObjectID == "" {
		return errors.New("directory entry object ID must not be empty")
	}
	if entry.RemoteName == "" {
		return errors.New("remote name must not be empty")
	}
	if entry.DisplayName == "" || entry.DisplayName == "." || entry.DisplayName == ".." || strings.ContainsAny(entry.DisplayName, "/\x00") {
		return errors.New("display name must be a valid POSIX path component")
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
