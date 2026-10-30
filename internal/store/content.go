package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"google-drive-linux/internal/domain"
)

var (
	ErrIdempotencyMismatch   = errors.New("idempotency key was already used for a different operation")
	ErrIncompleteOperation   = errors.New("pending operation could not be recovered")
	ErrInsufficientDiskSpace = errors.New("insufficient free disk space for durable content")
	ErrCachedObjectChanged   = errors.New("Drive object changed while caching content")
	ErrCachedObjectDirty     = errors.New("Drive object has uncommitted local changes")
	ErrContentNotEvictable   = errors.New("cached content is pinned, dirty, or otherwise protected")
	contentIDPattern         = regexp.MustCompile(`^cv-[0-9a-f]{32}$`)
)

const contentCopyBufferSize = 128 * 1024

type WriteRequest struct {
	OperationID      domain.OperationID
	ObjectID         domain.ObjectID
	Kind             domain.OperationKind
	IdempotencyKey   string
	BaseDriveVersion string
	ExpectedSize     int64
}

type WriteResult struct {
	Operation      domain.Operation
	ContentVersion domain.ContentVersion
}

type ContentPaths struct {
	Root           string
	EvictableCache string
	Pinned         string
	Transient      string
	Pending        string
	Working        string
}

// WorkingFile is a private mutable file shared by the open descriptors of one FUSE inode.
// It is temporary; WritePending publishes an immutable durable snapshot before fsync succeeds.
type WorkingFile struct {
	file *os.File
	path string
	root string
}

type RecoveryReport struct {
	RecoveredOperations int
	FailedOperations    int
	RemovedOrphanFiles  int
}

type ContentStore struct {
	metadata *Store
	paths    ContentPaths

	gate            sync.RWMutex
	objectMu        [64]sync.Mutex
	reservationMu   sync.Mutex
	reservedBytes   uint64
	filesystemSpace func(string) (available, total uint64, err error)
	checkpoint      func(string)
}

type preparedContent struct {
	operationID   domain.OperationID
	objectID      domain.ObjectID
	operationKind domain.OperationKind
	state         domain.OperationState
	idempotency   string
	baseVersion   string
	versionID     domain.ContentVersionID
	localPath     string
	size          int64
	sha256        sql.NullString
	createdAt     string
}

func NewContentStore(metadata *Store, root string) (*ContentStore, error) {
	if metadata == nil || metadata.db == nil {
		return nil, errors.New("metadata store must be open")
	}
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("content root must not be empty")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve content root: %w", err)
	}
	cacheRoot := filepath.Join(absoluteRoot, "cache")
	paths := ContentPaths{
		Root:           absoluteRoot,
		EvictableCache: filepath.Join(cacheRoot, "evictable"),
		Pinned:         filepath.Join(cacheRoot, "pinned"),
		Transient:      filepath.Join(cacheRoot, "transient"),
		Pending:        filepath.Join(absoluteRoot, "pending"),
		Working:        filepath.Join(absoluteRoot, "working"),
	}
	for _, directory := range []string{paths.Root, cacheRoot, paths.EvictableCache, paths.Pinned, paths.Transient, paths.Pending, paths.Working} {
		if err := ensurePrivateDirectory(directory); err != nil {
			return nil, err
		}
	}
	for _, directory := range []string{paths.Root, cacheRoot, paths.EvictableCache, paths.Transient, paths.Pending, paths.Working} {
		if err := syncDirectory(directory); err != nil {
			return nil, fmt.Errorf("sync content directory %q: %w", directory, err)
		}
	}
	return &ContentStore{metadata: metadata, paths: paths, filesystemSpace: filesystemSpace}, nil
}

func (s *ContentStore) Paths() ContentPaths {
	return s.paths
}

// NewWorkingFile creates a private mutable copy of source. The copy is bounded and checked
// against expectedSize so an incomplete remote read cannot become an editable file.
func (s *ContentStore) NewWorkingFile(ctx context.Context, expectedSize int64, source io.Reader) (*WorkingFile, error) {
	if ctx == nil || source == nil {
		return nil, errors.New("context and working file source are required")
	}
	if expectedSize < 0 || expectedSize >= math.MaxInt64 {
		return nil, errors.New("working file size is outside the supported range")
	}
	token, err := randomContentToken()
	if err != nil {
		return nil, fmt.Errorf("create working file ID: %w", err)
	}
	path := filepath.Join(s.paths.Working, "working-"+token+".tmp")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create working file: %w", err)
	}
	cleanup := func(cause error) (*WorkingFile, error) {
		closeErr := file.Close()
		removeErr := os.Remove(path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		return nil, errors.Join(cause, closeErr, removeErr)
	}
	size, copyErr := copyWithContext(ctx, file, io.LimitReader(source, expectedSize))
	if copyErr == nil && size != expectedSize {
		copyErr = fmt.Errorf("working file source contains %d bytes, expected %d", size, expectedSize)
	}
	if copyErr == nil {
		var extra [1]byte
		read, extraErr := (contextReader{ctx: ctx, source: source}).Read(extra[:])
		if read > 0 {
			copyErr = fmt.Errorf("working file source exceeds expected size of %d bytes", expectedSize)
		} else if extraErr != nil && !errors.Is(extraErr, io.EOF) {
			copyErr = fmt.Errorf("check working file source size: %w", extraErr)
		}
	}
	if copyErr != nil {
		return cleanup(fmt.Errorf("copy working file source: %w", copyErr))
	}
	if err := file.Sync(); err != nil {
		return cleanup(fmt.Errorf("sync initial working file: %w", err))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return cleanup(fmt.Errorf("rewind initial working file: %w", err))
	}
	return &WorkingFile{file: file, path: path, root: s.paths.Working}, nil
}

func (f *WorkingFile) File() *os.File {
	if f == nil {
		return nil
	}
	return f.file
}

func (f *WorkingFile) Remove() error {
	if f == nil {
		return nil
	}
	var closeErr error
	if f.file != nil {
		closeErr = f.file.Close()
		f.file = nil
	}
	removeErr := os.Remove(f.path)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if removeErr == nil && closeErr == nil {
		removeErr = syncDirectory(f.root)
	}
	return errors.Join(closeErr, removeErr)
}

func (s *ContentStore) WritePending(ctx context.Context, request WriteRequest, source io.Reader) (WriteResult, error) {
	if ctx == nil {
		return WriteResult{}, errors.New("context must not be nil")
	}
	if source == nil {
		return WriteResult{}, errors.New("content source must not be nil")
	}
	if request.ObjectID == "" || request.IdempotencyKey == "" {
		return WriteResult{}, errors.New("object ID and idempotency key are required")
	}
	if request.Kind != domain.OperationCreate && request.Kind != domain.OperationWrite {
		return WriteResult{}, fmt.Errorf("operation kind %q cannot carry file content", request.Kind)
	}
	if request.OperationID == "" {
		token, err := randomContentToken()
		if err != nil {
			return WriteResult{}, fmt.Errorf("create operation ID: %w", err)
		}
		request.OperationID = domain.OperationID("op-" + token)
	}
	if request.ExpectedSize < 0 || request.ExpectedSize >= math.MaxInt64 {
		return WriteResult{}, fmt.Errorf("expected content size must be between 0 and %d bytes", int64(math.MaxInt64)-1)
	}
	lock := s.objectLock(request.ObjectID)
	lock.Lock()
	defer lock.Unlock()
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := ctx.Err(); err != nil {
		return WriteResult{}, err
	}

	if existing, found, err := s.preparedByIdempotencyKey(ctx, request.IdempotencyKey); err != nil {
		return WriteResult{}, err
	} else if found {
		if existing.objectID != request.ObjectID || existing.operationKind != request.Kind || existing.baseVersion != request.BaseDriveVersion || existing.size != request.ExpectedSize {
			return WriteResult{}, ErrIdempotencyMismatch
		}
		if existing.state == domain.OperationPrepared {
			if err := s.recoverPrepared(ctx, existing); err != nil {
				return WriteResult{}, err
			}
			existing, _, err = s.preparedByID(ctx, existing.operationID)
			if err != nil {
				return WriteResult{}, err
			}
		}
		if existing.state == domain.OperationPermanentError {
			return WriteResult{}, ErrIncompleteOperation
		}
		return s.writeResult(ctx, existing.operationID, existing.versionID)
	}
	reservation, err := s.reserveDiskSpace(request.ExpectedSize)
	if err != nil {
		return WriteResult{}, err
	}
	defer reservation.release()

	versionToken, err := randomContentToken()
	if err != nil {
		return WriteResult{}, fmt.Errorf("create content version ID: %w", err)
	}
	versionID := domain.ContentVersionID("cv-" + versionToken)
	if _, err := s.createPrepared(ctx, request, versionID); err != nil {
		return WriteResult{}, err
	}
	s.hitCheckpoint("prepared-commit")

	stagingPath := s.stagingPath(versionID)
	stagingFile, err := os.OpenFile(stagingPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return WriteResult{}, errors.Join(fmt.Errorf("create pending staging file: %w", err), s.cleanupUncommitted(ctx, request.OperationID))
	}
	digest := sha256.New()
	reservedFile := reservedWriter{file: stagingFile, reservation: reservation}
	size, copyErr := copyWithContext(ctx, io.MultiWriter(&reservedFile, digest), io.LimitReader(source, request.ExpectedSize))
	if copyErr == nil && size != request.ExpectedSize {
		copyErr = fmt.Errorf("content size is %d bytes, expected %d", size, request.ExpectedSize)
	}
	if copyErr == nil {
		var extra [1]byte
		extraSize, extraErr := (contextReader{ctx: ctx, source: source}).Read(extra[:])
		if extraSize > 0 {
			copyErr = fmt.Errorf("content exceeds declared size of %d bytes", request.ExpectedSize)
		} else if extraErr != nil && !errors.Is(extraErr, io.EOF) {
			copyErr = fmt.Errorf("check declared content size: %w", extraErr)
		}
	}
	syncErr := stagingFile.Sync()
	closeErr := stagingFile.Close()
	if copyErr != nil {
		return WriteResult{}, errors.Join(fmt.Errorf("write pending content: %w", copyErr), s.cleanupUncommitted(ctx, request.OperationID))
	}
	if syncErr != nil {
		return WriteResult{}, errors.Join(fmt.Errorf("sync pending content: %w", syncErr), s.cleanupUncommitted(ctx, request.OperationID))
	}
	if closeErr != nil {
		return WriteResult{}, errors.Join(fmt.Errorf("close pending content: %w", closeErr), s.cleanupUncommitted(ctx, request.OperationID))
	}
	if err := syncDirectory(s.paths.Pending); err != nil {
		return WriteResult{}, errors.Join(fmt.Errorf("sync pending staging entry: %w", err), s.cleanupUncommitted(ctx, request.OperationID))
	}
	s.hitCheckpoint("staging-sync")
	if err := ctx.Err(); err != nil {
		return WriteResult{}, errors.Join(err, s.cleanupUncommitted(ctx, request.OperationID))
	}

	checksum := hex.EncodeToString(digest.Sum(nil))
	if err := s.recordExpectedContent(ctx, versionID, size, checksum); err != nil {
		return WriteResult{}, errors.Join(err, s.cleanupUncommitted(ctx, request.OperationID))
	}
	s.hitCheckpoint("expected-commit")

	finalizeContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	finalPath := s.blobPath(versionID)
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return WriteResult{}, fmt.Errorf("publish pending content version: %w", err)
	}
	s.hitCheckpoint("rename")
	if err := syncDirectory(s.paths.Pending); err != nil {
		return WriteResult{}, fmt.Errorf("sync pending content directory: %w", err)
	}
	s.hitCheckpoint("directory-sync")
	if err := s.finalizeContent(finalizeContext, request, versionID, size, checksum); err != nil {
		return WriteResult{}, err
	}
	s.hitCheckpoint("final-commit")
	return s.writeResult(finalizeContext, request.OperationID, versionID)
}

func (s *ContentStore) CurrentContentVersion(ctx context.Context, objectID domain.ObjectID) (domain.ContentVersion, error) {
	return scanContentVersion(s.metadata.db.QueryRowContext(ctx, `SELECT version_id, object_id, kind, local_path,
		size_bytes, COALESCE(sha256, ''), COALESCE(drive_version, ''), is_complete, is_current, created_at
		FROM content_versions WHERE object_id = ? AND is_current = 1`, string(objectID)))
}

func (s *ContentStore) OpenContentVersion(ctx context.Context, versionID domain.ContentVersionID) (*os.File, domain.ContentVersion, error) {
	version, err := scanContentVersion(s.metadata.db.QueryRowContext(ctx, `SELECT version_id, object_id, kind, local_path,
		size_bytes, COALESCE(sha256, ''), COALESCE(drive_version, ''), is_complete, is_current, created_at
		FROM content_versions WHERE version_id = ?`, string(versionID)))
	if err != nil {
		return nil, domain.ContentVersion{}, err
	}
	if !version.Complete {
		return nil, domain.ContentVersion{}, errors.New("content version is incomplete")
	}
	path, err := s.pathForVersion(version)
	if err != nil {
		return nil, domain.ContentVersion{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, domain.ContentVersion{}, fmt.Errorf("open content version: %w", err)
	}
	return file, version, nil
}

func (s *ContentStore) Recover(ctx context.Context) (RecoveryReport, error) {
	if ctx == nil {
		return RecoveryReport{}, errors.New("context must not be nil")
	}
	s.gate.Lock()
	defer s.gate.Unlock()
	report := RecoveryReport{}
	prepared, err := s.listPrepared(ctx)
	if err != nil {
		return report, err
	}
	for _, operation := range prepared {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := s.recoverPrepared(ctx, operation); err != nil {
			return report, fmt.Errorf("recover operation %q: %w", operation.operationID, err)
		}
		updated, _, err := s.preparedByID(ctx, operation.operationID)
		if err != nil {
			return report, err
		}
		if updated.state == domain.OperationQueued {
			report.RecoveredOperations++
		} else if updated.state == domain.OperationPermanentError {
			report.FailedOperations++
		}
	}
	removed, err := s.removeOrphanFiles(ctx)
	if err != nil {
		return report, err
	}
	workingRemoved, err := s.removeAbandonedWorkingFiles(ctx)
	if err != nil {
		return report, err
	}
	report.RemovedOrphanFiles = removed + workingRemoved
	return report, nil
}

func (s *ContentStore) removeAbandonedWorkingFiles(ctx context.Context) (int, error) {
	entries, err := os.ReadDir(s.paths.Working)
	if err != nil {
		return 0, fmt.Errorf("list abandoned working files: %w", err)
	}
	removed := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if !generatedWorkingFileName(entry.Name()) {
			continue
		}
		deleted, err := removeRegularFile(filepath.Join(s.paths.Working, entry.Name()))
		if err != nil {
			return removed, err
		}
		if deleted {
			removed++
		}
	}
	if removed > 0 {
		if err := syncDirectory(s.paths.Working); err != nil {
			return removed, fmt.Errorf("sync abandoned working file cleanup: %w", err)
		}
	}
	return removed, nil
}

func (s *ContentStore) createPrepared(ctx context.Context, request WriteRequest, versionID domain.ContentVersionID) (domain.Operation, error) {
	tx, err := s.metadata.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("begin prepared write transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO content_versions (
		version_id, object_id, kind, local_path, size_bytes, sha256, is_complete, is_current, created_at
	) VALUES (?, ?, 'pending', ?, ?, NULL, 0, 0, ?)`, string(versionID), string(request.ObjectID), s.relativeBlobPath(versionID), request.ExpectedSize, formatTime(time.Now())); err != nil {
		return domain.Operation{}, fmt.Errorf("reserve content version: %w", err)
	}
	now := formatTime(time.Now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (
		operation_id, object_id, kind, state, idempotency_key, base_drive_version, content_version_id,
		payload_json, created_at, updated_at
	) VALUES (?, ?, ?, 'prepared', ?, ?, ?, '{}', ?, ?)`, string(request.OperationID), string(request.ObjectID),
		string(request.Kind), request.IdempotencyKey, request.BaseDriveVersion, string(versionID), now, now); err != nil {
		return domain.Operation{}, fmt.Errorf("record prepared write operation: %w", err)
	}
	dependencies, err := operationDependenciesForContentTx(ctx, tx, request.ObjectID, request.OperationID)
	if err != nil {
		return domain.Operation{}, err
	}
	if err := insertOperationDependenciesTx(ctx, tx, request.OperationID, dependencies); err != nil {
		return domain.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Operation{}, fmt.Errorf("commit prepared write operation: %w", err)
	}
	return domain.Operation{
		ID: request.OperationID, ObjectID: request.ObjectID, Kind: request.Kind,
		State: domain.OperationPrepared, IdempotencyKey: request.IdempotencyKey,
		BaseDriveVersion: request.BaseDriveVersion, ContentVersionID: &versionID,
		PayloadJSON: "{}", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}, nil
}

func (s *ContentStore) recordExpectedContent(ctx context.Context, versionID domain.ContentVersionID, size int64, checksum string) error {
	tx, err := s.metadata.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin content checksum transaction: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE content_versions SET size_bytes = ?, sha256 = ?
		WHERE version_id = ? AND is_complete = 0 AND EXISTS (
			SELECT 1 FROM operations WHERE content_version_id = ? AND state = 'prepared'
		)`, size, checksum, string(versionID), string(versionID))
	if err != nil {
		return fmt.Errorf("record pending content checksum: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("record pending content checksum affected %d rows (err=%v)", affected, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending content checksum: %w", err)
	}
	return nil
}

func (s *ContentStore) finalizeContent(ctx context.Context, request WriteRequest, versionID domain.ContentVersionID, size int64, checksum string) error {
	tx, err := s.metadata.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin content publish transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE content_versions SET is_current = 0 WHERE object_id = ? AND is_current = 1`, string(request.ObjectID)); err != nil {
		return fmt.Errorf("retire prior current content version: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE content_versions SET size_bytes = ?, sha256 = ?, is_complete = 1, is_current = 1
		WHERE version_id = ? AND object_id = ? AND is_complete = 0 AND sha256 = ?`, size, checksum, string(versionID), string(request.ObjectID), checksum)
	if err != nil {
		return fmt.Errorf("publish content version metadata: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("publish content version affected %d rows (err=%v)", affected, err)
	}
	now := formatTime(time.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET size_bytes = ?, updated_at = ? WHERE object_id = ?`, size, now, string(request.ObjectID)); err != nil {
		return fmt.Errorf("update object content size: %w", err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE operations SET state = 'queued', last_error_code = '', last_error_message = '', updated_at = ?
		WHERE operation_id = ? AND state = 'prepared' AND content_version_id = ?`, now, string(request.OperationID), string(versionID))
	if err != nil {
		return fmt.Errorf("queue durable content version: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("queue durable content version affected %d rows (err=%v)", affected, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit durable content version: %w", err)
	}
	return nil
}

func (s *ContentStore) listPrepared(ctx context.Context) ([]preparedContent, error) {
	rows, err := s.metadata.db.QueryContext(ctx, `SELECT o.operation_id, o.object_id, o.kind, o.state,
		o.idempotency_key, o.base_drive_version, c.version_id, c.local_path, c.size_bytes, c.sha256, o.created_at
		FROM operations o JOIN content_versions c ON c.version_id = o.content_version_id
		WHERE o.state = 'prepared' ORDER BY o.created_at, o.operation_id`)
	if err != nil {
		return nil, fmt.Errorf("list prepared content operations: %w", err)
	}
	defer rows.Close()
	var result []preparedContent
	for rows.Next() {
		var item preparedContent
		var operationID, objectID, kind, state, versionID string
		if err := rows.Scan(&operationID, &objectID, &kind, &state, &item.idempotency, &item.baseVersion,
			&versionID, &item.localPath, &item.size, &item.sha256, &item.createdAt); err != nil {
			return nil, fmt.Errorf("scan prepared content operation: %w", err)
		}
		item.operationID = domain.OperationID(operationID)
		item.objectID = domain.ObjectID(objectID)
		item.operationKind = domain.OperationKind(kind)
		item.state = domain.OperationState(state)
		item.versionID = domain.ContentVersionID(versionID)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read prepared content operations: %w", err)
	}
	return result, nil
}

func (s *ContentStore) preparedByIdempotencyKey(ctx context.Context, key string) (preparedContent, bool, error) {
	item, err := s.scanPrepared(s.metadata.db.QueryRowContext(ctx, `SELECT o.operation_id, o.object_id, o.kind, o.state,
		o.idempotency_key, o.base_drive_version, c.version_id, c.local_path, c.size_bytes, c.sha256, o.created_at
		FROM operations o JOIN content_versions c ON c.version_id = o.content_version_id WHERE o.idempotency_key = ?`, key))
	if errors.Is(err, sql.ErrNoRows) {
		return preparedContent{}, false, nil
	}
	if err != nil {
		return preparedContent{}, false, err
	}
	return item, true, nil
}

func (s *ContentStore) preparedByID(ctx context.Context, id domain.OperationID) (preparedContent, bool, error) {
	item, err := s.scanPrepared(s.metadata.db.QueryRowContext(ctx, `SELECT o.operation_id, o.object_id, o.kind, o.state,
		o.idempotency_key, o.base_drive_version, c.version_id, c.local_path, c.size_bytes, c.sha256, o.created_at
		FROM operations o JOIN content_versions c ON c.version_id = o.content_version_id WHERE o.operation_id = ?`, string(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return preparedContent{}, false, sql.ErrNoRows
	}
	if err != nil {
		return preparedContent{}, false, err
	}
	return item, true, nil
}

func (s *ContentStore) scanPrepared(row rowScanner) (preparedContent, error) {
	var item preparedContent
	var operationID, objectID, kind, state, versionID string
	err := row.Scan(&operationID, &objectID, &kind, &state, &item.idempotency, &item.baseVersion,
		&versionID, &item.localPath, &item.size, &item.sha256, &item.createdAt)
	if err != nil {
		return preparedContent{}, err
	}
	item.operationID = domain.OperationID(operationID)
	item.objectID = domain.ObjectID(objectID)
	item.operationKind = domain.OperationKind(kind)
	item.state = domain.OperationState(state)
	item.versionID = domain.ContentVersionID(versionID)
	return item, nil
}

func (s *ContentStore) recoverPrepared(ctx context.Context, item preparedContent) error {
	if item.state != domain.OperationPrepared {
		return nil
	}
	if !contentIDPattern.MatchString(string(item.versionID)) || item.localPath != s.relativeBlobPath(item.versionID) {
		return s.markPermanentError(ctx, item.operationID, "invalid-content-path", "prepared content has an invalid local path")
	}
	if !item.sha256.Valid || item.sha256.String == "" {
		if err := s.removeIncompleteVersionFiles(ctx, item); err != nil {
			return err
		}
		return s.markPermanentError(ctx, item.operationID, "incomplete-content", "prepared content has no durable checksum")
	}
	if item.size < 0 || !validSHA256(item.sha256.String) {
		return s.markPermanentError(ctx, item.operationID, "invalid-content-checksum", "prepared content metadata is invalid")
	}
	finalPath := s.blobPath(item.versionID)
	stagingPath := s.stagingPath(item.versionID)
	finalExists, finalValid, err := verifyFile(finalPath, item.size, item.sha256.String)
	if err != nil {
		return err
	}
	if finalExists && !finalValid {
		return s.markPermanentError(ctx, item.operationID, "content-checksum-mismatch", "published content does not match its durable checksum")
	}
	if !finalValid {
		stagingExists, stagingValid, err := verifyFile(stagingPath, item.size, item.sha256.String)
		if err != nil {
			return err
		}
		if !stagingExists || !stagingValid {
			return s.markPermanentError(ctx, item.operationID, "content-unavailable", "durable pending content is missing or corrupt")
		}
		file, err := os.OpenFile(stagingPath, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("open staged content for recovery: %w", err)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil {
			return fmt.Errorf("sync staged content during recovery: %w", syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close staged content during recovery: %w", closeErr)
		}
		if err := os.Rename(stagingPath, finalPath); err != nil {
			return fmt.Errorf("publish recovered content: %w", err)
		}
	}
	file, err := os.OpenFile(finalPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open published content during recovery: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return fmt.Errorf("sync published content during recovery: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close published content during recovery: %w", closeErr)
	}
	if err := syncDirectory(s.paths.Pending); err != nil {
		return fmt.Errorf("sync recovered content directory: %w", err)
	}
	request := WriteRequest{
		OperationID: item.operationID, ObjectID: item.objectID,
		Kind: item.operationKind, IdempotencyKey: item.idempotency,
		BaseDriveVersion: item.baseVersion,
	}
	return s.finalizeContent(ctx, request, item.versionID, item.size, item.sha256.String)
}

func (s *ContentStore) removeIncompleteVersionFiles(ctx context.Context, item preparedContent) error {
	var complete, current int
	if err := s.metadata.db.QueryRowContext(ctx, `SELECT is_complete, is_current FROM content_versions WHERE version_id = ?`,
		string(item.versionID)).Scan(&complete, &current); err != nil {
		return fmt.Errorf("inspect incomplete content before cleanup: %w", err)
	}
	if complete != 0 || current != 0 {
		return fmt.Errorf("cannot remove content version %q because it is marked complete or current", item.versionID)
	}
	var references int
	if err := s.metadata.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations WHERE content_version_id = ? AND operation_id <> ?`,
		string(item.versionID), string(item.operationID)).Scan(&references); err != nil {
		return fmt.Errorf("check incomplete content references: %w", err)
	}
	if references != 0 {
		return fmt.Errorf("cannot remove incomplete content version %q because another operation references it", item.versionID)
	}
	if err := s.metadata.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_versions WHERE local_path = ? AND version_id <> ?`,
		item.localPath, string(item.versionID)).Scan(&references); err != nil {
		return fmt.Errorf("check incomplete content path references: %w", err)
	}
	if references != 0 {
		return fmt.Errorf("cannot remove incomplete content path %q because another version references it", item.localPath)
	}
	for _, path := range []string{s.stagingPath(item.versionID), s.blobPath(item.versionID)} {
		removed, err := removeRegularFile(path)
		if err != nil {
			return err
		}
		if removed {
			if err := syncDirectory(s.paths.Pending); err != nil {
				return fmt.Errorf("sync removal of incomplete content: %w", err)
			}
		}
	}
	return nil
}

func (s *ContentStore) markPermanentError(ctx context.Context, operationID domain.OperationID, code, message string) error {
	result, err := s.metadata.db.ExecContext(ctx, `UPDATE operations SET state = 'permanent-error', last_error_code = ?,
		last_error_message = ?, updated_at = ? WHERE operation_id = ? AND state = 'prepared'`,
		code, message, formatTime(time.Now()), string(operationID))
	if err != nil {
		return fmt.Errorf("mark prepared operation as failed: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected > 1 {
		return fmt.Errorf("mark prepared operation affected %d rows (err=%v)", affected, err)
	}
	return nil
}

func (s *ContentStore) removeOrphanFiles(ctx context.Context) (int, error) {
	rows, err := s.metadata.db.QueryContext(ctx, `SELECT version_id, local_path FROM content_versions`)
	if err != nil {
		return 0, fmt.Errorf("read content references: %w", err)
	}
	referenced := make(map[string]struct{})
	for rows.Next() {
		var versionID string
		var path string
		if err := rows.Scan(&versionID, &path); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan content reference: %w", err)
		}
		referenced[filepath.ToSlash(filepath.Clean(path))] = struct{}{}
		if contentIDPattern.MatchString(versionID) {
			referenced[filepath.ToSlash(filepath.Join("pending", "."+versionID+".tmp"))] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("read content references: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close content references: %w", err)
	}
	removedCount := 0
	for _, directory := range []struct {
		path     string
		relative string
		filter   func(string) bool
		keepRef  bool
	}{
		{path: s.paths.Pending, relative: "pending", filter: generatedContentFileName, keepRef: true},
		{path: s.paths.EvictableCache, relative: filepath.ToSlash(filepath.Join("cache", "evictable")), filter: generatedContentFileName, keepRef: true},
		{path: s.paths.Transient, filter: generatedDownloadFileName},
	} {
		entries, err := os.ReadDir(directory.path)
		if err != nil {
			return removedCount, fmt.Errorf("list content files in %q: %w", directory.path, err)
		}
		removedHere := false
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return removedCount, err
			}
			if !directory.filter(entry.Name()) {
				continue
			}
			if directory.keepRef {
				relative := filepath.ToSlash(filepath.Join(directory.relative, entry.Name()))
				if _, exists := referenced[relative]; exists {
					continue
				}
			}
			removed, err := removeRegularFile(filepath.Join(directory.path, entry.Name()))
			if err != nil {
				return removedCount, err
			}
			if removed {
				removedCount++
				removedHere = true
			}
		}
		if removedHere {
			if err := syncDirectory(directory.path); err != nil {
				return removedCount, fmt.Errorf("sync orphan cleanup in %q: %w", directory.path, err)
			}
		}
	}
	return removedCount, nil
}

func (s *ContentStore) writeResult(ctx context.Context, operationID domain.OperationID, versionID domain.ContentVersionID) (WriteResult, error) {
	operation, err := s.scanOperation(s.metadata.db.QueryRowContext(ctx, `SELECT operation_id, object_id, kind, state,
		idempotency_key, base_drive_version, content_version_id, payload_json, resumable_session_url,
		resumable_offset, attempt_count, next_attempt_at, last_error_code, last_error_message, created_at, updated_at
		FROM operations WHERE operation_id = ?`, string(operationID)))
	if err != nil {
		return WriteResult{}, err
	}
	version, err := scanContentVersion(s.metadata.db.QueryRowContext(ctx, `SELECT version_id, object_id, kind, local_path,
		size_bytes, COALESCE(sha256, ''), COALESCE(drive_version, ''), is_complete, is_current, created_at
		FROM content_versions WHERE version_id = ?`, string(versionID)))
	if err != nil {
		return WriteResult{}, err
	}
	if operation.State == domain.OperationPermanentError {
		return WriteResult{}, ErrIncompleteOperation
	}
	return WriteResult{Operation: operation, ContentVersion: version}, nil
}

func (s *ContentStore) scanOperation(row rowScanner) (domain.Operation, error) {
	var operation domain.Operation
	var operationID, objectID, kind, state string
	var contentVersionID, nextAttempt sql.NullString
	var createdAt, updatedAt string
	err := row.Scan(&operationID, &objectID, &kind, &state, &operation.IdempotencyKey, &operation.BaseDriveVersion,
		&contentVersionID, &operation.PayloadJSON, &operation.ResumableSessionURL, &operation.ResumableOffset,
		&operation.AttemptCount, &nextAttempt, &operation.LastErrorCode, &operation.LastErrorMessage, &createdAt, &updatedAt)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("read write operation: %w", err)
	}
	operation.ID = domain.OperationID(operationID)
	operation.ObjectID = domain.ObjectID(objectID)
	operation.Kind = domain.OperationKind(kind)
	operation.State = domain.OperationState(state)
	if contentVersionID.Valid {
		versionID := domain.ContentVersionID(contentVersionID.String)
		operation.ContentVersionID = &versionID
	}
	if nextAttempt.Valid {
		operation.NextAttemptAt, err = time.Parse(time.RFC3339Nano, nextAttempt.String)
		if err != nil {
			return domain.Operation{}, fmt.Errorf("parse operation retry time: %w", err)
		}
	}
	if operation.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return domain.Operation{}, fmt.Errorf("parse operation creation time: %w", err)
	}
	if operation.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt); err != nil {
		return domain.Operation{}, fmt.Errorf("parse operation update time: %w", err)
	}
	return operation, nil
}

func scanContentVersion(row rowScanner) (domain.ContentVersion, error) {
	var version domain.ContentVersion
	var versionID, objectID, kind, path, createdAt string
	var complete, current int
	err := row.Scan(&versionID, &objectID, &kind, &path, &version.Size, &version.SHA256, &version.DriveVersion,
		&complete, &current, &createdAt)
	if err != nil {
		return domain.ContentVersion{}, fmt.Errorf("read content version: %w", err)
	}
	version.ID = domain.ContentVersionID(versionID)
	version.ObjectID = domain.ObjectID(objectID)
	version.Kind = domain.ContentVersionKind(kind)
	version.LocalPath = path
	version.Complete = complete != 0
	version.Current = current != 0
	if version.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return domain.ContentVersion{}, fmt.Errorf("parse content version creation time: %w", err)
	}
	return version, nil
}

func (s *ContentStore) pathForVersion(version domain.ContentVersion) (string, error) {
	if !contentIDPattern.MatchString(string(version.ID)) {
		return "", errors.New("content version has an invalid local path")
	}
	expected := s.relativeBlobPath(version.ID)
	if version.Kind == domain.ContentCached {
		expected = s.relativeCachedBlobPath(version.ID)
	}
	if version.LocalPath != expected {
		return "", errors.New("content version has an invalid local path")
	}
	return filepath.Join(s.paths.Root, filepath.FromSlash(version.LocalPath)), nil
}

func (s *ContentStore) relativeBlobPath(versionID domain.ContentVersionID) string {
	return filepath.ToSlash(filepath.Join("pending", string(versionID)+".blob"))
}

func (s *ContentStore) stagingPath(versionID domain.ContentVersionID) string {
	return filepath.Join(s.paths.Pending, "."+string(versionID)+".tmp")
}

func (s *ContentStore) blobPath(versionID domain.ContentVersionID) string {
	return filepath.Join(s.paths.Pending, string(versionID)+".blob")
}

func (s *ContentStore) relativeCachedBlobPath(versionID domain.ContentVersionID) string {
	return filepath.ToSlash(filepath.Join("cache", "evictable", string(versionID)+".blob"))
}

func (s *ContentStore) cachedBlobPath(versionID domain.ContentVersionID) string {
	return filepath.Join(s.paths.EvictableCache, string(versionID)+".blob")
}

func (s *ContentStore) objectLock(objectID domain.ObjectID) *sync.Mutex {
	var hash uint64 = 1469598103934665603
	for index := 0; index < len(objectID); index++ {
		hash ^= uint64(objectID[index])
		hash *= 1099511628211
	}
	return &s.objectMu[hash%uint64(len(s.objectMu))]
}

func (s *ContentStore) reserveDiskSpace(size int64) (*diskReservation, error) {
	if size < 0 {
		return nil, errors.New("disk reservation size must not be negative")
	}
	s.reservationMu.Lock()
	defer s.reservationMu.Unlock()
	available, total, err := s.filesystemSpace(s.paths.Root)
	if err != nil {
		return nil, fmt.Errorf("check free space for pending write: %w", err)
	}
	requested := uint64(size)
	if total == 0 || s.reservedBytes > available {
		return nil, ErrInsufficientDiskSpace
	}
	freeAfterReservations := available - s.reservedBytes
	if requested > freeAfterReservations {
		return nil, ErrInsufficientDiskSpace
	}
	freeAfterWrite := freeAfterReservations - requested
	if freeAfterWrite < total/10 {
		return nil, fmt.Errorf("%w: write would leave less than 10%% free", ErrInsufficientDiskSpace)
	}
	s.reservedBytes += requested
	return &diskReservation{store: s, remaining: requested}, nil
}

type diskReservation struct {
	store     *ContentStore
	remaining uint64
}

func (r *diskReservation) consume(size int) {
	if size <= 0 {
		return
	}
	r.store.reservationMu.Lock()
	defer r.store.reservationMu.Unlock()
	consumed := uint64(size)
	if consumed > r.remaining {
		consumed = r.remaining
	}
	r.remaining -= consumed
	r.store.reservedBytes -= consumed
}

func (r *diskReservation) release() {
	r.store.reservationMu.Lock()
	defer r.store.reservationMu.Unlock()
	r.store.reservedBytes -= r.remaining
	r.remaining = 0
}

type reservedWriter struct {
	file        io.Writer
	reservation *diskReservation
}

func (w *reservedWriter) Write(buffer []byte) (int, error) {
	written, err := w.file.Write(buffer)
	w.reservation.consume(written)
	return written, err
}

func (s *ContentStore) hitCheckpoint(name string) {
	if s.checkpoint != nil {
		s.checkpoint(name)
	}
}

func (s *ContentStore) cleanupUncommitted(ctx context.Context, operationID domain.OperationID) error {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	item, found, err := s.preparedByID(cleanupContext, operationID)
	if err != nil {
		return fmt.Errorf("read operation during failed-write cleanup: %w", err)
	}
	if !found || item.state != domain.OperationPrepared || (item.sha256.Valid && item.sha256.String != "") {
		return nil
	}
	if err := s.recoverPrepared(cleanupContext, item); err != nil {
		return fmt.Errorf("clean failed unacknowledged write: %w", err)
	}
	return nil
}

func randomContentToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create private content directory %q: %w", path, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect content directory %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("content path %q is not a real directory", path)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("restrict content directory permissions %q: %w", path, err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func filesystemSpace(path string) (available uint64, total uint64, err error) {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		return 0, 0, err
	}
	if stats.Bsize <= 0 {
		return 0, 0, errors.New("filesystem reported an invalid block size")
	}
	blockSize := uint64(stats.Bsize)
	maxUint64 := ^uint64(0)
	if stats.Bavail > maxUint64/blockSize || stats.Blocks > maxUint64/blockSize {
		return 0, 0, errors.New("filesystem capacity exceeds supported size")
	}
	return stats.Bavail * blockSize, stats.Blocks * blockSize, nil
}

func copyWithContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, contentCopyBufferSize)
	return io.CopyBuffer(destination, contextReader{ctx: ctx, source: source}, buffer)
}

type contextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(buffer)
}

func verifyFile(path string, expectedSize int64, expectedChecksum string) (bool, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("inspect recovery candidate %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return true, false, fmt.Errorf("recovery candidate %q is not a regular file", path)
	}
	if info.Size() != expectedSize {
		return true, false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return false, false, fmt.Errorf("open recovery candidate %q: %w", path, err)
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if copyErr != nil {
		return false, false, fmt.Errorf("hash recovery candidate %q: %w", path, copyErr)
	}
	if closeErr != nil {
		return false, false, fmt.Errorf("close recovery candidate %q: %w", path, closeErr)
	}
	return true, hex.EncodeToString(digest.Sum(nil)) == expectedChecksum, nil
}

func removeRegularFile(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect file before cleanup %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("refusing to remove non-regular content path %q", path)
	}
	if err := os.Remove(path); err != nil {
		return false, fmt.Errorf("remove unused content file %q: %w", path, err)
	}
	return true, nil
}

func generatedContentFileName(name string) bool {
	if strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".tmp") {
		name = strings.TrimSuffix(strings.TrimPrefix(name, "."), ".tmp")
	} else if strings.HasSuffix(name, ".blob") {
		name = strings.TrimSuffix(name, ".blob")
	} else {
		return false
	}
	return contentIDPattern.MatchString(name)
}

func generatedDownloadFileName(name string) bool {
	if !strings.HasPrefix(name, ".download-") || !strings.HasSuffix(name, ".tmp") {
		return false
	}
	return contentIDPattern.MatchString("cv-" + strings.TrimSuffix(strings.TrimPrefix(name, ".download-"), ".tmp"))
}

func generatedWorkingFileName(name string) bool {
	if !strings.HasPrefix(name, "working-") || !strings.HasSuffix(name, ".tmp") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimSuffix(strings.TrimPrefix(name, "working-"), ".tmp"))
	return err == nil && len(decoded) == 16
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
