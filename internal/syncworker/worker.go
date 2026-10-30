package syncworker

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

const (
	GoogleFolderMIMEType = "application/vnd.google-apps.folder"
	operationProperty    = "gdlOperationId"
	defaultChunkSize     = int64(8 * 1024 * 1024)
)

var (
	ErrRemoteConflict = errors.New("Drive object changed while a local operation was pending")
	ErrNeedsReconcile = errors.New("Drive operation outcome needs reconciliation")
)

type Drive interface {
	GetFile(context.Context, string) (driveapi.File, error)
	CreateFile(context.Context, driveapi.FileMetadata) (driveapi.File, error)
	UpdateFile(context.Context, string, driveapi.FilePatch, []string, []string) (driveapi.File, error)
	GenerateIDs(context.Context, int) ([]string, error)
	StartResumableUpload(context.Context, driveapi.UploadRequest) (string, error)
	UploadChunk(context.Context, string, driveapi.UploadChunkRequest) (driveapi.UploadProgress, error)
	QueryUploadStatus(context.Context, string, int64) (driveapi.UploadProgress, error)
}

type Worker struct {
	metadata   *store.Store
	content    *store.ContentStore
	drive      Drive
	chunkSize  int64
	retryDelay time.Duration
	mu         sync.Mutex
}

const maxQueueRetryDelay = 30 * time.Minute

type operationPayload struct {
	ParentObjectID    domain.ObjectID `json:"parent_object_id,omitempty"`
	Name              string          `json:"name,omitempty"`
	OldParentObjectID domain.ObjectID `json:"old_parent_object_id,omitempty"`
	OldName           string          `json:"old_name,omitempty"`
	NewParentObjectID domain.ObjectID `json:"new_parent_object_id,omitempty"`
	NewName           string          `json:"new_name,omitempty"`
}

func New(metadata *store.Store, content *store.ContentStore, drive Drive) (*Worker, error) {
	if metadata == nil || content == nil || drive == nil {
		return nil, errors.New("metadata store, content store, and Drive API are required")
	}
	return &Worker{metadata: metadata, content: content, drive: drive, chunkSize: defaultChunkSize, retryDelay: time.Minute}, nil
}

// RunOne processes the next dependency-ready operation. An uploading record is
// always reconciled with its persisted session before more bytes are sent.
func (w *Worker) RunOne(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, errors.New("context must not be nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	operation, found, err := w.metadata.ClaimNextOperation(ctx, time.Now())
	if err != nil || !found {
		return found, err
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	object, err := w.metadata.ObjectByID(ctx, operation.ObjectID)
	if err != nil {
		return true, fmt.Errorf("read queued object %q: %w", operation.ObjectID, err)
	}
	if (operation.Kind == domain.OperationCreate || operation.Kind == domain.OperationMkdir) && object.Deleted && object.DriveFileID == nil {
		return true, w.metadata.CommitSkippedOperation(ctx, operation.ID)
	}
	if (operation.Kind == domain.OperationRename || operation.Kind == domain.OperationTrash || operation.Kind == domain.OperationWrite) && object.DriveFileID == nil {
		return true, w.metadata.CommitSkippedOperation(ctx, operation.ID)
	}
	switch operation.Kind {
	case domain.OperationMkdir:
		err = w.processMetadataCreate(ctx, operation, object, true)
	case domain.OperationCreate:
		err = w.processCreate(ctx, operation, object)
	case domain.OperationWrite:
		err = w.processWrite(ctx, operation, object)
	case domain.OperationRename:
		err = w.processRename(ctx, operation, object)
	case domain.OperationTrash:
		err = w.processTrash(ctx, operation, object)
	default:
		err = w.setFailure(ctx, operation, errors.New("unsupported queue operation"), false)
	}
	return true, err
}

// RunUntilIdle drains ready work and stops on the first error or when all
// remaining operations are waiting for dependencies, retries, or user action.
func (w *Worker) RunUntilIdle(ctx context.Context, maxOperations int) (int, error) {
	if maxOperations < 1 {
		return 0, errors.New("maximum operation count must be positive")
	}
	processed := 0
	for processed < maxOperations {
		worked, err := w.RunOne(ctx)
		if err != nil {
			return processed + boolInt(worked), err
		}
		if !worked {
			return processed, nil
		}
		processed++
	}
	return processed, nil
}

func (w *Worker) processCreate(ctx context.Context, operation domain.Operation, object domain.Object) error {
	if strings.HasPrefix(object.MIMEType, "application/vnd.google-apps.") {
		return w.setFailure(ctx, operation, errors.New("native Google Workspace documents are not supported as local uploads"), false)
	}
	version, file, err := w.openOperationVersion(ctx, operation)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	defer file.Close()
	checksum, err := checksumContent(file, version)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	metadata, parentID, name, err := w.fileMetadata(ctx, operation, object)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	object, err = w.ensureDriveID(ctx, object)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	metadata.ID = string(*object.DriveFileID)
	if version.Size == 0 {
		return w.processMetadataCreateWithExpected(ctx, operation, object, metadata, parentID, name, &version, checksum)
	}
	return w.processResumable(ctx, operation, object, metadata, parentID, name, version, checksum, true)
}

func (w *Worker) processWrite(ctx context.Context, operation domain.Operation, object domain.Object) error {
	version, file, err := w.openOperationVersion(ctx, operation)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	defer file.Close()
	checksum, err := checksumContent(file, version)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	if object.DriveFileID == nil {
		return w.setFailure(ctx, operation, errors.New("write operation has no Drive file ID"), false)
	}
	metadata := driveapi.FileMetadata{AppProperties: map[string]string{operationProperty: string(operation.ID)}}
	return w.processResumable(ctx, operation, object, metadata, "", "", version, checksum, false)
}

func (w *Worker) processResumable(ctx context.Context, operation domain.Operation, object domain.Object, metadata driveapi.FileMetadata,
	parentID, name string, version domain.ContentVersion, checksum string, create bool) error {
	if object.DriveFileID == nil {
		return w.setFailure(ctx, operation, errors.New("resumable content operation has no reserved Drive file ID"), false)
	}
	driveID := string(*object.DriveFileID)
	if operation.ResumableSessionURL != "" {
		progress, err := w.drive.QueryUploadStatus(ctx, operation.ResumableSessionURL, version.Size)
		if errors.Is(err, driveapi.ErrUploadSessionExpired) {
			if err := w.metadata.ResetResumableSession(ctx, operation.ID); err != nil {
				return err
			}
			operation.ResumableSessionURL = ""
		} else if err != nil {
			return w.setFailure(ctx, operation, err, false)
		} else if progress.Complete {
			return w.commitUploadedFile(ctx, operation, object, progress.File, parentID, name, version, checksum, create)
		} else {
			if progress.NextOffset < 0 || progress.NextOffset > version.Size {
				return w.holdForReconciliation(ctx, operation, "invalid_upload_offset", "Drive returned an invalid upload offset")
			}
			operation.ResumableOffset = progress.NextOffset
			if err := w.metadata.SaveResumableOffset(ctx, operation.ID, progress.NextOffset); err != nil {
				return err
			}
			return w.uploadFromOffset(ctx, operation, object, version, checksum, create, parentID, name, progress)
		}
	}

	remote, getErr := w.drive.GetFile(ctx, driveID)
	if getErr == nil {
		if w.uploadMatches(remote, operation, object, version, checksum, create, parentID, name) {
			return w.commit(ctx, operation, remote)
		}
		if create {
			return w.markConflict(ctx, operation, "reserved_id_in_use")
		}
		if remote.AppProperties[operationProperty] == string(operation.ID) {
			return w.holdForReconciliation(ctx, operation, "upload_marker_without_matching_content", "Drive reports this operation but its content could not be verified")
		}
	} else if !errors.Is(getErr, driveapi.ErrNotFound) {
		return w.setFailure(ctx, operation, getErr, false)
	} else if !create {
		return w.markConflict(ctx, operation, "remote_object_missing")
	}

	if !create && getErr == nil {
		baseVersion := operation.BaseDriveVersion
		if baseVersion == "" {
			baseVersion = object.DriveVersion
		}
		if baseVersion != "" && remote.Version != baseVersion {
			return w.markConflict(ctx, operation, "remote_version_changed")
		}
	}

	fileID := driveID
	if create {
		fileID = ""
	}
	sessionURL, err := w.drive.StartResumableUpload(ctx, driveapi.UploadRequest{
		FileID: fileID, Metadata: metadata,
		Size: version.Size, MIMEType: object.MIMEType,
	})
	if err != nil {
		return w.setFailure(ctx, operation, err, true)
	}
	if err := w.metadata.SaveResumableSession(ctx, operation.ID, sessionURL, 0); err != nil {
		return err
	}
	operation.ResumableSessionURL = sessionURL
	operation.ResumableOffset = 0
	progress, err := w.drive.QueryUploadStatus(ctx, sessionURL, version.Size)
	if errors.Is(err, driveapi.ErrUploadSessionExpired) {
		_ = w.metadata.ResetResumableSession(ctx, operation.ID)
		return w.holdForReconciliation(ctx, operation, "new_session_expired", "The new upload session expired before content was sent")
	}
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	if progress.Complete {
		return w.commitUploadedFile(ctx, operation, object, progress.File, parentID, name, version, checksum, create)
	}
	if progress.NextOffset < 0 || progress.NextOffset > version.Size {
		return w.holdForReconciliation(ctx, operation, "invalid_upload_offset", "Drive returned an invalid upload offset")
	}
	if err := w.metadata.SaveResumableOffset(ctx, operation.ID, progress.NextOffset); err != nil {
		return err
	}
	return w.uploadFromOffset(ctx, operation, object, version, checksum, create, parentID, name, progress)
}

func (w *Worker) uploadFromOffset(ctx context.Context, operation domain.Operation, object domain.Object, version domain.ContentVersion,
	checksum string, create bool, parentID, name string, progress driveapi.UploadProgress) error {
	file, _, err := w.content.OpenContentVersion(ctx, version.ID)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	defer file.Close()
	for {
		if progress.Complete {
			return w.commitUploadedFile(ctx, operation, object, progress.File, parentID, name, version, checksum, create)
		}
		offset := progress.NextOffset
		if version.Size == 0 {
			progress, err = w.drive.UploadChunk(ctx, operation.ResumableSessionURL, driveapi.UploadChunkRequest{
				Start: 0, End: -1, Total: 0, MIMEType: object.MIMEType, Body: strings.NewReader(""), Empty: true,
			})
		} else {
			if offset >= version.Size {
				return w.holdForReconciliation(ctx, operation, "upload_stalled_at_end", "Drive has not confirmed completion of the uploaded content")
			}
			length := w.chunkSize
			if length <= 0 {
				length = defaultChunkSize
			}
			if remaining := version.Size - offset; length > remaining {
				length = remaining
			}
			progress, err = w.drive.UploadChunk(ctx, operation.ResumableSessionURL, driveapi.UploadChunkRequest{
				Start: offset, End: offset + length - 1, Total: version.Size,
				MIMEType: object.MIMEType, Body: io.NewSectionReader(file, offset, length),
			})
		}
		if err != nil {
			return w.setFailure(ctx, operation, err, false)
		}
		if progress.Complete {
			return w.commitUploadedFile(ctx, operation, object, progress.File, parentID, name, version, checksum, create)
		}
		if progress.NextOffset < offset || progress.NextOffset > version.Size {
			return w.holdForReconciliation(ctx, operation, "invalid_upload_offset", "Drive returned an invalid upload offset")
		}
		if progress.NextOffset == offset {
			if err := w.metadata.SaveResumableOffset(ctx, operation.ID, offset); err != nil {
				return err
			}
			return errors.New("Drive upload made no forward progress; the next attempt will query session status")
		}
		if err := w.metadata.SaveResumableOffset(ctx, operation.ID, progress.NextOffset); err != nil {
			return err
		}
	}
}

func (w *Worker) processMetadataCreate(ctx context.Context, operation domain.Operation, object domain.Object, folder bool) error {
	metadata, parentID, name, err := w.fileMetadata(ctx, operation, object)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	if folder {
		metadata.MIMEType = GoogleFolderMIMEType
	}
	object, err = w.ensureDriveID(ctx, object)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	metadata.ID = string(*object.DriveFileID)
	return w.processMetadataCreateWithExpected(ctx, operation, object, metadata, parentID, name, nil, "")
}

func (w *Worker) processMetadataCreateWithExpected(ctx context.Context, operation domain.Operation, object domain.Object,
	metadata driveapi.FileMetadata, parentID, name string, version *domain.ContentVersion, checksum string) error {
	driveID := string(*object.DriveFileID)
	remote, err := w.drive.GetFile(ctx, driveID)
	if err == nil {
		if w.metadataCreateMatches(remote, operation, object, metadata, parentID, name, version, checksum) {
			return w.commit(ctx, operation, remote)
		}
		return w.markConflict(ctx, operation, "reserved_id_in_use")
	}
	if !errors.Is(err, driveapi.ErrNotFound) {
		return w.setFailure(ctx, operation, err, false)
	}
	remote, err = w.drive.CreateFile(ctx, metadata)
	if err != nil {
		return w.setFailure(ctx, operation, err, true)
	}
	if !w.metadataCreateMatches(remote, operation, object, metadata, parentID, name, version, checksum) {
		verified, ok, reconcileErr := w.refetchVerifiedFile(ctx, operation, driveID,
			func(file driveapi.File) bool {
				return w.metadataCreateMatches(file, operation, object, metadata, parentID, name, version, checksum)
			})
		if reconcileErr != nil {
			return reconcileErr
		}
		if !ok {
			return w.holdForReconciliation(ctx, operation, "create_response_unverified", "Drive did not return enough matching metadata to confirm creation")
		}
		remote = verified
	}
	return w.commit(ctx, operation, remote)
}

func (w *Worker) processRename(ctx context.Context, operation domain.Operation, object domain.Object) error {
	if object.DriveFileID == nil {
		return w.metadata.CommitSkippedOperation(ctx, operation.ID)
	}
	var payload operationPayload
	if err := json.Unmarshal([]byte(operation.PayloadJSON), &payload); err != nil {
		return w.setFailure(ctx, operation, errors.New("invalid rename queue payload"), false)
	}
	newParent, err := w.metadata.ObjectByID(ctx, payload.NewParentObjectID)
	if err != nil {
		return w.setFailure(ctx, operation, err, false)
	}
	if newParent.DriveFileID == nil {
		return w.setFailure(ctx, operation, errors.New("rename parent has not been created in Drive"), false)
	}
	remote, err := w.drive.GetFile(ctx, string(*object.DriveFileID))
	if err != nil {
		if errors.Is(err, driveapi.ErrNotFound) {
			return w.holdForReconciliation(ctx, operation, "rename_target_not_found", "Drive could not distinguish deletion from loss of access")
		}
		return w.setFailure(ctx, operation, err, false)
	}
	if remote.Name == payload.NewName && contains(remote.Parents, string(*newParent.DriveFileID)) {
		return w.commit(ctx, operation, remote)
	}
	var addParent, removeParent []string
	if payload.OldParentObjectID != payload.NewParentObjectID {
		oldParent, err := w.metadata.ObjectByID(ctx, payload.OldParentObjectID)
		if err != nil || oldParent.DriveFileID == nil {
			return w.setFailure(ctx, operation, errors.New("rename source parent has no Drive file ID"), false)
		}
		addParent = []string{string(*newParent.DriveFileID)}
		removeParent = []string{string(*oldParent.DriveFileID)}
	}
	name := payload.NewName
	updated, err := w.drive.UpdateFile(ctx, string(*object.DriveFileID), driveapi.FilePatch{
		Name: &name, AppProperties: map[string]string{operationProperty: string(operation.ID)},
	}, addParent, removeParent)
	if err != nil {
		return w.setFailure(ctx, operation, err, true)
	}
	matches := func(file driveapi.File) bool {
		return file.ID == string(*object.DriveFileID) && file.Name == payload.NewName &&
			contains(file.Parents, string(*newParent.DriveFileID)) && file.Version != "" &&
			file.MIMEType != "" && file.Size >= 0
	}
	if !matches(updated) {
		verified, ok, reconcileErr := w.refetchVerifiedFile(ctx, operation, string(*object.DriveFileID), matches)
		if reconcileErr != nil {
			return reconcileErr
		}
		if !ok {
			return w.holdForReconciliation(ctx, operation, "rename_response_unverified", "Drive did not confirm the requested name and parent")
		}
		updated = verified
	}
	return w.commit(ctx, operation, updated)
}

func (w *Worker) processTrash(ctx context.Context, operation domain.Operation, object domain.Object) error {
	if object.DriveFileID == nil {
		return w.metadata.CommitSkippedOperation(ctx, operation.ID)
	}
	remote, err := w.drive.GetFile(ctx, string(*object.DriveFileID))
	if err != nil {
		if errors.Is(err, driveapi.ErrNotFound) {
			return w.holdForReconciliation(ctx, operation, "trash_target_not_found", "Drive could not distinguish deletion from loss of access")
		}
		return w.setFailure(ctx, operation, err, false)
	}
	if remote.Trashed {
		return w.commit(ctx, operation, remote)
	}
	trashed := true
	updated, err := w.drive.UpdateFile(ctx, string(*object.DriveFileID), driveapi.FilePatch{
		Trashed: &trashed, AppProperties: map[string]string{operationProperty: string(operation.ID)},
	}, nil, nil)
	if err != nil {
		return w.setFailure(ctx, operation, err, true)
	}
	matches := func(file driveapi.File) bool {
		return file.ID == string(*object.DriveFileID) && file.Trashed && file.Version != "" &&
			file.MIMEType != "" && file.Size >= 0
	}
	if !matches(updated) {
		verified, ok, reconcileErr := w.refetchVerifiedFile(ctx, operation, string(*object.DriveFileID), matches)
		if reconcileErr != nil {
			return reconcileErr
		}
		if !ok {
			return w.holdForReconciliation(ctx, operation, "trash_response_unverified", "Drive did not confirm moving the file to trash")
		}
		updated = verified
	}
	return w.commit(ctx, operation, updated)
}

func (w *Worker) fileMetadata(ctx context.Context, operation domain.Operation, object domain.Object) (driveapi.FileMetadata, string, string, error) {
	var payload operationPayload
	if err := json.Unmarshal([]byte(operation.PayloadJSON), &payload); err != nil {
		return driveapi.FileMetadata{}, "", "", fmt.Errorf("decode queued file path: %w", err)
	}
	if payload.ParentObjectID == "" || payload.Name == "" {
		entry, err := w.metadata.EntryForObject(ctx, object.ID)
		if err != nil {
			return driveapi.FileMetadata{}, "", "", fmt.Errorf("resolve queued object path: %w", err)
		}
		payload.ParentObjectID, payload.Name = entry.ParentID, entry.RemoteName
	}
	parent, err := w.metadata.ObjectByID(ctx, payload.ParentObjectID)
	if err != nil {
		return driveapi.FileMetadata{}, "", "", fmt.Errorf("resolve Drive parent: %w", err)
	}
	if parent.DriveFileID == nil {
		return driveapi.FileMetadata{}, "", "", errors.New("Drive parent operation has not committed")
	}
	mimeType := object.MIMEType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	return driveapi.FileMetadata{Name: payload.Name, MIMEType: mimeType, Parents: []string{string(*parent.DriveFileID)},
		AppProperties: map[string]string{operationProperty: string(operation.ID)}}, string(*parent.DriveFileID), payload.Name, nil
}

func (w *Worker) ensureDriveID(ctx context.Context, object domain.Object) (domain.Object, error) {
	if object.DriveFileID != nil {
		return object, nil
	}
	ids, err := w.drive.GenerateIDs(ctx, 1)
	if err != nil {
		return domain.Object{}, err
	}
	if len(ids) != 1 || ids[0] == "" {
		return domain.Object{}, driveapi.ErrUnexpectedResponse
	}
	if err := w.metadata.ReserveDriveFileID(ctx, object.ID, ids[0]); err != nil {
		return domain.Object{}, err
	}
	object.DriveFileID = (*domain.DriveFileID)(&ids[0])
	return object, nil
}

func (w *Worker) openOperationVersion(ctx context.Context, operation domain.Operation) (domain.ContentVersion, *os.File, error) {
	if operation.ContentVersionID == nil {
		return domain.ContentVersion{}, nil, errors.New("content operation has no immutable content version")
	}
	file, version, err := w.content.OpenContentVersion(ctx, *operation.ContentVersionID)
	if err != nil {
		return domain.ContentVersion{}, nil, err
	}
	if version.ObjectID != operation.ObjectID || !version.Complete || version.Size < 0 {
		_ = file.Close()
		return domain.ContentVersion{}, nil, errors.New("content operation references an invalid local version")
	}
	return version, file, nil
}

func checksumContent(file *os.File, version domain.ContentVersion) (string, error) {
	stat, err := file.Stat()
	if err != nil || stat.Size() != version.Size {
		return "", errors.New("immutable upload content size does not match its metadata")
	}
	md5Hash := md5.New()
	shaHash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(md5Hash, shaHash), file); err != nil {
		return "", fmt.Errorf("checksum immutable upload content: %w", err)
	}
	if version.SHA256 == "" || !strings.EqualFold(hex.EncodeToString(shaHash.Sum(nil)), version.SHA256) {
		return "", errors.New("immutable upload content failed its SHA-256 integrity check")
	}
	return hex.EncodeToString(md5Hash.Sum(nil)), nil
}

func (w *Worker) uploadMatches(file driveapi.File, operation domain.Operation, object domain.Object, version domain.ContentVersion,
	checksum string, create bool, parentID, name string) bool {
	if file.ID == "" || file.Version == "" || object.DriveFileID == nil || file.ID != string(*object.DriveFileID) ||
		file.AppProperties[operationProperty] != string(operation.ID) || file.Size != version.Size || file.MIMEType != object.MIMEType {
		return false
	}
	if version.Size > 0 && (file.MD5Checksum == "" || !strings.EqualFold(file.MD5Checksum, checksum)) {
		return false
	}
	if create && (file.Name != name || !contains(file.Parents, parentID)) {
		return false
	}
	return true
}

func (w *Worker) metadataCreateMatches(file driveapi.File, operation domain.Operation, object domain.Object, metadata driveapi.FileMetadata,
	parentID, name string, version *domain.ContentVersion, checksum string) bool {
	if object.DriveFileID == nil || file.ID != string(*object.DriveFileID) || file.Version == "" || file.Name != name || file.MIMEType != metadata.MIMEType ||
		file.AppProperties[operationProperty] != string(operation.ID) || !contains(file.Parents, parentID) {
		return false
	}
	if version == nil {
		return file.Size == 0
	}
	if file.Size != version.Size {
		return false
	}
	return version.Size == 0 || (file.MD5Checksum != "" && strings.EqualFold(file.MD5Checksum, checksum))
}

func (w *Worker) commitUploadedFile(ctx context.Context, operation domain.Operation, object domain.Object, file *driveapi.File,
	parentID, name string, version domain.ContentVersion, checksum string, create bool) error {
	if file == nil || !w.uploadMatches(*file, operation, object, version, checksum, create, parentID, name) {
		driveID := ""
		if object.DriveFileID != nil {
			driveID = string(*object.DriveFileID)
		}
		verified, ok, reconcileErr := w.refetchVerifiedFile(ctx, operation, driveID,
			func(remote driveapi.File) bool {
				return w.uploadMatches(remote, operation, object, version, checksum, create, parentID, name)
			})
		if reconcileErr != nil {
			return reconcileErr
		}
		if !ok {
			return w.holdForReconciliation(ctx, operation, "upload_completion_unverified", "Drive did not return matching ID, operation marker, size, checksum, and path")
		}
		file = &verified
	}
	return w.commit(ctx, operation, *file)
}

func (w *Worker) refetchVerifiedFile(ctx context.Context, operation domain.Operation, driveID string,
	matches func(driveapi.File) bool) (driveapi.File, bool, error) {
	if driveID == "" {
		return driveapi.File{}, false, nil
	}
	file, err := w.drive.GetFile(ctx, driveID)
	if errors.Is(err, driveapi.ErrNotFound) {
		return driveapi.File{}, false, nil
	}
	if err != nil {
		return driveapi.File{}, false, w.setFailure(ctx, operation, err, false)
	}
	return file, matches(file), nil
}

func (w *Worker) commit(ctx context.Context, operation domain.Operation, file driveapi.File) error {
	if file.ID == "" || file.Version == "" || file.MIMEType == "" || file.Size < 0 {
		return w.holdForReconciliation(ctx, operation, "remote_metadata_incomplete", "Drive did not return the versioned file metadata needed to confirm this operation")
	}
	capabilities, err := json.Marshal(file.Capabilities)
	if err != nil {
		return fmt.Errorf("encode verified Drive capabilities: %w", err)
	}
	return w.metadata.CommitRemoteOperation(ctx, operation.ID, store.RemoteCommit{
		DriveFileID: file.ID, MIMEType: file.MIMEType, Size: file.Size, DriveVersion: file.Version,
		ModifiedAt: file.ModifiedTime, WebViewLink: file.WebViewLink, CapabilitiesJSON: string(capabilities),
	})
}

func (w *Worker) markConflict(ctx context.Context, operation domain.Operation, code string) error {
	if err := w.metadata.SetOperationState(ctx, operation.ID, domain.OperationConflict, code,
		"The local version was kept because Drive could not be safely updated", time.Time{}); err != nil {
		return err
	}
	return fmt.Errorf("%w (%s)", ErrRemoteConflict, code)
}

func (w *Worker) holdForReconciliation(ctx context.Context, operation domain.Operation, code, message string) error {
	if err := w.metadata.SetOperationState(ctx, operation.ID, domain.OperationNeedsReconcile, code, message, time.Time{}); err != nil {
		return err
	}
	return fmt.Errorf("%w (%s)", ErrNeedsReconcile, code)
}

func (w *Worker) setFailure(ctx context.Context, operation domain.Operation, cause error, mutationMayHaveApplied bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if mutationMayHaveApplied {
		var apiErr *driveapi.APIError
		knownRejection := errors.As(cause, &apiErr) && apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 && apiErr.StatusCode != 409
		if !knownRejection || errors.Is(cause, driveapi.ErrTemporary) || isHTTPConflict(cause) || errors.Is(cause, driveapi.ErrUnexpectedResponse) {
			if err := w.metadata.SetOperationState(ctx, operation.ID, domain.OperationNeedsReconcile, "outcome_unknown",
				"The remote result will be checked before retrying", time.Now()); err != nil {
				return errors.Join(cause, err)
			}
			return cause
		}
	}
	state, code, message := failureState(cause)
	var retryAt time.Time
	if state == domain.OperationRetryableError {
		retryAt = w.nextRetryAt(operation)
	}
	if err := w.metadata.SetOperationState(ctx, operation.ID, state, code, message, retryAt); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (w *Worker) nextRetryAt(operation domain.Operation) time.Time {
	if w.retryDelay <= 0 {
		return time.Time{}
	}
	delay := w.retryDelay
	if delay > maxQueueRetryDelay {
		delay = maxQueueRetryDelay
	}
	for attempt := 1; attempt < operation.AttemptCount && delay < maxQueueRetryDelay; attempt++ {
		if delay >= maxQueueRetryDelay/2 {
			delay = maxQueueRetryDelay
		} else {
			delay *= 2
		}
	}
	// Jitter each queue retry so multiple objects failing together do not
	// synchronize their next requests.
	jittered := delay/2 + time.Duration(rand.Float64()*float64(delay))
	if jittered > maxQueueRetryDelay {
		jittered = maxQueueRetryDelay
	}
	return time.Now().Add(jittered)
}

func failureState(err error) (domain.OperationState, string, string) {
	switch {
	case errors.Is(err, driveapi.ErrUnauthenticated):
		return domain.OperationRetryableError, "authorization_required", "Sign in again to continue synchronization"
	case errors.Is(err, driveapi.ErrRateLimited):
		return domain.OperationRetryableError, "rate_limited", "Drive asked the client to slow down"
	case errors.Is(err, driveapi.ErrTemporary):
		return domain.OperationRetryableError, "temporary_failure", "A temporary Drive or network error occurred"
	case errors.Is(err, driveapi.ErrForbidden):
		return domain.OperationPermanentError, "access_denied", "Drive denied this operation"
	case errors.Is(err, driveapi.ErrQuotaExceeded):
		return domain.OperationPermanentError, "quota_exceeded", "Drive quota prevents this operation"
	case errors.Is(err, driveapi.ErrNotFound):
		return domain.OperationPermanentError, "remote_missing", "The Drive object is missing or access was removed"
	default:
		return domain.OperationPermanentError, "operation_failed", "The operation could not be completed safely"
	}
}

func isHTTPConflict(err error) bool {
	var apiErr *driveapi.APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 409
}

func contains(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
