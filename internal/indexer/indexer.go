package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

const driveFolderMIMEType = "application/vnd.google-apps.folder"

type Indexer struct {
	drive driveapi.API
	store *store.Store
}

func New(drive driveapi.API, metadata *store.Store) (*Indexer, error) {
	if drive == nil {
		return nil, errors.New("Drive API must not be nil")
	}
	if metadata == nil {
		return nil, errors.New("metadata store must not be nil")
	}
	return &Indexer{drive: drive, store: metadata}, nil
}

func (i *Indexer) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	status, err := i.store.InitialIndexStatus(ctx)
	if err != nil {
		return err
	}
	switch status.State {
	case "ready":
		return nil
	case "empty":
		if err := i.start(ctx); err != nil {
			return err
		}
	case "indexing":
	default:
		return fmt.Errorf("cannot start initial index from state %q", status.State)
	}
	for {
		status, err = i.store.InitialIndexStatus(ctx)
		if err != nil {
			return err
		}
		if status.State == "ready" {
			return nil
		}
		if status.State != "indexing" {
			return fmt.Errorf("initial index entered unexpected state %q", status.State)
		}
		switch status.Phase {
		case "scanning":
			if err := i.scanNextFolderPage(ctx); err != nil {
				return err
			}
		case "catching-up":
			if err := i.applyNextChangePage(ctx, status); err != nil {
				return err
			}
		default:
			return fmt.Errorf("initial index has unknown phase %q", status.Phase)
		}
	}
}

func (i *Indexer) start(ctx context.Context) error {
	// Capturing this token before reading the root or traversing children closes
	// the snapshot/change gap: all later remote edits are replayed in catch-up.
	snapshotToken, err := i.drive.GetStartPageToken(ctx)
	if err != nil {
		return fmt.Errorf("get Drive snapshot token: %w", err)
	}
	if snapshotToken == "" {
		return errors.New("Drive returned an empty snapshot token")
	}
	root, err := i.drive.GetFile(ctx, "root")
	if err != nil {
		return fmt.Errorf("read My Drive root: %w", err)
	}
	if root.ID == "" || root.MIMEType != driveFolderMIMEType || root.DriveID != "" {
		return errors.New("Drive returned an invalid My Drive root")
	}
	if err := i.store.BeginInitialIndex(ctx, snapshotToken, root.ID); err != nil {
		return err
	}
	return nil
}

// Reindex rebuilds remote-derived namespace state after Drive rejects the
// saved change token. The store keeps objects with pending local operations.
func (i *Indexer) Reindex(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	snapshotToken, err := i.drive.GetStartPageToken(ctx)
	if err != nil {
		return fmt.Errorf("get Drive reindex snapshot token: %w", err)
	}
	if snapshotToken == "" {
		return errors.New("Drive returned an empty reindex snapshot token")
	}
	root, err := i.drive.GetFile(ctx, "root")
	if err != nil {
		return fmt.Errorf("read My Drive root for reindex: %w", err)
	}
	if root.ID == "" || root.MIMEType != driveFolderMIMEType || root.DriveID != "" {
		return errors.New("Drive returned an invalid My Drive root during reindex")
	}
	if err := i.store.BeginReindex(ctx, snapshotToken, root.ID); err != nil {
		return err
	}
	return i.Run(ctx)
}

func (i *Indexer) scanNextFolderPage(ctx context.Context) error {
	folders, err := i.store.InitialIndexFolders(ctx)
	if err != nil {
		return err
	}
	if len(folders) == 0 {
		return errors.New("initial index scan has no pending folders")
	}
	folder := folders[0]
	page, err := i.drive.ListFiles(ctx, driveapi.FileListOptions{
		Query:     parentQuery(folder.DriveFileID),
		PageToken: folder.PageToken,
		PageSize:  1000,
	})
	if err != nil {
		var apiErr *driveapi.APIError
		if folder.PageToken != "" && errors.As(err, &apiErr) && errors.Is(err, driveapi.ErrInvalidRequest) {
			// Drive says rejected list tokens must be discarded. Replaying from
			// page one is safe because page application upserts by stable Drive ID.
			if resetErr := i.store.ResetInitialIndexFolder(ctx, folder.DriveFileID); resetErr != nil {
				return resetErr
			}
			return nil
		}
		return fmt.Errorf("list children of Drive folder: %w", err)
	}
	entries := make([]store.RemoteEntry, 0, len(page.Files))
	for _, file := range page.Files {
		if file.Trashed || file.DriveID != "" || !hasParent(file.Parents, folder.DriveFileID) {
			continue
		}
		entry, err := remoteEntry(file, folder.DriveFileID)
		if err != nil {
			return err
		}
		entries = append(entries, entry)
	}
	if err := i.store.CommitInitialIndexPage(ctx, folder.DriveFileID, page.NextPageToken, entries); err != nil {
		return err
	}
	return nil
}

func (i *Indexer) applyNextChangePage(ctx context.Context, status store.InitialIndexStatus) error {
	if status.CatchupPageToken == "" {
		return errors.New("initial index catch-up token is empty")
	}
	page, err := i.drive.ListChanges(ctx, status.CatchupPageToken)
	if err != nil {
		return fmt.Errorf("list Drive changes during initial index: %w", err)
	}
	changes, err := remoteChanges(page.Changes)
	if err != nil {
		return err
	}
	if err := i.store.CommitInitialChanges(ctx, changes, page.NextPageToken, page.NewStartPageToken); err != nil {
		return err
	}
	return nil
}

func remoteChanges(apiChanges []driveapi.Change) ([]store.RemoteChange, error) {
	changes := make([]store.RemoteChange, 0, len(apiChanges))
	for _, change := range apiChanges {
		if change.FileID == "" {
			return nil, errors.New("Drive change has an empty file ID")
		}
		if change.Removed || change.File == nil || change.File.Trashed || change.File.DriveID != "" {
			changes = append(changes, store.RemoteChange{DriveFileID: change.FileID, Removed: true})
			continue
		}
		if change.File.ID != change.FileID {
			return nil, errors.New("Drive change file ID does not match its metadata")
		}
		if len(change.File.Parents) != 1 {
			changes = append(changes, store.RemoteChange{DriveFileID: change.FileID, Removed: true})
			continue
		}
		entry, err := remoteEntry(*change.File, change.File.Parents[0])
		if err != nil {
			return nil, err
		}
		changes = append(changes, store.RemoteChange{DriveFileID: change.FileID, Entry: &entry})
	}
	return changes, nil
}

func remoteEntry(file driveapi.File, parentDriveFileID string) (store.RemoteEntry, error) {
	if file.ID == "" || file.MIMEType == "" || file.Name == "" || parentDriveFileID == "" {
		return store.RemoteEntry{}, errors.New("Drive returned incomplete file metadata during indexing")
	}
	if file.Size < 0 {
		return store.RemoteEntry{}, errors.New("Drive returned a negative file size during indexing")
	}
	driveID := domain.DriveFileID(file.ID)
	capabilities, err := json.Marshal(file.Capabilities)
	if err != nil {
		return store.RemoteEntry{}, fmt.Errorf("encode Drive capabilities: %w", err)
	}
	object := domain.Object{
		ID:               domain.ObjectID("drive:" + file.ID),
		DriveFileID:      &driveID,
		MIMEType:         file.MIMEType,
		Size:             file.Size,
		DriveVersion:     file.Version,
		ModifiedAt:       file.ModifiedTime,
		WebViewLink:      file.WebViewLink,
		CapabilitiesJSON: string(capabilities),
		Deleted:          file.Trashed,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	return store.RemoteEntry{Object: object, RemoteName: file.Name, ParentDriveFileID: parentDriveFileID,
		AppProperties: file.AppProperties}, nil
}

func parentQuery(driveFileID string) string {
	escaped := strings.ReplaceAll(driveFileID, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return fmt.Sprintf("'%s' in parents and trashed = false", escaped)
}

func hasParent(parents []string, expected string) bool {
	for _, parent := range parents {
		if parent == expected {
			return true
		}
	}
	return false
}
