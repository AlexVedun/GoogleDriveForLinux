package contentcache

import (
	"context"
	"errors"
	"fmt"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/store"
)

type PinProgress struct {
	ObjectID        domain.ObjectID
	Name            string
	State           string
	CompletedFiles  int
	FailedFiles     int
	TotalFiles      int
	CompletedBytes  int64
	TotalBytes      int64
	CurrentFileSize int64
	CurrentFileRead int64
	Err             error
}

type PinManager struct {
	metadata *store.Store
	cache    *Cache
}

func NewPinManager(metadata *store.Store, cache *Cache) (*PinManager, error) {
	if metadata == nil || cache == nil {
		return nil, errors.New("metadata store and content cache are required")
	}
	return &PinManager{metadata: metadata, cache: cache}, nil
}

func (m *PinManager) Pin(ctx context.Context, objectID domain.ObjectID, recursive bool) error {
	if m == nil || m.metadata == nil {
		return errors.New("pin manager is not initialized")
	}
	return m.metadata.SetOfflinePin(ctx, objectID, recursive)
}

func (m *PinManager) Unpin(ctx context.Context, objectID domain.ObjectID) error {
	if m == nil || m.metadata == nil {
		return errors.New("pin manager is not initialized")
	}
	return m.metadata.RemoveOfflinePin(ctx, objectID)
}

// Sync downloads every indexed Drive blob covered by a direct or recursive pin.
// It continues after individual file failures so the remaining pinned files can
// still become available offline.
func (m *PinManager) Sync(ctx context.Context, report func(PinProgress)) error {
	if m == nil || m.metadata == nil || m.cache == nil {
		return errors.New("pin manager is not initialized")
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	objects, err := m.metadata.OfflinePinnedFiles(ctx)
	if err != nil {
		return err
	}
	var totalBytes int64
	for _, object := range objects {
		totalBytes += object.Size
	}
	progress := PinProgress{State: "queued", TotalFiles: len(objects), TotalBytes: totalBytes}
	var completedBytes int64
	emit := func() {
		if report != nil {
			report(progress)
		}
	}
	emit()
	var failures []error
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			progress.State, progress.Err = "cancelled", err
			emit()
			return errors.Join(append(failures, err)...)
		}
		entry, err := m.metadata.EntryForObject(ctx, object.ID)
		if err != nil {
			progress.ObjectID, progress.Name = object.ID, string(object.ID)
			progress.State, progress.Err = "error", err
			progress.CurrentFileSize, progress.CurrentFileRead = 0, 0
			progress.FailedFiles++
			failures = append(failures, fmt.Errorf("%s: read indexed path: %w", object.ID, err))
			emit()
			continue
		}
		progress.ObjectID, progress.Name = object.ID, entry.DisplayName
		progress.State, progress.Err = "downloading", nil
		progress.CurrentFileSize, progress.CurrentFileRead = object.Size, 0
		progress.CompletedBytes = completedBytes
		emit()
		err = m.cache.Prefetch(ctx, object, func(downloaded int64) {
			progress.CurrentFileRead = min(downloaded, object.Size)
			progress.CompletedBytes = completedBytes + progress.CurrentFileRead
			emit()
		})
		if err != nil {
			progress.State, progress.Err = "error", err
			progress.CompletedBytes = completedBytes
			failures = append(failures, fmt.Errorf("%s: %w", entry.DisplayName, err))
			emit()
			progress.FailedFiles++
			continue
		}
		progress.CompletedFiles++
		completedBytes += object.Size
		progress.CompletedBytes = completedBytes
		progress.CurrentFileSize, progress.CurrentFileRead = 0, 0
		progress.State, progress.Err = "ready", nil
		emit()
	}
	progress.ObjectID, progress.Name = "", ""
	if len(failures) > 0 {
		progress.State, progress.Err = "partial", errors.Join(failures...)
		emit()
		return progress.Err
	}
	progress.State, progress.Err = "ready", nil
	emit()
	return nil
}

func (m *PinManager) OverLimitBytes(ctx context.Context) (int64, error) {
	if m == nil || m.metadata == nil || m.cache == nil {
		return 0, errors.New("pin manager is not initialized")
	}
	return m.metadata.ProtectedContentOverage(ctx, m.cache.MaxBytes())
}
