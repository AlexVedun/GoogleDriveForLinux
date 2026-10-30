//go:build linux

package drivecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/ipc"
	"google-drive-linux/internal/store"
)

type MountHandle struct {
	Unmount func() error
	Done    <-chan struct{}
}

type MountFactory func(*store.Store, string) (MountHandle, error)

type Controller struct {
	mu       sync.Mutex
	factory  MountFactory
	metadata *store.Store
	database string
	point    string
	mount    *MountHandle
}

func New(factory MountFactory) *Controller {
	return &Controller{factory: factory}
}

// Stop is called by the service after its IPC listener stops, including OS
// signals. It unmounts any active namespace but leaves the durable database
// and queued content untouched.
func (c *Controller) Stop() error { return c.unmount() }

type Status struct {
	Mounted      bool   `json:"mounted"`
	Mountpoint   string `json:"mountpoint,omitempty"`
	Database     string `json:"database,omitempty"`
	IndexState   string `json:"index_state,omitempty"`
	IndexPhase   string `json:"index_phase,omitempty"`
	QueuePending int    `json:"queue_pending"`
}

type MountRequest struct {
	Database   string `json:"database"`
	Mountpoint string `json:"mountpoint"`
}

type objectRequest struct {
	ObjectID string `json:"object_id"`
}

type recursivePinRequest struct {
	ObjectID  string `json:"object_id"`
	Recursive bool   `json:"recursive"`
}

type conflictResolveRequest struct {
	EventID string `json:"event_id"`
	Action  string `json:"action"`
}

type QueueItem struct {
	ID             domain.OperationID    `json:"id"`
	ObjectID       domain.ObjectID       `json:"object_id"`
	Kind           domain.OperationKind  `json:"kind"`
	State          domain.OperationState `json:"state"`
	AttemptCount   int                   `json:"attempt_count"`
	ResumableBytes int64                 `json:"resumable_bytes"`
	ErrorCode      string                `json:"error_code,omitempty"`
	ErrorMessage   string                `json:"error_message,omitempty"`
	UpdatedAt      time.Time             `json:"updated_at"`
}

func (c *Controller) Handle(ctx context.Context, request ipc.Request) (any, bool, error) {
	switch request.Command {
	case ipc.CommandStatus:
		return c.status(ctx)
	case ipc.CommandMount:
		var input MountRequest
		if err := decodePayload(request.Payload, &input); err != nil {
			return nil, false, err
		}
		if err := c.mountIndex(input); err != nil {
			return nil, false, err
		}
		return c.status(ctx)
	case ipc.CommandUnmount:
		if err := c.unmount(); err != nil {
			return nil, false, err
		}
		return c.status(ctx)
	case ipc.CommandQuit:
		if err := c.unmount(); err != nil {
			return nil, false, err
		}
		return map[string]bool{"stopped": true}, true, nil
	case ipc.CommandQueueList:
		return c.queue(ctx)
	case ipc.CommandEventsList:
		return c.events(ctx)
	case ipc.CommandConflictsList:
		return c.conflicts(ctx)
	case ipc.CommandConflictResolve:
		var input conflictResolveRequest
		if err := decodePayload(request.Payload, &input); err != nil {
			return nil, false, err
		}
		metadata, err := c.activeStore()
		if err != nil {
			return nil, false, err
		}
		if err := metadata.ResolveConflict(ctx, domain.EventID(input.EventID), input.Action); err != nil {
			return nil, false, err
		}
		return map[string]bool{"resolved": true}, false, nil
	case ipc.CommandPinsList:
		metadata, err := c.activeStore()
		if err != nil {
			return nil, false, err
		}
		pins, err := metadata.OfflinePins(ctx)
		return pins, false, err
	case ipc.CommandPin:
		var input recursivePinRequest
		if err := decodePayload(request.Payload, &input); err != nil {
			return nil, false, err
		}
		metadata, err := c.activeStore()
		if err != nil {
			return nil, false, err
		}
		if err := metadata.SetOfflinePin(ctx, domain.ObjectID(input.ObjectID), input.Recursive); err != nil {
			return nil, false, err
		}
		return map[string]bool{"pinned": true}, false, nil
	case ipc.CommandUnpin:
		var input objectRequest
		if err := decodePayload(request.Payload, &input); err != nil {
			return nil, false, err
		}
		metadata, err := c.activeStore()
		if err != nil {
			return nil, false, err
		}
		if err := metadata.RemoveOfflinePin(ctx, domain.ObjectID(input.ObjectID)); err != nil {
			return nil, false, err
		}
		return map[string]bool{"unpinned": true}, false, nil
	case ipc.CommandSignOut:
		return nil, false, errors.New("account sign-out is unavailable until OAuth is connected to drivefs")
	case ipc.CommandSettingsGet, ipc.CommandSettingsSet:
		return nil, false, errors.New("settings storage is not connected to drivefs yet")
	default:
		return nil, false, fmt.Errorf("unknown IPC command %q", request.Command)
	}
}

func decodePayload(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		return errors.New("request payload is required")
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode request payload: %w", err)
	}
	return nil
}

func (c *Controller) mountIndex(input MountRequest) error {
	if c.factory == nil {
		return errors.New("FUSE mount factory is unavailable")
	}
	if input.Database == "" || input.Mountpoint == "" {
		return errors.New("database and mountpoint are required")
	}
	database, err := filepath.Abs(input.Database)
	if err != nil {
		return fmt.Errorf("resolve metadata database path: %w", err)
	}
	point, err := filepath.Abs(input.Mountpoint)
	if err != nil {
		return fmt.Errorf("resolve mountpoint: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mount != nil {
		if c.database == database && c.point == point {
			return nil
		}
		return errors.New("drivefs already has a different mount active")
	}
	metadata, err := store.Open(database)
	if err != nil {
		return fmt.Errorf("open metadata database: %w", err)
	}
	handle, err := c.factory(metadata, point)
	if err != nil {
		_ = metadata.Close()
		return fmt.Errorf("mount indexed namespace: %w", err)
	}
	if handle.Unmount == nil || handle.Done == nil {
		_ = metadata.Close()
		return errors.New("mount factory returned an incomplete handle")
	}
	c.metadata, c.database, c.point, c.mount = metadata, database, point, &handle
	go c.watchMount(&handle, metadata)
	return nil
}

func (c *Controller) watchMount(handle *MountHandle, metadata *store.Store) {
	<-handle.Done
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mount == handle {
		c.mount, c.metadata, c.database, c.point = nil, nil, "", ""
		_ = metadata.Close()
	}
}

func (c *Controller) unmount() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mount == nil {
		return nil
	}
	handle, metadata := c.mount, c.metadata
	if err := handle.Unmount(); err != nil {
		return fmt.Errorf("unmount Drive namespace: %w", err)
	}
	select {
	case <-handle.Done:
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for FUSE unmount")
	}
	c.mount, c.metadata, c.database, c.point = nil, nil, "", ""
	if err := metadata.Close(); err != nil {
		return fmt.Errorf("close metadata database: %w", err)
	}
	return nil
}

func (c *Controller) activeStore() (*store.Store, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.metadata == nil {
		return nil, errors.New("Drive index is not open; mount or initialize it first")
	}
	return c.metadata, nil
}

func (c *Controller) status(ctx context.Context) (Status, bool, error) {
	c.mu.Lock()
	metadata, mounted, point, database := c.metadata, c.mount != nil, c.point, c.database
	c.mu.Unlock()
	status := Status{Mounted: mounted, Mountpoint: point, Database: database}
	if metadata == nil {
		return status, false, nil
	}
	index, err := metadata.InitialIndexStatus(ctx)
	if err != nil {
		return Status{}, false, err
	}
	status.IndexState, status.IndexPhase = index.State, index.Phase
	operations, err := metadata.ListOperations(ctx)
	if err != nil {
		return Status{}, false, err
	}
	for _, operation := range operations {
		if operation.State != domain.OperationCommitted {
			status.QueuePending++
		}
	}
	return status, false, nil
}

func (c *Controller) queue(ctx context.Context) ([]QueueItem, bool, error) {
	metadata, err := c.activeStore()
	if err != nil {
		return nil, false, err
	}
	operations, err := metadata.ListOperations(ctx)
	if err != nil {
		return nil, false, err
	}
	items := make([]QueueItem, 0, len(operations))
	for _, operation := range operations {
		if operation.State == domain.OperationCommitted {
			continue
		}
		items = append(items, QueueItem{ID: operation.ID, ObjectID: operation.ObjectID, Kind: operation.Kind,
			State: operation.State, AttemptCount: operation.AttemptCount, ResumableBytes: operation.ResumableOffset,
			ErrorCode: operation.LastErrorCode, ErrorMessage: operation.LastErrorMessage, UpdatedAt: operation.UpdatedAt})
	}
	return items, false, nil
}

func (c *Controller) events(ctx context.Context) ([]domain.Event, bool, error) {
	metadata, err := c.activeStore()
	if err != nil {
		return nil, false, err
	}
	events, err := metadata.RecentEvents(ctx, time.Time{}, 100)
	return events, false, err
}

func (c *Controller) conflicts(ctx context.Context) ([]store.ConflictRecord, bool, error) {
	metadata, err := c.activeStore()
	if err != nil {
		return nil, false, err
	}
	conflicts, err := metadata.Conflicts(ctx)
	return conflicts, false, err
}

func DefaultDatabasePath() (string, error) {
	dataDir := os.Getenv("XDG_DATA_HOME")
	if dataDir == "" {
		var err error
		dataDir, err = os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("find user data directory: %w", err)
		}
		dataDir = filepath.Join(dataDir, ".local", "share")
	}
	dataDir = filepath.Join(dataDir, "google-drive-linux")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return "", fmt.Errorf("create application data directory: %w", err)
	}
	if err := os.Chmod(dataDir, 0o700); err != nil {
		return "", fmt.Errorf("secure application data directory: %w", err)
	}
	return filepath.Join(dataDir, "metadata.sqlite3"), nil
}
