//go:build linux

package drivecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

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
	mu              sync.Mutex
	factory         MountFactory
	defaultDatabase string
	settingsPath    string
	metadata        *store.Store
	database        string
	point           string
	mount           *MountHandle
	auth            *AccountActions
	authBusy        bool
	authState       string
	authError       string
	syncStatus      func() SyncStatus
	pinChanged      func()
	cacheLimit      func(int64) error
}

func New(factory MountFactory) *Controller {
	return &Controller{factory: factory}
}

// Stop is called by the service after its IPC listener stops, including OS
// signals. It unmounts any active namespace but leaves the durable database
// and queued content untouched.
func (c *Controller) Stop() error { return c.unmount() }

type AccountActions struct {
	UnavailableMessage string
	IsAuthenticated    func(context.Context) (bool, error)
	SignIn             func(context.Context) error
	SignOut            func(context.Context) error
}

type SyncStatus struct {
	NetworkState    string
	NetworkError    string
	IndexState      string
	IndexPhase      string
	SyncError       string
	PinState        string
	PinCurrentName  string
	PinCurrentState string
	PinFilesDone    int
	PinFilesTotal   int
	PinBytesDone    int64
	PinBytesTotal   int64
	PinError        string
}

func (c *Controller) SetPaths(database, settingsPath string) {
	c.mu.Lock()
	c.defaultDatabase, c.settingsPath = database, settingsPath
	c.mu.Unlock()
}

func (c *Controller) SetAccountActions(actions *AccountActions) {
	c.mu.Lock()
	c.auth = actions
	if actions == nil {
		c.authState, c.authError = "not-configured", ""
	} else if actions.SignIn == nil {
		c.authState, c.authError = "unavailable", actions.UnavailableMessage
	} else {
		c.authState, c.authError = "signed-out", ""
	}
	c.mu.Unlock()
}

func (c *Controller) SetSyncHooks(status func() SyncStatus, pinsChanged func(), cacheLimit func(int64) error) {
	c.mu.Lock()
	c.syncStatus, c.pinChanged, c.cacheLimit = status, pinsChanged, cacheLimit
	c.mu.Unlock()
}

type Status struct {
	Mounted               bool   `json:"mounted"`
	Mountpoint            string `json:"mountpoint,omitempty"`
	Database              string `json:"database,omitempty"`
	IndexState            string `json:"index_state,omitempty"`
	IndexPhase            string `json:"index_phase,omitempty"`
	QueuePending          int    `json:"queue_pending"`
	AccountState          string `json:"account_state"`
	AccountError          string `json:"account_error,omitempty"`
	CacheLimitBytes       int64  `json:"cache_limit_bytes"`
	CachedBytes           int64  `json:"cached_bytes"`
	ProtectedBytes        int64  `json:"protected_bytes"`
	ProtectedOverageBytes int64  `json:"protected_overage_bytes"`
	DiskFreeBytes         uint64 `json:"disk_free_bytes"`
	DiskTotalBytes        uint64 `json:"disk_total_bytes"`
	NetworkState          string `json:"network_state"`
	NetworkError          string `json:"network_error,omitempty"`
	SyncError             string `json:"sync_error,omitempty"`
	PinState              string `json:"pin_state,omitempty"`
	PinCurrentName        string `json:"pin_current_name,omitempty"`
	PinCurrentState       string `json:"pin_current_state,omitempty"`
	PinFilesDone          int    `json:"pin_files_done"`
	PinFilesTotal         int    `json:"pin_files_total"`
	PinBytesDone          int64  `json:"pin_bytes_done"`
	PinBytesTotal         int64  `json:"pin_bytes_total"`
	PinError              string `json:"pin_error,omitempty"`
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

type settingsPayload struct {
	CacheLimitBytes int64   `json:"cache_limit_bytes"`
	OAuthClientID   *string `json:"oauth_client_id,omitempty"`
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
	case ipc.CommandSignIn:
		return c.beginSignIn()
	case ipc.CommandSignOut:
		return c.beginSignOut()
	case ipc.CommandSettingsGet:
		settings, err := c.getSettings()
		return settings, false, err
	case ipc.CommandSettingsSet:
		var input settingsPayload
		if err := decodePayload(request.Payload, &input); err != nil {
			return nil, false, err
		}
		settings, err := c.setSettings(input)
		return settings, false, err
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
		metadata, closeMetadata, err := c.openMetadata()
		if err != nil {
			return nil, false, err
		}
		defer closeMetadata()
		if err := metadata.ResolveConflict(ctx, domain.EventID(input.EventID), input.Action); err != nil {
			return nil, false, err
		}
		return map[string]bool{"resolved": true}, false, nil
	case ipc.CommandPinsList:
		metadata, closeMetadata, err := c.openMetadata()
		if err != nil {
			return nil, false, err
		}
		defer closeMetadata()
		pins, err := metadata.OfflinePins(ctx)
		return pins, false, err
	case ipc.CommandPin:
		var input recursivePinRequest
		if err := decodePayload(request.Payload, &input); err != nil {
			return nil, false, err
		}
		metadata, closeMetadata, err := c.openMetadata()
		if err != nil {
			return nil, false, err
		}
		defer closeMetadata()
		if err := metadata.SetOfflinePin(ctx, domain.ObjectID(input.ObjectID), input.Recursive); err != nil {
			return nil, false, err
		}
		c.notifyPinsChanged()
		return map[string]bool{"pinned": true}, false, nil
	case ipc.CommandUnpin:
		var input objectRequest
		if err := decodePayload(request.Payload, &input); err != nil {
			return nil, false, err
		}
		metadata, closeMetadata, err := c.openMetadata()
		if err != nil {
			return nil, false, err
		}
		defer closeMetadata()
		if err := metadata.RemoveOfflinePin(ctx, domain.ObjectID(input.ObjectID)); err != nil {
			return nil, false, err
		}
		c.notifyPinsChanged()
		return map[string]bool{"unpinned": true}, false, nil
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
	if c.authBusy {
		return errors.New("account action is in progress")
	}
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
	c.defaultDatabase = database
	if c.settingsPath != "" {
		settings, settingsErr := c.readSettingsLocked()
		if settingsErr == nil {
			settings.Database, settings.Mountpoint = database, point
			settingsErr = writeSettings(c.settingsPath, settings)
		}
		if settingsErr != nil {
			c.authError = "mount preference was not saved: " + settingsErr.Error()
		}
	}
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

func (c *Controller) openMetadata() (*store.Store, func(), error) {
	c.mu.Lock()
	if c.metadata != nil {
		metadata := c.metadata
		c.mu.Unlock()
		return metadata, func() {}, nil
	}
	database, settingsPath := c.defaultDatabase, c.settingsPath
	c.mu.Unlock()
	if settingsPath != "" {
		if settings, err := readSettings(settingsPath, defaultSettings(database)); err == nil && settings.Database != "" {
			database = settings.Database
		}
	}
	if database == "" {
		return nil, nil, errors.New("Drive index is not configured")
	}
	metadata, err := store.Open(database)
	if err != nil {
		return nil, nil, fmt.Errorf("open Drive metadata database: %w", err)
	}
	return metadata, func() { _ = metadata.Close() }, nil
}

func (c *Controller) status(ctx context.Context) (Status, bool, error) {
	c.mu.Lock()
	mounted, point, database := c.mount != nil, c.point, c.database
	if database == "" {
		database = c.defaultDatabase
	}
	if point == "" {
		point = c.readMountpointLocked()
	}
	auth, authBusy, authState, authError := c.auth, c.authBusy, c.authState, c.authError
	syncStatus := c.syncStatus
	c.mu.Unlock()
	settings, settingsErr := c.getSettings()
	if settingsErr == nil && settings.Database != "" {
		database = settings.Database
	}
	status := Status{Mounted: mounted, Mountpoint: point, Database: database,
		CacheLimitBytes: settings.CacheLimitBytes, NetworkState: "not-connected", PinState: "idle"}
	reauthorizationRequired := false
	if syncStatus != nil {
		runtimeStatus := syncStatus()
		status.NetworkState, status.NetworkError = runtimeStatus.NetworkState, runtimeStatus.NetworkError
		status.SyncError = runtimeStatus.SyncError
		status.PinState = runtimeStatus.PinState
		status.PinCurrentName, status.PinCurrentState = runtimeStatus.PinCurrentName, runtimeStatus.PinCurrentState
		status.PinFilesDone, status.PinFilesTotal = runtimeStatus.PinFilesDone, runtimeStatus.PinFilesTotal
		status.PinBytesDone, status.PinBytesTotal = runtimeStatus.PinBytesDone, runtimeStatus.PinBytesTotal
		status.PinError = runtimeStatus.PinError
		if runtimeStatus.NetworkState == "reauthorization-required" && !authBusy {
			reauthorizationRequired = true
		}
	}
	if settingsErr != nil {
		authError = settingsErr.Error()
	}
	if auth != nil && auth.IsAuthenticated != nil && !authBusy {
		authenticated, err := auth.IsAuthenticated(ctx)
		if err != nil {
			authState, authError = "credential-error", err.Error()
		} else if authenticated && authState != "authorization-failed" && authState != "sign-out-failed" {
			authState = "signed-in"
		} else if !authenticated && authState != "authorization-failed" {
			authState = "signed-out"
		}
	}
	if reauthorizationRequired {
		authState = "reauthorization-required"
	}
	status.AccountState, status.AccountError = authState, authError
	metadata, closeMetadata, err := c.openMetadata()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return status, false, nil
		}
		return status, false, err
	}
	defer closeMetadata()
	index, err := metadata.InitialIndexStatus(ctx)
	if err != nil {
		return status, false, err
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
	status.CachedBytes, err = metadata.CachedContentBytes(ctx)
	if err != nil {
		return Status{}, false, err
	}
	status.ProtectedBytes, err = metadata.ProtectedContentBytes(ctx)
	if err != nil {
		return Status{}, false, err
	}
	if status.ProtectedBytes > status.CacheLimitBytes {
		status.ProtectedOverageBytes = status.ProtectedBytes - status.CacheLimitBytes
	}
	if space, spaceErr := diskSpace(filepath.Dir(database)); spaceErr == nil {
		status.DiskFreeBytes, status.DiskTotalBytes = space[0], space[1]
	}
	return status, false, nil
}

func (c *Controller) queue(ctx context.Context) ([]QueueItem, bool, error) {
	metadata, closeMetadata, err := c.openMetadata()
	if err != nil {
		return nil, false, err
	}
	defer closeMetadata()
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
	metadata, closeMetadata, err := c.openMetadata()
	if err != nil {
		return nil, false, err
	}
	defer closeMetadata()
	events, err := metadata.RecentEvents(ctx, time.Time{}, 100)
	return events, false, err
}

func (c *Controller) conflicts(ctx context.Context) ([]store.ConflictRecord, bool, error) {
	metadata, closeMetadata, err := c.openMetadata()
	if err != nil {
		return nil, false, err
	}
	defer closeMetadata()
	conflicts, err := metadata.Conflicts(ctx)
	return conflicts, false, err
}

func (c *Controller) getSettings() (Settings, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readSettingsLocked()
}

func (c *Controller) readSettingsLocked() (Settings, error) {
	fallback := defaultSettings(c.defaultDatabase)
	if c.settingsPath == "" {
		return fallback, nil
	}
	return readSettings(c.settingsPath, fallback)
}

func (c *Controller) setSettings(input settingsPayload) (Settings, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	settings, err := c.readSettingsLocked()
	if err != nil {
		return Settings{}, err
	}
	previous := settings.CacheLimitBytes
	settings.CacheLimitBytes = input.CacheLimitBytes
	if input.OAuthClientID != nil {
		settings.OAuthClientID = strings.TrimSpace(*input.OAuthClientID)
	}
	if err := validateSettings(settings); err != nil {
		return Settings{}, err
	}
	if c.settingsPath == "" {
		return Settings{}, errors.New("settings path is not configured")
	}
	if c.cacheLimit != nil {
		if err := c.cacheLimit(input.CacheLimitBytes); err != nil {
			return Settings{}, fmt.Errorf("apply content cache limit: %w", err)
		}
	}
	if err := writeSettings(c.settingsPath, settings); err != nil {
		if c.cacheLimit != nil {
			_ = c.cacheLimit(previous)
		}
		return Settings{}, err
	}
	return settings, nil
}

func (c *Controller) notifyPinsChanged() {
	c.mu.Lock()
	hook := c.pinChanged
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (c *Controller) readMountpointLocked() string {
	settings, err := c.readSettingsLocked()
	if err != nil {
		return ""
	}
	return settings.Mountpoint
}

func (c *Controller) beginSignIn() (any, bool, error) {
	c.mu.Lock()
	if c.auth == nil {
		c.mu.Unlock()
		return nil, false, errors.New("Google OAuth is not configured; set GOOGLE_OAUTH_CLIENT_ID")
	}
	if c.auth.SignIn == nil {
		message := c.auth.UnavailableMessage
		c.mu.Unlock()
		if message == "" {
			message = "Google OAuth is unavailable"
		}
		return nil, false, errors.New(message)
	}
	if c.authBusy {
		state := c.authState
		c.mu.Unlock()
		return map[string]string{"state": state}, false, nil
	}
	actions := c.auth
	c.authBusy, c.authState, c.authError = true, "authorizing", ""
	c.mu.Unlock()
	go func() {
		err := actions.SignIn(context.Background())
		c.mu.Lock()
		defer c.mu.Unlock()
		c.authBusy = false
		if err != nil {
			c.authState, c.authError = "authorization-failed", err.Error()
		} else {
			c.authState, c.authError = "signed-in", ""
		}
	}()
	return map[string]string{"state": "authorizing"}, false, nil
}

func (c *Controller) beginSignOut() (any, bool, error) {
	c.mu.Lock()
	if c.auth == nil {
		c.mu.Unlock()
		return nil, false, errors.New("Google OAuth is not configured")
	}
	if c.auth.SignOut == nil {
		message := c.auth.UnavailableMessage
		c.mu.Unlock()
		if message == "" {
			message = "Google sign-out is unavailable"
		}
		return nil, false, errors.New(message)
	}
	if c.authBusy {
		state := c.authState
		c.mu.Unlock()
		return map[string]string{"state": state}, false, errors.New("account action is already in progress")
	}
	actions := c.auth
	c.authBusy, c.authState, c.authError = true, "revoking", ""
	c.mu.Unlock()
	if err := c.unmount(); err != nil {
		c.mu.Lock()
		c.authBusy, c.authState, c.authError = false, "signed-in", err.Error()
		c.mu.Unlock()
		return nil, false, err
	}
	go func() {
		err := actions.SignOut(context.Background())
		c.mu.Lock()
		defer c.mu.Unlock()
		c.authBusy = false
		if err != nil {
			c.authState, c.authError = "sign-out-failed", err.Error()
		} else {
			c.authState, c.authError = "signed-out", ""
		}
	}()
	return map[string]string{"state": "revoking"}, false, nil
}

func diskSpace(path string) ([2]uint64, error) {
	var result [2]uint64
	var info unix.Statfs_t
	if err := unix.Statfs(path, &info); err != nil {
		return result, err
	}
	blockSize := uint64(info.Bsize)
	result[0] = uint64(info.Bavail) * blockSize
	result[1] = uint64(info.Blocks) * blockSize
	return result, nil
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
