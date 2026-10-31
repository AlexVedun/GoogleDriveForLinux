//go:build linux

package drivecontrol

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/ipc"
	"google-drive-linux/internal/store"
)

type fakeMount struct {
	done chan struct{}
	once sync.Once
}

func (m *fakeMount) stop() { m.once.Do(func() { close(m.done) }) }

func TestMountUnmountAreIdempotentAndQuitPreservesDatabase(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "metadata.sqlite3")
	metadata, err := store.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "pending-folder"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	mountpoint := filepath.Join(directory, "mount")
	if err := os.Mkdir(mountpoint, 0o700); err != nil {
		t.Fatal(err)
	}
	var mountCalls, unmountCalls atomic.Int32
	var mounted *fakeMount
	controller := New(func(_ *store.Store, _ string) (MountHandle, error) {
		mountCalls.Add(1)
		mounted = &fakeMount{done: make(chan struct{})}
		return MountHandle{Unmount: func() error { unmountCalls.Add(1); mounted.stop(); return nil }, Done: mounted.done}, nil
	})
	input := MountRequest{Database: database, Mountpoint: mountpoint}
	for range 2 {
		if _, _, err := controller.Handle(context.Background(), request(ipc.CommandMount, input)); err != nil {
			t.Fatal(err)
		}
	}
	if mountCalls.Load() != 1 {
		t.Fatalf("mount factory called %d times", mountCalls.Load())
	}
	status, _, err := controller.status(context.Background())
	if err != nil || !status.Mounted || status.QueuePending != 1 {
		t.Fatalf("status after mount = %+v, %v", status, err)
	}
	for range 2 {
		if _, _, err := controller.Handle(context.Background(), request(ipc.CommandUnmount, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if unmountCalls.Load() != 1 {
		t.Fatalf("unmount called %d times", unmountCalls.Load())
	}
	_, shutdown, err := controller.Handle(context.Background(), request(ipc.CommandQuit, nil))
	if err != nil || !shutdown {
		t.Fatalf("quit result shutdown=%v err=%v", shutdown, err)
	}
	if _, err := os.Stat(database); err != nil {
		t.Fatalf("pending metadata database was removed: %v", err)
	}
	reopened, err := store.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	operations, err := reopened.ListOperations(context.Background())
	if err != nil || len(operations) != 1 || operations[0].State != domain.OperationQueued {
		t.Fatalf("pending operation did not survive quit: %#v, %v", operations, err)
	}
}

func TestUnexpectedMountExitClearsServiceStatus(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "metadata.sqlite3")
	mountpoint := filepath.Join(directory, "mount")
	if err := os.Mkdir(mountpoint, 0o700); err != nil {
		t.Fatal(err)
	}
	var mounted *fakeMount
	controller := New(func(_ *store.Store, _ string) (MountHandle, error) {
		mounted = &fakeMount{done: make(chan struct{})}
		return MountHandle{Unmount: func() error { mounted.stop(); return nil }, Done: mounted.done}, nil
	})
	if _, _, err := controller.Handle(context.Background(), request(ipc.CommandMount, MountRequest{Database: database, Mountpoint: mountpoint})); err != nil {
		t.Fatal(err)
	}
	mounted.stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status, _, err := controller.status(context.Background())
		if err == nil && !status.Mounted {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("unexpected mount exit left the service marked mounted")
}

func TestSettingsPersistAndRejectUnsafeCacheLimit(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	settingsPath := filepath.Join(directory, "settings.json")
	controller := New(nil)
	controller.SetPaths(filepath.Join(directory, "metadata.sqlite3"), settingsPath)

	result, _, err := controller.Handle(context.Background(), request(ipc.CommandSettingsGet, nil))
	if err != nil {
		t.Fatal(err)
	}
	settings := result.(Settings)
	if settings.CacheLimitBytes != DefaultCacheLimitBytes {
		t.Fatalf("default cache limit = %d", settings.CacheLimitBytes)
	}
	result, _, err = controller.Handle(context.Background(), request(ipc.CommandSettingsSet,
		settingsPayload{CacheLimitBytes: 512 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	settings = result.(Settings)
	if settings.CacheLimitBytes != 512<<20 {
		t.Fatalf("saved cache limit = %d", settings.CacheLimitBytes)
	}
	clientID := "  desktop-client.apps.googleusercontent.com  "
	result, _, err = controller.Handle(context.Background(), request(ipc.CommandSettingsSet,
		settingsPayload{CacheLimitBytes: 512 << 20, OAuthClientID: &clientID}))
	if err != nil {
		t.Fatal(err)
	}
	if got := result.(Settings).OAuthClientID; got != "desktop-client.apps.googleusercontent.com" {
		t.Fatalf("saved OAuth client ID = %q", got)
	}
	loaded, err := LoadSettings(settingsPath, "")
	if err != nil || loaded.OAuthClientID != "desktop-client.apps.googleusercontent.com" {
		t.Fatalf("persisted OAuth client ID = %q, error = %v", loaded.OAuthClientID, err)
	}
	result, _, err = controller.Handle(context.Background(), request(ipc.CommandSettingsSet,
		settingsPayload{CacheLimitBytes: 256 << 20}))
	if err != nil || result.(Settings).OAuthClientID != loaded.OAuthClientID {
		t.Fatalf("cache-only update lost OAuth client ID: result = %v, error = %v", result, err)
	}
	if _, _, err := controller.Handle(context.Background(), request(ipc.CommandSettingsSet,
		settingsPayload{CacheLimitBytes: 1})); err == nil {
		t.Fatal("unsafe cache limit was accepted")
	}
	info, err := os.Stat(settingsPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("settings permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestSettingsApplyCacheLimitToConnectedRuntime(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	settingsPath := filepath.Join(directory, "settings.json")
	controller := New(nil)
	controller.SetPaths(filepath.Join(directory, "metadata.sqlite3"), settingsPath)
	var applied int64
	controller.SetSyncHooks(nil, nil, func(limit int64) error {
		applied = limit
		return nil
	})
	if _, _, err := controller.Handle(context.Background(), request(ipc.CommandSettingsSet,
		settingsPayload{CacheLimitBytes: 256 << 20})); err != nil {
		t.Fatal(err)
	}
	if applied != 256<<20 {
		t.Fatalf("runtime cache limit = %d, want %d", applied, 256<<20)
	}
}

func TestAccountActionsRunAsynchronouslyAndPreserveMountData(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	signedOut := make(chan struct{})
	controller := New(nil)
	controller.SetAccountActions(&AccountActions{
		IsAuthenticated: func(context.Context) (bool, error) { return false, nil },
		SignIn: func(context.Context) error {
			close(started)
			<-finish
			return nil
		},
		SignOut: func(context.Context) error { close(signedOut); return nil },
	})
	if _, _, err := controller.Handle(context.Background(), request(ipc.CommandSignIn, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("sign-in action did not start")
	}
	close(finish)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		controller.mu.Lock()
		state, busy := controller.authState, controller.authBusy
		controller.mu.Unlock()
		if !busy && state == "signed-in" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err := controller.Handle(context.Background(), request(ipc.CommandSignOut, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signedOut:
	case <-time.After(time.Second):
		t.Fatal("sign-out action did not start")
	}
}

func TestAccountStatusExplainsUnavailableSecretService(t *testing.T) {
	controller := New(nil)
	controller.SetPaths(filepath.Join(t.TempDir(), "metadata.sqlite3"), "")
	controller.SetAccountActions(&AccountActions{UnavailableMessage: "Secret Service is unavailable"})
	status, _, err := controller.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.AccountState != "unavailable" || status.AccountError != "Secret Service is unavailable" {
		t.Fatalf("account setup status = %+v", status)
	}
	if _, _, err := controller.Handle(context.Background(), request(ipc.CommandSignIn, nil)); err == nil || err.Error() != "Secret Service is unavailable" {
		t.Fatalf("sign-in error = %v", err)
	}
}

func request(command ipc.Command, payload any) ipc.Request {
	var raw json.RawMessage
	if payload != nil {
		raw, _ = json.Marshal(payload)
	}
	return ipc.Request{Version: ipc.ProtocolVersion, ID: "test", Command: command, Payload: raw}
}
