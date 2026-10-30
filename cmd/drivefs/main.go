package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/oauth2"

	"google-drive-linux/internal/auth"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/drivecontrol"
	"google-drive-linux/internal/fusefs"
	"google-drive-linux/internal/ipc"
	"google-drive-linux/internal/secretstore"
	"google-drive-linux/internal/store"
	"google-drive-linux/internal/syncservice"
)

const version = "0.0.0-dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	ipcServer := flag.Bool("ipc-server", false, "run as the per-user drivefs service")
	socket := flag.String("socket", "", "Unix socket path for the per-user service")
	database := flag.String("database", "", "SQLite metadata database for the indexed Drive namespace")
	source := flag.String("source", "", "local source directory for the loopback FUSE spike")
	mountpoint := flag.String("mountpoint", "", "existing empty directory to mount on")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *ipcServer {
		if *database != "" || *source != "" || *mountpoint != "" {
			fmt.Fprintln(os.Stderr, "--ipc-server cannot be combined with direct mount flags")
			os.Exit(2)
		}
		if err := runIPCServer(*socket); err != nil {
			if errors.Is(err, ipc.ErrAlreadyRunning) {
				return
			}
			log.Fatalf("drivefs service: %v", err)
		}
		return
	}

	if *mountpoint == "" || (*database == "" && *source == "") || (*database != "" && *source != "") {
		flag.Usage()
		os.Exit(2)
	}

	var server *fuse.Server
	var metadata *store.Store
	var err error
	if *database != "" {
		metadata, err = store.Open(*database)
		if err != nil {
			log.Fatalf("open metadata database: %v", err)
		}
		server, err = fusefs.MountIndexed(metadata, *mountpoint)
	} else {
		server, err = fusefs.MountLoopback(*source, *mountpoint)
	}
	if err != nil {
		if metadata != nil {
			_ = metadata.Close()
		}
		log.Fatalf("start FUSE mount: %v", err)
	}
	if metadata != nil {
		defer metadata.Close()
		log.Printf("read-only indexed Drive namespace mounted at %s", *mountpoint)
	} else {
		log.Printf("FUSE loopback spike mounted at %s; this is a local-only prototype", *mountpoint)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		if err := server.Unmount(); err != nil {
			log.Printf("unmount: %v", err)
		}
	}()
	server.Wait()
}

func runIPCServer(socketPath string) error {
	if socketPath == "" {
		var err error
		socketPath, err = ipc.DefaultSocketPath()
		if err != nil {
			return err
		}
	}
	database, err := drivecontrol.DefaultDatabasePath()
	if err != nil {
		return err
	}
	settingsPath, err := drivecontrol.DefaultSettingsPath()
	if err != nil {
		return err
	}
	settings, settingsErr := drivecontrol.LoadSettings(settingsPath, database)
	if settingsErr != nil {
		log.Printf("settings unavailable; using default cache limit: %v", settingsErr)
		settings.CacheLimitBytes = drivecontrol.DefaultCacheLimitBytes
	}
	runtimeManager := newSyncRuntimeManager(database, settings.CacheLimitBytes)
	controller := drivecontrol.New(func(metadata *store.Store, mountpoint string) (drivecontrol.MountHandle, error) {
		var server *fuse.Server
		var mountErr error
		activeRuntime := runtimeManager.current()
		if activeRuntime != nil {
			server, mountErr = fusefs.MountIndexedWithWriter(metadata, activeRuntime.Cache(), activeRuntime.ContentStore(), mountpoint)
		} else {
			server, mountErr = fusefs.MountIndexedWithReader(metadata, nil, mountpoint)
		}
		if mountErr != nil {
			return drivecontrol.MountHandle{}, mountErr
		}
		done := make(chan struct{})
		go func() {
			server.Wait()
			close(done)
		}()
		return drivecontrol.MountHandle{Unmount: server.Unmount, Done: done}, nil
	})
	controller.SetPaths(database, settingsPath)
	controller.SetSyncHooks(runtimeManager.status, runtimeManager.notifyPinsChanged, runtimeManager.setCacheLimit)
	clientID := os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	if clientID != "" {
		secrets, err := secretstore.NewSystemStore("google-drive-linux")
		if err != nil {
			log.Printf("OAuth credential store unavailable: %v", err)
			controller.SetAccountActions(&drivecontrol.AccountActions{UnavailableMessage: "The system Secret Service is unavailable"})
		} else if oauth, err := auth.NewService(auth.Config{ClientID: clientID, Secrets: secrets}); err != nil {
			log.Printf("OAuth configuration unavailable: %v", err)
			controller.SetAccountActions(&drivecontrol.AccountActions{UnavailableMessage: "Google OAuth configuration is invalid"})
		} else {
			runtimeManager.oauth = oauth
			controller.SetAccountActions(&drivecontrol.AccountActions{
				IsAuthenticated: oauth.IsAuthenticated,
				SignIn: func(ctx context.Context) error {
					_, err := oauth.Authorize(ctx)
					if err != nil {
						return err
					}
					return runtimeManager.start(ctx)
				},
				SignOut: func(ctx context.Context) error {
					if err := runtimeManager.stop(); err != nil {
						return err
					}
					if err := oauth.Revoke(ctx); err != nil {
						return errors.Join(err, runtimeManager.start(ctx))
					}
					return nil
				},
			})
			if authenticated, authErr := oauth.IsAuthenticated(context.Background()); authErr != nil {
				log.Printf("stored Google credential unavailable: %v", authErr)
			} else if authenticated {
				if err := runtimeManager.start(context.Background()); err != nil {
					log.Printf("start Drive synchronization: %v", err)
				}
			}
		}
	} else {
		controller.SetAccountActions(nil)
	}
	server, err := ipc.Listen(filepath.Clean(socketPath), controller)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("drivefs IPC service listening on %s", socketPath)
	serveErr := server.Serve(ctx)
	controllerErr := controller.Stop()
	runtimeErr := runtimeManager.stop()
	if controllerErr != nil || runtimeErr != nil {
		return errors.Join(serveErr, wrap("stop drivefs controller", controllerErr), wrap("stop Drive synchronization", runtimeErr))
	}
	return serveErr
}

type syncRuntimeManager struct {
	mu            sync.RWMutex
	database      string
	cacheLimit    int64
	oauth         *auth.Service
	runtime       *syncservice.Runtime
	runtimeCancel context.CancelFunc
	lastError     string
}

func newSyncRuntimeManager(database string, cacheLimit int64) *syncRuntimeManager {
	return &syncRuntimeManager{database: database, cacheLimit: cacheLimit}
}

func (m *syncRuntimeManager) start(ctx context.Context) error {
	m.mu.RLock()
	if m.runtime != nil {
		m.mu.RUnlock()
		return nil
	}
	oauth, database, cacheLimit := m.oauth, m.database, m.cacheLimit
	m.mu.RUnlock()
	if oauth == nil {
		return m.setError(errors.New("Google OAuth is not configured"))
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	tokenSource, err := oauth.TokenSource(runtimeCtx)
	if err != nil {
		cancel()
		return m.setError(fmt.Errorf("load Google authorization: %w", err))
	}
	client, err := driveapi.NewClient(driveapi.Config{HTTPClient: oauth2.NewClient(runtimeCtx, tokenSource)})
	if err != nil {
		cancel()
		return m.setError(fmt.Errorf("configure Google Drive API: %w", err))
	}
	runtime, err := syncservice.New(syncservice.Config{
		Database: database, ContentRoot: filepath.Join(filepath.Dir(database), "content"),
		CacheLimit: cacheLimit, Drive: client,
	})
	if err != nil {
		cancel()
		return m.setError(err)
	}
	m.mu.Lock()
	if m.runtime == nil {
		m.runtime, m.runtimeCancel = runtime, cancel
		m.lastError = ""
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	cancel()
	return runtime.Close()
}

func (m *syncRuntimeManager) stop() error {
	m.mu.Lock()
	runtime := m.runtime
	cancel := m.runtimeCancel
	m.runtime, m.runtimeCancel = nil, nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if runtime != nil {
		err := runtime.Close()
		if err != nil {
			return m.setError(err)
		}
	}
	return nil
}

func (m *syncRuntimeManager) current() *syncservice.Runtime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtime
}

func (m *syncRuntimeManager) status() drivecontrol.SyncStatus {
	runtime := m.current()
	if runtime == nil {
		m.mu.RLock()
		lastError := m.lastError
		m.mu.RUnlock()
		return drivecontrol.SyncStatus{NetworkState: "not-connected", NetworkError: lastError, PinState: "idle"}
	}
	status := runtime.Status()
	return drivecontrol.SyncStatus{
		NetworkState: status.NetworkState, NetworkError: status.NetworkError,
		IndexState: status.IndexState, IndexPhase: status.IndexPhase, SyncError: status.SyncError,
		PinState: status.PinState, PinFilesDone: status.PinFilesDone, PinFilesTotal: status.PinFilesTotal,
		PinCurrentName: status.PinCurrentName, PinCurrentState: status.PinCurrentState,
		PinBytesDone: status.PinBytesDone, PinBytesTotal: status.PinBytesTotal, PinError: status.PinError,
	}
}

func (m *syncRuntimeManager) notifyPinsChanged() {
	if runtime := m.current(); runtime != nil {
		runtime.NotifyPinsChanged()
	}
}

func (m *syncRuntimeManager) setCacheLimit(limit int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runtime != nil {
		if err := m.runtime.SetCacheLimit(limit); err != nil {
			return err
		}
	}
	m.cacheLimit = limit
	return nil
}

func (m *syncRuntimeManager) setError(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.lastError = err.Error()
	}
	return err
}

func wrap(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}
