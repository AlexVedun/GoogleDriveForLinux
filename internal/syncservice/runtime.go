package syncservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google-drive-linux/internal/auth"
	"google-drive-linux/internal/contentcache"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/indexer"
	"google-drive-linux/internal/store"
	"google-drive-linux/internal/syncworker"
)

const (
	defaultPollInterval = 30 * time.Second
	defaultWorkInterval = 2 * time.Second
	defaultPinInterval  = time.Minute
)

type Config struct {
	Database     string
	ContentRoot  string
	CacheLimit   int64
	Drive        driveapi.API
	PollInterval time.Duration
	WorkInterval time.Duration
	PinInterval  time.Duration
}

type Status struct {
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

// Runtime owns the persistent metadata connection and the synchronization
// components used by reads, durable writes, uploads, and offline pins.
type Runtime struct {
	metadata *store.Store
	content  *store.ContentStore
	cache    *contentcache.Cache
	poller   *indexer.Poller
	worker   *syncworker.Worker
	pins     *contentcache.PinManager

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.RWMutex
	status Status

	pinsChanged chan struct{}
	pollNow     chan struct{}
	workNow     chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func New(config Config) (*Runtime, error) {
	if config.Database == "" || config.ContentRoot == "" || config.Drive == nil {
		return nil, errors.New("database, content root, and Drive API are required")
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.WorkInterval <= 0 {
		config.WorkInterval = defaultWorkInterval
	}
	if config.PinInterval <= 0 {
		config.PinInterval = defaultPinInterval
	}
	metadata, err := store.Open(config.Database)
	if err != nil {
		return nil, fmt.Errorf("open sync metadata database: %w", err)
	}
	closeOnError := func(cause error) (*Runtime, error) {
		return nil, errors.Join(cause, metadata.Close())
	}
	content, err := store.NewContentStore(metadata, config.ContentRoot)
	if err != nil {
		return closeOnError(fmt.Errorf("open durable content store: %w", err))
	}
	if _, err := content.Recover(context.Background()); err != nil {
		return closeOnError(fmt.Errorf("recover durable content: %w", err))
	}
	cache, err := contentcache.New(config.Drive, metadata, content, config.CacheLimit)
	if err != nil {
		return closeOnError(fmt.Errorf("initialize content cache: %w", err))
	}
	poller, err := indexer.NewPoller(config.Drive, metadata)
	if err != nil {
		return closeOnError(fmt.Errorf("initialize Drive indexer: %w", err))
	}
	worker, err := syncworker.New(metadata, content, config.Drive)
	if err != nil {
		return closeOnError(fmt.Errorf("initialize upload worker: %w", err))
	}
	pins, err := contentcache.NewPinManager(metadata, cache)
	if err != nil {
		return closeOnError(fmt.Errorf("initialize offline pin manager: %w", err))
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &Runtime{
		metadata: metadata, content: content, cache: cache, poller: poller,
		worker: worker, pins: pins, ctx: ctx, cancel: cancel,
		status:      Status{NetworkState: "connecting", IndexState: "not-started", PinState: "idle"},
		pinsChanged: make(chan struct{}, 1),
		pollNow:     make(chan struct{}, 1),
		workNow:     make(chan struct{}, 1),
	}
	// Start all loops only after recovery and every component have initialized.
	runtime.wg.Add(3)
	go runtime.pollLoop(config.PollInterval)
	go runtime.workLoop(config.WorkInterval)
	go runtime.pinLoop(config.PinInterval)
	return runtime, nil
}

func (r *Runtime) Cache() *contentcache.Cache { return r.cache }

func (r *Runtime) ContentStore() *store.ContentStore { return r.content }

func (r *Runtime) SetCacheLimit(limit int64) error { return r.cache.SetMaxBytes(limit) }

func (r *Runtime) NotifyPinsChanged() {
	select {
	case r.pinsChanged <- struct{}{}:
	default:
	}
}

func (r *Runtime) NotifyAuthChanged() {
	_ = r.metadata.RetryAuthorizationErrors(r.ctx)
	select {
	case r.pollNow <- struct{}{}:
	default:
	}
	select {
	case r.workNow <- struct{}{}:
	default:
	}
}

func (r *Runtime) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

func (r *Runtime) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
		r.closeErr = r.metadata.Close()
	})
	return r.closeErr
}

func (r *Runtime) pollLoop(interval time.Duration) {
	defer r.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		err := r.poller.Poll(r.ctx)
		index, indexErr := r.metadata.InitialIndexStatus(r.ctx)
		r.mu.Lock()
		if indexErr == nil {
			r.status.IndexState, r.status.IndexPhase = index.State, index.Phase
		}
		if err != nil {
			r.status.NetworkState = networkState(err)
			r.status.NetworkError = err.Error()
		} else {
			r.status.NetworkState, r.status.NetworkError = "online", ""
		}
		r.mu.Unlock()
		if indexErr == nil && index.State == "ready" {
			r.NotifyPinsChanged()
		}
		select {
		case <-r.ctx.Done():
			return
		case <-r.pollNow:
		case <-ticker.C:
		}
	}
}

func (r *Runtime) workLoop(interval time.Duration) {
	defer r.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		index, indexErr := r.metadata.InitialIndexStatus(r.ctx)
		var err error
		if indexErr != nil {
			err = indexErr
		} else if index.State == "ready" {
			_, err = r.worker.RunUntilIdle(r.ctx, 64)
		}
		r.mu.Lock()
		if err != nil {
			r.status.SyncError = err.Error()
		} else {
			r.status.SyncError = ""
		}
		r.mu.Unlock()
		select {
		case <-r.ctx.Done():
			return
		case <-r.workNow:
		case <-ticker.C:
		}
	}
}

func (r *Runtime) pinLoop(interval time.Duration) {
	defer r.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		index, indexErr := r.metadata.InitialIndexStatus(r.ctx)
		var latest contentcache.PinProgress
		var err error
		if indexErr != nil {
			err = indexErr
		} else if index.State == "ready" {
			runCtx, cancel := context.WithCancel(r.ctx)
			err = r.pins.Sync(runCtx, func(progress contentcache.PinProgress) {
				latest = progress
				r.mu.Lock()
				r.status.PinState = progress.State
				r.status.PinCurrentName, r.status.PinCurrentState = progress.Name, progress.State
				r.status.PinFilesDone, r.status.PinFilesTotal = progress.CompletedFiles, progress.TotalFiles
				r.status.PinBytesDone, r.status.PinBytesTotal = progress.CompletedBytes, progress.TotalBytes
				if progress.Err != nil {
					r.status.PinError = progress.Err.Error()
				} else {
					r.status.PinError = ""
				}
				r.mu.Unlock()
			})
			cancel()
		} else {
			r.mu.Lock()
			r.status.PinState = "waiting-for-index"
			r.mu.Unlock()
		}
		if err != nil && r.ctx.Err() == nil {
			r.mu.Lock()
			r.status.PinState = latest.State
			r.status.PinError = err.Error()
			r.mu.Unlock()
		}
		select {
		case <-r.ctx.Done():
			return
		case <-r.pinsChanged:
		case <-ticker.C:
		}
	}
}

func networkState(err error) string {
	switch {
	case errors.Is(err, auth.ErrReauthorizationRequired):
		return "reauthorization-required"
	case errors.Is(err, driveapi.ErrUnauthenticated):
		return "reauthorization-required"
	case errors.Is(err, driveapi.ErrForbidden):
		return "access-denied"
	case errors.Is(err, driveapi.ErrQuotaExceeded):
		return "quota-exceeded"
	case errors.Is(err, driveapi.ErrRateLimited):
		return "rate-limited"
	case errors.Is(err, driveapi.ErrTemporary):
		return "temporarily-unavailable"
	default:
		return "offline-or-error"
	}
}
