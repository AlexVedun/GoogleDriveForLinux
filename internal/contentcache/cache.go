package contentcache

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

const DefaultMaxBytes int64 = 1 << 30

var (
	ErrStaleRemoteVersion = errors.New("Drive version changed while opening the file")
	ErrNotDownloadable    = errors.New("Drive does not allow downloading this file")
	ErrUnsupportedType    = errors.New("Google Workspace documents cannot be downloaded as file content")
	ErrCorruptCache       = errors.New("cached content is incomplete or corrupt")
)

type Cache struct {
	drive    driveapi.API
	metadata *store.Store
	content  *store.ContentStore
	maxBytes int64

	mu       sync.Mutex
	refs     map[domain.ContentVersionID]int
	lastUsed map[domain.ContentVersionID]time.Time
	objectMu [64]sync.Mutex
}

type Handle struct {
	file      *os.File
	size      int64
	cache     *Cache
	versionID domain.ContentVersionID
	staged    *store.StagedContent
	content   *store.ContentStore
	closeOnce sync.Once
	closeErr  error
}

func New(drive driveapi.API, metadata *store.Store, content *store.ContentStore, maxBytes int64) (*Cache, error) {
	if drive == nil || metadata == nil || content == nil {
		return nil, errors.New("Drive API, metadata store, and content store are required")
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxBytes < 0 {
		return nil, errors.New("cache size limit must not be negative")
	}
	return &Cache{drive: drive, metadata: metadata, content: content, maxBytes: maxBytes,
		refs: make(map[domain.ContentVersionID]int), lastUsed: make(map[domain.ContentVersionID]time.Time)}, nil
}

func (c *Cache) Open(ctx context.Context, object domain.Object) (*Handle, error) {
	return c.open(ctx, object, nil)
}

// Prefetch downloads and verifies a pinned object while reporting bytes written
// during this file. The returned handle is closed before this method returns.
func (c *Cache) Prefetch(ctx context.Context, object domain.Object, progress func(downloaded int64)) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	pinned, err := c.metadata.IsOfflinePinned(ctx, object.ID)
	if err != nil {
		return err
	}
	if !pinned {
		return errors.New("object is not covered by an offline pin")
	}
	handle, err := c.open(ctx, object, progress)
	if err != nil {
		return err
	}
	return handle.Close()
}

func (c *Cache) open(ctx context.Context, object domain.Object, progress func(downloaded int64)) (*Handle, error) {
	if ctx == nil {
		return nil, errors.New("context must not be nil")
	}
	if object.Deleted {
		return nil, os.ErrNotExist
	}
	if object.MIMEType == "application/vnd.google-apps.folder" {
		return nil, syscall.EISDIR
	}
	if strings.HasPrefix(object.MIMEType, "application/vnd.google-apps.") {
		return nil, ErrUnsupportedType
	}
	lock := c.objectLock(object.ID)
	lock.Lock()
	defer lock.Unlock()

	if current, err := c.content.CurrentContentVersion(ctx, object.ID); err == nil && current.Kind == domain.ContentPending && current.Complete {
		file, version, err := c.content.OpenContentVersion(ctx, current.ID)
		if err != nil {
			return nil, fmt.Errorf("open locally saved content: %w", err)
		}
		return &Handle{file: file, size: version.Size}, nil
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("check local content: %w", err)
	}

	if object.DriveVersion == "" || object.DriveFileID == nil {
		return nil, errors.New("Drive file identity or indexed version is unavailable")
	}
	if version, err := c.metadata.FindCachedContentVersion(ctx, object.ID, object.DriveVersion); err == nil {
		handle, openErr := c.openCached(ctx, version)
		if openErr == nil {
			return handle, nil
		}
		if !errors.Is(openErr, ErrCorruptCache) {
			return nil, openErr
		}
		_ = c.content.DiscardCorruptCachedContent(ctx, version)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("find cached Drive version: %w", err)
	}

	fileID := string(*object.DriveFileID)
	before, err := c.drive.GetFile(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("check Drive file before download: %w", err)
	}
	if err := validateRemoteFile(object, before); err != nil {
		return nil, err
	}
	download, err := c.drive.DownloadFile(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("download Drive file: %w", err)
	}
	if download.Body == nil {
		return nil, errors.New("Drive returned an empty download body")
	}
	if download.ContentLength >= 0 && download.ContentLength != before.Size {
		_ = download.Body.Close()
		return nil, fmt.Errorf("Drive download length is %d bytes, expected %d", download.ContentLength, before.Size)
	}
	staged, stageErr := c.content.StageDownloadProgress(ctx, download.Body, before.Size, before.MD5Checksum, progress)
	closeErr := download.Body.Close()
	if stageErr != nil {
		return nil, fmt.Errorf("stage Drive file download: %w", stageErr)
	}
	if closeErr != nil {
		_ = c.content.DiscardStagedContent(staged)
		return nil, fmt.Errorf("close Drive download: %w", closeErr)
	}
	after, err := c.drive.GetFile(ctx, fileID)
	if err != nil {
		_ = c.content.DiscardStagedContent(staged)
		return nil, fmt.Errorf("verify Drive file version after download: %w", err)
	}
	if !sameRemoteVersion(before, after) || after.Version != object.DriveVersion || after.Size != object.Size {
		_ = c.content.DiscardStagedContent(staged)
		return nil, ErrStaleRemoteVersion
	}

	pinned, err := c.metadata.IsOfflinePinned(ctx, object.ID)
	if err != nil {
		_ = c.content.DiscardStagedContent(staged)
		return nil, fmt.Errorf("check offline pin before publishing download: %w", err)
	}
	if pinned || staged.Size <= c.maxBytes {
		if handle, err := c.publishWithinLimit(ctx, object, staged); err == nil {
			return handle, nil
		} else if !errors.Is(err, errCacheLimitUnavailable) {
			_ = c.content.DiscardStagedContent(staged)
			return nil, err
		}
	}
	file, err := c.content.OpenStagedContent(staged)
	if err != nil {
		_ = c.content.DiscardStagedContent(staged)
		return nil, fmt.Errorf("open completed temporary download: %w", err)
	}
	return &Handle{file: file, size: staged.Size, content: c.content, staged: staged}, nil
}

var errCacheLimitUnavailable = errors.New("cache entry cannot fit without evicting protected content")

func (c *Cache) publishWithinLimit(ctx context.Context, object domain.Object, staged *store.StagedContent) (*Handle, error) {
	c.mu.Lock()
	version, err := func() (domain.ContentVersion, error) {
		defer c.mu.Unlock()
		pinned, err := c.metadata.IsOfflinePinned(ctx, object.ID)
		if err != nil {
			return domain.ContentVersion{}, err
		}
		used, err := c.metadata.CachedContentBytes(ctx)
		if err != nil {
			return domain.ContentVersion{}, err
		}
		if !pinned && !fits(used, staged.Size, c.maxBytes) {
			candidates, err := c.metadata.EvictableCachedVersions(ctx)
			if err != nil {
				return domain.ContentVersion{}, err
			}
			sort.SliceStable(candidates, func(i, j int) bool {
				iTime, iOK := c.lastUsed[candidates[i].ID]
				jTime, jOK := c.lastUsed[candidates[j].ID]
				if iOK && jOK {
					return iTime.Before(jTime)
				}
				if iOK != jOK {
					return !iOK
				}
				return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
			})
			for _, candidate := range candidates {
				if fits(used, staged.Size, c.maxBytes) {
					break
				}
				if c.refs[candidate.ID] != 0 {
					continue
				}
				if err := c.content.EvictCachedContent(ctx, candidate); err != nil {
					if errors.Is(err, store.ErrContentNotEvictable) {
						continue
					}
					return domain.ContentVersion{}, err
				}
				used -= candidate.Size
				delete(c.lastUsed, candidate.ID)
			}
		}
		if !pinned && !fits(used, staged.Size, c.maxBytes) {
			return domain.ContentVersion{}, errCacheLimitUnavailable
		}
		version, err := c.content.PublishCachedContent(ctx, object.ID, object.DriveVersion, staged)
		if err != nil {
			return domain.ContentVersion{}, err
		}
		c.refs[version.ID]++
		c.lastUsed[version.ID] = time.Now()
		return version, nil
	}()
	if err != nil {
		return nil, err
	}
	return c.openReferenced(ctx, version)
}

func fits(used, incoming, limit int64) bool {
	return used >= 0 && incoming >= 0 && incoming <= limit && used <= limit-incoming
}

func (c *Cache) openCached(ctx context.Context, version domain.ContentVersion) (*Handle, error) {
	c.mu.Lock()
	c.refs[version.ID]++
	c.mu.Unlock()
	return c.openReferenced(ctx, version)
}

func (c *Cache) openReferenced(ctx context.Context, version domain.ContentVersion) (*Handle, error) {
	file, openedVersion, err := c.content.OpenContentVersion(ctx, version.ID)
	if err != nil {
		c.release(version.ID)
		return nil, err
	}
	if err := verifyOpenFile(ctx, file, openedVersion.Size, openedVersion.SHA256); err != nil {
		_ = file.Close()
		c.release(version.ID)
		if errors.Is(err, ErrCorruptCache) {
			return nil, ErrCorruptCache
		}
		return nil, fmt.Errorf("verify cached content: %w", err)
	}
	c.mu.Lock()
	c.lastUsed[version.ID] = time.Now()
	c.mu.Unlock()
	return &Handle{file: file, size: openedVersion.Size, cache: c, versionID: version.ID}, nil
}

func (c *Cache) release(versionID domain.ContentVersionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refs[versionID] <= 1 {
		delete(c.refs, versionID)
		return
	}
	c.refs[versionID]--
}

func (c *Cache) objectLock(objectID domain.ObjectID) *sync.Mutex {
	var hash uint64 = 1469598103934665603
	for index := range len(objectID) {
		hash ^= uint64(objectID[index])
		hash *= 1099511628211
	}
	return &c.objectMu[hash%uint64(len(c.objectMu))]
}

func validateRemoteFile(object domain.Object, remote driveapi.File) error {
	if remote.ID == "" || object.DriveFileID == nil || remote.ID != string(*object.DriveFileID) || remote.Trashed {
		return ErrStaleRemoteVersion
	}
	if !remote.Capabilities.CanDownload {
		return ErrNotDownloadable
	}
	if remote.Version != object.DriveVersion || remote.Size != object.Size || remote.MIMEType != object.MIMEType {
		return ErrStaleRemoteVersion
	}
	return nil
}

func sameRemoteVersion(first, second driveapi.File) bool {
	return first.ID == second.ID && first.Version == second.Version && first.Size == second.Size &&
		first.MIMEType == second.MIMEType && first.MD5Checksum == second.MD5Checksum && !second.Trashed && second.Capabilities.CanDownload
}

func verifyOpenFile(ctx context.Context, file *os.File, expectedSize int64, expectedSHA256 string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != expectedSize || len(expectedSHA256) != sha256.Size*2 {
		return ErrCorruptCache
	}
	digest := sha256.New()
	if _, err := io.CopyBuffer(digest, contextualReader{ctx: ctx, source: io.NewSectionReader(file, 0, expectedSize)}, make([]byte, 128*1024)); err != nil {
		return err
	}
	if hex.EncodeToString(digest.Sum(nil)) != expectedSHA256 {
		return ErrCorruptCache
	}
	return nil
}

type contextualReader struct {
	ctx    context.Context
	source io.Reader
}

func (r contextualReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(buffer)
}

func (h *Handle) ReadAt(buffer []byte, offset int64) (int, error) {
	if h == nil || h.file == nil {
		return 0, os.ErrClosed
	}
	if offset < 0 {
		return 0, errors.New("negative file offset")
	}
	if offset >= h.size {
		return 0, io.EOF
	}
	read, err := h.file.ReadAt(buffer, offset)
	if err == io.EOF && read > 0 {
		return read, nil
	}
	return read, err
}

func (h *Handle) Size() int64 {
	if h == nil {
		return 0
	}
	return h.size
}

func (h *Handle) Close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		if h.file != nil {
			h.closeErr = h.file.Close()
		}
		if h.cache != nil {
			h.cache.release(h.versionID)
		}
		if h.staged != nil && h.content != nil {
			h.closeErr = errors.Join(h.closeErr, h.content.DiscardStagedContent(h.staged))
		}
	})
	return h.closeErr
}
