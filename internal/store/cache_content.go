package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google-drive-linux/internal/domain"
)

type StagedContent struct {
	path   string
	Size   int64
	SHA256 string
}

func (s *ContentStore) StageDownload(ctx context.Context, source io.Reader, expectedSize int64, expectedMD5 string) (*StagedContent, error) {
	return s.StageDownloadProgress(ctx, source, expectedSize, expectedMD5, nil)
}

func (s *ContentStore) StageDownloadProgress(ctx context.Context, source io.Reader, expectedSize int64, expectedMD5 string,
	progress func(downloaded int64)) (*StagedContent, error) {
	if ctx == nil || source == nil {
		return nil, errors.New("context and download source are required")
	}
	if expectedSize < 0 || expectedSize >= int64(^uint64(0)>>1) {
		return nil, errors.New("download size is invalid")
	}
	reservation, err := s.reserveDiskSpace(expectedSize)
	if err != nil {
		return nil, err
	}
	defer reservation.release()
	token, err := randomContentToken()
	if err != nil {
		return nil, fmt.Errorf("create download staging ID: %w", err)
	}
	path := filepath.Join(s.paths.Transient, ".download-"+token+".tmp")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create download staging file: %w", err)
	}
	sha := sha256.New()
	md5Digest := md5.New()
	writer := &reservedWriter{file: file, reservation: reservation}
	var destination io.Writer = writer
	if progress != nil {
		destination = &progressWriter{writer: writer, progress: progress}
	}
	limitReader := io.LimitReader(contextReader{ctx: ctx, source: source}, expectedSize)
	size, copyErr := io.CopyBuffer(io.MultiWriter(destination, sha, md5Digest), limitReader, make([]byte, contentCopyBufferSize))
	if copyErr == nil && size != expectedSize {
		copyErr = fmt.Errorf("downloaded %d bytes, expected %d", size, expectedSize)
	}
	if copyErr == nil {
		var extra [1]byte
		extraSize, extraErr := contextReader{ctx: ctx, source: source}.Read(extra[:])
		if extraSize > 0 {
			copyErr = fmt.Errorf("download exceeds expected size of %d bytes", expectedSize)
		} else if extraErr != nil && !errors.Is(extraErr, io.EOF) {
			copyErr = fmt.Errorf("verify downloaded content length: %w", extraErr)
		}
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if copyErr == nil && syncErr != nil {
		copyErr = fmt.Errorf("sync downloaded content: %w", syncErr)
	}
	if copyErr == nil && closeErr != nil {
		copyErr = fmt.Errorf("close downloaded content: %w", closeErr)
	}
	if copyErr == nil && expectedMD5 != "" && !strings.EqualFold(hex.EncodeToString(md5Digest.Sum(nil)), expectedMD5) {
		copyErr = errors.New("downloaded content does not match Drive MD5 checksum")
	}
	if copyErr == nil {
		copyErr = syncDirectory(s.paths.Transient)
	}
	if copyErr != nil {
		_ = os.Remove(path)
		_ = syncDirectory(s.paths.Transient)
		return nil, copyErr
	}
	return &StagedContent{path: path, Size: size, SHA256: hex.EncodeToString(sha.Sum(nil))}, nil
}

func (s *ContentStore) OpenStagedContent(staged *StagedContent) (*os.File, error) {
	if staged == nil || staged.path == "" || filepath.Dir(staged.path) != s.paths.Transient {
		return nil, errors.New("invalid staged content")
	}
	return os.Open(staged.path)
}

func (s *ContentStore) DiscardStagedContent(staged *StagedContent) error {
	if staged == nil || staged.path == "" || filepath.Dir(staged.path) != s.paths.Transient {
		return errors.New("invalid staged content")
	}
	removed, err := removeRegularFile(staged.path)
	if err != nil {
		return err
	}
	if removed {
		return syncDirectory(s.paths.Transient)
	}
	return nil
}

func (s *ContentStore) PublishCachedContent(ctx context.Context, objectID domain.ObjectID, driveVersion string, staged *StagedContent) (domain.ContentVersion, error) {
	if staged == nil || staged.path == "" || filepath.Dir(staged.path) != s.paths.Transient {
		return domain.ContentVersion{}, errors.New("invalid staged content")
	}
	token, err := randomContentToken()
	if err != nil {
		return domain.ContentVersion{}, fmt.Errorf("create cached content version ID: %w", err)
	}
	versionID := domain.ContentVersionID("cv-" + token)
	pinned, err := s.metadata.IsOfflinePinned(ctx, objectID)
	if err != nil {
		return domain.ContentVersion{}, err
	}
	localPath := s.relativeCachedBlobPath(versionID)
	finalPath := s.cachedBlobPath(versionID)
	cacheDirectory := s.paths.EvictableCache
	if pinned {
		localPath = s.relativePinnedBlobPath(versionID)
		finalPath = s.pinnedBlobPath(versionID)
		cacheDirectory = s.paths.Pinned
	}
	if err := os.Rename(staged.path, finalPath); err != nil {
		return domain.ContentVersion{}, fmt.Errorf("publish cached content file: %w", err)
	}
	if err := syncDirectory(s.paths.Transient); err != nil {
		return domain.ContentVersion{}, fmt.Errorf("sync download staging directory: %w", err)
	}
	if err := syncDirectory(cacheDirectory); err != nil {
		return domain.ContentVersion{}, fmt.Errorf("sync cached content directory: %w", err)
	}
	version := domain.ContentVersion{
		ID: versionID, ObjectID: objectID, Kind: domain.ContentCached,
		LocalPath: localPath, Size: staged.Size,
		SHA256: staged.SHA256, DriveVersion: driveVersion,
		Complete: true, CreatedAt: time.Now().UTC(),
	}
	if err := s.metadata.RegisterCachedContentVersion(ctx, version); err != nil {
		_ = os.Remove(finalPath)
		_ = syncDirectory(cacheDirectory)
		return domain.ContentVersion{}, err
	}
	return version, nil
}

func (s *ContentStore) VerifyContentVersion(ctx context.Context, versionID domain.ContentVersionID) error {
	file, version, err := s.OpenContentVersion(ctx, versionID)
	if err != nil {
		return err
	}
	digest := sha256.New()
	size, readErr := copyWithContext(ctx, digest, io.NewSectionReader(file, 0, version.Size))
	closeErr := file.Close()
	if readErr != nil {
		return fmt.Errorf("verify cached content: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close verified cached content: %w", closeErr)
	}
	if size != version.Size || hex.EncodeToString(digest.Sum(nil)) != version.SHA256 {
		return errors.New("cached content is incomplete or corrupt")
	}
	return nil
}

func (s *ContentStore) EvictCachedContent(ctx context.Context, version domain.ContentVersion) error {
	if version.Kind != domain.ContentCached || !s.isCachedBlobPath(version.ID, version.LocalPath) {
		return errors.New("refusing to evict a non-cache content version")
	}
	localPath, err := s.metadata.DeleteEvictableCachedContent(ctx, version.ID)
	if err != nil {
		return err
	}
	if !s.isCachedBlobPath(version.ID, localPath) {
		return errors.New("cached content path changed during eviction")
	}
	if err := os.Remove(filepath.Join(s.paths.Root, filepath.FromSlash(localPath))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove evicted cache file: %w", err)
	}
	if err := syncDirectory(filepath.Dir(filepath.Join(s.paths.Root, filepath.FromSlash(localPath)))); err != nil {
		return fmt.Errorf("sync cache eviction: %w", err)
	}
	return nil
}

func (s *ContentStore) DiscardCorruptCachedContent(ctx context.Context, version domain.ContentVersion) error {
	if version.Kind != domain.ContentCached || !s.isCachedBlobPath(version.ID, version.LocalPath) {
		return errors.New("refusing to discard a non-cache content version")
	}
	localPath, err := s.metadata.DeleteCachedContentVersion(ctx, version.ID)
	if err != nil {
		return err
	}
	if !s.isCachedBlobPath(version.ID, localPath) {
		return errors.New("corrupt cached content path changed during cleanup")
	}
	if err := os.Remove(filepath.Join(s.paths.Root, filepath.FromSlash(localPath))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove corrupt cache file: %w", err)
	}
	if err := syncDirectory(filepath.Dir(filepath.Join(s.paths.Root, filepath.FromSlash(localPath)))); err != nil {
		return fmt.Errorf("sync corrupt cache cleanup: %w", err)
	}
	return nil
}
