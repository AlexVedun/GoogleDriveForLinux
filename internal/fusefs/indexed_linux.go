//go:build linux

package fusefs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"

	"google-drive-linux/internal/contentcache"
	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

const googleFolderMIMEType = "application/vnd.google-apps.folder"

var ErrIndexNotReady = errors.New("Drive index is not ready")

// IndexedNode exposes the persisted My Drive namespace and locally durable file contents.
type IndexedNode struct {
	fs.Inode
	metadata *store.Store
	reader   *contentcache.Cache
	writer   *writeManager
	objectID domain.ObjectID
}

type indexedFileHandle struct {
	content *contentcache.Handle
}

var (
	_ fs.InodeEmbedder = (*IndexedNode)(nil)
	_ fs.NodeLookuper  = (*IndexedNode)(nil)
	_ fs.NodeReaddirer = (*IndexedNode)(nil)
	_ fs.NodeGetattrer = (*IndexedNode)(nil)
	_ fs.NodeOpener    = (*IndexedNode)(nil)
	_ fs.NodeReader    = (*IndexedNode)(nil)
	_ fs.NodeReleaser  = (*IndexedNode)(nil)
	_ fs.NodeWriter    = (*IndexedNode)(nil)
	_ fs.NodeFsyncer   = (*IndexedNode)(nil)
	_ fs.NodeFlusher   = (*IndexedNode)(nil)
	_ fs.NodeSetattrer = (*IndexedNode)(nil)
	_ fs.NodeCreater   = (*IndexedNode)(nil)
	_ fs.NodeMkdirer   = (*IndexedNode)(nil)
	_ fs.NodeMknoder   = (*IndexedNode)(nil)
	_ fs.NodeLinker    = (*IndexedNode)(nil)
	_ fs.NodeSymlinker = (*IndexedNode)(nil)
	_ fs.NodeUnlinker  = (*IndexedNode)(nil)
	_ fs.NodeRmdirer   = (*IndexedNode)(nil)
	_ fs.NodeRenamer   = (*IndexedNode)(nil)
)

func NewIndexedRoot(metadata *store.Store) (*IndexedNode, error) {
	return NewIndexedRootWithReader(metadata, nil)
}

func NewIndexedRootWithReader(metadata *store.Store, reader *contentcache.Cache) (*IndexedNode, error) {
	return newIndexedRoot(metadata, reader, nil)
}

func NewIndexedRootWithWriter(metadata *store.Store, reader *contentcache.Cache, content *store.ContentStore) (*IndexedNode, error) {
	if content == nil {
		return nil, errors.New("content store must not be nil for writable FUSE")
	}
	return newIndexedRoot(metadata, reader, newWriteManager(metadata, reader, content))
}

func newIndexedRoot(metadata *store.Store, reader *contentcache.Cache, writer *writeManager) (*IndexedNode, error) {
	if metadata == nil {
		return nil, errors.New("metadata store must not be nil")
	}
	status, err := metadata.InitialIndexStatus(context.Background())
	if err != nil {
		return nil, fmt.Errorf("read Drive index status: %w", err)
	}
	if status.State != "ready" {
		return nil, fmt.Errorf("%w: current state is %q", ErrIndexNotReady, status.State)
	}
	root, err := metadata.ObjectByID(context.Background(), domain.RootObjectID)
	if err != nil {
		return nil, fmt.Errorf("read Drive namespace root: %w", err)
	}
	if root.Deleted || root.MIMEType != googleFolderMIMEType || root.Inode != store.RootInode {
		return nil, errors.New("metadata store has an invalid Drive namespace root")
	}
	return &IndexedNode{metadata: metadata, reader: reader, writer: writer, objectID: domain.RootObjectID}, nil
}

func MountIndexed(metadata *store.Store, mountpoint string) (*fuse.Server, error) {
	return MountIndexedWithReader(metadata, nil, mountpoint)
}

func MountIndexedWithReader(metadata *store.Store, reader *contentcache.Cache, mountpoint string) (*fuse.Server, error) {
	return mountIndexed(metadata, reader, nil, mountpoint)
}

func MountIndexedWithWriter(metadata *store.Store, reader *contentcache.Cache, content *store.ContentStore, mountpoint string) (*fuse.Server, error) {
	if content == nil {
		return nil, errors.New("content store must not be nil for writable FUSE")
	}
	return mountIndexed(metadata, reader, content, mountpoint)
}

func mountIndexed(metadata *store.Store, reader *contentcache.Cache, content *store.ContentStore, mountpoint string) (*fuse.Server, error) {
	resolvedMountpoint, err := ValidateMountpoint(mountpoint)
	if err != nil {
		return nil, err
	}
	var root *IndexedNode
	if content == nil {
		root, err = NewIndexedRootWithReader(metadata, reader)
	} else {
		root, err = NewIndexedRootWithWriter(metadata, reader, content)
	}
	if err != nil {
		return nil, err
	}
	zeroTimeout := time.Duration(0)
	options := &fs.Options{
		EntryTimeout:    &zeroTimeout,
		AttrTimeout:     &zeroTimeout,
		NegativeTimeout: &zeroTimeout,
		RootStableAttr:  &fs.StableAttr{Ino: uint64(store.RootInode)},
		UID:             uint32(os.Getuid()),
		GID:             uint32(os.Getgid()),
		MountOptions: fuse.MountOptions{
			FsName:        "google-drive-linux",
			Name:          "google-drive-linux",
			DisableXAttrs: true,
		},
	}
	if content == nil {
		options.MountOptions.Options = []string{"ro"}
	}
	server, err := fs.Mount(resolvedMountpoint, root, options)
	if err != nil {
		return nil, fmt.Errorf("mount Drive namespace at %q: %w", resolvedMountpoint, err)
	}
	return server, nil
}

func ValidateMountpoint(mountpoint string) (string, error) {
	resolvedMountpoint, err := resolveDirectory(mountpoint)
	if err != nil {
		return "", fmt.Errorf("mountpoint: %w", err)
	}
	entries, err := os.ReadDir(resolvedMountpoint)
	if err != nil {
		return "", fmt.Errorf("read mountpoint: %w", err)
	}
	if len(entries) != 0 {
		return "", errors.New("mountpoint must be empty")
	}
	return resolvedMountpoint, nil
}

func (n *IndexedNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	parent, errno := n.object(ctx)
	if errno != 0 {
		return nil, errno
	}
	if parent.MIMEType != googleFolderMIMEType {
		return nil, syscall.ENOTDIR
	}
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return nil, syscall.ENOENT
	}
	entry, err := n.metadata.DirectoryEntry(ctx, n.objectID, name)
	if err != nil {
		return nil, errnoForError(err)
	}
	child, err := n.metadata.ObjectByID(ctx, entry.ObjectID)
	if err != nil {
		return nil, errnoForError(err)
	}
	if child.Deleted {
		return nil, syscall.ENOENT
	}
	mode, errno := n.modeForObject(child)
	if errno != 0 {
		return nil, errno
	}
	fillAttr(&out.Attr, child, mode)
	return n.NewInode(ctx, &IndexedNode{metadata: n.metadata, reader: n.reader, writer: n.writer, objectID: child.ID}, fs.StableAttr{
		Mode: mode & syscall.S_IFMT,
		Ino:  uint64(child.Inode),
	}), 0
}

func (n *IndexedNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	object, errno := n.object(ctx)
	if errno != 0 {
		return nil, errno
	}
	if object.MIMEType != googleFolderMIMEType {
		return nil, syscall.ENOTDIR
	}
	children, err := n.metadata.ChildrenWithMetadata(ctx, n.objectID)
	if err != nil {
		return nil, errnoForError(err)
	}
	entries := make([]fuse.DirEntry, 0, len(children))
	for _, child := range children {
		mode := uint32(0)
		switch child.MIMEType {
		case googleFolderMIMEType:
			mode = fuse.S_IFDIR
		default:
			if !strings.HasPrefix(child.MIMEType, "application/vnd.google-apps.") {
				mode = fuse.S_IFREG
			}
		}
		entries = append(entries, fuse.DirEntry{
			Name: child.Entry.DisplayName,
			Ino:  uint64(child.Inode),
			Mode: mode,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *IndexedNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	object, errno := n.object(ctx)
	if errno != 0 {
		return errno
	}
	mode, errno := n.modeForObject(object)
	if errno != 0 {
		return errno
	}
	if file, ok := handle.(*writeHandle); ok {
		if size, err := n.writer.size(file); err == nil {
			object.Size = size
		} else {
			return ioErrno(err)
		}
	}
	fillAttr(&out.Attr, object, mode)
	return 0
}

func (n *IndexedNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	object, errno := n.object(ctx)
	if errno != 0 {
		return nil, 0, errno
	}
	if object.MIMEType == googleFolderMIMEType {
		return nil, 0, syscall.EISDIR
	}
	if _, errno := n.modeForObject(object); errno != 0 {
		return nil, 0, errno
	}
	if flags&uint32(os.O_WRONLY|os.O_RDWR) != 0 || flags&uint32(os.O_TRUNC) != 0 {
		if n.writer == nil {
			return nil, 0, syscall.EROFS
		}
		return n.writer.open(ctx, object, flags)
	}
	if n.writer != nil {
		if handle, ok := n.writer.openExisting(object, flags); ok {
			return handle, fuse.FOPEN_DIRECT_IO, 0
		}
	}
	if n.reader == nil {
		return nil, 0, syscall.EOPNOTSUPP
	}
	content, err := n.reader.Open(ctx, object)
	if err != nil {
		return nil, 0, contentErrno(err)
	}
	return &indexedFileHandle{content: content}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *IndexedNode) Read(_ context.Context, handle fs.FileHandle, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	if file, ok := handle.(*writeHandle); ok {
		if n.writer == nil {
			return nil, syscall.EBADF
		}
		read, err := n.writer.read(file, destination, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, ioErrno(err)
		}
		return fuse.ReadResultData(destination[:read]), 0
	}
	file, ok := handle.(*indexedFileHandle)
	if !ok || file.content == nil {
		return nil, syscall.EBADF
	}
	read, err := file.content.ReadAt(destination, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, contentErrno(err)
	}
	return fuse.ReadResultData(destination[:read]), 0
}

func (n *IndexedNode) Release(ctx context.Context, handle fs.FileHandle) syscall.Errno {
	if file, ok := handle.(*writeHandle); ok {
		if n.writer == nil {
			return syscall.EBADF
		}
		return n.writer.release(ctx, file)
	}
	file, ok := handle.(*indexedFileHandle)
	if !ok || file.content == nil {
		return syscall.EBADF
	}
	if err := file.content.Close(); err != nil {
		return syscall.EIO
	}
	return 0
}

func (n *IndexedNode) Write(_ context.Context, handle fs.FileHandle, data []byte, offset int64) (uint32, syscall.Errno) {
	if n.writer == nil {
		return 0, syscall.EROFS
	}
	file, ok := handle.(*writeHandle)
	if !ok {
		return 0, syscall.EBADF
	}
	written, err := n.writer.write(file, data, offset)
	if err != nil && written == 0 {
		return 0, ioErrno(err)
	}
	return uint32(written), 0
}

func (n *IndexedNode) Setattr(ctx context.Context, handle fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if in == nil {
		return syscall.EINVAL
	}
	allowed := uint32(fuse.FATTR_SIZE | fuse.FATTR_FH | fuse.FATTR_LOCKOWNER | fuse.FATTR_KILL_SUIDGID)
	if in.Valid&^allowed != 0 {
		return syscall.EOPNOTSUPP
	}
	if size, ok := in.GetSize(); ok {
		if errno := n.truncate(ctx, handle, size); errno != 0 {
			return errno
		}
	}
	return n.Getattr(ctx, handle, out)
}

func (n *IndexedNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return n.create(ctx, name, flags, mode, out)
}

func (n *IndexedNode) Mkdir(ctx context.Context, name string, _ uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.writer == nil {
		return nil, syscall.EROFS
	}
	if !validCreateName(name) {
		return nil, syscall.EINVAL
	}
	parent, errno := n.object(ctx)
	if errno != 0 {
		return nil, errno
	}
	if parent.MIMEType != googleFolderMIMEType {
		return nil, syscall.ENOTDIR
	}
	if !canAddChildren(parent) {
		return nil, syscall.EACCES
	}
	object, _, err := n.metadata.CreateLocalFolder(ctx, parent.ID, name)
	if err != nil {
		return nil, errnoForError(err)
	}
	mode, errno := n.modeForObject(object)
	if errno != 0 {
		return nil, errno
	}
	fillAttr(&out.Attr, object, mode)
	out.Mode = mode
	return n.NewInode(ctx, &IndexedNode{metadata: n.metadata, reader: n.reader, writer: n.writer, objectID: object.ID}, fs.StableAttr{
		Mode: mode & syscall.S_IFMT,
		Ino:  uint64(object.Inode),
	}), 0
}

func (n *IndexedNode) Mknod(context.Context, string, uint32, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}

func (n *IndexedNode) Link(context.Context, fs.InodeEmbedder, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}

func (n *IndexedNode) Symlink(context.Context, string, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}

func (n *IndexedNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.writer == nil {
		return syscall.EROFS
	}
	if !validCreateName(name) {
		return syscall.EINVAL
	}
	parent, errno := n.object(ctx)
	if errno != 0 {
		return errno
	}
	if parent.MIMEType != googleFolderMIMEType {
		return syscall.ENOTDIR
	}
	if !canRemoveChildren(parent) {
		return syscall.EACCES
	}
	entry, err := n.metadata.DirectoryEntry(ctx, parent.ID, name)
	if err != nil {
		return errnoForError(err)
	}
	object, err := n.metadata.ObjectByID(ctx, entry.ObjectID)
	if err != nil {
		return errnoForError(err)
	}
	if object.MIMEType == googleFolderMIMEType {
		return syscall.EISDIR
	}
	if !canTrashObject(object) {
		return syscall.EACCES
	}
	_, err = n.metadata.QueueTrash(ctx, parent.ID, name, false)
	return errnoForError(err)
}

func (n *IndexedNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.writer == nil {
		return syscall.EROFS
	}
	if !validCreateName(name) {
		return syscall.EINVAL
	}
	parent, errno := n.object(ctx)
	if errno != 0 {
		return errno
	}
	if parent.MIMEType != googleFolderMIMEType {
		return syscall.ENOTDIR
	}
	if !canRemoveChildren(parent) {
		return syscall.EACCES
	}
	entry, err := n.metadata.DirectoryEntry(ctx, parent.ID, name)
	if err != nil {
		return errnoForError(err)
	}
	object, err := n.metadata.ObjectByID(ctx, entry.ObjectID)
	if err != nil {
		return errnoForError(err)
	}
	if object.MIMEType != googleFolderMIMEType {
		return syscall.ENOTDIR
	}
	if !canTrashObject(object) {
		return syscall.EACCES
	}
	_, err = n.metadata.QueueTrash(ctx, parent.ID, name, true)
	return errnoForError(err)
}

func (n *IndexedNode) Rename(ctx context.Context, oldName string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if n.writer == nil {
		return syscall.EROFS
	}
	if !validCreateName(oldName) || !validCreateName(newName) {
		return syscall.EINVAL
	}
	if flags&^uint32(unix.RENAME_NOREPLACE) != 0 {
		return syscall.EOPNOTSUPP
	}
	newParentNode, ok := newParent.(*IndexedNode)
	if !ok || newParentNode.metadata != n.metadata {
		return syscall.EXDEV
	}
	oldParentObject, errno := n.object(ctx)
	if errno != 0 {
		return errno
	}
	newParentObject, errno := newParentNode.object(ctx)
	if errno != 0 {
		return errno
	}
	if oldParentObject.MIMEType != googleFolderMIMEType || newParentObject.MIMEType != googleFolderMIMEType {
		return syscall.ENOTDIR
	}
	entry, err := n.metadata.DirectoryEntry(ctx, oldParentObject.ID, oldName)
	if err != nil {
		return errnoForError(err)
	}
	source, err := n.metadata.ObjectByID(ctx, entry.ObjectID)
	if err != nil {
		return errnoForError(err)
	}
	if !canRenameObject(source) {
		return syscall.EACCES
	}
	if oldParentObject.ID != newParentObject.ID &&
		(!canRemoveChildren(oldParentObject) || !canAddChildren(newParentObject) || !canMoveItemWithinDrive(source)) {
		return syscall.EACCES
	}
	if destination, err := n.metadata.DirectoryEntry(ctx, newParentObject.ID, newName); err == nil {
		if flags&uint32(unix.RENAME_NOREPLACE) != 0 {
			return syscall.EEXIST
		}
		if destination.ObjectID != source.ID {
			destinationObject, objectErr := n.metadata.ObjectByID(ctx, destination.ObjectID)
			if objectErr != nil {
				return errnoForError(objectErr)
			}
			if !canTrashObject(destinationObject) {
				return syscall.EACCES
			}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return errnoForError(err)
	}
	_, err = n.metadata.QueueRename(ctx, oldParentObject.ID, oldName, newParentObject.ID, newName, flags&uint32(unix.RENAME_NOREPLACE) != 0)
	return errnoForError(err)
}

func (n *IndexedNode) object(ctx context.Context) (domain.Object, syscall.Errno) {
	object, err := n.metadata.ObjectByID(ctx, n.objectID)
	if err != nil {
		return domain.Object{}, errnoForError(err)
	}
	if object.Deleted {
		return domain.Object{}, syscall.ENOENT
	}
	return object, 0
}

func modeForObject(object domain.Object) (uint32, syscall.Errno) {
	switch object.MIMEType {
	case googleFolderMIMEType:
		return fuse.S_IFDIR | 0o555, 0
	default:
		if strings.HasPrefix(object.MIMEType, "application/vnd.google-apps.") {
			return 0, syscall.EOPNOTSUPP
		}
		return fuse.S_IFREG | 0o444, 0
	}
}

func (n *IndexedNode) modeForObject(object domain.Object) (uint32, syscall.Errno) {
	mode, errno := modeForObject(object)
	if errno != 0 || n.writer == nil {
		return mode, errno
	}
	if object.MIMEType == googleFolderMIMEType {
		if canModifyDirectory(object) {
			return fuse.S_IFDIR | 0o755, 0
		}
		return fuse.S_IFDIR | 0o555, 0
	}
	if canEdit(object) {
		return fuse.S_IFREG | 0o644, 0
	}
	return mode, 0
}

func fillAttr(attr *fuse.Attr, object domain.Object, mode uint32) {
	size := object.Size
	if size < 0 {
		size = 0
	}
	attr.Ino = uint64(object.Inode)
	attr.Size = uint64(size)
	attr.Blocks = (uint64(size) + 511) / 512
	attr.Blksize = 4096
	attr.Mode = mode
	attr.Nlink = 1
	if object.MIMEType == googleFolderMIMEType {
		attr.Nlink = 2
	}
	attr.Owner = fuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}
	setFuseTime(object.ModifiedAt, &attr.Mtime, &attr.Mtimensec)
	setFuseTime(object.UpdatedAt, &attr.Ctime, &attr.Ctimensec)
	setFuseTime(object.ModifiedAt, &attr.Atime, &attr.Atimensec)
}

func setFuseTime(value time.Time, seconds *uint64, nanoseconds *uint32) {
	if value.IsZero() {
		*seconds, *nanoseconds = 0, 0
		return
	}
	value = value.UTC()
	*seconds = uint64(value.Unix())
	*nanoseconds = uint32(value.Nanosecond())
}

func errnoForError(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, store.ErrDirectoryNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, store.ErrNotDirectory):
		return syscall.ENOTDIR
	case errors.Is(err, store.ErrIsDirectory):
		return syscall.EISDIR
	case errors.Is(err, store.ErrDirectoryCycle):
		return syscall.EINVAL
	case errors.Is(err, store.ErrDirectoryEntryExists):
		return syscall.EEXIST
	case errors.Is(err, sql.ErrNoRows):
		return syscall.ENOENT
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return syscall.EINTR
	default:
		return syscall.EIO
	}
}

func contentErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, syscall.EISDIR):
		return syscall.EISDIR
	case errors.Is(err, driveapi.ErrUnauthenticated), errors.Is(err, driveapi.ErrForbidden), errors.Is(err, contentcache.ErrNotDownloadable):
		return syscall.EACCES
	case errors.Is(err, driveapi.ErrNotFound), errors.Is(err, os.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, driveapi.ErrRateLimited):
		return syscall.EAGAIN
	case errors.Is(err, driveapi.ErrTemporary):
		return syscall.EHOSTUNREACH
	case errors.Is(err, contentcache.ErrUnsupportedType):
		return syscall.EOPNOTSUPP
	case errors.Is(err, contentcache.ErrStaleRemoteVersion):
		return syscall.EAGAIN
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return syscall.EINTR
	case errors.Is(err, syscall.ENOSPC):
		return syscall.ENOSPC
	case errors.Is(err, store.ErrInsufficientDiskSpace):
		return syscall.ENOSPC
	case errors.Is(err, store.ErrDirectoryEntryExists):
		return syscall.EEXIST
	default:
		var networkError net.Error
		if errors.As(err, &networkError) {
			return syscall.EHOSTUNREACH
		}
		return syscall.EIO
	}
}
