//go:build linux

package fusefs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/store"
)

const googleFolderMIMEType = "application/vnd.google-apps.folder"

var ErrIndexNotReady = errors.New("Drive index is not ready")

// IndexedNode exposes the persisted My Drive namespace. This step intentionally
// provides metadata-only, read-only access; content reads are added separately.
type IndexedNode struct {
	fs.Inode
	metadata *store.Store
	objectID domain.ObjectID
}

var (
	_ fs.InodeEmbedder = (*IndexedNode)(nil)
	_ fs.NodeLookuper  = (*IndexedNode)(nil)
	_ fs.NodeReaddirer = (*IndexedNode)(nil)
	_ fs.NodeGetattrer = (*IndexedNode)(nil)
	_ fs.NodeOpener    = (*IndexedNode)(nil)
	_ fs.NodeWriter    = (*IndexedNode)(nil)
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
	return &IndexedNode{metadata: metadata, objectID: domain.RootObjectID}, nil
}

func MountIndexed(metadata *store.Store, mountpoint string) (*fuse.Server, error) {
	resolvedMountpoint, err := ValidateMountpoint(mountpoint)
	if err != nil {
		return nil, err
	}
	root, err := NewIndexedRoot(metadata)
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
			Options:       []string{"ro"},
			DisableXAttrs: true,
		},
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
	mode, errno := modeForObject(child)
	if errno != 0 {
		return nil, errno
	}
	fillAttr(&out.Attr, child, mode)
	return n.NewInode(ctx, &IndexedNode{metadata: n.metadata, objectID: child.ID}, fs.StableAttr{
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

func (n *IndexedNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	object, errno := n.object(ctx)
	if errno != 0 {
		return errno
	}
	mode, errno := modeForObject(object)
	if errno != 0 {
		return errno
	}
	fillAttr(&out.Attr, object, mode)
	return 0
}

func (n *IndexedNode) Open(ctx context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	object, errno := n.object(ctx)
	if errno != 0 {
		return nil, 0, errno
	}
	if object.MIMEType == googleFolderMIMEType {
		return nil, 0, syscall.EISDIR
	}
	if _, errno := modeForObject(object); errno != 0 {
		return nil, 0, errno
	}
	return nil, 0, syscall.EOPNOTSUPP
}

func (n *IndexedNode) Write(context.Context, fs.FileHandle, []byte, int64) (uint32, syscall.Errno) {
	return 0, syscall.EROFS
}

func (n *IndexedNode) Setattr(context.Context, fs.FileHandle, *fuse.SetAttrIn, *fuse.AttrOut) syscall.Errno {
	return syscall.EROFS
}

func (n *IndexedNode) Create(context.Context, string, uint32, uint32, *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return nil, nil, 0, syscall.EROFS
}

func (n *IndexedNode) Mkdir(context.Context, string, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
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

func (n *IndexedNode) Unlink(context.Context, string) syscall.Errno { return syscall.EROFS }

func (n *IndexedNode) Rmdir(context.Context, string) syscall.Errno { return syscall.EROFS }

func (n *IndexedNode) Rename(context.Context, string, fs.InodeEmbedder, string, uint32) syscall.Errno {
	return syscall.EROFS
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
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return syscall.ENOENT
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return syscall.EINTR
	default:
		return syscall.EIO
	}
}
