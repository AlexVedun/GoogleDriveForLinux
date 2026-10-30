//go:build linux

package fusefs

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google-drive-linux/internal/contentcache"
	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/store"
)

type writeManager struct {
	metadata *store.Store
	content  *store.ContentStore
	reader   *contentcache.Cache

	mu       sync.Mutex
	states   map[domain.ObjectID]*workingState
	objectMu [64]sync.Mutex
}

type workingState struct {
	mu             sync.Mutex
	object         domain.Object
	file           *store.WorkingFile
	generation     uint64
	persisted      uint64
	idempotencyKey string
	openHandles    int
}

type writeHandle struct {
	state    *workingState
	readable bool
	writable bool
	append   bool
}

func newWriteManager(metadata *store.Store, reader *contentcache.Cache, content *store.ContentStore) *writeManager {
	return &writeManager{metadata: metadata, reader: reader, content: content, states: make(map[domain.ObjectID]*workingState)}
}

func (m *writeManager) objectLock(id domain.ObjectID) *sync.Mutex {
	var hash uint64 = 1469598103934665603
	for index := range len(id) {
		hash ^= uint64(id[index])
		hash *= 1099511628211
	}
	return &m.objectMu[hash%uint64(len(m.objectMu))]
}

func (m *writeManager) open(ctx context.Context, object domain.Object, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	access := flags & uint32(os.O_WRONLY|os.O_RDWR)
	writable := access != 0
	if writable && !canEdit(object) {
		return nil, 0, syscall.EACCES
	}
	if !writable && flags&uint32(os.O_TRUNC) != 0 {
		return nil, 0, syscall.EACCES
	}
	lock := m.objectLock(object.ID)
	lock.Lock()
	defer lock.Unlock()

	m.mu.Lock()
	state := m.states[object.ID]
	m.mu.Unlock()
	if state == nil {
		working, err := m.newWorkingCopy(ctx, object, flags&uint32(os.O_TRUNC) != 0)
		if err != nil {
			return nil, 0, contentErrno(err)
		}
		state = &workingState{object: object, file: working}
		if flags&uint32(os.O_TRUNC) != 0 {
			state.generation = 1
		}
		m.mu.Lock()
		m.states[object.ID] = state
		m.mu.Unlock()
	}
	state.mu.Lock()
	if flags&uint32(os.O_TRUNC) != 0 {
		info, err := state.file.File().Stat()
		if err != nil {
			state.mu.Unlock()
			return nil, 0, ioErrno(err)
		}
		if info.Size() != 0 {
			if err := state.file.File().Truncate(0); err != nil {
				state.mu.Unlock()
				return nil, 0, ioErrno(err)
			}
			state.generation++
			state.idempotencyKey = ""
		}
	}
	state.openHandles++
	state.mu.Unlock()
	return &writeHandle{
		state: state, readable: access != uint32(os.O_WRONLY), writable: writable,
		append: flags&uint32(os.O_APPEND) != 0,
	}, fuse.FOPEN_DIRECT_IO, 0
}

func (m *writeManager) openExisting(object domain.Object, flags uint32) (fs.FileHandle, bool) {
	lock := m.objectLock(object.ID)
	lock.Lock()
	defer lock.Unlock()
	m.mu.Lock()
	state := m.states[object.ID]
	m.mu.Unlock()
	if state == nil {
		return nil, false
	}
	state.mu.Lock()
	state.openHandles++
	state.mu.Unlock()
	access := flags & uint32(os.O_WRONLY|os.O_RDWR)
	return &writeHandle{state: state, readable: access != uint32(os.O_WRONLY), writable: access != 0,
		append: flags&uint32(os.O_APPEND) != 0}, true
}

func (m *writeManager) newWorkingCopy(ctx context.Context, object domain.Object, truncate bool) (*store.WorkingFile, error) {
	if truncate {
		return m.content.NewWorkingFile(ctx, 0, bytes.NewReader(nil))
	}
	current, err := m.content.CurrentContentVersion(ctx, object.ID)
	if err == nil && current.Kind == domain.ContentPending && current.Complete {
		file, version, err := m.content.OpenContentVersion(ctx, current.ID)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		return m.content.NewWorkingFile(ctx, version.Size, io.NewSectionReader(file, 0, version.Size))
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if object.DriveFileID == nil {
		return m.content.NewWorkingFile(ctx, 0, bytes.NewReader(nil))
	}
	if m.reader == nil {
		return nil, errors.New("content reader is unavailable for this Drive file")
	}
	remote, err := m.reader.Open(ctx, object)
	if err != nil {
		return nil, err
	}
	defer remote.Close()
	return m.content.NewWorkingFile(ctx, remote.Size(), io.NewSectionReader(remote, 0, remote.Size()))
}

func (m *writeManager) registerCreated(object domain.Object, file *store.WorkingFile, flags uint32) *writeHandle {
	lock := m.objectLock(object.ID)
	lock.Lock()
	defer lock.Unlock()
	state := &workingState{object: object, file: file, generation: 1, openHandles: 1}
	m.mu.Lock()
	m.states[object.ID] = state
	m.mu.Unlock()
	access := flags & uint32(os.O_WRONLY|os.O_RDWR)
	return &writeHandle{state: state, readable: access != uint32(os.O_WRONLY), writable: access != 0,
		append: flags&uint32(os.O_APPEND) != 0}
}

func (m *writeManager) write(handle *writeHandle, data []byte, offset int64) (int, error) {
	if handle == nil || !handle.writable || handle.state == nil {
		return 0, syscall.EBADF
	}
	state := handle.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if handle.append {
		info, err := state.file.File().Stat()
		if err != nil {
			return 0, err
		}
		offset = info.Size()
	}
	n, err := state.file.File().WriteAt(data, offset)
	if n > 0 {
		state.generation++
		state.idempotencyKey = ""
	}
	return n, err
}

func (m *writeManager) read(handle *writeHandle, data []byte, offset int64) (int, error) {
	if handle == nil || !handle.readable || handle.state == nil {
		return 0, syscall.EBADF
	}
	state := handle.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.file.File().ReadAt(data, offset)
}

func (m *writeManager) truncate(handle *writeHandle, size int64) error {
	if handle == nil || !handle.writable || handle.state == nil {
		return syscall.EBADF
	}
	if size < 0 {
		return syscall.EINVAL
	}
	state := handle.state
	state.mu.Lock()
	defer state.mu.Unlock()
	info, err := state.file.File().Stat()
	if err != nil {
		return err
	}
	if info.Size() == size {
		return nil
	}
	if err := state.file.File().Truncate(size); err != nil {
		return err
	}
	state.generation++
	state.idempotencyKey = ""
	return nil
}

func (m *writeManager) size(handle *writeHandle) (int64, error) {
	if handle == nil || handle.state == nil {
		return 0, syscall.EBADF
	}
	handle.state.mu.Lock()
	defer handle.state.mu.Unlock()
	info, err := handle.state.file.File().Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (m *writeManager) persist(ctx context.Context, handle *writeHandle) error {
	if handle == nil || handle.state == nil {
		return syscall.EBADF
	}
	state := handle.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return m.persistLocked(ctx, state)
}

func (m *writeManager) persistLocked(ctx context.Context, state *workingState) error {
	if state.generation == state.persisted {
		return nil
	}
	file := state.file.File()
	if file == nil {
		return os.ErrClosed
	}
	if err := file.Sync(); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if state.idempotencyKey == "" {
		token, err := newWriteToken()
		if err != nil {
			return err
		}
		state.idempotencyKey = "fuse-write:" + token
	}
	kind := domain.OperationWrite
	if state.object.DriveFileID == nil {
		kind = domain.OperationCreate
		hasCreate, err := m.metadata.HasUncommittedCreate(ctx, state.object.ID)
		if err != nil {
			return err
		}
		if hasCreate {
			kind = domain.OperationWrite
		}
	}
	_, err = m.content.WritePending(ctx, store.WriteRequest{
		ObjectID: state.object.ID, Kind: kind, IdempotencyKey: state.idempotencyKey,
		BaseDriveVersion: state.object.DriveVersion, ExpectedSize: info.Size(),
	}, io.NewSectionReader(file, 0, info.Size()))
	if err != nil {
		return err
	}
	state.persisted = state.generation
	state.idempotencyKey = ""
	return nil
}

func (m *writeManager) release(ctx context.Context, handle *writeHandle) syscall.Errno {
	if handle == nil || handle.state == nil {
		return syscall.EBADF
	}
	state := handle.state
	lock := m.objectLock(state.object.ID)
	lock.Lock()
	defer lock.Unlock()
	state.mu.Lock()
	var persistErr error
	persistErr = m.persistLocked(ctx, state)
	if state.openHandles > 0 {
		state.openHandles--
	}
	if persistErr == nil && state.openHandles == 0 {
		removeErr := state.file.Remove()
		m.mu.Lock()
		if m.states[state.object.ID] == state {
			delete(m.states, state.object.ID)
		}
		m.mu.Unlock()
		state.mu.Unlock()
		if removeErr != nil {
			return ioErrno(removeErr)
		}
		return 0
	}
	state.mu.Unlock()
	if persistErr != nil {
		return contentErrno(persistErr)
	}
	return 0
}

func newWriteToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("create write idempotency token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func canEdit(object domain.Object) bool {
	if object.DriveFileID == nil {
		return true
	}
	var capabilities struct {
		CanEdit bool `json:"canEdit"`
	}
	return json.Unmarshal([]byte(object.CapabilitiesJSON), &capabilities) == nil && capabilities.CanEdit
}

func capabilityOrEdit(object domain.Object, key string) bool {
	if object.DriveFileID == nil {
		return true
	}
	var capabilities map[string]json.RawMessage
	if err := json.Unmarshal([]byte(object.CapabilitiesJSON), &capabilities); err != nil {
		return false
	}
	encoded, exists := capabilities[key]
	if !exists {
		return canEdit(object)
	}
	var allowed bool
	return json.Unmarshal(encoded, &allowed) == nil && allowed
}

func canAddChildren(object domain.Object) bool {
	return object.ID == domain.RootObjectID || capabilityOrEdit(object, "canAddChildren")
}

func canRemoveChildren(object domain.Object) bool {
	return object.ID == domain.RootObjectID || capabilityOrEdit(object, "canRemoveChildren")
}

func canRenameObject(object domain.Object) bool {
	return capabilityOrEdit(object, "canRename")
}

func canMoveItemWithinDrive(object domain.Object) bool {
	return capabilityOrEdit(object, "canMoveItemWithinDrive")
}

func canTrashObject(object domain.Object) bool {
	if object.DriveFileID == nil {
		return true
	}
	var capabilities struct {
		CanTrash bool `json:"canTrash"`
	}
	return json.Unmarshal([]byte(object.CapabilitiesJSON), &capabilities) == nil && capabilities.CanTrash
}

func canModifyDirectory(object domain.Object) bool {
	return canAddChildren(object) || canRemoveChildren(object)
}

func contentTypeForName(name string) string {
	contentType := mime.TypeByExtension(filepath.Ext(name))
	if contentType == "" {
		return "application/octet-stream"
	}
	if semicolon := strings.IndexByte(contentType, ';'); semicolon >= 0 {
		contentType = strings.TrimSpace(contentType[:semicolon])
	}
	return contentType
}

func validCreateName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\x00")
}

func ioErrno(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return contentErrno(err)
}

func (n *IndexedNode) Fsync(ctx context.Context, handle fs.FileHandle, _ uint32) syscall.Errno {
	if n.writer == nil {
		if _, readOnly := handle.(*indexedFileHandle); readOnly {
			return 0
		}
		return syscall.EROFS
	}
	file, ok := handle.(*writeHandle)
	if !ok {
		if _, readOnly := handle.(*indexedFileHandle); readOnly {
			return 0
		}
		return syscall.EBADF
	}
	if err := n.writer.persist(ctx, file); err != nil {
		return contentErrno(err)
	}
	return 0
}

func (n *IndexedNode) Flush(ctx context.Context, handle fs.FileHandle) syscall.Errno {
	if n.writer == nil {
		return 0
	}
	file, ok := handle.(*writeHandle)
	if !ok {
		if _, readOnly := handle.(*indexedFileHandle); readOnly {
			return 0
		}
		return syscall.EBADF
	}
	if err := n.writer.persist(ctx, file); err != nil {
		return contentErrno(err)
	}
	return 0
}

func (n *IndexedNode) create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if n.writer == nil {
		return nil, nil, 0, syscall.EROFS
	}
	if !validCreateName(name) {
		return nil, nil, 0, syscall.EINVAL
	}
	parent, errno := n.object(ctx)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	if parent.MIMEType != googleFolderMIMEType {
		return nil, nil, 0, syscall.ENOTDIR
	}
	if !canAddChildren(parent) {
		return nil, nil, 0, syscall.EACCES
	}
	if _, err := n.metadata.DirectoryEntry(ctx, parent.ID, name); err == nil {
		return nil, nil, 0, syscall.EEXIST
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, 0, errnoForError(err)
	}
	work, err := n.writer.content.NewWorkingFile(ctx, 0, bytes.NewReader(nil))
	if err != nil {
		return nil, nil, 0, contentErrno(err)
	}
	object, err := n.metadata.CreateLocalFile(ctx, parent.ID, name, contentTypeForName(name))
	if err != nil {
		_ = work.Remove()
		return nil, nil, 0, errnoForError(err)
	}
	handle := n.writer.registerCreated(object, work, flags)
	objectMode, errno := n.modeForObject(object)
	if errno != 0 {
		_ = n.metadata.DiscardUnqueuedLocalFile(ctx, object.ID)
		_ = n.writer.release(ctx, handle)
		return nil, nil, 0, errno
	}
	fillAttr(&out.Attr, object, objectMode)
	out.Mode = objectMode
	child := n.NewInode(ctx, &IndexedNode{metadata: n.metadata, reader: n.reader, writer: n.writer, objectID: object.ID}, fs.StableAttr{
		Mode: objectMode & syscall.S_IFMT,
		Ino:  uint64(object.Inode),
	})
	_ = mode // POSIX mode is synthesized from Drive capabilities until metadata support is added.
	return child, handle, fuse.FOPEN_DIRECT_IO, 0
}

func setattrWriterHandle(ctx context.Context, n *IndexedNode, object domain.Object, handle fs.FileHandle) (*writeHandle, bool, syscall.Errno) {
	if n.writer == nil {
		return nil, false, syscall.EROFS
	}
	if handle != nil {
		writeFile, ok := handle.(*writeHandle)
		if !ok || !writeFile.writable {
			return nil, false, syscall.EBADF
		}
		return writeFile, false, 0
	}
	opened, _, errno := n.writer.open(ctx, object, uint32(os.O_RDWR))
	if errno != 0 {
		return nil, false, errno
	}
	return opened.(*writeHandle), true, 0
}

func (n *IndexedNode) truncate(ctx context.Context, handle fs.FileHandle, size uint64) syscall.Errno {
	object, errno := n.object(ctx)
	if errno != 0 {
		return errno
	}
	if size > uint64(^uint64(0)>>1) {
		return syscall.EFBIG
	}
	writeFile, temporary, errno := setattrWriterHandle(ctx, n, object, handle)
	if errno != 0 {
		return errno
	}
	if temporary {
		defer n.writer.release(ctx, writeFile)
	}
	if err := n.writer.truncate(writeFile, int64(size)); err != nil {
		return ioErrno(err)
	}
	if temporary {
		return contentErrno(n.writer.persist(ctx, writeFile))
	}
	return 0
}

var _ fs.NodeWriter = (*IndexedNode)(nil)
var _ fs.NodeFsyncer = (*IndexedNode)(nil)
var _ fs.NodeFlusher = (*IndexedNode)(nil)
