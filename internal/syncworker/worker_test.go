package syncworker

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

type memoryDrive struct {
	files                   map[string]driveapi.File
	contents                map[string][]byte
	sessions                map[string]*memorySession
	generated               int
	sessionCount            int
	createCount             int
	completedUploads        int
	loseNextCreateResponse  bool
	loseNextFinalChunk      bool
	incompleteNextFinalFile bool
	failFirstStatus         bool
	queries                 []string
	updates                 []string
}

type memorySession struct {
	fileID   string
	metadata driveapi.FileMetadata
	total    int64
	data     []byte
	complete bool
	expired  bool
}

func newMemoryDrive() *memoryDrive {
	return &memoryDrive{files: map[string]driveapi.File{}, contents: map[string][]byte{}, sessions: map[string]*memorySession{}}
}

func (d *memoryDrive) GetFile(_ context.Context, fileID string) (driveapi.File, error) {
	d.queries = append(d.queries, "get:"+fileID)
	file, ok := d.files[fileID]
	if !ok {
		return driveapi.File{}, driveapi.ErrNotFound
	}
	return cloneFile(file), nil
}

func (d *memoryDrive) CreateFile(_ context.Context, metadata driveapi.FileMetadata) (driveapi.File, error) {
	d.createCount++
	file := driveapi.File{ID: metadata.ID, Name: metadata.Name, MIMEType: metadata.MIMEType,
		Parents: append([]string(nil), metadata.Parents...), Size: 0, Version: "1", ModifiedTime: time.Now().UTC(),
		AppProperties: cloneProperties(metadata.AppProperties)}
	d.files[file.ID] = file
	d.contents[file.ID] = nil
	if d.loseNextCreateResponse {
		d.loseNextCreateResponse = false
		return driveapi.File{}, driveapi.ErrTemporary
	}
	return cloneFile(file), nil
}

func (d *memoryDrive) UpdateFile(_ context.Context, fileID string, patch driveapi.FilePatch, addParents, removeParents []string) (driveapi.File, error) {
	d.updates = append(d.updates, fileID)
	file, ok := d.files[fileID]
	if !ok {
		return driveapi.File{}, driveapi.ErrNotFound
	}
	if patch.Name != nil {
		file.Name = *patch.Name
	}
	if patch.Trashed != nil {
		file.Trashed = *patch.Trashed
	}
	if patch.AppProperties != nil {
		file.AppProperties = cloneProperties(patch.AppProperties)
	}
	for _, remove := range removeParents {
		parents := file.Parents[:0]
		for _, parent := range file.Parents {
			if parent != remove {
				parents = append(parents, parent)
			}
		}
		file.Parents = parents
	}
	for _, add := range addParents {
		if !contains(file.Parents, add) {
			file.Parents = append(file.Parents, add)
		}
	}
	file.Version = nextVersion(file.Version)
	file.ModifiedTime = time.Now().UTC()
	d.files[fileID] = file
	return cloneFile(file), nil
}

func (d *memoryDrive) GenerateIDs(_ context.Context, count int) ([]string, error) {
	ids := make([]string, count)
	for index := range ids {
		d.generated++
		ids[index] = fmt.Sprintf("reserved-%d", d.generated)
	}
	return ids, nil
}

func (d *memoryDrive) StartResumableUpload(_ context.Context, upload driveapi.UploadRequest) (string, error) {
	d.sessionCount++
	fileID := upload.FileID
	if fileID == "" {
		fileID = upload.Metadata.ID
	}
	key := fmt.Sprintf("session-%d", d.sessionCount)
	d.sessions[key] = &memorySession{fileID: fileID, metadata: upload.Metadata, total: upload.Size}
	return key, nil
}

func (d *memoryDrive) UploadChunk(_ context.Context, sessionURL string, chunk driveapi.UploadChunkRequest) (driveapi.UploadProgress, error) {
	session := d.sessions[sessionURL]
	if session == nil || session.expired {
		return driveapi.UploadProgress{}, driveapi.ErrUploadSessionExpired
	}
	if session.complete {
		return driveapi.UploadProgress{Complete: true, NextOffset: session.total, File: filePointer(d.files[session.fileID])}, nil
	}
	if chunk.Empty {
		session.data = nil
	} else {
		data, err := io.ReadAll(chunk.Body)
		if err != nil {
			return driveapi.UploadProgress{}, err
		}
		if chunk.Start != int64(len(session.data)) {
			return driveapi.UploadProgress{NextOffset: int64(len(session.data))}, nil
		}
		session.data = append(session.data, data...)
	}
	if int64(len(session.data)) < session.total {
		return driveapi.UploadProgress{NextOffset: int64(len(session.data))}, nil
	}
	if int64(len(session.data)) != session.total {
		return driveapi.UploadProgress{}, errors.New("fake Drive received more than the declared upload size")
	}
	file, exists := d.files[session.fileID]
	if !exists {
		file = driveapi.File{ID: session.fileID, Name: session.metadata.Name, MIMEType: session.metadata.MIMEType,
			Parents: append([]string(nil), session.metadata.Parents...), Version: "1"}
	} else {
		file.Version = nextVersion(file.Version)
	}
	if session.metadata.Name != "" {
		file.Name = session.metadata.Name
	}
	if session.metadata.MIMEType != "" {
		file.MIMEType = session.metadata.MIMEType
	}
	if len(session.metadata.Parents) > 0 {
		file.Parents = append([]string(nil), session.metadata.Parents...)
	}
	if session.metadata.AppProperties != nil {
		file.AppProperties = cloneProperties(session.metadata.AppProperties)
	}
	file.Size = session.total
	file.ModifiedTime = time.Now().UTC()
	if len(session.data) > 0 {
		sum := md5.Sum(session.data)
		file.MD5Checksum = hex.EncodeToString(sum[:])
	}
	d.files[session.fileID] = file
	d.contents[session.fileID] = append([]byte(nil), session.data...)
	session.complete = true
	d.completedUploads++
	if d.loseNextFinalChunk {
		d.loseNextFinalChunk = false
		return driveapi.UploadProgress{}, driveapi.ErrTemporary
	}
	if d.incompleteNextFinalFile {
		d.incompleteNextFinalFile = false
		return driveapi.UploadProgress{Complete: true, NextOffset: session.total,
			File: &driveapi.File{ID: file.ID}}, nil
	}
	return driveapi.UploadProgress{Complete: true, NextOffset: session.total, File: filePointer(file)}, nil
}

func (d *memoryDrive) QueryUploadStatus(_ context.Context, sessionURL string, total int64) (driveapi.UploadProgress, error) {
	if d.failFirstStatus {
		d.failFirstStatus = false
		return driveapi.UploadProgress{}, driveapi.ErrTemporary
	}
	session := d.sessions[sessionURL]
	if session == nil || session.expired {
		return driveapi.UploadProgress{}, driveapi.ErrUploadSessionExpired
	}
	if session.total != total {
		return driveapi.UploadProgress{}, errors.New("status query total did not match upload")
	}
	if session.complete {
		return driveapi.UploadProgress{Complete: true, NextOffset: total, File: filePointer(d.files[session.fileID])}, nil
	}
	return driveapi.UploadProgress{NextOffset: int64(len(session.data))}, nil
}

func TestWorkerOrdersParentBeforeChildAndPersistsGeneratedIDs(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	parent, parentOperation, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := metadata.CreateLocalFile(ctx, parent.ID, "child.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	write, err := content.WritePending(ctx, store.WriteRequest{ObjectID: child.ID, Kind: domain.OperationCreate,
		IdempotencyKey: "child-create", ExpectedSize: 4}, strings.NewReader("data"))
	if err != nil {
		t.Fatal(err)
	}
	worker.chunkSize = 256 * 1024
	if worked, err := worker.RunOne(ctx); err != nil || !worked {
		t.Fatalf("first queue operation worked=%v err=%v", worked, err)
	}
	parent, err = metadata.ObjectByID(ctx, parent.ID)
	if err != nil || parent.DriveFileID == nil || *parent.DriveFileID != "reserved-1" {
		t.Fatalf("parent Drive ID = %#v err=%v", parent.DriveFileID, err)
	}
	parentState, err := metadata.OperationByID(ctx, parentOperation.ID)
	if err != nil || parentState.State != domain.OperationCommitted {
		t.Fatalf("parent operation state = %#v err=%v", parentState, err)
	}
	if worked, err := worker.RunOne(ctx); err != nil || !worked {
		t.Fatalf("child queue operation worked=%v err=%v", worked, err)
	}
	child, err = metadata.ObjectByID(ctx, child.ID)
	if err != nil || child.DriveFileID == nil || *child.DriveFileID != "reserved-2" {
		t.Fatalf("child Drive ID = %#v err=%v", child.DriveFileID, err)
	}
	state, err := metadata.OperationByID(ctx, write.Operation.ID)
	if err != nil || state.State != domain.OperationCommitted {
		t.Fatalf("child create state = %#v err=%v", state, err)
	}
	if len(drive.files) != 2 || drive.files[string(*child.DriveFileID)].Parents[0] != string(*parent.DriveFileID) {
		t.Fatalf("Drive files = %#v; child was not created under its parent", drive.files)
	}
}

func TestQueueRetryBackoffUsesJitterAndHasACap(t *testing.T) {
	worker := &Worker{retryDelay: time.Minute}
	now := time.Now()
	first := worker.nextRetryAt(domain.Operation{AttemptCount: 1}).Sub(now)
	if first < 30*time.Second || first > 90*time.Second+time.Second {
		t.Fatalf("first retry delay %s is outside jittered one-minute range", first)
	}
	now = time.Now()
	maximum := worker.nextRetryAt(domain.Operation{AttemptCount: 100}).Sub(now)
	if maximum < 15*time.Minute || maximum > maxQueueRetryDelay+time.Second {
		t.Fatalf("capped retry delay %s is outside jittered 30-minute range", maximum)
	}
	worker.retryDelay = 0
	if retryAt := worker.nextRetryAt(domain.Operation{AttemptCount: 1}); !retryAt.IsZero() {
		t.Fatalf("disabled retry delay = %s, want zero", retryAt)
	}
}

func TestWorkerReconcilesLostFinalUploadResponseWithoutDuplicate(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	_, operation := createQueuedFile(t, ctx, metadata, content, "payload.bin", bytesRepeated(512*1024+3, 'x'), "create-lost-response")
	drive.loseNextFinalChunk = true
	worker.chunkSize = 256 * 1024
	worker.retryDelay = 0
	if worked, err := worker.RunOne(ctx); !worked || !errors.Is(err, driveapi.ErrTemporary) {
		t.Fatalf("first upload worked=%v err=%v; want lost response", worked, err)
	}
	interrupted, err := metadata.OperationByID(ctx, operation.ID)
	if err != nil || interrupted.State != domain.OperationRetryableError || interrupted.ResumableSessionURL == "" {
		t.Fatalf("interrupted upload state = %#v err=%v", interrupted, err)
	}
	metadata, content, worker = reopenWorker(t, metadata, content, drive)
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("resume after lost final response worked=%v err=%v", worked, err)
	}
	completed, err := metadata.OperationByID(ctx, operation.ID)
	if err != nil || completed.State != domain.OperationCommitted || completed.ResumableSessionURL != "" {
		t.Fatalf("completed upload state = %#v err=%v", completed, err)
	}
	if drive.sessionCount != 1 || drive.completedUploads != 1 || len(drive.files) != 1 || drive.contents["reserved-1"] == nil {
		t.Fatalf("duplicate or missing upload: sessions=%d completed=%d files=%d", drive.sessionCount, drive.completedUploads, len(drive.files))
	}
}

func TestWorkerRefetchesIncompleteFinalUploadResponse(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	_, operation := createQueuedFile(t, ctx, metadata, content, "incomplete-response.bin",
		bytesRepeated(256*1024+5, 'r'), "create-incomplete-response")
	drive.incompleteNextFinalFile = true
	worker.chunkSize = 256 * 1024
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("upload reconciliation worked=%v err=%v", worked, err)
	}
	state, err := metadata.OperationByID(ctx, operation.ID)
	if err != nil || state.State != domain.OperationCommitted {
		t.Fatalf("reconciled upload state=%#v err=%v", state, err)
	}
	if drive.sessionCount != 1 || drive.completedUploads != 1 || len(drive.files) != 1 {
		t.Fatalf("reconciliation duplicated upload: sessions=%d completed=%d files=%d",
			drive.sessionCount, drive.completedUploads, len(drive.files))
	}
	if len(drive.queries) < 2 || drive.queries[len(drive.queries)-1] != "get:reserved-1" {
		t.Fatalf("incomplete final response was not verified by file ID: queries=%v", drive.queries)
	}
}

func TestWorkerRestartsExpiredSessionOnlyAfterCheckingRemoteObject(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	_, operation := createQueuedFile(t, ctx, metadata, content, "expired.bin", []byte("resume me"), "create-expired-session")
	drive.failFirstStatus = true
	worker.retryDelay = 0
	if worked, err := worker.RunOne(ctx); !worked || !errors.Is(err, driveapi.ErrTemporary) {
		t.Fatalf("initial upload worked=%v err=%v", worked, err)
	}
	interrupted, err := metadata.OperationByID(ctx, operation.ID)
	if err != nil || interrupted.ResumableSessionURL == "" {
		t.Fatalf("resumable session was not persisted before status query: %#v err=%v", interrupted, err)
	}
	metadata, content, worker = reopenWorker(t, metadata, content, drive)
	interrupted, err = metadata.OperationByID(ctx, operation.ID)
	if err != nil || interrupted.State != domain.OperationRetryableError {
		t.Fatalf("operation did not recover in uploading state: %#v err=%v", interrupted, err)
	}
	drive.sessions[interrupted.ResumableSessionURL].expired = true
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("expired session recovery worked=%v err=%v", worked, err)
	}
	completed, err := metadata.OperationByID(ctx, operation.ID)
	if err != nil || completed.State != domain.OperationCommitted {
		t.Fatalf("recovered operation = %#v err=%v", completed, err)
	}
	if drive.sessionCount != 2 || drive.completedUploads != 1 || len(drive.files) != 1 {
		t.Fatalf("expired session recovery duplicated upload: sessions=%d completed=%d files=%d", drive.sessionCount, drive.completedUploads, len(drive.files))
	}
}

func TestWorkerSIGKILLAfterAcceptedChunkRecoversFromDriveOffset(t *testing.T) {
	for _, stage := range []string{"queue_claim", "id_reserved", "session_status", "accepted_chunk", "completed_upload"} {
		t.Run(stage, func(t *testing.T) {
			runSIGKILLRecoveryScenario(t, stage, false)
		})
	}
}

func TestWorkerSIGKILLAfterMetadataCreateRecoversWithoutDuplicate(t *testing.T) {
	runSIGKILLRecoveryScenario(t, "metadata_created", true)
}

func TestWorkerCrashHelperProcess(t *testing.T) {
	if os.Getenv("GDL_WORKER_CRASH_HELPER") != "1" {
		return
	}
	runWorkerCrashHelper(t)
}

func runSIGKILLRecoveryScenario(t *testing.T, stage string, folder bool) {
	t.Helper()
	ctx := context.Background()
	metadata, err := store.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	initialMetadata := metadata
	t.Cleanup(func() { _ = initialMetadata.Close() })
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live"); err != nil {
		t.Fatal(err)
	}
	contentRoot := filepath.Join(t.TempDir(), "content")
	content, err := store.NewContentStore(metadata, contentRoot)
	if err != nil {
		t.Fatal(err)
	}
	data := bytesRepeated(512*1024+17, 'k')
	var operation domain.Operation
	if folder {
		_, operation, err = metadata.CreateLocalFolder(ctx, domain.RootObjectID, "kill-folder")
		if err != nil {
			t.Fatal(err)
		}
	} else {
		_, operation = createQueuedFile(t, ctx, metadata, content, "kill.bin", data, "kill-"+stage)
	}
	databasePath := metadata.Path()
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	serverState := &crashDriveServer{checkpoint: make(chan struct{}, 1), pauseAt: stage}
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)
	var childOutput bytes.Buffer
	command := exec.Command(os.Args[0], "-test.run=^TestWorkerCrashHelperProcess$")
	command.Env = append(os.Environ(),
		"GDL_WORKER_CRASH_HELPER=1",
		"GDL_WORKER_DB="+databasePath,
		"GDL_WORKER_CONTENT="+contentRoot,
		"GDL_WORKER_DRIVE_URL="+server.URL,
	)
	command.Stdout, command.Stderr = &childOutput, &childOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverState.checkpoint:
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("child did not reach the accepted-chunk boundary: %s", childOutput.String())
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := command.Wait()
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) || exitErr.ProcessState == nil {
		t.Fatalf("worker helper did not exit after a signal: err=%v output=%s", waitErr, childOutput.String())
	}
	waitStatus, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("worker helper did not die by SIGKILL: err=%v output=%s", waitErr, childOutput.String())
	}

	metadata, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	reopenedMetadata := metadata
	t.Cleanup(func() { _ = reopenedMetadata.Close() })
	content, err = store.NewContentStore(metadata, contentRoot)
	if err != nil {
		t.Fatal(err)
	}
	client, err := driveapi.NewClient(driveapi.Config{
		BaseURL: server.URL + "/drive/v3", UploadURL: server.URL + "/upload/drive/v3", HTTPClient: server.Client(),
		Retry: driveapi.RetryPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(metadata, content, client)
	if err != nil {
		t.Fatal(err)
	}
	worker.chunkSize = 256 * 1024
	worked, err := worker.RunUntilIdle(ctx, 5)
	if worked != 1 || err != nil {
		t.Fatalf("recovered worker operations=%d err=%v", worked, err)
	}
	state, err := metadata.OperationByID(ctx, operation.ID)
	if err != nil || state.State != domain.OperationCommitted {
		t.Fatalf("post-crash operation state=%#v err=%v", state, err)
	}
	serverState.mu.Lock()
	defer serverState.mu.Unlock()
	wantRemoteCount := 1
	if folder {
		if serverState.file == nil || serverState.file.MIMEType != GoogleFolderMIMEType || serverState.createCount != 1 {
			t.Fatalf("recovered folder=%#v create calls=%d, want exactly one folder", serverState.file, serverState.createCount)
		}
		return
	}
	if serverState.file == nil || string(serverState.fileData) != string(data) || serverState.uploadCount != wantRemoteCount {
		t.Fatalf("recovered remote file size=%d uploads=%d, want one exact upload", len(serverState.fileData), serverState.uploadCount)
	}
}

func runWorkerCrashHelper(t *testing.T) {
	metadata, err := store.Open(os.Getenv("GDL_WORKER_DB"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := store.NewContentStore(metadata, os.Getenv("GDL_WORKER_CONTENT"))
	if err != nil {
		t.Fatal(err)
	}
	baseURL := os.Getenv("GDL_WORKER_DRIVE_URL")
	client, err := driveapi.NewClient(driveapi.Config{
		BaseURL: baseURL + "/drive/v3", UploadURL: baseURL + "/upload/drive/v3",
		Retry: driveapi.RetryPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(metadata, content, client)
	if err != nil {
		t.Fatal(err)
	}
	worker.chunkSize = 256 * 1024
	if worked, err := worker.RunOne(context.Background()); !worked || err != nil {
		t.Fatalf("helper worker worked=%v err=%v", worked, err)
	}
}

type crashDriveServer struct {
	mu          sync.Mutex
	checkpoint  chan struct{}
	pauseAt     string
	session     *crashUploadSession
	file        *driveapi.File
	fileData    []byte
	uploadCount int
	createCount int
	paused      bool
}

type crashUploadSession struct {
	id       string
	metadata driveapi.FileMetadata
	total    int64
	data     []byte
}

func (s *crashDriveServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/files/generateIds":
		s.mu.Lock()
		pause := s.pauseAt == "queue_claim" && !s.paused
		if pause {
			s.paused = true
		}
		s.mu.Unlock()
		if pause {
			s.checkpoint <- struct{}{}
			<-request.Context().Done()
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"ids":["crash-generated-id"]}`)
	case request.Method == http.MethodGet && request.URL.Path == "/drive/v3/files/crash-generated-id":
		s.mu.Lock()
		file := s.file
		if file != nil {
			file = filePointer(*file)
		}
		pause := file == nil && s.pauseAt == "id_reserved" && !s.paused
		if pause {
			s.paused = true
		}
		s.mu.Unlock()
		if file == nil {
			if pause {
				s.checkpoint <- struct{}{}
				<-request.Context().Done()
				return
			}
			http.Error(writer, `{"error":{"errors":[{"reason":"notFound"}]}}`, http.StatusNotFound)
			return
		}
		writeCrashFile(writer, *file)
	case request.Method == http.MethodPost && request.URL.Path == "/drive/v3/files":
		var metadata driveapi.FileMetadata
		if err := json.NewDecoder(request.Body).Decode(&metadata); err != nil {
			http.Error(writer, "{\"error\":\"bad metadata\"}", http.StatusBadRequest)
			return
		}
		file := driveapi.File{ID: metadata.ID, Name: metadata.Name, MIMEType: metadata.MIMEType,
			Parents: append([]string(nil), metadata.Parents...), Version: "1",
			AppProperties: cloneProperties(metadata.AppProperties)}
		s.mu.Lock()
		s.file = &file
		s.createCount++
		pause := s.pauseAt == "metadata_created" && !s.paused
		if pause {
			s.paused = true
		}
		s.mu.Unlock()
		if pause {
			s.checkpoint <- struct{}{}
			<-request.Context().Done()
			return
		}
		writeCrashFile(writer, file)
	case request.Method == http.MethodPost && request.URL.Path == "/upload/drive/v3/files":
		var metadata driveapi.FileMetadata
		if err := json.NewDecoder(request.Body).Decode(&metadata); err != nil {
			http.Error(writer, `{"error":"bad metadata"}`, http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.session = &crashUploadSession{id: "crash-session", metadata: metadata, total: parseInt64(request.Header.Get("X-Upload-Content-Length"))}
		s.mu.Unlock()
		writer.Header().Set("Location", "http://"+request.Host+"/upload/crash-session")
		writer.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPut && request.URL.Path == "/upload/crash-session":
		s.handleUpload(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (s *crashDriveServer) handleUpload(writer http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	if s.session == nil {
		s.mu.Unlock()
		http.Error(writer, `{"error":"session missing"}`, http.StatusNotFound)
		return
	}
	if strings.HasPrefix(request.Header.Get("Content-Range"), "bytes */") {
		if s.file != nil {
			file := filePointer(*s.file)
			s.mu.Unlock()
			writeCrashFile(writer, *file)
			return
		}
		pause := s.pauseAt == "session_status" && !s.paused
		if pause {
			s.paused = true
			s.mu.Unlock()
			s.checkpoint <- struct{}{}
			<-request.Context().Done()
			return
		}
		offset := len(s.session.data)
		s.mu.Unlock()
		writer.Header().Set("Content-Range", "")
		if offset > 0 {
			writer.Header().Set("Range", fmt.Sprintf("bytes=0-%d", offset-1))
		}
		writer.WriteHeader(http.StatusPermanentRedirect)
		return
	}
	var start, end, total int64
	if _, err := fmt.Sscanf(request.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total); err != nil {
		s.mu.Unlock()
		http.Error(writer, `{"error":"bad content range"}`, http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(request.Body)
	if err != nil || int64(len(data)) != end-start+1 || total != s.session.total || start != int64(len(s.session.data)) {
		s.mu.Unlock()
		http.Error(writer, `{"error":"bad chunk"}`, http.StatusBadRequest)
		return
	}
	s.session.data = append(s.session.data, data...)
	if int64(len(s.session.data)) < s.session.total {
		pause := s.pauseAt == "accepted_chunk" && !s.paused
		if pause {
			s.paused = true
			s.mu.Unlock()
			s.checkpoint <- struct{}{}
			<-request.Context().Done()
			return
		}
		offset := len(s.session.data)
		s.mu.Unlock()
		writer.Header().Set("Range", fmt.Sprintf("bytes=0-%d", offset-1))
		writer.WriteHeader(http.StatusPermanentRedirect)
		return
	}
	file := driveapi.File{ID: s.session.metadata.ID, Name: s.session.metadata.Name, MIMEType: s.session.metadata.MIMEType,
		Parents: append([]string(nil), s.session.metadata.Parents...), Size: int64(len(s.session.data)), Version: "1",
		AppProperties: cloneProperties(s.session.metadata.AppProperties)}
	sum := md5.Sum(s.session.data)
	file.MD5Checksum = hex.EncodeToString(sum[:])
	s.file, s.fileData = &file, append([]byte(nil), s.session.data...)
	s.uploadCount++
	pause := s.pauseAt == "completed_upload" && !s.paused
	if pause {
		s.paused = true
	}
	s.mu.Unlock()
	if pause {
		s.checkpoint <- struct{}{}
		<-request.Context().Done()
		return
	}
	writeCrashFile(writer, file)
}

func writeCrashFile(writer http.ResponseWriter, file driveapi.File) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(struct {
		ID            string            `json:"id"`
		Name          string            `json:"name"`
		MIMEType      string            `json:"mimeType"`
		Parents       []string          `json:"parents"`
		Size          string            `json:"size"`
		Version       string            `json:"version"`
		MD5Checksum   string            `json:"md5Checksum"`
		AppProperties map[string]string `json:"appProperties"`
	}{file.ID, file.Name, file.MIMEType, file.Parents, strconv.FormatInt(file.Size, 10), file.Version, file.MD5Checksum, file.AppProperties})
}

func parseInt64(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func TestWorkerReconcilesUnknownMetadataCreateAndSkipsLocalCreateThenDelete(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	folder, mkdir, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "uncertain")
	if err != nil {
		t.Fatal(err)
	}
	drive.loseNextCreateResponse = true
	if worked, err := worker.RunOne(ctx); !worked || !errors.Is(err, driveapi.ErrTemporary) {
		t.Fatalf("unknown folder create worked=%v err=%v", worked, err)
	}
	state, err := metadata.OperationByID(ctx, mkdir.ID)
	if err != nil || state.State != domain.OperationNeedsReconcile {
		t.Fatalf("unknown create state = %#v err=%v", state, err)
	}
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("folder create reconciliation worked=%v err=%v", worked, err)
	}
	state, err = metadata.OperationByID(ctx, mkdir.ID)
	if err != nil || state.State != domain.OperationCommitted || drive.createCount != 1 {
		t.Fatalf("reconciled mkdir state=%#v creates=%d err=%v", state, drive.createCount, err)
	}
	if _, err := metadata.ObjectByID(ctx, folder.ID); err != nil {
		t.Fatal(err)
	}

	local, err := metadata.CreateLocalFile(ctx, domain.RootObjectID, "discard.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	write, err := content.WritePending(ctx, store.WriteRequest{ObjectID: local.ID, Kind: domain.OperationCreate,
		IdempotencyKey: "discard-create", ExpectedSize: 0}, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	_, err = metadata.QueueTrash(ctx, domain.RootObjectID, "discard.txt", false)
	if err != nil {
		t.Fatal(err)
	}
	trashOperations, err := metadata.OperationsForObject(ctx, local.ID)
	if err != nil || len(trashOperations) != 2 {
		t.Fatalf("create/trash operations = %#v err=%v", trashOperations, err)
	}
	createCount := drive.createCount
	if worked, err := worker.RunUntilIdle(ctx, 5); worked != 2 || err != nil {
		t.Fatalf("delete-before-send work=%d err=%v", worked, err)
	}
	createState, err := metadata.OperationByID(ctx, write.Operation.ID)
	if err != nil || createState.State != domain.OperationCommitted || drive.createCount != createCount {
		t.Fatalf("local create was sent despite deletion: state=%#v creates=%d err=%v", createState, drive.createCount, err)
	}
	trashState, err := metadata.OperationByID(ctx, trashOperations[1].ID)
	if err != nil || trashState.State != domain.OperationCommitted {
		t.Fatalf("local trash state = %#v err=%v", trashState, err)
	}
}

func TestWorkerConflictsWhenRemoteVersionChangedBeforeWrite(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	object := addRemoteFile(t, ctx, metadata, drive, "remote-1", "remote.txt", []byte("remote"), "v2")
	_, err := content.WritePending(ctx, store.WriteRequest{ObjectID: object.ID, Kind: domain.OperationWrite,
		IdempotencyKey: "write-stale", BaseDriveVersion: "v1", ExpectedSize: 5}, strings.NewReader("local"))
	if err != nil {
		t.Fatal(err)
	}
	worked, err := worker.RunOne(ctx)
	if !worked || !errors.Is(err, ErrRemoteConflict) {
		t.Fatalf("stale write worked=%v err=%v; want conflict", worked, err)
	}
	operations, err := metadata.OperationsForObject(ctx, object.ID)
	if err != nil || len(operations) != 1 || operations[0].State != domain.OperationConflict {
		t.Fatalf("stale write queue = %#v err=%v", operations, err)
	}
	if drive.sessionCount != 0 || len(drive.updates) != 0 || string(drive.contents["remote-1"]) != "remote" {
		t.Fatal("worker overwrote changed remote content")
	}
}

func TestWorkerAdvancesBaseVersionAcrossQueuedLocalWrites(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	object := addRemoteFile(t, ctx, metadata, drive, "remote-sequence", "sequence.txt", []byte("base"), "v1")
	first, err := content.WritePending(ctx, store.WriteRequest{ObjectID: object.ID, Kind: domain.OperationWrite,
		IdempotencyKey: "write-sequence-one", BaseDriveVersion: "v1", ExpectedSize: 5}, strings.NewReader("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := content.WritePending(ctx, store.WriteRequest{ObjectID: object.ID, Kind: domain.OperationWrite,
		IdempotencyKey: "write-sequence-two", BaseDriveVersion: "v1", ExpectedSize: 6}, strings.NewReader("second"))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("first local write worked=%v err=%v", worked, err)
	}
	secondState, err := metadata.OperationByID(ctx, second.Operation.ID)
	if err != nil || secondState.BaseDriveVersion == "v1" {
		t.Fatalf("second write base version was not advanced: %#v err=%v", secondState, err)
	}
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("second local write worked=%v err=%v", worked, err)
	}
	firstState, err := metadata.OperationByID(ctx, first.Operation.ID)
	if err != nil || firstState.State != domain.OperationCommitted {
		t.Fatalf("first local write state = %#v err=%v", firstState, err)
	}
	secondState, err = metadata.OperationByID(ctx, second.Operation.ID)
	if err != nil || secondState.State != domain.OperationCommitted {
		t.Fatalf("second local write state = %#v err=%v", secondState, err)
	}
	if string(drive.contents["remote-sequence"]) != "second" || drive.completedUploads != 2 {
		t.Fatalf("sequential local writes left remote content=%q completed uploads=%d", drive.contents["remote-sequence"], drive.completedUploads)
	}
}

func TestWorkerRejectsCorruptImmutableContentBeforeNetwork(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	_, operation := createQueuedFile(t, ctx, metadata, content, "corrupt.bin", []byte("original"), "create-corrupt")
	version, err := content.CurrentContentVersion(ctx, operation.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(content.Paths().Root, filepath.FromSlash(version.LocalPath))
	if err := os.WriteFile(path, []byte("modified"), 0o600); err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOne(ctx); !worked || err == nil {
		t.Fatalf("corrupt content worked=%v err=%v; want refusal", worked, err)
	}
	state, err := metadata.OperationByID(ctx, operation.ID)
	if err != nil || state.State != domain.OperationPermanentError || drive.sessionCount != 0 || drive.createCount != 0 {
		t.Fatalf("corrupt content state=%#v sessions=%d creates=%d err=%v", state, drive.sessionCount, drive.createCount, err)
	}
}

func TestWorkerExecutesRenameTrashAndZeroLengthUpload(t *testing.T) {
	ctx, metadata, content, drive, worker := newWorkerFixture(t)
	object := addRemoteFile(t, ctx, metadata, drive, "remote-empty", "empty.txt", nil, "v1")
	_, writeErr := content.WritePending(ctx, store.WriteRequest{ObjectID: object.ID, Kind: domain.OperationWrite,
		IdempotencyKey: "truncate-empty", BaseDriveVersion: "v1", ExpectedSize: 0}, strings.NewReader(""))
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("zero-byte update worked=%v err=%v", worked, err)
	}
	if drive.files["remote-empty"].Size != 0 || drive.files["remote-empty"].AppProperties[operationProperty] == "" {
		t.Fatalf("zero-byte file metadata = %#v", drive.files["remote-empty"])
	}

	destination, _, err := metadata.CreateLocalFolder(ctx, domain.RootObjectID, "destination")
	if err != nil {
		t.Fatal(err)
	}
	// The destination folder must be created before a remote move depends on it.
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("destination folder create worked=%v err=%v", worked, err)
	}
	destination, err = metadata.ObjectByID(ctx, destination.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.QueueRename(ctx, domain.RootObjectID, "empty.txt", destination.ID, "moved.txt", false); err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("remote move worked=%v err=%v", worked, err)
	}
	if drive.files["remote-empty"].Name != "moved.txt" || !contains(drive.files["remote-empty"].Parents, string(*destination.DriveFileID)) {
		t.Fatalf("remote move was not applied: %#v", drive.files["remote-empty"])
	}
	if _, err := metadata.QueueTrash(ctx, destination.ID, "moved.txt", false); err != nil {
		t.Fatal(err)
	}
	if worked, err := worker.RunOne(ctx); !worked || err != nil {
		t.Fatalf("remote trash worked=%v err=%v", worked, err)
	}
	if !drive.files["remote-empty"].Trashed {
		t.Fatal("remote file was not moved to Drive trash")
	}
}

func newWorkerFixture(t *testing.T) (context.Context, *store.Store, *store.ContentStore, *memoryDrive, *Worker) {
	t.Helper()
	ctx := context.Background()
	metadata, err := store.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metadata.Close() })
	if err := metadata.BeginInitialIndex(ctx, "snapshot", "root-drive"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialIndexPage(ctx, "root-drive", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitInitialChanges(ctx, nil, "", "live"); err != nil {
		t.Fatal(err)
	}
	content, err := store.NewContentStore(metadata, filepath.Join(t.TempDir(), "content"))
	if err != nil {
		t.Fatal(err)
	}
	drive := newMemoryDrive()
	worker, err := New(metadata, content, drive)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, metadata, content, drive, worker
}

func reopenWorker(t *testing.T, metadata *store.Store, content *store.ContentStore, drive *memoryDrive) (*store.Store, *store.ContentStore, *Worker) {
	t.Helper()
	databasePath, contentRoot := metadata.Path(), content.Paths().Root
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedContent, err := store.NewContentStore(reopened, contentRoot)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(reopened, reopenedContent, drive)
	if err != nil {
		t.Fatal(err)
	}
	return reopened, reopenedContent, worker
}

func createQueuedFile(t *testing.T, ctx context.Context, metadata *store.Store, content *store.ContentStore,
	name string, data []byte, idempotency string) (domain.Object, domain.Operation) {
	t.Helper()
	object, err := metadata.CreateLocalFile(ctx, domain.RootObjectID, name, "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	result, err := content.WritePending(ctx, store.WriteRequest{ObjectID: object.ID, Kind: domain.OperationCreate,
		IdempotencyKey: idempotency, ExpectedSize: int64(len(data))}, strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	return object, result.Operation
}

func addRemoteFile(t *testing.T, ctx context.Context, metadata *store.Store, drive *memoryDrive, driveID, name string,
	data []byte, version string) domain.Object {
	t.Helper()
	id := domain.DriveFileID(driveID)
	entry := store.RemoteEntry{Object: domain.Object{ID: domain.ObjectID("drive:" + driveID), DriveFileID: &id,
		MIMEType: "text/plain", Size: int64(len(data)), DriveVersion: version, CapabilitiesJSON: `{"canEdit":true,"canTrash":true}`},
		RemoteName: name, ParentDriveFileID: "root-drive"}
	page, err := metadata.BeginChangePoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CommitChangePage(ctx, page, []store.RemoteChange{{DriveFileID: driveID, Entry: &entry}}, "", "live-"+driveID); err != nil {
		t.Fatal(err)
	}
	file := driveapi.File{ID: driveID, Name: name, MIMEType: entry.Object.MIMEType, Parents: []string{"root-drive"},
		Size: int64(len(data)), Version: version, AppProperties: map[string]string{}, Capabilities: driveapi.Capabilities{CanEdit: true, CanTrash: true}}
	if len(data) > 0 {
		sum := md5.Sum(data)
		file.MD5Checksum = hex.EncodeToString(sum[:])
	}
	drive.files[driveID] = file
	drive.contents[driveID] = append([]byte(nil), data...)
	object, err := metadata.ObjectByID(ctx, domain.ObjectID("drive:"+driveID))
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func bytesRepeated(size int, value byte) []byte {
	return []byte(strings.Repeat(string([]byte{value}), size))
}

func cloneFile(file driveapi.File) driveapi.File {
	file.Parents = append([]string(nil), file.Parents...)
	file.AppProperties = cloneProperties(file.AppProperties)
	return file
}

func cloneProperties(properties map[string]string) map[string]string {
	copyOf := make(map[string]string, len(properties))
	for key, value := range properties {
		copyOf[key] = value
	}
	return copyOf
}

func filePointer(file driveapi.File) *driveapi.File {
	copyOf := cloneFile(file)
	return &copyOf
}

func nextVersion(version string) string {
	var number int
	_, _ = fmt.Sscanf(version, "%d", &number)
	return fmt.Sprint(number + 1)
}
