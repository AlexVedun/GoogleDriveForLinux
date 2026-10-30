package store

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
)

func TestWritePendingPersistsZeroAndLargeFiles(t *testing.T) {
	metadata, content := openContentFixture(t)
	defer metadata.Close()

	testCases := []struct {
		name   string
		size   int64
		reader func() io.Reader
	}{
		{name: "zero", reader: func() io.Reader { return bytes.NewReader(nil) }},
		{name: "large", size: 32 * 1024 * 1024, reader: func() io.Reader { return &patternReader{remaining: 32 * 1024 * 1024} }},
	}
	for index, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			checksum := sha256.New()
			if _, err := io.Copy(checksum, testCase.reader()); err != nil {
				t.Fatal(err)
			}
			request := WriteRequest{
				OperationID: domain.OperationID(fmt.Sprintf("op-content-%d", index)),
				ObjectID:    "content-object", Kind: domain.OperationWrite,
				IdempotencyKey: fmt.Sprintf("write-content-%d", index), BaseDriveVersion: "drive-version-1",
				ExpectedSize: testCase.size,
			}
			result, err := content.WritePending(context.Background(), request, testCase.reader())
			if err != nil {
				t.Fatalf("WritePending(%d bytes): %v", testCase.size, err)
			}
			if result.Operation.State != domain.OperationQueued || !result.ContentVersion.Complete || !result.ContentVersion.Current {
				t.Fatalf("write result is not current and queued: %#v", result)
			}
			if result.ContentVersion.Size != testCase.size {
				t.Fatalf("content size = %d, want %d", result.ContentVersion.Size, testCase.size)
			}
			if result.ContentVersion.SHA256 != hex.EncodeToString(checksum.Sum(nil)) {
				t.Fatalf("content checksum = %q, want %x", result.ContentVersion.SHA256, checksum.Sum(nil))
			}
			file, openedVersion, err := content.OpenContentVersion(context.Background(), result.ContentVersion.ID)
			if err != nil {
				t.Fatal(err)
			}
			actualHash := sha256.New()
			actualSize, readErr := io.Copy(actualHash, file)
			closeErr := file.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("read content version: read=%v close=%v", readErr, closeErr)
			}
			if actualSize != testCase.size || !bytes.Equal(actualHash.Sum(nil), checksum.Sum(nil)) || openedVersion.ID != result.ContentVersion.ID {
				t.Fatalf("persisted content differs for %d-byte write", testCase.size)
			}
			object, err := metadata.ObjectByID(context.Background(), request.ObjectID)
			if err != nil {
				t.Fatal(err)
			}
			if object.Size != testCase.size {
				t.Fatalf("object size = %d, want %d", object.Size, testCase.size)
			}
		})
	}

	paths := content.Paths()
	for _, directory := range []string{paths.EvictableCache, paths.Pinned, paths.Pending} {
		info, err := os.Stat(directory)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Errorf("content directory %q mode = %04o, want 0700", directory, info.Mode().Perm())
		}
	}
}

func TestWritePendingIdempotencyReturnsExistingVersion(t *testing.T) {
	metadata, content := openContentFixture(t)
	defer metadata.Close()
	request := WriteRequest{
		OperationID: "op-idempotent", ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "same-write", BaseDriveVersion: "base-1", ExpectedSize: int64(len("durable contents")),
	}
	first, err := content.WritePending(context.Background(), request, strings.NewReader("durable contents"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := content.WritePending(context.Background(), request, strings.NewReader("different retry bytes are ignored"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Operation.ID != first.Operation.ID || second.ContentVersion.ID != first.ContentVersion.ID {
		t.Fatalf("idempotent retry created a new operation: first=%#v second=%#v", first, second)
	}
	if _, err := content.WritePending(context.Background(), WriteRequest{
		ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "same-write", BaseDriveVersion: "different-base", ExpectedSize: int64(len("durable contents")),
	}, strings.NewReader("ignored")); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("mismatched idempotency retry error = %v, want ErrIdempotencyMismatch", err)
	}
}

func TestFailedWriteKeepsPriorAcknowledgedVersion(t *testing.T) {
	metadata, content := openContentFixture(t)
	defer metadata.Close()
	baseline, err := content.WritePending(context.Background(), WriteRequest{
		OperationID: "op-before-failure", ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "before-failure", ExpectedSize: int64(len("previously acknowledged contents")),
	}, strings.NewReader("previously acknowledged contents"))
	if err != nil {
		t.Fatal(err)
	}
	request := WriteRequest{
		OperationID: "op-write-failure", ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "write-failure", ExpectedSize: 64,
	}
	if _, err := content.WritePending(context.Background(), request, &failAfterReader{remaining: []byte("partial new contents")}); err == nil {
		t.Fatal("WritePending succeeded despite source read failure")
	}
	operation, found, err := content.preparedByIdempotencyKey(context.Background(), request.IdempotencyKey)
	if err != nil || !found || operation.state != domain.OperationPermanentError {
		t.Fatalf("failed operation = %#v found=%v err=%v", operation, found, err)
	}
	current, err := content.CurrentContentVersion(context.Background(), request.ObjectID)
	if err != nil || current.ID != baseline.ContentVersion.ID {
		t.Fatalf("failed write changed current version: current=%#v err=%v", current, err)
	}
	if _, err := os.Lstat(content.stagingPath(operation.versionID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed write left its partial staging file behind: %v", err)
	}
}

func TestDeclaredSizeMustMatchAndDiskSpaceIsReserved(t *testing.T) {
	metadata, content := openContentFixture(t)
	defer metadata.Close()
	available := uint64(6000)
	content.filesystemSpace = func(string) (uint64, uint64, error) { return available, 10000, nil }
	reservation, err := content.reserveDiskSpace(3000)
	if err != nil {
		t.Fatal(err)
	}
	available -= 1500
	reservation.consume(1500)
	if _, err := content.reserveDiskSpace(2001); !errors.Is(err, ErrInsufficientDiskSpace) {
		t.Fatalf("second overlapping reservation error = %v, want ErrInsufficientDiskSpace", err)
	}
	reservation.release()

	for _, testCase := range []struct {
		name string
		size int64
		data string
	}{
		{name: "short", size: 10, data: "short"},
		{name: "long", size: 2, data: "longer"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := content.WritePending(context.Background(), WriteRequest{
				OperationID: domain.OperationID("op-size-" + testCase.name),
				ObjectID:    "content-object", Kind: domain.OperationWrite,
				IdempotencyKey: "size-" + testCase.name, ExpectedSize: testCase.size,
			}, strings.NewReader(testCase.data)); err == nil {
				t.Fatal("write with a mismatched declared size was accepted")
			}
			operation, found, err := content.preparedByIdempotencyKey(context.Background(), "size-"+testCase.name)
			if err != nil || !found || operation.state != domain.OperationPermanentError {
				t.Fatalf("mismatched-size operation = %#v found=%v err=%v", operation, found, err)
			}
		})
	}

	available = 999
	if _, err := content.WritePending(context.Background(), WriteRequest{
		OperationID: "op-low-space", ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "low-space", ExpectedSize: 0,
	}, bytes.NewReader(nil)); !errors.Is(err, ErrInsufficientDiskSpace) {
		t.Fatalf("low-space write error = %v, want ErrInsufficientDiskSpace", err)
	}
	var count int
	if err := metadata.db.QueryRow(`SELECT COUNT(*) FROM operations WHERE idempotency_key = 'low-space'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("low-space write created %d queue operations, want none", count)
	}
}

func TestConcurrentWritesToOneObjectAreSerialized(t *testing.T) {
	metadata, content := openContentFixture(t)
	defer metadata.Close()
	const writers = 8
	type result struct {
		write WriteResult
		err   error
	}
	results := make(chan result, writers)
	var wait sync.WaitGroup
	for index := 0; index < writers; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			payload := fmt.Sprintf("concurrent writer %d", index)
			write, err := content.WritePending(context.Background(), WriteRequest{
				OperationID: domain.OperationID(fmt.Sprintf("op-concurrent-%d", index)),
				ObjectID:    "content-object", Kind: domain.OperationWrite,
				IdempotencyKey: fmt.Sprintf("concurrent-%d", index),
				ExpectedSize:   int64(len(payload)),
			}, strings.NewReader(payload))
			results <- result{write: write, err: err}
		}(index)
	}
	wait.Wait()
	close(results)
	seen := make(map[domain.ContentVersionID]struct{}, writers)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.write.Operation.State != domain.OperationQueued || !result.write.ContentVersion.Complete {
			t.Fatalf("concurrent write was not queued: %#v", result.write)
		}
		seen[result.write.ContentVersion.ID] = struct{}{}
	}
	if len(seen) != writers {
		t.Fatalf("got %d unique content versions, want %d", len(seen), writers)
	}
	current, err := content.CurrentContentVersion(context.Background(), "content-object")
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := seen[current.ID]; !exists {
		t.Fatalf("current content version %q was not produced by a writer", current.ID)
	}
	var currentCount int
	if err := metadata.db.QueryRow(`SELECT COUNT(*) FROM content_versions WHERE object_id = ? AND is_current = 1`, "content-object").Scan(&currentCount); err != nil {
		t.Fatal(err)
	}
	if currentCount != 1 {
		t.Fatalf("current content version count = %d, want exactly one", currentCount)
	}
}

func TestRecoveryRemovesOnlyGeneratedUnreferencedPendingFiles(t *testing.T) {
	metadata, content := openContentFixture(t)
	defer metadata.Close()
	result, err := content.WritePending(context.Background(), WriteRequest{
		OperationID: "op-preserved", ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "preserve-me", ExpectedSize: int64(len("referenced data")),
	}, strings.NewReader("referenced data"))
	if err != nil {
		t.Fatal(err)
	}
	orphanVersion := domain.ContentVersionID("cv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	orphanStage := filepath.Join(content.Paths().Pending, "."+string(orphanVersion)+".tmp")
	orphanBlob := filepath.Join(content.Paths().Pending, string(orphanVersion)+".blob")
	unrecognized := filepath.Join(content.Paths().Pending, "user-notes.txt")
	for _, path := range []string{orphanStage, orphanBlob, unrecognized} {
		if err := os.WriteFile(path, []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := content.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{orphanStage, orphanBlob} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("generated orphan %q was not removed: %v", path, err)
		}
	}
	for _, path := range []string{unrecognized, content.blobPath(result.ContentVersion.ID)} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("unrelated or referenced file %q was removed: %v", path, err)
		}
	}
}

func TestRecoveryPreservesChecksumMismatchForInspection(t *testing.T) {
	metadata, content := openContentFixture(t)
	defer metadata.Close()
	request := WriteRequest{
		OperationID: "op-corrupt-stage", ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "corrupt-stage", ExpectedSize: 9,
	}
	versionID := domain.ContentVersionID("cv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if _, err := content.createPrepared(context.Background(), request, versionID); err != nil {
		t.Fatal(err)
	}
	stagingPath := content.stagingPath(versionID)
	staged := []byte("bad bytes")
	if err := os.WriteFile(stagingPath, staged, 0o600); err != nil {
		t.Fatal(err)
	}
	if file, err := os.OpenFile(stagingPath, os.O_RDWR, 0); err != nil {
		t.Fatal(err)
	} else {
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := syncDirectory(content.Paths().Pending); err != nil {
		t.Fatal(err)
	}
	wrongDigest := sha256.Sum256([]byte("ok bytes!"))
	if err := content.recordExpectedContent(context.Background(), versionID, int64(len(staged)), hex.EncodeToString(wrongDigest[:])); err != nil {
		t.Fatal(err)
	}
	if _, err := content.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stagingPath); err != nil {
		t.Fatalf("checksum-mismatched stage was discarded instead of preserved: %v", err)
	}
	operation, found, err := content.preparedByIdempotencyKey(context.Background(), request.IdempotencyKey)
	if err != nil || !found || operation.state != domain.OperationPermanentError {
		t.Fatalf("mismatched operation = %#v found=%v err=%v", operation, found, err)
	}
}

func TestRecoveryAfterSIGKILLAtEveryDurabilityBoundary(t *testing.T) {
	checkpoints := []string{
		"prepared-commit", "staging-sync", "expected-commit", "rename", "directory-sync", "final-commit",
	}
	for _, checkpoint := range checkpoints {
		t.Run(checkpoint, func(t *testing.T) {
			databasePath := filepath.Join(t.TempDir(), "state", "metadata.db")
			contentRoot := filepath.Join(filepath.Dir(databasePath), "content")
			metadata, err := Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			entry := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "crash-object", RemoteName: "file", DisplayName: "file"}
			if _, err := metadata.CreateObject(context.Background(), domain.Object{ID: entry.ObjectID, MIMEType: "text/plain"}, entry); err != nil {
				t.Fatal(err)
			}
			content, err := NewContentStore(metadata, contentRoot)
			if err != nil {
				t.Fatal(err)
			}
			baseline, err := content.WritePending(context.Background(), WriteRequest{
				OperationID: "op-prior-ack", ObjectID: "crash-object", Kind: domain.OperationWrite,
				IdempotencyKey: "prior-ack", ExpectedSize: int64(len("previously acknowledged local contents")),
			}, strings.NewReader("previously acknowledged local contents"))
			if err != nil {
				t.Fatal(err)
			}
			if err := metadata.Close(); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(os.Args[0], "-test.run=^TestContentStoreCrashWorker$")
			command.Env = append(os.Environ(),
				"GDL_CRASH_WORKER=1",
				"GDL_CRASH_DB="+databasePath,
				"GDL_CRASH_CONTENT="+contentRoot,
				"GDL_CRASH_CHECKPOINT="+checkpoint,
			)
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			checkpointSeen := make(chan error, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if scanner.Text() == "CHECKPOINT:"+checkpoint {
						checkpointSeen <- nil
						return
					}
				}
				if err := scanner.Err(); err != nil {
					checkpointSeen <- err
					return
				}
				checkpointSeen <- fmt.Errorf("worker exited before checkpoint %q", checkpoint)
			}()
			select {
			case err := <-checkpointSeen:
				if err != nil {
					_ = command.Process.Kill()
					_ = command.Wait()
					t.Fatalf("wait for crash checkpoint: %v; stderr: %s", err, stderr.String())
				}
			case <-time.After(20 * time.Second):
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatalf("timed out waiting for checkpoint %q; stderr: %s", checkpoint, stderr.String())
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatalf("send SIGKILL to worker: %v", err)
			}
			waitErr := command.Wait()
			exitErr, ok := waitErr.(*exec.ExitError)
			if !ok {
				t.Fatalf("worker exit = %v, want SIGKILL; stderr: %s", waitErr, stderr.String())
			}
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("worker status = %#v, want SIGKILL", exitErr.Sys())
			}

			metadata, err = Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer metadata.Close()
			content, err = NewContentStore(metadata, contentRoot)
			if err != nil {
				t.Fatal(err)
			}
			report, err := content.Recover(context.Background())
			if err != nil {
				t.Fatalf("recover after %s: %v", checkpoint, err)
			}
			wantQueued := checkpoint == "expected-commit" || checkpoint == "rename" || checkpoint == "directory-sync" || checkpoint == "final-commit"
			operation, found, err := content.preparedByIdempotencyKey(context.Background(), "crash-write")
			if err != nil || !found {
				t.Fatalf("read recovered operation: found=%v err=%v", found, err)
			}
			if wantQueued {
				if operation.state != domain.OperationQueued {
					t.Fatalf("operation state = %q, want queued after %s", operation.state, checkpoint)
				}
				file, version, err := content.OpenContentVersion(context.Background(), operation.versionID)
				if err != nil {
					t.Fatalf("open recovered version: %v", err)
				}
				actual, readErr := io.ReadAll(file)
				closeErr := file.Close()
				if readErr != nil || closeErr != nil {
					t.Fatalf("read recovered version: read=%v close=%v", readErr, closeErr)
				}
				if string(actual) != crashPayload || !version.Complete || !version.Current {
					t.Fatalf("recovered data/version incorrect at %s: bytes=%q version=%#v", checkpoint, actual, version)
				}
			} else if operation.state != domain.OperationPermanentError {
				t.Fatalf("operation state = %q, want permanent-error before checksum commit", operation.state)
			}
			current, err := content.CurrentContentVersion(context.Background(), "crash-object")
			if err != nil {
				t.Fatalf("read current version after %s: %v", checkpoint, err)
			}
			if wantQueued && current.ID != operation.versionID {
				t.Fatalf("current version = %q, want recovered %q", current.ID, operation.versionID)
			}
			if !wantQueued && current.ID != baseline.ContentVersion.ID {
				t.Fatalf("incomplete write replaced prior acknowledged version: current=%q prior=%q", current.ID, baseline.ContentVersion.ID)
			}
			previousFile, previousVersion, err := content.OpenContentVersion(context.Background(), baseline.ContentVersion.ID)
			if err != nil {
				t.Fatalf("open prior acknowledged version after %s: %v", checkpoint, err)
			}
			previousBytes, readErr := io.ReadAll(previousFile)
			closeErr := previousFile.Close()
			if readErr != nil || closeErr != nil || string(previousBytes) != "previously acknowledged local contents" || !previousVersion.Complete {
				t.Fatalf("prior acknowledged version was not preserved: bytes=%q version=%#v read=%v close=%v", previousBytes, previousVersion, readErr, closeErr)
			}
			if _, err := content.Recover(context.Background()); err != nil {
				t.Fatalf("second recovery after %s: %v", checkpoint, err)
			}
			if wantQueued && report.RecoveredOperations != 1 && checkpoint != "final-commit" {
				t.Fatalf("recovery report = %#v, want one recovered operation", report)
			}
			if !wantQueued && report.FailedOperations != 1 {
				t.Fatalf("recovery report = %#v, want one safely failed incomplete operation", report)
			}
		})
	}
}

const crashPayload = "bytes committed before the injected SIGKILL"

func TestContentStoreCrashWorker(t *testing.T) {
	if os.Getenv("GDL_CRASH_WORKER") != "1" {
		return
	}
	metadata, err := Open(os.Getenv("GDL_CRASH_DB"))
	if err != nil {
		t.Fatalf("open worker database: %v", err)
	}
	content, err := NewContentStore(metadata, os.Getenv("GDL_CRASH_CONTENT"))
	if err != nil {
		t.Fatalf("open worker content store: %v", err)
	}
	wanted := os.Getenv("GDL_CRASH_CHECKPOINT")
	content.checkpoint = func(actual string) {
		if actual == wanted {
			fmt.Printf("CHECKPOINT:%s\n", actual)
			_ = os.Stdout.Sync()
			select {}
		}
	}
	_, err = content.WritePending(context.Background(), WriteRequest{
		OperationID: "op-crash-worker", ObjectID: "crash-object", Kind: domain.OperationWrite,
		IdempotencyKey: "crash-write", BaseDriveVersion: "base-crash", ExpectedSize: int64(len(crashPayload)),
	}, strings.NewReader(crashPayload))
	if err != nil {
		t.Fatalf("write crash fixture: %v", err)
	}
	t.Fatalf("worker completed without reaching checkpoint %q", wanted)
}

func openContentFixture(t *testing.T) (*Store, *ContentStore) {
	t.Helper()
	root := t.TempDir()
	metadata, err := Open(filepath.Join(root, "state", "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	entry := domain.DirectoryEntry{ParentID: domain.RootObjectID, ObjectID: "content-object", RemoteName: "file", DisplayName: "file"}
	if _, err := metadata.CreateObject(context.Background(), domain.Object{ID: entry.ObjectID, MIMEType: "text/plain"}, entry); err != nil {
		_ = metadata.Close()
		t.Fatal(err)
	}
	content, err := NewContentStore(metadata, filepath.Join(root, "content"))
	if err != nil {
		_ = metadata.Close()
		t.Fatal(err)
	}
	return metadata, content
}

type failAfterReader struct {
	remaining []byte
}

func (r *failAfterReader) Read(buffer []byte) (int, error) {
	if len(r.remaining) == 0 {
		return 0, errors.New("injected source read failure")
	}
	n := copy(buffer, r.remaining)
	r.remaining = r.remaining[n:]
	return n, nil
}

type patternReader struct {
	remaining int64
	offset    int64
}

func (r *patternReader) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	count := len(buffer)
	if int64(count) > r.remaining {
		count = int(r.remaining)
	}
	for index := 0; index < count; index++ {
		position := r.offset + int64(index)
		buffer[index] = byte((position*31 + position/251) % 251)
	}
	r.offset += int64(count)
	r.remaining -= int64(count)
	return count, nil
}
