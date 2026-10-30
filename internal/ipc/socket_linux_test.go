//go:build linux

package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testHandler struct{}

func (testHandler) Handle(_ context.Context, request Request) (any, bool, error) {
	if request.Command == CommandQuit {
		return map[string]string{"result": "stopped"}, true, nil
	}
	return map[string]string{"command": string(request.Command)}, false, nil
}

func TestServerClientSecurityAndSingleInstance(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "drivefs.sock")
	server, err := Listen(path, testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(context.Background()) }()

	client, err := NewClient(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]string
	if _, err := client.Call(context.Background(), CommandStatus, nil, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["command"] != string(CommandStatus) {
		t.Fatalf("unexpected response payload: %#v", payload)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o, want 600", info.Mode().Perm())
	}
	if _, err := Listen(path, testHandler{}); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second listen error = %v, want ErrAlreadyRunning", err)
	}
	if _, err := client.Call(context.Background(), CommandQuit, nil, &payload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.Done():
	case <-time.After(time.Second):
		t.Fatal("server did not stop after quit command")
	}
	if err := <-serveResult; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket path remains after stop: %v", err)
	}
}

func TestListenRejectsInsecureDirectoryAndSocketPath(t *testing.T) {
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(filepath.Join(parent, "drivefs.sock"), testHandler{}); err == nil {
		t.Fatal("insecure parent directory was accepted")
	}

	private := t.TempDir()
	if err := os.Chmod(private, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(private, "drivefs.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path, testHandler{}); err == nil {
		t.Fatal("regular file at socket path was accepted")
	}
}

func TestListenRemovesStaleOwnedSocketAndRejectsBadVersion(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "drivefs.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := Listen(path, testHandler{})
	if err != nil {
		t.Fatalf("listen should replace a stale owned socket: %v", err)
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(context.Background()) }()
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(conn).Encode(Request{Version: ProtocolVersion + 1, ID: "bad-version", Command: CommandStatus}); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if response.OK || response.ErrorCode != "unsupported_version" {
		t.Fatalf("wrong protocol response: %+v", response)
	}
	_ = server.Close()
	select {
	case <-server.Done():
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
	if err := <-serveResult; err != nil {
		t.Fatal(err)
	}
}
