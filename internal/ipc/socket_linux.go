//go:build linux

package ipc

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const maxMessageBytes = 4 << 20

var ErrAlreadyRunning = errors.New("drivefs IPC server is already running")

// DefaultSocketPath returns a socket path in a verified, private runtime
// directory, or in a private per-user config directory when no suitable
// runtime directory is available.
func DefaultSocketPath() (string, error) {
	if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir != "" {
		if err := verifyPrivateDirectory(runtimeDir); err == nil {
			return filepath.Join(runtimeDir, "google-drive-linux.sock"), nil
		}
	}
	configDir := os.Getenv("XDG_CONFIG_HOME")
	if configDir == "" {
		var err error
		configDir, err = os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("find user config directory: %w", err)
		}
	}
	configDir = filepath.Join(configDir, "google-drive-linux")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return "", fmt.Errorf("create IPC directory: %w", err)
	}
	if err := os.Chmod(configDir, 0o700); err != nil {
		return "", fmt.Errorf("secure IPC directory: %w", err)
	}
	if err := verifyPrivateDirectory(configDir); err != nil {
		return "", err
	}
	return filepath.Join(configDir, "drivefs.sock"), nil
}

func verifyPrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect IPC directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("IPC directory %q must be a private directory with mode 0700 or stricter", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) {
		return fmt.Errorf("IPC directory %q is not owned by the current user", path)
	}
	return nil
}

func verifySocket(path string) (os.FileInfo, error) {
	if err := verifyPrivateDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("IPC path %q must be a private Unix socket with mode 0600 or stricter", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("IPC socket %q is not owned by the current user", path)
	}
	return info, nil
}

type Server struct {
	listener *net.UnixListener
	path     string
	info     os.FileInfo
	handler  Handler
	done     chan struct{}
	close    sync.Once
	serve    sync.Once
	wg       sync.WaitGroup
}

func Listen(path string, handler Handler) (*Server, error) {
	if handler == nil {
		return nil, errors.New("IPC handler is required")
	}
	if len(path) >= 104 {
		return nil, errors.New("IPC socket path is too long")
	}
	if err := verifyPrivateDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(path); err == nil {
		if _, err := verifySocket(path); err != nil {
			return nil, err
		}
		conn, dialErr := net.DialTimeout("unix", path, 150*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return nil, ErrAlreadyRunning
		}
		current, statErr := os.Lstat(path)
		if statErr != nil || !os.SameFile(info, current) {
			return nil, errors.New("IPC socket changed while checking stale socket")
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale IPC socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect IPC socket path: %w", err)
	}
	addr := &net.UnixAddr{Name: path, Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on IPC socket: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = removeSocketIfOwned(path, nil)
		return nil, fmt.Errorf("secure IPC socket: %w", err)
	}
	info, err := verifySocket(path)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	return &Server{listener: listener, path: path, info: info, handler: handler, done: make(chan struct{})}, nil
}

func (s *Server) Done() <-chan struct{} { return s.done }

func (s *Server) Serve(ctx context.Context) error {
	if ctx == nil {
		return errors.New("IPC serve context is required")
	}
	var serveErr error
	s.serve.Do(func() {
		stop := context.AfterFunc(ctx, func() { _ = s.listener.Close() })
		defer stop()
		defer func() {
			s.wg.Wait()
			_ = removeSocketIfOwned(s.path, s.info)
			close(s.done)
		}()
		for {
			conn, err := s.listener.AcceptUnix()
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
					return
				}
				serveErr = fmt.Errorf("accept IPC connection: %w", err)
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serveConn(conn)
			}()
		}
	})
	return serveErr
}

func (s *Server) Close() error {
	var err error
	s.close.Do(func() {
		err = s.listener.Close()
	})
	return err
}

func (s *Server) serveConn(conn *net.UnixConn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := verifyPeerUID(conn); err != nil {
		return
	}
	request, err := decodeRequest(conn)
	if err != nil {
		_ = writeResponse(conn, Response{Version: ProtocolVersion, OK: false, ErrorCode: "bad_request", Error: err.Error()})
		return
	}
	response := Response{Version: ProtocolVersion, ID: request.ID, OK: true}
	if request.Version != ProtocolVersion {
		response.OK, response.ErrorCode, response.Error = false, "unsupported_version", ErrUnsupportedVersion.Error()
	} else if request.ID == "" || request.Command == "" {
		response.OK, response.ErrorCode, response.Error = false, "bad_request", "request ID and command are required"
	} else {
		payload, shutdown, handleErr := s.handler.Handle(context.Background(), request)
		if handleErr != nil {
			response.OK, response.ErrorCode, response.Error = false, "command_failed", handleErr.Error()
		} else if payload != nil {
			response.Payload, err = json.Marshal(payload)
			if err != nil {
				response.OK, response.ErrorCode, response.Error = false, "encode_failed", err.Error()
			}
		}
		response.Shutdown = shutdown
	}
	if writeResponse(conn, response) == nil && response.Shutdown {
		_ = s.Close()
	}
}

func verifyPeerUID(conn *net.UnixConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var credential *unix.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		credential, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return err
	}
	if socketErr != nil {
		return socketErr
	}
	if credential == nil || credential.Uid != uint32(os.Geteuid()) {
		return errors.New("IPC peer belongs to another user")
	}
	return nil
}

func decodeRequest(conn net.Conn) (Request, error) {
	var request Request
	line, err := bufio.NewReader(io.LimitReader(conn, maxMessageBytes+1)).ReadBytes('\n')
	if err != nil {
		return Request{}, fmt.Errorf("decode IPC request: %w", err)
	}
	if len(line) > maxMessageBytes {
		return Request{}, errors.New("IPC request exceeds the size limit")
	}
	if err := json.Unmarshal(line, &request); err != nil {
		return Request{}, fmt.Errorf("decode IPC request: %w", err)
	}
	return request, nil
}

func writeResponse(conn net.Conn, response Response) error {
	encoder := json.NewEncoder(conn)
	return encoder.Encode(response)
}

func removeSocketIfOwned(path string, expected os.FileInfo) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := verifySocket(path); err != nil {
		return err
	}
	if expected != nil && !os.SameFile(expected, info) {
		return errors.New("IPC socket changed before cleanup")
	}
	return os.Remove(path)
}

type Client struct {
	path    string
	timeout time.Duration
}

func NewClient(path string, timeout time.Duration) (*Client, error) {
	if _, err := verifySocket(path); err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{path: path, timeout: timeout}, nil
}

func (c *Client) Call(ctx context.Context, command Command, payload any, out any) (Response, error) {
	if ctx == nil {
		return Response{}, errors.New("IPC call context is required")
	}
	var payloadJSON json.RawMessage
	var err error
	if payload != nil {
		payloadJSON, err = json.Marshal(payload)
		if err != nil {
			return Response{}, fmt.Errorf("encode IPC payload: %w", err)
		}
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return Response{}, fmt.Errorf("create IPC request ID: %w", err)
	}
	request := Request{Version: ProtocolVersion, ID: hex.EncodeToString(idBytes), Command: command, Payload: payloadJSON}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.path)
	if err != nil {
		return Response{}, fmt.Errorf("connect to drivefs: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return Response{}, fmt.Errorf("send IPC request: %w", err)
	}
	var response Response
	if err := json.NewDecoder(io.LimitReader(conn, maxMessageBytes)).Decode(&response); err != nil {
		return Response{}, fmt.Errorf("read IPC response: %w", err)
	}
	if response.Version != ProtocolVersion {
		return response, ErrUnsupportedVersion
	}
	if response.ID != request.ID {
		return response, errors.New("IPC response ID does not match request")
	}
	if !response.OK {
		return response, &Error{Code: response.ErrorCode, Message: response.Error}
	}
	if out != nil && len(response.Payload) != 0 {
		if err := json.Unmarshal(response.Payload, out); err != nil {
			return response, fmt.Errorf("decode IPC response payload: %w", err)
		}
	}
	return response, nil
}
