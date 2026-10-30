//go:build linux

package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"google-drive-linux/internal/ipc"
)

func ConnectOrStartDriveFS(socketPath string) (*ipc.Client, error) {
	if socketPath == "" {
		var err error
		socketPath, err = ipc.DefaultSocketPath()
		if err != nil {
			return nil, err
		}
	}
	if client, err := ipc.NewClient(socketPath, 3*time.Second); err == nil {
		if _, callErr := client.Call(context.Background(), ipc.CommandStatus, nil, nil); callErr == nil {
			return client, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		if _, statErr := os.Lstat(socketPath); statErr == nil {
			return nil, err
		}
	}
	if err := startDriveFS(socketPath); err != nil && !errors.Is(err, ipc.ErrAlreadyRunning) {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if client, err := ipc.NewClient(socketPath, 3*time.Second); err == nil {
			if _, callErr := client.Call(context.Background(), ipc.CommandStatus, nil, nil); callErr == nil {
				return client, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, errors.New("drivefs did not create its IPC socket within five seconds")
}

func startDriveFS(socketPath string) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find driveapp executable: %w", err)
	}
	candidates := []string{filepath.Join(filepath.Dir(executable), "drivefs")}
	if found, err := exec.LookPath("drivefs"); err == nil {
		candidates = append(candidates, found)
	}
	var drivefs string
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
			drivefs = candidate
			break
		}
	}
	if drivefs == "" {
		return errors.New("drivefs executable was not found next to driveapp or on PATH")
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open detached process output: %w", err)
	}
	defer devNull.Close()
	command := exec.Command(drivefs, "--ipc-server", "--socket", socketPath)
	command.Stdin, command.Stdout, command.Stderr = devNull, devNull, devNull
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return fmt.Errorf("start drivefs service: %w", err)
	}
	if err := command.Process.Release(); err != nil {
		return fmt.Errorf("detach drivefs service: %w", err)
	}
	return nil
}
