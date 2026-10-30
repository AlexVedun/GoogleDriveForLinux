package auth

import (
	"context"
	"io"
	"os/exec"
)

type SystemBrowser struct{}

func (SystemBrowser) OpenURL(ctx context.Context, target string) error {
	command := exec.CommandContext(ctx, "xdg-open", target)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return err
	}
	go func() { _ = command.Wait() }()
	return nil
}
