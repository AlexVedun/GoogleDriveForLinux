//go:build linux

package fusefs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidatePaths resolves and validates a loopback source and an empty mountpoint.
// Neither directory may contain the other, which prevents recursive mounts.
func ValidatePaths(source, mountpoint string) (string, string, error) {
	resolvedSource, err := resolveDirectory(source)
	if err != nil {
		return "", "", fmt.Errorf("source: %w", err)
	}
	resolvedMount, err := resolveDirectory(mountpoint)
	if err != nil {
		return "", "", fmt.Errorf("mountpoint: %w", err)
	}
	if isWithin(resolvedSource, resolvedMount) || isWithin(resolvedMount, resolvedSource) {
		return "", "", fmt.Errorf("source and mountpoint must be separate, non-nested directories")
	}

	entries, err := os.ReadDir(resolvedMount)
	if err != nil {
		return "", "", fmt.Errorf("read mountpoint: %w", err)
	}
	if len(entries) != 0 {
		return "", "", fmt.Errorf("mountpoint must be empty")
	}
	return resolvedSource, resolvedMount, nil
}

func resolveDirectory(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%q is not a directory", resolved)
	}
	return filepath.Clean(resolved), nil
}

func isWithin(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
