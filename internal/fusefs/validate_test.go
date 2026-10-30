//go:build linux

package fusefs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidatePathsAcceptsSeparateEmptyDirectories(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source")
	mountpoint := filepath.Join(base, "mount")
	mustMkdir(t, source)
	mustMkdir(t, mountpoint)

	gotSource, gotMount, err := ValidatePaths(source, mountpoint)
	if err != nil {
		t.Fatalf("ValidatePaths() error = %v", err)
	}
	if gotSource != source || gotMount != mountpoint {
		t.Fatalf("ValidatePaths() = (%q, %q), want (%q, %q)", gotSource, gotMount, source, mountpoint)
	}
}

func TestValidatePathsRejectsNestedDirectories(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source")
	mountpoint := filepath.Join(source, "mount")
	mustMkdir(t, source)
	mustMkdir(t, mountpoint)

	_, _, err := ValidatePaths(source, mountpoint)
	if err == nil || !strings.Contains(err.Error(), "separate, non-nested") {
		t.Fatalf("ValidatePaths() error = %v, want nested-path error", err)
	}
}

func TestValidatePathsRejectsNonEmptyMountpoint(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source")
	mountpoint := filepath.Join(base, "mount")
	mustMkdir(t, source)
	mustMkdir(t, mountpoint)
	if err := os.WriteFile(filepath.Join(mountpoint, "keep.txt"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err := ValidatePaths(source, mountpoint)
	if err == nil || !strings.Contains(err.Error(), "must be empty") {
		t.Fatalf("ValidatePaths() error = %v, want non-empty mountpoint error", err)
	}
}

func TestValidatePathsRejectsSameDirectory(t *testing.T) {
	dir := t.TempDir()
	_, _, err := ValidatePaths(dir, dir)
	if err == nil || !strings.Contains(err.Error(), "separate, non-nested") {
		t.Fatalf("ValidatePaths() error = %v, want same-path error", err)
	}
}

func TestValidatePathsRejectsNonDirectorySource(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source.txt")
	if err := os.WriteFile(source, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	mountpoint := filepath.Join(base, "mount")
	mustMkdir(t, mountpoint)

	_, _, err := ValidatePaths(source, mountpoint)
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("ValidatePaths() error = %v, want not-a-directory error", err)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
}
