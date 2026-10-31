//go:build linux

package drivecontrol

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const (
	DefaultCacheLimitBytes int64 = 1 << 30
	MinCacheLimitBytes     int64 = 64 << 20
	MaxCacheLimitBytes     int64 = 1 << 40
)

type Settings struct {
	CacheLimitBytes int64  `json:"cache_limit_bytes"`
	OAuthClientID   string `json:"oauth_client_id,omitempty"`
	Database        string `json:"database,omitempty"`
	Mountpoint      string `json:"mountpoint,omitempty"`
}

type SettingsUpdate struct {
	CacheLimitBytes int64  `json:"cache_limit_bytes"`
	Database        string `json:"database,omitempty"`
	Mountpoint      string `json:"mountpoint,omitempty"`
}

func DefaultSettingsPath() (string, error) {
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
		return "", fmt.Errorf("create application config directory: %w", err)
	}
	if err := os.Chmod(configDir, 0o700); err != nil {
		return "", fmt.Errorf("secure application config directory: %w", err)
	}
	if err := verifySettingsDirectory(configDir); err != nil {
		return "", err
	}
	return filepath.Join(configDir, "settings.json"), nil
}

func defaultSettings(database string) Settings {
	return Settings{CacheLimitBytes: DefaultCacheLimitBytes, Database: database}
}

func LoadSettings(path, database string) (Settings, error) {
	return readSettings(path, defaultSettings(database))
}

func readSettings(path string, fallback Settings) (Settings, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return fallback, nil
	}
	if err != nil {
		return Settings{}, fmt.Errorf("inspect settings file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Mode()&os.ModeSymlink != 0 {
		return Settings{}, errors.New("settings file must be a private regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) {
		return Settings{}, errors.New("settings file is not owned by the current user")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Settings{}, fmt.Errorf("read settings file: %w", err)
	}
	var settings Settings
	if err := json.Unmarshal(data, &settings); err != nil {
		return Settings{}, fmt.Errorf("decode settings file: %w", err)
	}
	if err := validateSettings(settings); err != nil {
		return Settings{}, err
	}
	if settings.Database == "" {
		settings.Database = fallback.Database
	}
	return settings, nil
}

func writeSettings(path string, settings Settings) error {
	if err := validateSettings(settings); err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := verifySettingsDirectory(directory); err != nil {
		return err
	}
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("encode settings: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".settings-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary settings file: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure temporary settings file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write settings: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync settings: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close settings: %w", err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("publish settings: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open settings directory: %w", err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return fmt.Errorf("sync settings directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close settings directory: %w", closeErr)
	}
	return nil
}

func verifySettingsDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect settings directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("settings directory %q must be a private directory with mode 0700 or stricter", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) {
		return fmt.Errorf("settings directory %q is not owned by the current user", path)
	}
	return nil
}

func validateSettings(settings Settings) error {
	if settings.CacheLimitBytes < MinCacheLimitBytes || settings.CacheLimitBytes > MaxCacheLimitBytes {
		return fmt.Errorf("cache limit must be between %d and %d bytes", MinCacheLimitBytes, MaxCacheLimitBytes)
	}
	if len(settings.OAuthClientID) > 512 {
		return errors.New("OAuth client ID is too long")
	}
	return nil
}
