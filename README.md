# Google Drive for Linux

Go and Qt 6 desktop client that mounts a user's Google Drive **My Drive** folder through FUSE.

The product is at the prototype stage. The repository has a local-only FUSE loopback spike and a read-only, metadata-only FUSE namespace backed by a ready SQLite index, plus a Qt 6 Widgets window/tray spike and OAuth, Drive API, initial-indexing, and incremental-polling packages. Login, index creation, the mount process, content reads, and scheduled synchronization are not yet wired into one application flow. See [the product plan](plan-google-drive-linux-go-qt.md) and [the implementation plan](LUNA_IMPLEMENTATION_PLAN.md).

The FUSE and sync behavior contract is in [docs/semantics.md](docs/semantics.md); unresolved technical choices are tracked in [docs/decisions.md](docs/decisions.md).

OAuth setup, Secret Service requirements, and the limits of the current sign-in implementation are documented in [docs/oauth.md](docs/oauth.md).

Drive API error categories and retry policy are documented in [docs/drive-api-errors.md](docs/drive-api-errors.md).

## Build and check

```sh
go test ./...
go vet ./...
make build
make qt-smoke
```

Building requires Go, a C++ compiler, `pkg-config`, Qt 6 Widgets development files, and the Qt Wayland plugin package. The MIQT Qt 6 package generates a large set of C++ wrappers, so the first Qt build can take several minutes. FUSE device access is required to run mount integration tests.

`make appimage` downloads pinned linuxdeploy tools into the ignored `.tools/` directory, verifies their SHA-256 digests, and produces `dist/Google_Drive_for_Linux-x86_64.AppImage`. The packaging tools are fetched from the official [linuxdeploy](https://github.com/linuxdeploy/linuxdeploy), [Qt plugin](https://github.com/linuxdeploy/linuxdeploy-plugin-qt), and [AppImage plugin](https://github.com/linuxdeploy/linuxdeploy-plugin-appimage) releases. The Drive API adapter is not yet connected to the indexed `drivefs` mount or Qt app.

The `drivefs` command still supports a **local FUSE loopback spike** for protocol testing: `./bin/drivefs --source SOURCE_DIR --mountpoint MOUNT_DIR` uses an existing empty mount directory and a separate disposable source directory. For an already populated, ready SQLite index, `./bin/drivefs --database PATH --mountpoint MOUNT_DIR` mounts the metadata namespace read-only. Content reads, Google login, and index creation are not connected to this command yet. Run `./scripts/fuse-smoke.sh` for the destructive-operation loopback test; it creates and removes files only in its own temporary directory.

## Data safety contract

Once implemented, a successful local `fsync` will mean that file contents and the queued change are durably stored locally. It will not mean that the change has already reached Google Drive. Pending data must survive restarts and must not be evicted from the cache.

