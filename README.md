# Google Drive for Linux

Go and Qt 6 desktop client that mounts a user's Google Drive **My Drive** folder through FUSE.

The product is at the prototype stage. The per-user service and Qt client now share OAuth, initial and incremental indexing, a bounded content cache, durable queued writes, resumable uploads, offline pin downloads, conflict status, and a writable FUSE mount. The GUI can sign in, mount/unmount, show the queue and conflicts, manage pins, change the cache limit, and quit the service. See [the product plan](plan-google-drive-linux-go-qt.md) and [the implementation plan](LUNA_IMPLEMENTATION_PLAN.md).

The FUSE and sync behavior contract is in [docs/semantics.md](docs/semantics.md); unresolved technical choices are tracked in [docs/decisions.md](docs/decisions.md).

OAuth setup, Secret Service requirements, and sign-in behavior are documented in [docs/oauth.md](docs/oauth.md).

Drive API error categories and retry policy are documented in [docs/drive-api-errors.md](docs/drive-api-errors.md).

## Build and check

```sh
go test ./...
go vet ./...
make build
make qt-smoke
```

Building requires Go, a C++ compiler, `pkg-config`, Qt 6 Widgets development files, and the Qt Wayland plugin package. The MIQT Qt 6 package generates a large set of C++ wrappers, so the first Qt build can take several minutes. FUSE device access is required to run mount integration tests.

`make appimage` downloads pinned linuxdeploy tools into the ignored `.tools/` directory, verifies their SHA-256 digests, and produces `dist/Google_Drive_for_Linux-x86_64.AppImage`. The packaging tools are fetched from the official [linuxdeploy](https://github.com/linuxdeploy/linuxdeploy), [Qt plugin](https://github.com/linuxdeploy/linuxdeploy-plugin-qt), and [AppImage plugin](https://github.com/linuxdeploy/linuxdeploy-plugin-appimage) releases.

Make the AppImage executable and launch it with `./Google_Drive_for_Linux-x86_64.AppImage`. Configure `GOOGLE_OAUTH_CLIENT_ID` for the desktop OAuth client before launching. The system needs a working FUSE setup to mount the Drive filesystem; extracting the AppImage with `--appimage-extract` avoids FUSE for launching the bundle but does not provide FUSE access to the mounted Drive. The image contains its third-party license texts under `usr/share/doc/google-drive-linux/`; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the bundled components and release licensing checks.

The direct `drivefs --source SOURCE_DIR --mountpoint MOUNT_DIR` mode remains a **local FUSE loopback spike** for protocol tests. The normal Qt app starts one per-user `drivefs --ipc-server` service. Set `GOOGLE_OAUTH_CLIENT_ID` before launching `driveapp` (for example, `GOOGLE_OAUTH_CLIENT_ID=your-desktop-client-id ./bin/driveapp`); after signing in, the service indexes My Drive and enables the write-enabled FUSE mount. Without OAuth configuration, a ready index can still be mounted read-only. Run `./scripts/fuse-smoke.sh` for the loopback test; it creates and removes files only in its own temporary directory.

## Data safety contract

In the write-enabled FUSE implementation, a successful local `flush` or `fsync` means that file contents and the queued change are durably stored locally. It does not mean that the change has reached Google Drive. The sync worker advances the operation asynchronously and commits it only after verifying the Drive result. The Qt queue shows local pending work separately from confirmed Drive uploads. Pending data must survive restarts and must not be evicted from the cache.

When a remote edit is detected before or after an upload, the implementation keeps the remote file and saves local bytes under a neighboring “local conflict” filename. Namespace conflicts retain the requested rename/trash operation for an explicit choice. Google Drive does not document an atomic version precondition for `files.update`, so an edit made after the worker's preflight read and before its update can still be overwritten without detection; the client does not claim race-free conflict protection.

Offline pins protect files from cache eviction. Pinning a folder includes its descendants and children discovered later. Pinned and queued content may exceed the ordinary cache limit; the local worker reports download progress and the protected-data overage. New durable writes are refused if they would leave less than 10% of the backing filesystem free.

