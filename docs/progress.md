# Implementation progress

## Step 1 — repository and build skeleton

- Added the Go module, placeholder command entry points, build targets, README, and secret/data ignore rules.
- Validation: `GOCACHE=/tmp/google-drive-linux-gocache go test ./...`, `go vet ./...`, `make build`, and both binary `--version` commands passed. The Go cache had to be redirected from the read-only home directory to `/tmp`.
- Git commit: `84906c7` (`chore: initialize Go project and build checks`).
- Limitations: product functionality is not implemented yet; this commit only establishes the project skeleton.

## Step 2 — filesystem and synchronization contracts

- Added the operation contract, durability ordering, recovery rules, queue states, collision handling, cache policy, and conflict behavior in `docs/semantics.md`.
- Recorded accepted architecture choices and experiments still required in `docs/decisions.md`.
- Validation: documentation cross-check against the product and execution plans; no code changed in this step.
- Git commit: `ba82cbd` (`docs: define filesystem and sync semantics`).

## Step 3 — Go FUSE loopback spike

- Added a disposable local loopback mount using `go-fuse/v2` v2.12.0, with path checks that require a separate empty mount directory and reject nested source/mount paths.
- Added validation tests and `scripts/fuse-smoke.sh` for read, write + `sync -f`, rename, mkdir/rmdir, delete, and unmount.
- Validation: `go test ./...`, `go vet ./...`, `make build`, `bash -n scripts/fuse-smoke.sh`, and the binary `--version` passed.
- Live FUSE mount check: not run because `/dev/fuse` is not visible in this execution environment. The smoke script returns its documented skip code 77. Run it in the host session with FUSE access.
- This is an explicitly local-only protocol spike, not a Drive filesystem or durable queue.
- Git commit: `7f4edbc` (`feat: prove Go FUSE read write and recovery path`).

## Step 4 — Qt and AppImage spike

- Added a MIQT/Qt 6 Widgets window with a directory picker, system tray icon and Open/Quit menu, plus a background status update delivered through the Qt event loop. The Qt application is pinned to its OS thread.
- Added pinned, SHA-256-verified AppImage build tools and an x86_64 packaging script for `driveapp`, `drivefs`, Qt libraries, and X11/Wayland/offscreen platform plugins. The AppImage type-2 runtime is also fetched before packaging, so the build does not download it mid-run.
- Validation: `go test ./...`, `go vet ./...`, `make build`, `QT_QPA_PLATFORM=offscreen ./bin/driveapp --smoke`, both binary `--version` commands, shell syntax checks, package build, `--appimage-extract`, extracted `AppRun --smoke`, and `APPIMAGE_EXTRACT_AND_RUN=1 QT_QPA_PLATFORM=offscreen ./dist/Google_Drive_for_Linux-x86_64.AppImage --smoke` passed.
- Toolchain tested: Go 1.26.10, MIQT 0.15.0, Qt 6.11.2, GCC 16.2.1, Fedora 44 x86_64; CGO and `pkg-config Qt6Widgets` are required.
- Desktop limitation: although this process has GNOME Wayland environment variables, opening the compositor socket returns `EPERM`; a minimal C++ Qt application also aborts in that condition. Visual tray display, folder picker, and close/reopen behavior therefore remain unverified in an unrestricted desktop session. FUSE mounting remains unavailable because `/dev/fuse` is absent.
- Packaging limitation: this proves extraction and an offscreen Qt launch on Fedora 44 only. Old glibc compatibility, actual Wayland/X11 rendering, and a real Drive client are not verified. The ordinary AppImage launch also needs a host FUSE configuration; extraction-and-run is only a packaging test.
- Git commit: `7884c94` (`feat: prove Qt Widgets tray and AppImage build`).

## Step 5 — domain model and SQLite schema

- Added typed object, Drive file, inode, content-version, operation, account, sync-state and event models under `internal/domain`.
- Added a versioned SQLite schema for objects, parent/name mappings, immutable content-version references, queue operations and dependencies, account metadata, Drive change token, settings, pins, and error/conflict events. The schema fixes the root inode, allocates persistent non-reused inodes, allows duplicate exact Drive names with distinct persisted visible names, and enforces unique Drive IDs, inode IDs, operation IDs/idempotency keys, and parent/display-name pairs.
- Added transactional object creation, root bootstrap, foreign-key enforcement, mode `0600` for the database, migration SHA-256 checks, and documentation for migration versioning and backups before incompatible changes.
- Driver: `mattn/go-sqlite3` v1.14.52 was provisional during this schema step; step 6 compares it with a CGO-free driver and records the final choice.
- Validation: `go test ./internal/store -count=1`, `go test ./...`, `go vet ./...`, `make build`, and `git diff --check` passed. Tests cover fresh/reopened databases, root and object inode stability, duplicate remote names, unique constraints, foreign keys, atomic failed inserts, and migration checksum protection.
- Limitations: the visible-name collision suffix generator is for the Drive index step; content-file durability, WAL/synchronous policy, and forced-termination recovery are step 6. This step does not connect to Drive or FUSE.
- Git commit: `fec215f` (`feat: add persistent metadata and queue schema`).

## Step 6 — durable content versions and recovery journal

- Added private, separate directories for evictable cache, pinned data, and pending immutable versions. Pending writes stream through a 128 KiB buffer into a private staging file, sync the bytes and directory entry, record size/checksum in SQLite, atomically rename, sync the directory, then commit the complete/current version and queued operation.
- SQLite now uses WAL, `synchronous=FULL`, foreign keys, a 5-second busy timeout, and a 1000-page WAL auto-checkpoint. A connection hook configures the checkpoint threshold; startup verifies all four pooled connections before using the database.
- Startup recovery verifies prepared content by size and SHA-256, publishes valid staged versions, and commits them to the queue. It removes only incomplete generated files with no completed/current or other-version references, preserves checksum-mismatched files for inspection, and removes generated orphans only when no metadata row references them. Repeated recovery is idempotent.
- Writes to the same object are serialized. Each write declares its expected size; a serialized reservation checks `statfs` and rejects a write that would leave less than 10% of the filesystem available. In-flight reservations shrink as bytes are written, so concurrent writers cannot reserve the same free space. A size mismatch or read error cleans up unacknowledged partial writes while retaining the previous acknowledged version; idempotency retries return the recorded version.
- Driver experiment: the complete store suite passed with `mattn/go-sqlite3` v1.14.52 and with CGO-free `modernc.org/sqlite` v1.61.0 under `CGO_ENABLED=0`. Both passed SIGKILL/reopen at all six durability boundaries. The mattn store-test executable was 9.4 MB; modernc was 12.0 MB. Since MIQT already requires CGO and mattn is smaller, D-009 selects mattn. Decision O-005 is resolved.
- Validation: `go test ./internal/store -count=1`, `go test -race ./internal/store -count=1`, the same store suite with CGO-free modernc, `make check` (`go test ./...`, `go vet ./...`, both builds), and `git diff --check` passed. Tests cover zero-byte and streamed 32 MiB content, concurrent writers and reservations, insufficient-space and size-mismatch refusal, failed reads, idempotency, safe orphan cleanup, preserved old versions, recovery idempotence, and actual SIGKILL at each boundary.
- Limitations: SIGKILL tests validate process termination and restart on the current Fedora 44 filesystem; they do not simulate power failure or storage-controller behavior. Real FUSE mounts remain unavailable in this environment, and this step does not upload to Google Drive.
- Git commit: `382e12b` (`feat: persist content versions and recover journal`).

## Next

Step 7: implement desktop OAuth and secure token storage without persisting credentials in SQLite.

