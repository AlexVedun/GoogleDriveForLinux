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
- Driver: `mattn/go-sqlite3` v1.14.52 is provisional for the schema spike because the selected Qt stack already requires CGO. Compare it with a CGO-free SQLite driver during step 6 before fixing crash-durability settings or treating the driver choice as final.
- Validation: `go test ./internal/store -count=1`, `go test ./...`, `go vet ./...`, `make build`, and `git diff --check` passed. Tests cover fresh/reopened databases, root and object inode stability, duplicate remote names, unique constraints, foreign keys, atomic failed inserts, and migration checksum protection.
- Limitations: the visible-name collision suffix generator is for the Drive index step; content-file durability, WAL/synchronous policy, and forced-termination recovery are step 6. This step does not connect to Drive or FUSE.
- Git commit: pending.

## Next

Step 6: implement durable content versions and the recovery journal, then run forced-termination tests.

