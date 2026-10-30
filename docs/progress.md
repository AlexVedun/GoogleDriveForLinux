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

## Step 7 — desktop OAuth and system credential storage

- Added an installed-app OAuth flow with an IPv4 loopback listener on a random port, random state validation, PKCE S256, offline access, browser launch, callback timeout, token refresh with persistence of rotated refresh tokens, reauthorization handling for `invalid_grant`, and token revocation.
- Added a Secret Service-backed credential store through go-keyring. It has no plaintext or SQLite fallback, maps missing entries, honors caller deadlines, and hides backend diagnostics that could contain sensitive values.
- Documented manual Google Cloud setup and current sign-in boundaries in `docs/oauth.md`; no real credentials or account data are stored in the repository. The UI does not yet collect client settings or invoke sign-in; that wiring belongs to later app steps.
- Validation: `go test ./internal/auth ./internal/secretstore -count=1` passed with local loopback access. Tests cover PKCE, wrong-state rejection, valid callback, callback timeout, access denial, successful refresh and token rotation, invalid grant, credential-store failures, and revoke behavior. The secret-store suite covers round-trip/delete, missing entries, sanitized failures, and deadlines. No real Google account or live GNOME/KDE keyring was used.
- Limitations: this execution sandbox blocks loopback sockets by default, so the OAuth unit suite required an escalated test invocation. Live Google login, browser behavior, and GNOME Keyring/KDE Wallet availability remain unverified.
- Git commit: `5594fc9` (`feat: add desktop OAuth and secure token storage`).

## Step 8 — Google Drive API adapter and fake server

- Added a narrow Drive v3 REST client for paged `files.list`, `files.get`, metadata-only create/update, streamed download, trash, rename/move, start/change tokens, generated IDs, and resumable upload session creation, chunk transfer, and status queries.
- File and change requests select explicit `fields`. Drive error bodies are reduced to stable categories and reason codes; human-readable messages are not surfaced. Recognized rate-limit reasons, `429`, and `5xx` receive bounded exponential backoff with jitter for safe reads. Context cancels retry waits; request concurrency and per-request deadlines are bounded.
- Mutating requests are not blindly retried after uncertain outcomes. Resumable upload chunks return the confirmed offset or completed file, and interrupted sessions can be queried before another chunk is sent. Upload session URLs must stay on the configured origin, and cross-origin redirects are not followed.
- Documented error-to-state mapping and retry behavior in `docs/drive-api-errors.md`; updated README to clarify the new adapter is not yet wired into `drivefs` or the Qt app.
- Validation: local fake-server tests cover pagination, metadata fields, CRUD/move/trash, streamed download, changes, generated IDs, resumable start/chunk/status/completion, expired sessions, 401, distinct 403 reasons, 404, 429, 5xx, connection reset, retry cancellation, no retry for ambiguous mutations, cross-origin redirect rejection, request limits, and input validation. `go test -race ./internal/driveapi -count=1` passed. No requests were sent to Google.
- Limitations: only fake-server behavior is verified; Google Cloud project quotas, actual token refresh over HTTP, live Drive data, and real resumable-session behavior remain unverified.
- Git commit: `11c9aca` (`feat: add Drive API adapter and fake server`).

## Step 9 — initial My Drive indexing

- Added SQLite migration 2 with a durable folder/page work queue, a scanned-folder journal, snapshot token, catch-up page token, and index phase. Version-1 databases upgrade without losing their root or inode state.
- Added a parent-scoped recursive indexer. It captures `changes.getStartPageToken` before reading the root or listing children, pages through every reachable My Drive folder, stores file ID/parent/name/MIME/size/version/modified time/link/capabilities, and excludes trashed files, shared-drive files, and results outside the requested parent.
- Page data and its next-page cursor commit in one SQLite transaction. Rejected persisted file-list page tokens restart that folder from page one; upserts preserve object IDs/inodes and do not duplicate entries. Exact duplicate names receive stable ID-derived suffixes. Slash, NUL, `.` and `..` are mapped to safe POSIX display components while preserving the exact Drive name.
- The index remains `indexing` until changes from the pre-traversal token have been applied transactionally. If catch-up discovers an unscanned folder, the index scans it and performs another catch-up from the new start token before becoming `ready`.
- Validation: `go test -race ./internal/store ./internal/indexer -count=1` passed. Fake-API tests index more than 2,000 files across short pages, include empty/late folders, long and colliding names, metadata updates, deletes, moves, trashed/shared/out-of-tree items, and assert token-before-traversal order. Restart tests close and reopen SQLite after interruptions at each folder page and catch-up page; atomicity tests prove a failed batch does not advance its cursor or leave partial objects. No Google requests were made.
- Limitations: Google root ID behavior, actual API pagination/change ordering, account-specific shared-with-me behavior, and the end-to-end OAuth-to-index integration have not been tested against a real account. The `drivefs` and Qt processes still do not invoke the indexer.
- Git commit: `f103e26` (`feat: index My Drive with pagination and catch-up`).

## Step 10 — incremental Drive changes

- Added SQLite migration 3 for a remote-change shadow. Remote metadata, names, parents, capabilities, version, and removal state are retained separately when an object has an uncommitted local operation; local bytes, name, inode, and queue state remain intact. During reindex, pending objects and the ancestor folders needed to reach them remain visible, with provisional shadows until the snapshot and catch-up finish.
- Added a resumable changes poll cycle. The in-flight page cursor is committed with each applied page, while the durable change token advances only at the final page. Stale/replayed page application is rejected. New folders found during polling are scanned recursively and changes are caught up again before the index becomes ready.
- Classified invalid page tokens separately from generic bad requests. A rejected changes cursor triggers a controlled reindex that captures its new start token before listing the tree. API error docs and README now describe the poller and its integration boundary.
- Validation: `make check` passed; `go test -race ./internal/store ./internal/indexer -count=1` passed; `git diff --check` passed. Fake-API tests cover multi-page polling, transaction rollback and replay rejection, reopening after a committed page, removal/lost access, move/rename/capabilities, queued writes, invalid-token reindex, nested queued files, and newly discovered folders. No Google requests were made.
- Limitations: `Poller.Poll` drains a polling cycle but is not yet scheduled or wired into the desktop/FUSE processes; process lifecycle wiring is a later step. Real-account token behavior and live Drive changes remain unverified.
- Git commit: `4c373ac` (`feat: apply Drive changes incrementally`).

## Step 11 — stable indexed namespace through FUSE

- Added a read-only FUSE tree backed by the ready SQLite index: `lookup`, `getattr`, deterministic `readdir`, persistent database inode numbers, owner/mode/size/block/time attributes, and immediate metadata checks after deletion. Directory enumeration reads a single ordered SQLite snapshot with names, inode numbers, and MIME types.
- Added a `drivefs --database PATH --mountpoint DIR` mode for an existing ready index. The mountpoint must exist and be empty; the indexed mount is read-only while content reads and writes are implemented in later steps. The loopback spike remains available with `--source`.
- Native Google Workspace documents and other `application/vnd.google-apps.*` non-folder types remain visible with unknown dirent type and return `EOPNOTSUPP` on lookup/open. Unsupported mutations return `EROFS`; the mount also uses the read-only option.
- Validation: `make check` passed; `go test -race ./internal/fusefs ./internal/store -count=1` passed; CLI `--version` and `--help` passed; `git diff --check` passed. Tests cover a directory with over 2,000 entries, duplicate-name suffixes, stable inode values across FUSE trees, attributes, empty folders, unsupported native files, read-only operations, deletion, and concurrent readdir during index updates.
- Limitations: `/dev/fuse` is unavailable in this environment, so no live mount was performed. The CLI requires a ready database produced outside this command; OAuth/indexing integration and content reads remain later work.
- Git commit: `05e95c5` (`feat: expose stable Drive namespace through FUSE`).

## Step 12 — version-stable reads and bounded cache

- Added a streamed download pipeline that writes to a private temporary file using a fixed 128 KiB buffer. It checks Drive file ID/version/size/type and download permission before and after the stream, verifies exact byte count and Drive MD5 when provided, and records a local SHA-256 before publishing a complete cache row.
- Added migration 4 for unique `(object_id, Drive version)` cache entries. The default evictable-cache limit is 1 GiB and can be overridden by the `contentcache.New` byte-limit argument. Files that cannot fit without evicting a pinned, locally dirty, or open version are served from a temporary file for that handle and removed on release.
- Added verified offline cache reads and FUSE `open`/`read`/`release` handles using positional reads. Each handle stays attached to its immutable content version after the index advances. Interrupted downloads and corrupt cache entries are never returned as complete data; unsupported Workspace document types and network failures map to explicit FUSE errors.
- Drive API choice: use `files.get?alt=media` for stored blobs and verify the indexed version around the download; the [download guide](https://developers.google.com/workspace/drive/api/guides/manage-downloads) and [revision guide](https://developers.google.com/workspace/drive/api/guides/manage-revisions) describe limitations on downloading older revisions. This implementation does not depend on historical-revision access.
- Validation: full `make check` passed (`go test ./...`, `go vet ./...`, and both binary builds); `go test -race ./internal/store ./internal/contentcache ./internal/fusefs -count=1` and `git diff --check` passed. Fake-API tests cover a streamed 9 MiB file at multiple offsets, concurrent readers, interrupted/mismatched downloads, version changes during transfer, corrupt/offline cache, quota-boundary eviction, pin/dirty protection, and an open FUSE handle surviving a remote-version update.
- Limitations: no live Google Drive account or `/dev/fuse` mount was available. `drivefs --database` and the Qt process are not yet wired to OAuth/indexing/cache construction, so the content path is tested through the injected FUSE reader and fake Drive API.
- Git commit: `70804ff` (`feat: read Drive files through bounded disk cache`).

## Step 13 — locally durable FUSE writes

- Added an injectable write-enabled indexed FUSE constructor and mount function. `create` assigns a local object ID, inode, directory entry, and MIME type; writable Drive files require indexed `canEdit` capability. Writable descriptors share one private working file, serialize writes and truncation, and return current working size through `getattr`.
- `write`, `truncate`, `flush`, `fsync`, and `release` now persist immutable content versions and queue operations locally. Repeated calls for an unchanged generation add no operation; later edits to a local file queue as writes after its initial create. Insufficient durable storage maps to `ENOSPC`. Startup recovery removes abandoned mutable working files while preserving successful pending snapshots.
- Added an explanatory message in the Qt prototype: “Saved locally; waiting to upload to Google Drive.” It describes local durability without claiming that an upload completed. The GUI and FUSE processes still do not share live queue state.
- Validation: `make check` passed with local loopback available for fake-server tests; `go test -race ./internal/store ./internal/contentcache ./internal/fusefs -count=1`, `make qt-smoke`, and `git diff --check` passed. Tests cover offline create/edit, size truncation, two concurrent writer handles, idempotent repeat flush/fsync, `ENOSPC` mapping, restart recovery, and `SIGKILL` immediately after successful `fsync`.
- Limitations: `/dev/fuse` is unavailable, so no mounted file-manager test was possible. The `drivefs --database` CLI remains read-only and is not wired to the write-enabled constructor or OAuth/cache setup. The editor temporary-file → rename case will be verified after step 14 implements rename.
- Git commit: `a6bd98f` (`feat: persist local file writes before acknowledging FUSE`).

## Step 14 — folders, rename, move, and trash

- Added transactional local `mkdir`, `rename`, `unlink`, and `rmdir` operations. Each namespace mutation and its queued operation commit together; folder moves reject cycles and replacement rejects mismatched types or non-empty folders. FUSE returns POSIX `ENOTEMPTY`, `ENOTDIR`, `EISDIR`, `EEXIST`, and `EXDEV` where applicable.
- Added durable queue dependencies so child creation waits for parent creation, rename waits for source changes and replacement trash, and folder trash waits for direct child trash. Local editor temporary-file replacement preserves the new object's identity and content while queuing trash for the old target. Remote moves preserve the Drive file ID.
- Extended indexed Drive metadata fields and checks to use `canAddChildren`, `canRemoveChildren`, `canRename`, `canMoveItemWithinDrive`, and `canTrash`. README and D-015 document that the indexed FUSE implementation is injectable/tested but not yet connected to the CLI sync worker.
- Validation: `make check` passed with loopback available for OAuth and fake Drive HTTP tests; `go test -race ./internal/store ./internal/contentcache ./internal/fusefs -count=1`, `make qt-smoke`, and `git diff --check` passed. Tests cover offline rename/move, editor temp-file → atomic replacement, create/trash before any sender runs, recursive file-manager-style deletion, capability denial, dependency ordering, and SQLite close/reopen persistence.
- Limitations: `/dev/fuse` is unavailable, so file-manager operations were verified through FUSE node methods rather than a live mount. No Google Drive requests or live trash/move operations were performed. The CLI is still read-only and there is no queue worker yet.
- Git commit: `7dd82b9` (`feat: queue folder rename move and trash operations`).

## Step 15 — durable queue worker and resumable recovery

- Added `internal/syncworker` to process dependency-ready create, folder, write, rename/move, and trash operations. It reserves generated Drive IDs before create, uses the operation ID in `appProperties`, checks base versions before writes, validates immutable content SHA-256 and returned Drive checksums, and commits verified remote metadata with the queue transition in one SQLite transaction.
- Added persisted resumable-session URL/offset handling. After restart the worker queries Drive first, trusts the server's acknowledged offset, and checks the reserved file ID before restarting an expired session. Unknown create, upload, rename, and trash outcomes remain visible as reconciliation work; local pending bytes are preserved. Retryable queue failures use bounded exponential backoff with jitter.
- Added a real subprocess test matrix that sends `SIGKILL` after queue claim, after Drive ID reservation, after session persistence, after a chunk is accepted, after the final upload is accepted but before its response, and after folder creation but before its response. On reopen, each operation completes once; additional fake-Drive tests cover lost responses, expired sessions, dependencies, content corruption, version conflicts, zero-length uploads, rename/move, and trash.
- Validation: `make check`, `go test -race ./internal/store ./internal/indexer ./internal/driveapi ./internal/syncworker -count=1`, `make qt-smoke`, and `git diff --check` passed. Fake HTTP tests used local loopback. No live Google Drive requests were made.
- Limitations: the worker is an injectable package and is not yet wired into the OAuth, writable FUSE, or desktop process lifecycle. `/dev/fuse` is unavailable, so no live mount or file-manager test ran. Process-kill recovery does not simulate power loss.
- Git commit: `442d9e6` (`feat: sync queued operations with resumable recovery`).

## Step 16 — remote conflict preservation and race handling

- Changes from `changes.list` now compare the indexed Drive version with queued local work. A changed remote file remains at its current Drive name/version, while the latest complete pending local bytes are moved to a stable neighboring conflict file and queued as a separate Drive create. The local operation is marked `conflict`; an operation marker in `appProperties` prevents our own in-flight update from being misclassified.
- Rename/trash conflicts retain the local operation intent and remote metadata. Conflict records can be listed and resolved: content edits support keep-both or keep-remote; namespace changes support keep-local (requeue against latest observed version) or keep-remote. The conflict UI is wired in step 19.
- The worker compares the base Drive version before updating and reads the file after upload. If the postflight read finds another newer state, it preserves local bytes as a conflict file. These checks are best-effort: Google Drive v3 `files.update` documents no atomic version precondition/If-Match contract; a change between preflight and update can still be overwritten without detection. No live concurrent-account test is available.
- Validation: `go test ./internal/store ./internal/indexer ./internal/syncworker -count=1` passed. It covers poller conflict preservation across restart and remote deletion, conflict resolution, local rename rebase, own-operation markers, and a Drive edit arriving during upload. Full checks are recorded after final validation below.
- Limitations: real-account concurrency, visible Qt conflict controls, and a mounted FUSE conflict workflow remain unverified. The API's lack of documented conditional update semantics leaves a narrow overwrite race.

## Step 17 — recursive offline pins and space protection

- Added schema migration 5 for recursive folder pins, preserving older folder-pin intent during upgrade. Direct file pins and folder pins are stored once; effective status is derived from the indexed parent chain, so newly indexed descendants inherit the rule. The cache eviction query checks the full ancestor chain.
- Added `contentcache.PinManager` to enumerate pinned Drive blobs, prefetch and verify them, report per-file byte progress, continue across individual failures, and report protected-data overage. Newly downloaded pinned blobs use the private pinned directory and bypass the ordinary cache limit. Existing cached content is protected logically and can be evicted after unpinning. Startup recovery also removes generated orphans from the pinned directory.
- The durable-write reservation already enforces the 10% free-space floor and reserves bytes across concurrent writers. Existing tests cover overlapping reservations, threshold rejection, `ENOSPC` mapping, and preservation of the preceding acknowledged version; step 17 retains that behavior.
- Validation: `go test ./internal/store ./internal/contentcache -count=1`, `go test -race ./internal/store ./internal/contentcache -count=1`, `go vet ./internal/store ./internal/contentcache`, and `git diff --check` passed. Tests cover migration from schema 1, recursive inheritance for newly discovered files, pin/unpin eviction behavior, download progress, downloads larger than the configured cache limit, and metadata accounting for over 1 GiB of protected content.
- Limitations: the >1 GiB quota test simulates Drive size in SQLite metadata rather than downloading a physical 1 GiB file. `/dev/fuse` and a live Drive account are unavailable. The GUI display is step 19; the worker is not yet triggered by the IPC/process lifecycle.
- Git commit: `6902f28` (`feat: add offline pins and disk space guard`).

## Step 18 — local IPC and process lifecycle

- Added a version 1 JSON request/response protocol over a per-user Unix socket. The server checks that the parent directory is owned by the current UID and private, forces socket mode `0600`, rejects symlink/non-socket paths, verifies `SO_PEERCRED`, bounds messages, and removes only the same owned socket it created. A stale owned socket can be replaced; a live server is detected without creating a second service.
- Added commands for status, queue/progress and error summaries, recent events, conflicts and resolution, pins, mount/unmount, settings, sign-out, and quit. Settings and sign-out commands return an explicit unavailable error until their backends are connected. Queue IPC omits payload JSON and resumable session URLs.
- `drivefs --ipc-server` now owns the FUSE lifecycle. Mount/unmount are idempotent, the controller clears state after an unexpected mount exit, and explicit quit or SIGTERM unmounts before the process stops. The database and pending queue survive exit. The Qt client starts or connects to the one service, retries an existing service after disconnect, hides on window close when a tray exists, and provides an explicit quit/unmount control.
- Validation: `go test ./...` passed after the main IPC/lifecycle wiring. After the final socket-context and Qt reconnect-guard edits, `go vet ./internal/ipc ./internal/drivecontrol ./internal/store ./cmd/drivefs` and `go test -race ./internal/ipc ./internal/drivecontrol ./internal/store -count=1` passed; the Qt entry point still needs a fresh full build. `git diff --check` passed. A second full `go vet ./...` was stopped while it rebuilt the generated MIQT/Qt cgo package; the full gate is reserved for step 21.
- Lifecycle tests cover active-socket detection, stale-socket replacement, bad directory/file permissions, version mismatch, idempotent mount/unmount/quit, simulated mount-process exit, and preservation of a queued folder operation. No live `/dev/fuse` mount was possible. GUI disconnect during a real active upload is not covered because the sync worker is not yet connected to the service; the namespace mounted by this service is currently read-only.
- Git commit: `851de3d` (`feat: connect Qt app and filesystem over local IPC`).

## Next

Step 19: full Qt controls and sync status views.

