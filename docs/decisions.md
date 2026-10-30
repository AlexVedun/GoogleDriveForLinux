# Decisions and open implementation questions

## Accepted

| ID | Decision | Reason / evidence |
|---|---|---|
| D-001 | Go for core and UI control code; Qt 6 Widgets through MIQT for GUI. | User-selected stack; prove MIQT and deployment in an early spike. |
| D-002 | `go-fuse/v2` is the first FUSE prototype candidate. | User-space Go FUSE bindings; verify behavior on target Linux/FUSE setup. |
| D-003 | One account and My Drive only in version 1. | Product scope. |
| D-004 | A successful `flush`/`fsync` confirms durable local state; upload is asynchronous. | Required offline-write contract. |
| D-005 | SQLite stores metadata and durable queue state; content versions are separate immutable local files. | Separates queue transactions from large-file content and supports crash recovery. |
| D-006 | FUSE and Qt GUI are separate processes communicating over a private Unix socket. | GUI can close/reopen without losing mount/sync process state. |
| D-007 | Unsupported POSIX operations fail visibly. | Avoid claiming semantics that Drive cannot provide. |
| D-008 | The first Qt prototype uses Go 1.26.10, MIQT 0.15.0, Qt 6.11.2 and GCC 16.2.1 with CGO enabled. | The Widgets, tray, folder picker and background-status API compile; offscreen and extracted-AppImage smoke checks pass on Fedora 44 x86_64. |
| D-009 | Use `database/sql` with `mattn/go-sqlite3` v1.14.52 (bundled SQLite 3.53.4), WAL, `synchronous=FULL`, and a 1000-page auto-checkpoint. | The same store suite passed with this driver and with CGO-free `modernc.org/sqlite` v1.61.0 under `CGO_ENABLED=0`, including six SIGKILL recovery boundaries. The selected Qt build already requires CGO; the mattn store-test binary was 9.4 MB versus 12.0 MB for modernc. Every pooled connection checks the durability pragmas. These process-kill tests do not simulate power loss. See [SQLite WAL](https://www.sqlite.org/wal.html) and [synchronous pragma](https://www.sqlite.org/pragma.html#pragma_synchronous). |
| D-010 | Use OAuth installed-app flow with loopback callback, PKCE S256, random state, and the OS Secret Service for tokens; never fall back to SQLite or plaintext files. | OAuth behavior and credential-store mapping are covered by local fake-provider tests. Linux desktop integration uses go-keyring's Secret Service backend. Actual unlocked GNOME Keyring and KDE Wallet checks remain outstanding. |
| D-011 | Implement the narrow Drive v3 adapter directly over `net/http`, with explicit response fields and error types; retry only safe reads automatically. | This keeps the adapter small and lets the sync worker reconcile ambiguous mutation/upload outcomes instead of blindly repeating them. The fake-server suite covers HTTP/reason mapping, 403 rate limits, 429, 5xx, connection reset, cancellation, and request concurrency. |
| D-012 | Build the initial My Drive index by listing each reachable folder's children, journaling page tokens in SQLite, and replaying changes from a token captured before traversal. | Parent-scoped listing excludes items outside the My Drive tree. Each page and cursor commit atomically. New folders discovered in catch-up are scanned before a final catch-up pass, so the index is not marked ready while their children are missing. |

## Open; resolve with a small experiment before the dependent implementation

| ID | Question | Decision gate |
|---|---|---|
| O-001 | Which oldest Linux/glibc and Qt runtime versions will the release support? | The Fedora 44 build works, but it is not an old-system compatibility baseline; choose supported release distributions and build on the oldest one. |
| O-002 | Does the tray icon appear and reopen the hidden window under GNOME Wayland and KDE? | MIQT exposes the required tray, menu, folder-picker and event-loop APIs. Offscreen smoke passes; compositor socket access is denied in this sandbox, so verify visually in a normal desktop session. |
| O-003 | What representation of native Google Docs/Sheets/Slides behaves as a browser link in GNOME Files and KDE Dolphin? | Test a generated local launcher/link with allow-listed HTTPS URL handling; record security/trust prompts and final representation. |
| O-004 | Can Drive v3 updates be made atomically conditional on the observed file version? | Verify official API behavior and test concurrent edits. If no atomic condition is supported, document the residual race and use a conservative conflict path. |
| O-006 | Do GNOME Keyring and KDE Wallet expose a usable unlocked Secret Service in the target desktop sessions? | The implementation uses Secret Service through go-keyring and fails closed if unavailable. Live save/read/delete checks in GNOME and KDE are still required. |
| O-007 | Which minimum Linux/glibc and Qt runtime baseline should the AppImage support? | Select target release systems, then build in the oldest supported baseline and test on newer systems. |
