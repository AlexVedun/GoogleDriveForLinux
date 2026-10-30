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
| D-009 | The schema spike provisionally uses `database/sql` with `mattn/go-sqlite3` v1.14.52 and its bundled SQLite. | CGO is already required by MIQT/Qt. Compare it with a CGO-free driver under step 6 crash/reopen tests before finalizing the driver and durability settings. |

## Open; resolve with a small experiment before the dependent implementation

| ID | Question | Decision gate |
|---|---|---|
| O-001 | Which oldest Linux/glibc and Qt runtime versions will the release support? | The Fedora 44 build works, but it is not an old-system compatibility baseline; choose supported release distributions and build on the oldest one. |
| O-002 | Does the tray icon appear and reopen the hidden window under GNOME Wayland and KDE? | MIQT exposes the required tray, menu, folder-picker and event-loop APIs. Offscreen smoke passes; compositor socket access is denied in this sandbox, so verify visually in a normal desktop session. |
| O-003 | What representation of native Google Docs/Sheets/Slides behaves as a browser link in GNOME Files and KDE Dolphin? | Test a generated local launcher/link with allow-listed HTTPS URL handling; record security/trust prompts and final representation. |
| O-004 | Can Drive v3 updates be made atomically conditional on the observed file version? | Verify official API behavior and test concurrent edits. If no atomic condition is supported, document the residual race and use a conservative conflict path. |
| O-005 | Which SQLite driver and WAL/synchronous/checkpoint settings preserve acknowledged writes after forced termination? | Compare `mattn/go-sqlite3` with a CGO-free driver in step 6. Verify the selected driver and exact settings with crash/reopen tests before treating them as the durability contract. |
| O-006 | Which Secret Service integration works for GNOME Keyring and KDE Wallet in the target desktops? | Test token save/read/delete in GNOME and KDE; define behavior when no unlocked service exists. |
| O-007 | Which minimum Linux/glibc and Qt runtime baseline should the AppImage support? | Select target release systems, then build in the oldest supported baseline and test on newer systems. |
