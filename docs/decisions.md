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

## Open; resolve with a small experiment before the dependent implementation

| ID | Question | Decision gate |
|---|---|---|
| O-001 | Which exact Go/Qt/MIQT versions and build baseline work together? | Build a minimal Qt Widgets window and tray icon on Fedora 44; record versions and a reproducible build recipe. |
| O-002 | Does MIQT expose every required Qt API, especially `QSystemTrayIcon`, menu signals, and thread-safe UI dispatch? | Compile a runnable spike and test it under GNOME Wayland. Keep the UI backend behind a narrow package boundary until passed. |
| O-003 | What representation of native Google Docs/Sheets/Slides behaves as a browser link in GNOME Files and KDE Dolphin? | Test a generated local launcher/link with allow-listed HTTPS URL handling; record security/trust prompts and final representation. |
| O-004 | Can Drive v3 updates be made atomically conditional on the observed file version? | Verify official API behavior and test concurrent edits. If no atomic condition is supported, document the residual race and use a conservative conflict path. |
| O-005 | Which SQLite driver and settings satisfy CGO/build, WAL and crash-durability needs? | Choose after comparing a CGO-free driver and a CGo SQLite driver with forced-termination tests. Record WAL/synchronous/checkpoint settings. |
| O-006 | Which Secret Service integration works for GNOME Keyring and KDE Wallet in the target desktops? | Test token save/read/delete in GNOME and KDE; define behavior when no unlocked service exists. |
| O-007 | Which minimum Linux/glibc and Qt runtime baseline should the AppImage support? | Select target release systems, then build in the oldest supported baseline and test on newer systems. |
