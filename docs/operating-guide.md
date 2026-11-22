# Installation and operating guide

This is a development build, not a public release. The AppImage was built on
Fedora 44 x86_64. The oldest supported Linux/glibc release has not been
selected, and real Google-account and desktop-session tests are still pending.

## Requirements

- Linux x86_64 with a working kernel FUSE setup and permission to use
  `/dev/fuse` for the Google Drive mount.
- For ordinary AppImage launch, an AppImage-compatible FUSE setup, including
  the `fusermount` helper expected by the image runtime. Extracting the image
  avoids FUSE for starting the application, but the Drive filesystem itself
  still requires `/dev/fuse`.
- An X11 or Wayland desktop session. The AppImage includes Qt and its X11,
  Wayland, and offscreen platform plugins; it still uses host display and
  graphics components.
- A session D-Bus and an unlocked Secret Service implementation, such as
  GNOME Keyring or KDE Wallet, for OAuth credential storage.

## Build and launch

Building from source requires Go, CGO, a C++ compiler, `pkg-config`, Qt 6
Widgets development files, and the Qt Wayland plugin package. Run the local
checks with:

```sh
go test ./...
go vet ./...
make build
make qt-smoke
make appimage
```

To launch a built AppImage, make it executable:

```sh
chmod +x GoogleDriveForLinux-x86_64.AppImage
./GoogleDriveForLinux-x86_64.AppImage
```

Create a Google Cloud OAuth client of type **Desktop app**, enable the Drive
API, and configure a test user. Enter its client ID in the application's
**Settings** tab and click **Save settings** before signing in. If the Desktop app credential has a client secret, the adjacent optional field accepts it. The client ID is not a
secret; OAuth tokens are stored in Secret Service under service
`google-drive-linux`, key `primary`. The application does not save tokens to
files or SQLite. See [OAuth setup](oauth.md).

In the window, sign in, select an existing empty directory as the mountpoint,
and choose **Mount**. On later launches, the app mounts that saved directory
automatically once the account is signed in, synchronization is running, and
the Drive index is ready. If automatic mounting fails, the message appears in
the window and the manual **Mount Drive** button remains available. Closing
the window hides it when a system tray is
available. Without a tray, closing the window requests service shutdown and
unmount. **Quit and unmount** explicitly stops the service before the GUI
exits.

If the AppImage runtime cannot start because FUSE is unavailable, extract and
launch it:

```sh
./GoogleDriveForLinux-x86_64.AppImage --appimage-extract
./squashfs-root/AppRun
```

This is a launch workaround only. It does not enable a Drive mount without a
working kernel FUSE setup.

## Sync and data safety

A successful local `flush` or `fsync` means the complete file contents and
queued operation are durable on this computer. It does not mean Google Drive
has accepted the change. Keep the local data directory until the queue shows
the work as uploaded or you have copied pending files elsewhere. Network,
authorization, quota, disk-space, and conflict errors are shown in the app;
fix the cause and leave pending data in place so the worker can retry or
reconcile it.

The default cache limit is 1 GiB. Offline-pinned and pending data are protected
and may exceed it. New writes are refused if they would leave less than 10%
free space on the filesystem storing the local data.

When a remote edit conflicts with pending local changes, the application
preserves the detected local version as a neighboring conflict file and keeps
the remote version. The UI offers the supported conflict resolutions. Drive
does not document an atomic version precondition for the update call, so a
concurrent remote edit in a narrow upload window can still be overwritten
without detection.

## Local paths

The paths follow XDG variables when set:

| Data | Default path |
|---|---|
| SQLite metadata and durable local content, including pending writes and cached blobs | `~/.local/share/google-drive-linux/` |
| Settings, including cache limit and last mountpoint | `~/.config/google-drive-linux/settings.json` |
| Per-user IPC socket | `$XDG_RUNTIME_DIR/google-drive-linux.sock`; falls back to `~/.config/google-drive-linux/drivefs.sock` |
| OAuth token | OS Secret Service item `google-drive-linux` / `primary` |

The durable content store is in `~/.local/share/google-drive-linux/content/`;
evictable cache, pinned content, pending versions, and working files are kept
in separate subdirectories there. Changing `XDG_DATA_HOME` or
`XDG_CONFIG_HOME` changes the corresponding root.

## Sign out, uninstall, and remove local data

**Sign out** unmounts the filesystem and stops synchronization before trying
to revoke the OAuth token. If revocation fails, the credential remains in
Secret Service so the error can be retried. Sign out does not erase the local
database, cache, pending queue, conflict files, or pinned data. It does not
delete files from Google Drive.

Before removing local data, resolve conflicts, confirm the queue is empty,
unmount, choose **Sign out**, and then **Quit and unmount**. Review and remove
the configured application data and settings directories after the app exits.
Removing the data directory permanently deletes any unsynced
local work; it does not remove Drive files. Uninstalling the AppImage alone
does not remove those per-user directories. FUSE `unlink` moves Drive items
to Google Drive Trash; it is not a permanent remote deletion.

## Supported filesystem scope

Version 1 covers one account's **My Drive**, ordinary files and folders,
reading, writing, offline queueing, conflicts, and offline pins. Shared drives,
“Shared with me”, editing Google Docs/Sheets/Slides as ordinary file bytes,
POSIX ownership and permission emulation, symlinks, hard links, locks, special
files, and other unsupported operations are outside the current scope. The
application returns explicit errors where these operations are unsupported.
