# Google Drive for Linux

Go and Qt 6 desktop client that mounts a user's Google Drive **My Drive** folder through FUSE.

The product is at the initial project setup stage. FUSE mounting, Google authentication, synchronization, offline writes, and the Qt interface are not implemented yet. See [the product plan](plan-google-drive-linux-go-qt.md) and [the implementation plan](LUNA_IMPLEMENTATION_PLAN.md).

The FUSE and sync behavior contract is in [docs/semantics.md](docs/semantics.md); unresolved technical choices are tracked in [docs/decisions.md](docs/decisions.md).

## Build and check

```sh
go test ./...
go vet ./...
go build ./cmd/drivefs ./cmd/driveapp
```

Qt 6 development files and a C++ compiler are required once the Qt/MIQT application is added. FUSE device access is required to run mount integration tests.

## Data safety contract

Once implemented, a successful local `fsync` will mean that file contents and the queued change are durably stored locally. It will not mean that the change has already reached Google Drive. Pending data must survive restarts and must not be evicted from the cache.

