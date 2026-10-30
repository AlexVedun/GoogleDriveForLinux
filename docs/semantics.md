# Filesystem and synchronization semantics

This document is the contract between the FUSE layer, local store, Drive worker, and GUI. A successful local write acknowledgement does not imply that Google Drive has accepted the change.

## Identity and names

- Google `fileId` is the remote identity. Path and name are mutable metadata.
- Local inode numbers are persisted and are not reused while an object, open handle, pending operation, or tombstone can refer to them. The mounted root has a fixed inode.
- The database keeps the exact Drive name separately from its POSIX-visible component. Slash and NUL cannot be represented as a single POSIX component; `.` and `..` must never escape their directory. Implement a deterministic reversible escaping rule and test it.
- Drive names are not unique. For each collision group, append a short stable suffix derived from `fileId` to every colliding visible name; extend the suffix if needed to distinguish IDs. Persist the resulting visible name. A collision discovered later may require renaming the earlier visible entry once; thereafter the mapping remains stable across restarts.
- Google Workspace document types are not downloaded as ordinary file bytes in version 1. Their path/name representation and browser-launch mechanism are a decision gate below.

## Operation contract

| FUSE operation | Local result | Remote result and errors |
|---|---|---|
| `lookup`, `getattr`, `readdir` | Read the persistent metadata index; use persisted inode and visible-name mappings. | Drive changes refresh the index asynchronously. If the index is unavailable, report I/O error; do not invent an empty Drive. |
| `open`, `read` | Bind the handle to a particular local or remote content version. Read from a complete cached object or fetch into a temporary object and publish only after verification. | Offline uncached reads fail with a network/unavailable error. Never expose partial download bytes as a complete cache entry. |
| `create`, `mkdir` | Create a local object and durable queue intent. A child operation depends on its not-yet-committed parent. | Upload asynchronously. If the parent fails, keep the child and report the dependency error. |
| `write`, `truncate` | Modify an isolated working copy. A local failure must leave the previous acknowledged version intact. Sparse offsets read as zeroes if sparse writes are supported. | Queue an immutable content version; network work is asynchronous. |
| `flush`, `fsync` | Both make current bytes durable locally and make the corresponding queue state recoverable before returning success. Repeated calls are idempotent. `fsync` also syncs required directory/database metadata. | Attempt or schedule upload separately. Return local I/O/space errors; do not report successful remote sync from these callbacks. |
| `release` | Release handle and references; allow garbage collection only for versions with no handles and no pending operations. | Never wait for or initiate an unbounded network request. |
| `rename` | Atomically update the local namespace and enqueue a rename/move while retaining `fileId`. Temporary-file replacement must be supported. | Apply parent/name changes in order. On remote failure retain the local state and visible error. Reject unsupported cross-device changes. |
| `unlink` | Hide the entry locally, retain data needed by open handles and pending work, enqueue trash. | Move to Drive trash after dependencies. Do not permanently delete. |
| `rmdir` | Succeed only for an empty directory (`ENOTEMPTY` otherwise); enqueue trash. File managers may recursively call child removals first. | Trash only after dependent child operations. |
| `setattr` / permissions | Define supported metadata (at least size/truncate and timestamps needed by common editors). Do not claim chmod/ownership semantics Drive cannot preserve. | Unsupported metadata changes return a clear errno (`EOPNOTSUPP`/`ENOSYS` as appropriate). |
| symlink, hardlink, lock, special file, `fallocate` | Unsupported in version 1 unless a correctness-preserving implementation is proven. | Return a clear error; never silently succeed. |

### Handles and writes

- Each open handle refers to one local working generation. A remote refresh does not change bytes already observed through an open handle.
- Concurrent writers to one file are serialized by the local store. Until multi-writer behavior is tested, reject or serialize incompatible write/truncate transitions; never interleave snapshots and queue records.
- Use private staging files with restrictive permissions. A failed write or fsync cannot replace the last acknowledged complete local version.
- A GUI close/hide is not a FUSE unmount. Explicit application exit requests unmount and process shutdown; pending data stays on disk.

## Durability and recovery

Required order for publishing a content version:

1. Reserve local disk space for the write under a serialized reservation mechanism.
2. Commit a `prepared` operation and incomplete content-version row with stable IDs.
3. Stream bytes to a unique staging file, compute its size and SHA-256, `fsync` the file, then `fsync` the pending directory entry.
4. In a durable SQLite transaction, record the expected size and SHA-256 while keeping the operation `prepared`.
5. Atomically rename the staging file to its immutable version/blob path and `fsync` the containing directory.
6. In one durable SQLite transaction, mark the content version complete/current, update object metadata, and change the operation to `queued`.
7. Only after that transaction succeeds acknowledge the FUSE durability operation.

SQLite uses WAL with `synchronous=FULL`; connection settings are checked at startup. Recovery may remove a generated partial file for a prepared operation only when it has no durable checksum and the database proves that its version is incomplete, non-current, and not shared by another reference. When a checksum is present, recovery verifies the final or staging file before publishing it. Preserve files with mismatched or uncertain content for inspection; do not delete them to make the database look clean. Remove generated orphan files only after checking database references. If recovery cannot determine whether an upload committed, query Drive using the persisted operation identity before retrying. Never solve uncertainty by deleting the local version.

## Queue states and ordering

An operation has a stable local ID and one of these states:

```text
prepared -> queued -> uploading -> committed
                         |
                         +-> conflict / retryable-error / permanent-error
```

Store the operation kind, target `fileId` (or pending local object ID), base Drive version/checksum, local content-version ID, parent/name inputs, dependencies, resumable-session state and last classified error. Changes to state and the associated local index are transactional. Retryable errors use bounded exponential backoff with jitter and respect cancellation. Per-object operations are ordered; unrelated objects may upload concurrently within a global limit.

For creates, use a pre-generated Drive ID when supported and persist it before sending the request. Store a non-secret app operation ID in Drive `appProperties` when available. For an unknown HTTP outcome, reconcile by ID/operation marker and content metadata before retry. A retry that might create a duplicate must stop in a visible reconciliation state.

## Conflicts

- Compare the queued edit's recorded base remote version/checksum with current metadata before applying an update.
- On a detected remote change, preserve the remote object and local working copy; materialize the local copy under a deterministic, persisted conflict name in the same visible parent and notify the GUI. Never acknowledge a conflict by overwriting either copy.
- A local deletion or rename that conflicts with an independent remote update is also a conflict; retain sufficient metadata/content to restore both outcomes.
- Drive version preflight may have a race with a later update. Before claiming race-free conflict protection, prove whether the API supports an atomic conditional update. If it does not, document that limitation and choose a conservative policy for ambiguous results. Never describe a read-then-write check as atomic.

## Cache and disk-space rules

- The 1 GiB default limit applies only to complete, clean, unpinned, unused cache content.
- Pinned data and queue-referenced data are never evicted to satisfy the cache limit.
- Before a new offline write, reserve expected bytes and reject if the post-write free space would fall below 10%. Reservations prevent concurrent writers from both spending the same free space.
- Handle a real filesystem `ENOSPC` during write/rename/fsync even if the estimate passed. Keep the preceding acknowledged version intact and report an explicit error.
- If pins or queued changes exceed the ordinary cache limit, allow the disk use and show the overage.
