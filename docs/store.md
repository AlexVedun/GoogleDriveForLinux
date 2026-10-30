# Local metadata database

## Driver and file

The store uses `database/sql` with `github.com/mattn/go-sqlite3` v1.14.52 and bundled SQLite 3.53.4. Qt already requires CGO and a C++ toolchain, so the bundled SQLite amalgamation adds no build requirement or system SQLite runtime dependency. The complete store suite, including forced-termination recovery, also passed with CGO-free `modernc.org/sqlite` v1.61.0 under `CGO_ENABLED=0`; the project selected mattn because it is smaller in the tested store binary while CGO is already required for Qt. The database file is created with mode `0600`; a newly created parent directory uses mode `0700`. OAuth tokens are not stored in SQLite.

Each connection enables foreign-key checks, a 5 second busy timeout, WAL mode, `synchronous=FULL`, and a 1000-page WAL auto-checkpoint. Startup opens all four pooled connections and verifies these settings before applying migrations or accepting writes. SQLite documents that WAL with `synchronous=FULL` syncs the WAL on each commit; the application tests process termination and restart but does not claim to simulate power loss or storage-controller failures.

Every pending write supplies its expected snapshot size. Before creating a prepared row, the content store reserves that many available bytes under a shared in-process lock and requires at least 10% of the filesystem to remain available. The reservation is released as file bytes are accepted or when the write finishes; actual `write`, `fsync`, rename, and SQLite errors still propagate and leave the previous acknowledged version intact.

Open writable FUSE handles share one private mutable file under `working/` per local object. `flush` and `fsync` sync that file and publish an immutable pending snapshot plus queue operation before returning success. Those snapshots are independent of the mutable working file. `Recover` removes generated working files left by a process crash; an edit that did not complete a successful `flush` or `fsync` is not promised to survive a crash.

## Identity and visible names

- `object_id` is the stable local object identity. It is assigned before a remote Drive object exists and remains stable when a Drive `fileId` is later attached.
- `drive_file_id` is nullable and unique. The root object uses the fixed local ID `root`, inode 1, and has no Drive `fileId`.
- Inodes are allocated from a persistent `AUTOINCREMENT` sequence. The sequence is separate from object rows, so deleting a row cannot recycle an inode.
- `directory_entries` stores the exact `remote_name` and its persistent POSIX `display_name` separately. Drive allows duplicate names; the index may hold duplicates only when their visible names have distinct stable suffixes.
- Content versions use immutable IDs and file paths. Queue operations have stable IDs and unique idempotency keys; dependency rows preserve parent-before-child ordering.

## Schema migrations and backups

Migrations live in `internal/store/migrations/NNN_name.sql` and run in a transaction. `schema_migrations` stores a monotonically increasing version, the SHA-256 checksum of the exact migration file, and its application time. Startup rejects a database from a newer application and refuses to continue if an already-applied migration file has changed.

Never edit an applied migration. Add a new numbered migration instead. Before a migration that can discard, rewrite, or reinterpret user metadata, create and verify a consistent SQLite backup, including committed WAL state, then apply the migration transactionally. Keep the backup until the new schema has been opened and validated. The first migration is additive schema creation and does not rewrite user data.

The queue may contain resumable upload session URLs. Treat the database and its backups as private application data; never include it in logs or support bundles without explicit redaction.
