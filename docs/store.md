# Local metadata database

## Driver and file

The schema prototype uses `database/sql` with `github.com/mattn/go-sqlite3` v1.14.52. Qt already requires CGO and a C++ toolchain, so the bundled SQLite amalgamation does not add another build requirement or a system SQLite runtime dependency. Step 6 will compare it with a CGO-free driver under forced-termination tests before finalizing this choice. The database file is created with mode `0600`; a newly created parent directory uses mode `0700`. OAuth tokens are not stored in SQLite.

The driver currently enables foreign-key checks on every connection and sets a 5 second busy timeout. WAL mode and the final synchronous/checkpoint policy are deliberately deferred to step 6, where they must be tested with forced termination and recovery.

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
