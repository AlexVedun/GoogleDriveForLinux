CREATE TABLE inode_sequence (
    inode INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE objects (
    object_id TEXT PRIMARY KEY,
    drive_file_id TEXT UNIQUE,
    inode INTEGER NOT NULL UNIQUE CHECK (inode > 0),
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    drive_version TEXT,
    modified_at TEXT,
    web_view_link TEXT,
    capabilities_json TEXT NOT NULL DEFAULT '{}',
    is_deleted INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE directory_entries (
    parent_object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE RESTRICT,
    object_id TEXT NOT NULL UNIQUE REFERENCES objects(object_id) ON DELETE RESTRICT,
    remote_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    PRIMARY KEY (parent_object_id, display_name),
    CHECK (display_name <> '' AND display_name NOT IN ('.', '..') AND instr(display_name, '/') = 0)
);

CREATE INDEX directory_entries_parent_idx ON directory_entries(parent_object_id);

CREATE TABLE content_versions (
    version_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('cached', 'pending', 'conflict')),
    local_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT,
    drive_version TEXT,
    is_complete INTEGER NOT NULL DEFAULT 0 CHECK (is_complete IN (0, 1)),
    is_current INTEGER NOT NULL DEFAULT 0 CHECK (is_current IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE INDEX content_versions_object_idx ON content_versions(object_id, created_at);
CREATE UNIQUE INDEX content_versions_one_current_idx
    ON content_versions(object_id) WHERE is_current = 1;

CREATE TABLE accounts (
    account_key TEXT PRIMARY KEY CHECK (account_key = 'primary'),
    google_user_id TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    auth_state TEXT NOT NULL CHECK (auth_state IN ('signed-in', 'reauthorization-required', 'signed-out')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE sync_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    change_token TEXT NOT NULL DEFAULT '',
    index_state TEXT NOT NULL DEFAULT 'empty' CHECK (index_state IN ('empty', 'indexing', 'ready', 'error')),
    updated_at TEXT NOT NULL
);

CREATE TABLE settings (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE offline_pins (
    object_id TEXT PRIMARY KEY REFERENCES objects(object_id) ON DELETE RESTRICT,
    pinned_at TEXT NOT NULL
);

CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('create', 'write', 'mkdir', 'rename', 'trash')),
    state TEXT NOT NULL CHECK (state IN (
        'prepared', 'queued', 'uploading', 'committed', 'conflict',
        'retryable-error', 'permanent-error', 'needs-reconcile'
    )),
    idempotency_key TEXT NOT NULL UNIQUE,
    base_drive_version TEXT NOT NULL DEFAULT '',
    content_version_id TEXT REFERENCES content_versions(version_id) ON DELETE RESTRICT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    resumable_session_url TEXT NOT NULL DEFAULT '',
    resumable_offset INTEGER NOT NULL DEFAULT 0 CHECK (resumable_offset >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TEXT,
    last_error_code TEXT NOT NULL DEFAULT '',
    last_error_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX operations_state_idx ON operations(state, next_attempt_at);
CREATE INDEX operations_object_idx ON operations(object_id, created_at);

CREATE TABLE operation_dependencies (
    operation_id TEXT NOT NULL REFERENCES operations(operation_id) ON DELETE RESTRICT,
    dependency_id TEXT NOT NULL REFERENCES operations(operation_id) ON DELETE RESTRICT,
    PRIMARY KEY (operation_id, dependency_id),
    CHECK (operation_id <> dependency_id)
);

CREATE INDEX operation_dependencies_dependency_idx ON operation_dependencies(dependency_id);

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    object_id TEXT REFERENCES objects(object_id) ON DELETE RESTRICT,
    operation_id TEXT REFERENCES operations(operation_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('info', 'error', 'conflict')),
    code TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE INDEX events_unresolved_idx ON events(resolved_at, created_at);
CREATE INDEX events_object_idx ON events(object_id, created_at);
