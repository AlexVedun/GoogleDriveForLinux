CREATE TABLE remote_change_shadow (
    object_id TEXT PRIMARY KEY REFERENCES objects(object_id) ON DELETE RESTRICT,
    drive_file_id TEXT NOT NULL UNIQUE,
    parent_drive_file_id TEXT NOT NULL DEFAULT '',
    remote_name TEXT NOT NULL DEFAULT '',
    mime_type TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    drive_version TEXT NOT NULL DEFAULT '',
    modified_at TEXT,
    web_view_link TEXT NOT NULL DEFAULT '',
    capabilities_json TEXT NOT NULL DEFAULT '{}',
    is_removed INTEGER NOT NULL DEFAULT 0 CHECK (is_removed IN (0, 1)),
    is_confirmed INTEGER NOT NULL DEFAULT 1 CHECK (is_confirmed IN (0, 1)),
    observed_at TEXT NOT NULL
);
