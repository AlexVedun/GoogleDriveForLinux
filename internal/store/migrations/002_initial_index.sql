ALTER TABLE sync_state ADD COLUMN index_phase TEXT NOT NULL DEFAULT 'none'
    CHECK (index_phase IN ('none', 'scanning', 'catching-up', 'ready'));
ALTER TABLE sync_state ADD COLUMN snapshot_token TEXT NOT NULL DEFAULT '';
ALTER TABLE sync_state ADD COLUMN catchup_page_token TEXT NOT NULL DEFAULT '';

CREATE TABLE initial_index_work (
    folder_drive_file_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE RESTRICT,
    parent_object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE RESTRICT,
    page_token TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX initial_index_work_order_idx ON initial_index_work(created_at, folder_drive_file_id);

CREATE TABLE initial_index_seen_folders (
    folder_drive_file_id TEXT PRIMARY KEY,
    scanned_at TEXT NOT NULL
);
