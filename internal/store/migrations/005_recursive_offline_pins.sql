ALTER TABLE offline_pins ADD COLUMN recursive INTEGER NOT NULL DEFAULT 0 CHECK (recursive IN (0, 1));

UPDATE offline_pins SET recursive = 1
WHERE object_id IN (SELECT object_id FROM objects WHERE mime_type = 'application/vnd.google-apps.folder');

CREATE INDEX offline_pins_recursive_idx ON offline_pins(recursive, object_id);
