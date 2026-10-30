CREATE UNIQUE INDEX content_versions_cached_version_idx
    ON content_versions(object_id, drive_version)
    WHERE kind = 'cached';
