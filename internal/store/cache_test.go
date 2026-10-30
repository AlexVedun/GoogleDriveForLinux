package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
)

func TestCacheEvictionSkipsPinnedAndLocallyDirtyContent(t *testing.T) {
	ctx := context.Background()
	metadata, content := openContentFixture(t)
	defer metadata.Close()
	if _, err := metadata.db.ExecContext(ctx, `UPDATE objects SET drive_version = 'remote-1' WHERE object_id = 'content-object'`); err != nil {
		t.Fatal(err)
	}
	createCachedFixture := func() domain.ContentVersion {
		t.Helper()
		staged, err := content.StageDownload(ctx, strings.NewReader("remote bytes"), int64(len("remote bytes")), "")
		if err != nil {
			t.Fatal(err)
		}
		version, err := content.PublishCachedContent(ctx, "content-object", "remote-1", staged)
		if err != nil {
			t.Fatal(err)
		}
		return version
	}

	pinned := createCachedFixture()
	if _, err := metadata.db.ExecContext(ctx, `INSERT INTO offline_pins(object_id, pinned_at) VALUES ('content-object', ?)`, formatTime(time.Now())); err != nil {
		t.Fatal(err)
	}
	if candidates, err := metadata.EvictableCachedVersions(ctx); err != nil || len(candidates) != 0 {
		t.Fatalf("pinned candidates = %v, err = %v; want none", candidates, err)
	}
	if err := content.EvictCachedContent(ctx, pinned); !errors.Is(err, ErrContentNotEvictable) {
		t.Fatalf("evict pinned content error = %v, want ErrContentNotEvictable", err)
	}
	if _, err := metadata.db.ExecContext(ctx, `DELETE FROM offline_pins WHERE object_id = 'content-object'`); err != nil {
		t.Fatal(err)
	}
	if err := content.EvictCachedContent(ctx, pinned); err != nil {
		t.Fatalf("evict unpinned clean content: %v", err)
	}

	dirty := createCachedFixture()
	if _, err := content.WritePending(ctx, WriteRequest{
		OperationID: "op-cache-protection", ObjectID: "content-object", Kind: domain.OperationWrite,
		IdempotencyKey: "cache-protection-write", BaseDriveVersion: "remote-1", ExpectedSize: int64(len("local edit")),
	}, strings.NewReader("local edit")); err != nil {
		t.Fatal(err)
	}
	if candidates, err := metadata.EvictableCachedVersions(ctx); err != nil || len(candidates) != 0 {
		t.Fatalf("dirty candidates = %v, err = %v; want none", candidates, err)
	}
	if err := content.EvictCachedContent(ctx, dirty); !errors.Is(err, ErrContentNotEvictable) {
		t.Fatalf("evict dirty content error = %v, want ErrContentNotEvictable", err)
	}
	if _, err := metadata.db.ExecContext(ctx, `UPDATE operations SET state = 'committed' WHERE operation_id = 'op-cache-protection'`); err != nil {
		t.Fatal(err)
	}
	if candidates, err := metadata.EvictableCachedVersions(ctx); err != nil || len(candidates) != 1 || candidates[0].ID != dirty.ID {
		t.Fatalf("clean candidates after operation commit = %v, err = %v", candidates, err)
	}
	if err := content.EvictCachedContent(ctx, dirty); err != nil {
		t.Fatalf("evict cache after local operation commit: %v", err)
	}
}
