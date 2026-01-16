package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

func TestAppenderTableCacheReuseWithLockManager(t *testing.T) {
	tbl := &mockTable{id: TableIdentifier{"dray", "topic"}}
	cat := &mockCatalog{table: tbl}
	lockManager := NewIcebergLockManager(metadata.NewMockStore(), "writer-1")

	cfg := DefaultAppenderConfig(cat)
	cfg.TableCacheTTL = time.Minute
	cfg.LockManager = lockManager
	app := NewAppender(cfg)

	files := []DataFile{
		{
			Path:           "s3://bucket/topic/file.parquet",
			RecordCount:    1,
			FileSizeBytes:  1,
			PartitionValue: 0,
		},
	}

	if _, err := app.AppendFilesForStream(context.Background(), "topic", "job-1", files); err != nil {
		t.Fatalf("append job-1 failed: %v", err)
	}
	if _, err := app.AppendFilesForStream(context.Background(), "topic", "job-2", files); err != nil {
		t.Fatalf("append job-2 failed: %v", err)
	}

	if cat.loadCount != 1 {
		t.Fatalf("expected 1 table load, got %d", cat.loadCount)
	}
	if lockManager.HoldsLock("topic") {
		t.Fatal("expected lock to be released after append")
	}
}
