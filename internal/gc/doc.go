// Package gc implements garbage collection for WAL and Parquet objects.
//
// # WAL Garbage Collection
//
// The WAL GC worker ([WALGCWorker]) scans for WAL objects marked for garbage
// collection and deletes them after a grace period. When a WAL object's
// refcount reaches zero (after all streams referencing it have been
// compacted), the compaction process creates a GC marker at:
//
//	/wal/gc/<metaDomain>/<walId>
//
// The GC worker periodically scans these markers and deletes the objects
// once their deleteAfterMs grace period has passed.
//
// # Usage
//
//	worker := gc.NewWALGCWorker(metaStore, objStore, gc.WALGCWorkerConfig{
//	    ScanIntervalMs: 60000,
//	    NumDomains:     16,
//	    BatchSize:      100,
//	})
//	worker.Start()
//	defer worker.Stop()
//
// # Orphan Cleanup
//
// The WAL orphan GC worker ([WALOrphanGCWorker]) handles WAL objects that were
// written but never committed due to broker crashes. Per spec section 9.7,
// orphaned WALs are detected via staging markers at:
//
//	/wal/staging/<metaDomain>/<walId>
//
// When a WAL object is written, a staging marker is created first. On successful
// commit, the staging marker is deleted in the same transaction. If the broker
// crashes after writing the WAL but before commit, the staging marker remains.
//
// The orphan GC worker periodically scans staging markers and deletes those
// older than wal.orphan_ttl (default 24 hours), along with their WAL objects.
//
// Usage:
//
//	worker := gc.NewWALOrphanGCWorker(metaStore, objStore, gc.WALOrphanGCWorkerConfig{
//	    ScanIntervalMs: 60000,
//	    OrphanTTLMs:    86400000, // 24 hours
//	    NumDomains:     16,
//	    BatchSize:      100,
//	})
//	worker.Start()
//	defer worker.Stop()
//
// # Parquet Garbage Collection
//
// The Parquet GC worker ([ParquetGCWorker]) handles cleanup of old Parquet files
// after compaction rewrite (re-compaction). When a compaction job replaces
// existing Parquet files with a new consolidated Parquet file, the old files
// are scheduled for deletion via GC markers at:
//
//	/parquet/gc/<streamId>/<parquetId>
//
// The GC worker periodically scans these markers and deletes the objects
// once their grace period has passed, allowing in-flight reads to complete.
//
// Usage:
//
//	worker := gc.NewParquetGCWorker(metaStore, objStore, gc.ParquetGCWorkerConfig{
//	    ScanIntervalMs: 60000,
//	    GracePeriodMs:  600000, // 10 minutes
//	    BatchSize:      100,
//	})
//	worker.Start()
//	defer worker.Stop()
//
// To schedule a Parquet file for GC after compaction rewrite:
//
//	gc.ScheduleParquetGC(ctx, metaStore, gc.ParquetGCRecord{
//	    Path:          parquetPath,
//	    DeleteAfterMs: time.Now().Add(10 * time.Minute).UnixMilli(),
//	    CreatedAt:     originalCreatedAt,
//	    SizeBytes:     parquetSizeBytes,
//	    StreamID:      streamID,
//	    JobID:         jobID,
//	})
//
// # Retention Enforcement
//
// The retention worker ([RetentionWorker]) enforces retention.ms and
// retention.bytes policies configured on topics. It periodically scans all
// streams and deletes data that exceeds the configured limits:
//
//   - retention.ms: Data older than the specified milliseconds is deleted
//   - retention.bytes: Data beyond the byte limit is deleted (oldest first)
//
// Both limits can be set to -1 for unlimited retention. When both limits are
// set, data is deleted if it violates either limit. At least one entry is
// always kept per stream to preserve offset continuity.
//
// Usage:
//
//	worker := gc.NewRetentionWorker(metaStore, objStore, topicStore, gc.RetentionWorkerConfig{
//	    ScanIntervalMs: 300000,  // 5 minutes
//	    NumDomains:     16,
//	    GracePeriodMs:  600000,  // 10 minutes grace before deletion
//	})
//	worker.Start()
//	defer worker.Stop()
//
// For manual enforcement on a specific stream:
//
//	worker.EnforceStream(ctx, streamID, retentionMs, retentionBytes)
package gc
