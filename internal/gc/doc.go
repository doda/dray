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
// # Retention Enforcement
//
// Retention-based GC (not yet implemented) deletes data older than
// retention.ms or beyond retention.bytes limits.
package gc
