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
// WAL orphan GC (not yet implemented) handles WAL objects that were written
// but never committed due to broker crashes. These are detected via
// staging markers that remain after the orphan TTL.
//
// # Retention Enforcement
//
// Retention-based GC (not yet implemented) deletes data older than
// retention.ms or beyond retention.bytes limits.
package gc
