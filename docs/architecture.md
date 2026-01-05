# Dray Architecture Overview

This document summarizes Dray's runtime components, data flows, and how metadata
and storage layers interact. For full requirements and invariants, see
[`SPEC.md`](../SPEC.md).

## System goals and invariants (summary)

- Stateless, leaderless brokers; no inter-broker replication.
- Object storage WAL is the durability layer.
- Oxia provides authoritative metadata (offset index, HWM, group state).
- Produce acks only after WAL write + metadata commit.
- Compaction swaps WAL index entries for Parquet index entries atomically.

## Runtime components

- Broker (cmd/drayd)
  - `internal/server`: TCP listener, Kafka framing, connection lifecycle.
  - `internal/protocol`: Kafka request decoding, routing, and responses.
  - `internal/produce`: buffering + WAL commit pipeline.
  - `internal/fetch`: read planner for WAL/Parquet fetches.
  - `internal/topics`: topic metadata and admin operations.
  - `internal/groups`: classic and KIP-848 group coordination.
  - `internal/metrics`: Prometheus metrics and collectors.

- Compactor (cmd/drayd --mode compactor)
  - `internal/compaction/worker`: WAL to Parquet conversion + index swap.
  - `internal/iceberg`: Iceberg catalog client and commit helpers.

- Storage and metadata
  - `internal/wal`: WAL v1 encoding/decoding and staging markers.
  - `internal/objectstore`: object storage interface; S3 adapter in
    `internal/objectstore/s3`.
  - `internal/metadata/oxia`: Oxia-backed MetadataStore with CAS/Txn,
    notifications, and ephemeral keys.

## Storage and metadata model

- Object storage
  - WAL objects contain record batches for multiple streams, sorted by stream ID.
  - Parquet objects contain compacted data for a single stream (topic-partition).

- Oxia metadata (authoritative state)
  - Stream metadata and HWM (log end offset).
  - Offset index entries mapping offset ranges to WAL/Parquet objects.
  - Compaction staging keys and job state.
  - Group coordinator state and committed offsets.
  - Broker discovery (ephemeral keys) and change notifications.

- Iceberg (analytics plane)
  - Each topic maps to an Iceberg table.
  - Compaction appends or rewrites Parquet data files and commits snapshots.

## High-level data flow

```
Kafka Client
   |
   v
[Broker]
   |  Produce path
   |  1) buffer
   |  2) write WAL to object store
   |  3) commit offset index + HWM in Oxia
   v
Object Store (WAL)
   ^
   |  Fetch path
   |  1) read offset index from Oxia
   |  2) read WAL or Parquet ranges
   |  3) return Kafka batches
   v
Kafka Client

Compaction path
  WAL objects -> Compactor -> Parquet -> Iceberg catalog
                   |                |
                   +-> swap index in Oxia (atomic)
```

## Core flows

### Produce (write path)

1. Broker parses Produce request (`internal/protocol`).
2. Records are buffered per MetaDomain (`internal/produce`).
3. WAL writer flushes data to object storage (`internal/wal`, `internal/objectstore`).
4. Metadata commit writes offset index entry + updates HWM in Oxia (`internal/index`,
   `internal/metadata/oxia`).
5. Broker returns offsets to the client only after steps 3 and 4 succeed.

### Fetch (read path)

1. Broker resolves topic/partition to stream ID and reads HWM.
2. Offset index lookup determines which WAL/Parquet object covers the fetch offset.
3. Fetcher range-reads data from object store:
   - WAL entries return Kafka record batches directly.
   - Parquet entries are rehydrated into Kafka record batches.
4. Broker returns records + HWM/LSO to client.

### Compaction

1. Compactor scans metadata for WAL ranges eligible for compaction.
2. WAL entries are read, validated, and converted to Parquet.
3. Parquet files are committed to Iceberg (append or rewrite).
4. Offset index swap replaces WAL entries with a single Parquet entry atomically.
5. WAL/Parquet GC runs later based on metadata reference counts and grace periods.

## Metadata and notifications

- Oxia transactions guarantee atomic offset index updates and HWM changes.
- Notifications drive cache invalidation (index and HWM caches) and long-poll fetch.
- Ephemeral keys support broker registration and distributed locks.

## References

- Full specification: [`SPEC.md`](../SPEC.md)
- Runtime packages: `internal/server`, `internal/protocol`, `internal/produce`,
  `internal/fetch`, `internal/compaction/worker`, `internal/wal`,
  `internal/metadata/oxia`, `internal/objectstore/s3`, `internal/groups`,
  `internal/topics`, `internal/metrics`
