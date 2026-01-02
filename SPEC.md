# Dray v1.2 — Consolidated Technical Specification

Open-source, Golang-first implementation of StreamNative Ursa Engine (Kafka v4 compatible), with Oxia metadata + object-storage WAL + stream–table duality (Iceberg-first), zone-aware routing, and cost‑optimized durability.

---

## 0. Document control and context

**Version:** v1.2.1 (consolidated from v1.0 + v1.1; correctness-critical clarifications applied, includes all v1.1 technical corrections)
**Status:** Implementation-ready (LLM-friendly)
**Target protocol:** Apache Kafka v4.x wire protocol (focus on 4.0/4.1 behavior).
**Primary architectural reference:** Ursa Engine (leaderless/stateless brokers, object-storage WAL, Oxia metadata, compaction to Parquet + lakehouse tables).

**v1.2.1 corrections:**
- Added "Deliberate deviation from Ursa" documentation (§5.2.1)
- Added detailed IndexEntry schema with WAL vs Parquet field distinction (§6.3.3)
- Fixed WAL path format to include `domain=<metaDomain>` (§5.1)
- Aligned file extension to `.wo` (matches magic `DRAYWO1`)
- Fixed stale section references (§8.2→§9.2, §8.4→§9.5, §10.7→§11.6/§11.7, §13→§14.3)
- Corrected header size from 47 to 49 bytes (§5.2.2)

### 0.1 Context and framing

StreamNative’s Ursa Engine is a Kafka API-compatible, cloud-native streaming architecture that (a) persists data primarily to object storage via an S3-based WAL, (b) keeps brokers stateless/leaderless, and (c) uses Oxia for metadata.

Dray is a clean-room, open-source Go codebase that follows Ursa’s architecture and behavior *very closely*, while being intentionally minimalistic and correctness-focused.

---

## 1. Design principles

1. **Correctness-first**: every externally visible state transition has a single owner and is persisted; races are resolved via CAS/transactions in metadata.
2. **Operational simplicity**: minimal moving parts: stateless brokers + Oxia + object storage + (optional) compactor(s) + (optional) Iceberg catalog.
3. **Determinism**: routing and ownership are deterministic functions of `(topic, partition, zone)` and membership set.
4. **Idempotency everywhere**: WAL writes, metadata commits, compaction commits, and GC MUST be safely retryable.
5. **LLM implementability**: bake in invariants + golden tests + single-purpose packages.

---

## 2. Goals, non-goals, and product invariants

### 2.1 Primary goals (must-haves)

1. **Kafka v4 client compatibility** (Apache Kafka 4.x request/response semantics, including the Kafka 4 group coordinator + KIP-848 consumer rebalance protocol). ([Apache Kafka][1])
2. **Leaderless, stateless brokers**: any broker can serve produce/fetch for any partition; no data replication between brokers (object storage is durability). 
3. **Cost-optimized WAL on object storage only (v1)**: S3/GCS/Azure-compatible WAL as the sole durability layer.
4. **Oxia-backed metadata engine** (default) with a **pluggable metadata abstraction**. Ursa uses Oxia for metadata storage. 
5. **Ursa-style offset index**: offsets and object locations are stored in metadata (Oxia), enabling stateless brokers and linearizable writes. 
6. **Stream–table duality from day one**, Iceberg-first:

   * Every Kafka topic is also an Iceberg table (or is *optionally* mirrored into one depending on config, but the system design assumes duality is always available).
   * Compaction produces Parquet files and commits them into Iceberg metadata.
7. **Zone-aware routing** (AZ affinity), compatible with Ursa’s client-side `client.id` zone annotation format.
8. **Operational simplicity**:

   * Single container image (same binary can run broker/compactor roles).
   * Minimal required external dependencies: Oxia + object storage (+ optional Iceberg catalog).

### 2.2 Out-of-scope (explicitly deferred v1)

* Transactions (Kafka transactions)
* Idempotent producers
* Kafka-native tiered storage APIs
* Exactly-once semantics

These must be *explicitly rejected* with correct Kafka error codes / feature flags (details later), not silently misbehave.

### 2.3 Non-goals / "won't do"

* Implement Kafka’s internal replication/controller (KRaft) behaviors.
* Implement inter-broker replication and ISR.
* Maintain local disk logs as a source of truth.

### 2.4 Architectural invariants (Dray must preserve)

These are the “correctness contracts” that everything else is built around (and should be tested continuously):

1. **Durable acknowledged writes**: A produce is acknowledged only after:

   * WAL object data is durably written to object storage, and
   * the corresponding Oxia offset-index entry is committed.
     This matches Ursa’s durability & atomicity contract. 

2. **Linearizable write ordering per partition**: Offset assignment and index updates for a given partition are totally ordered by Oxia’s replication/transaction semantics. 

3. **Read-your-writes**: After a successful produce response, a subsequent fetch must observe offsets ≥ the acknowledged offsets (session-monotonicity via revision stamping). 

4. **Brokers are never authoritative**: caches may accelerate reads, but authoritative state is in object storage + metadata store. 

5. **Compaction/index consistency**: Swapping multiple WAL index entries for a single Parquet index entry must be atomic in metadata (no partial index state visible to consumers). 

---

## 3. System architecture

### 3.1 Components

**A. Dray Broker (`drayd`)**

* Kafka protocol server (TCP)
* Stateless request handler for:

  * Produce/Fetch
  * Admin APIs (topic lifecycle, configs)
  * Group coordination and offset management
* Interacts with:

  * **Metadata engine** (Oxia) for all authoritative metadata
  * **Object storage** for WAL and for reading compacted Parquet
  * **Iceberg catalog** *indirectly* (brokers may create tables on topic creation; compactor appends data files)

**B. Dray Compactor (`dray-compactor`)**

* Reads WAL objects, writes Parquet per topic-partition, commits to Iceberg, and updates offset index.

**C. Metadata Engine**

* Default: **Oxia cluster**

  * Sharded, leader-based metadata service; supports namespaces.
  * Versioning enables OCC / compare-then-set behavior.
  * Notifications feed supports efficient “wait for new data” and cache invalidation. 
  * Ephemeral records support service discovery / liveness.

**D. Object Storage**

* S3-compatible API first; adapters for GCS/Azure.
* Holds:

  * WAL objects (row-based)
  * Compacted Parquet objects (columnar)

Ursa’s cost-optimized mode writes records directly to object storage and keeps brokers stateless/leaderless.

**E. Iceberg Table Layer**

* Iceberg catalog integration (REST catalog spec preferred).
* Go implementation options:

  * Use `apache/iceberg-go` where possible.
  * Support REST catalog (standard), enabling compatibility with services that implement it (e.g., AWS Glue exposes an Iceberg REST endpoint supporting the spec).

### 3.2 Ursa-style "headless storage" concept

Ursa describes “Ursa Stream Storage” as a headless, multi-modal storage layer built on lakehouse formats, with an S3-based WAL at its heart.

Dray will implement the same conceptual model:

* **Streaming plane**: Kafka-compatible log semantics powered by WAL + offset index.
* **Analytical plane**: Iceberg tables pointing to Parquet data produced by compaction.
* **Duality**: both planes reference the same durable data files (eventually the compacted Parquet files).

### 3.3 Data-flow overview

**Write path (Produce)**

1. buffer → 2) flush WAL object → 3) commit Oxia offset index → 4) respond. 

**Read path (Fetch)**

1. query offset index → 2) range-read WAL/Parquet → 3) return Kafka batches (convert if Parquet). 

**Compaction**
Convert WAL objects into per-partition Parquet, then replace index entries (atomically) with a single entry referencing the compacted file. 

---

## 4. Data model and key abstractions

### 4.1 Entities

* **Topic**: Kafka topic name + config; maps to exactly one Iceberg table.
* **Partition**: topic-partition. In Dray/Ursa architecture, a “partition” corresponds to a **Stream**.
* **StreamID**: stable, internal identifier for a topic-partition.

  * Recommend UUID for topic, and StreamID = UUID(topic) + partitionId, or UUID per partition.
* **WAL Object (WO)**: object storage file containing batches from multiple streams, sorted by `stream_id`. 
* **Compacted Object (CO)**: Parquet file containing data for a single stream (topic-partition) for a contiguous offset range. 
* **Offset Index**: metadata mapping logical offsets to object locations. Stored in metadata store (Oxia).

### 4.2 Offset semantics (correctness-critical)

Dray MUST define offsets precisely and consistently:

* Offsets are **0-based**, monotonically increasing per stream.
* Each offset index entry covers a **half-open** range:
  **`[startOffset, endOffset)`** where **`endOffset` is exclusive**.
* `OffsetEnd == endOffset` and is the **exclusive upper bound**.
* `/hwm` is the Kafka **log end offset (LEO)**, also **end-exclusive**, equal to the max committed `OffsetEnd` for the stream.
* Invariant: `/hwm` MUST NOT advance beyond the max `OffsetEnd` visible in the index; ideally updated in the *same metadata commit* that adds the newest index entry.

### 4.3 Stream–table duality mapping

For each topic `T`:

* Streaming view: Kafka topic partitions `T[0..N-1]`.
* Table view: Iceberg table `T` (namespace configurable), containing rows with:

  * topic metadata (optional)
  * partition id
  * offset
  * timestamp
  * key/value/headers (raw bytes) + optional decoded columns later.

Iceberg table properties can carry custom metadata via its properties map; this is a good place to store Dray-specific table metadata (e.g., `dray.topic`, `dray.cluster_id`, `dray.schema_version`).

---

## 5. Storage layout and formats

### 5.1 Object naming and layout (v1)

All paths include a **cluster prefix** to support multi-cluster usage in a single bucket.

* **WAL objects**

  * `s3://<bucket>/<prefix>/wal/v1/zone=<brokerZone>/domain=<metaDomain>/date=YYYY/MM/DD/<walId>.wo`
  * Notes:
    * Including `zone=` is for observability/debugging only.
    * Including `domain=` aligns with MetaDomain-scoped atomic commits (§9.2).
    * Readers MUST NOT assume they only read same-zone objects; cross-zone reads are allowed.

* **Compacted Parquet**

  * `s3://<bucket>/<prefix>/compaction/v1/topic=<topic>/partition=<p>/date=YYYY/MM/DD/<parquetId>.parquet`

* **Iceberg metadata**

  * `s3://<bucket>/<prefix>/iceberg/topic=<topic>/metadata/...`

### 5.2 WAL object format (Dray WAL v1)

The WAL must enable:

* efficient sequential writes (streaming ingest)
* deterministic decoding and compaction
* efficient range reads for fetch (via metadata index pointing into file positions)

**Design principle**: Keep the WAL format *small, explicit, and versioned*, and keep Kafka protocol bytes either:

* stored directly (record batch bytes), or
* stored as an internal canonical row format.

**Recommended choice for v1 (minimal + correct)**:

* Persist Kafka **record batch bytes** as the payload, but **treat base offset as a logical patch** applied at read time based on assigned offsets (see §9.5).

### 5.2.1 Deliberate deviation from Ursa (explicit)

Ursa describes WAL objects as "row-based format" (paper).

**Dray v1.2 WAL format is deliberately minimal**: store **Kafka record batch bytes** (as received) plus a small Dray header and per-stream chunk table.

Implications:

* Compactor MUST parse Kafka record batches (including compressed batches).
* Fetch can return stored bytes with **patched baseOffset** without recomputing CRC (since baseOffset is outside CRC region in Kafka batch format; implementation detail to confirm in tests).
* Parquet fetch rebuilds batches (v1.2 may emit uncompressed batches).

This deviation MUST be documented in code and tests.

### 5.2.2 WAL file structure (v1.2 binary format)

**Header (fixed size: 49 bytes):**

| Field | Type | Size | Notes |
|-------|------|------|-------|
| magic | bytes | 7 | `"DRAYWO1"` |
| version | u16 | 2 | `1` for v1 |
| walId | uuid | 16 | Unique WAL object ID |
| metaDomain | u32 | 4 | MetaDomain this WAL belongs to |
| createdAtUnixMs | i64 | 8 | Creation timestamp |
| chunkCount | u32 | 4 | Number of stream chunks |
| chunkIndexOffset | u64 | 8 | Byte offset to chunk index |

**Chunk index (sorted by streamId):** repeated `chunkCount` times

| Field | Type | Size | Notes |
|-------|------|------|-------|
| streamId | u64 | 8 | Stream identifier |
| chunkOffset | u64 | 8 | Byte offset to chunk body |
| chunkLength | u32 | 4 | Length of chunk body |
| recordCount | u32 | 4 | Total records in chunk |
| batchCount | u32 | 4 | Number of Kafka record batches |
| minTimestampMs | i64 | 8 | Min record timestamp in chunk |
| maxTimestampMs | i64 | 8 | Max record timestamp in chunk |

**Chunk body:** for each chunk

* `batchCount` × batch entries:
  * `batchLength`: u32 (4 bytes)
  * `batchBytes`: bytes (Kafka record batch as provided)

**Design constraint:** Chunk boundaries MUST be aligned to batch boundaries (never split a record batch).

**Footer (optional v1):**

* CRC32C of entire file
* (optional) StreamID→(offset,len) index for faster skipping during compaction

**Why sorted by stream_id?**
Ursa’s WO captures a set of stream entries sorted by `stream_id`. 

### 5.3 Compacted Parquet schema (Iceberg)

A minimal row schema that supports:

* reconstructing Kafka fetch responses
* analytics over offsets/timestamps
* later evolution to typed payload columns

**Base schema (v1)**

| Column           | Type                                    | Notes                                         |
| ---------------- | --------------------------------------- | --------------------------------------------- |
| `partition`      | int                                     | Kafka partition id                            |
| `offset`         | long                                    | Kafka offset                                  |
| `timestamp`      | timestamp                               | Kafka record timestamp                        |
| `key`            | binary (nullable)                       | raw key bytes                                 |
| `value`          | binary (nullable)                       | raw value bytes                               |
| `headers`        | list<struct<key:string, value:binary>>  | **ordered list; preserves duplicates**        |
| `producer_id`    | long (nullable)                         | always null (idempotence deferred)            |
| `producer_epoch` | int (nullable)                          | always null                                   |
| `base_sequence`  | int (nullable)                          | always null                                   |
| `attributes`     | int                                     | from record batch                             |
| `record_crc`     | int (optional)                          | debugging/validation                          |

**IMPORTANT:** Do NOT use `map<string, binary>` for headers. Kafka headers are an **ordered list** that allows **duplicate keys**. Using a map would violate Kafka semantics.

**Iceberg partition spec (v1)**

* Partition by `partition` (identity) and optionally by `day(timestamp_ms)` if configured.

Config:
* `table.iceberg.partitioning = ["partition"]` (default)

This partitioning aligns with operational simplicity (reasonable file sizes) and queryability.

**Parquet file naming:**
See §5.1 for full path format: `s3://<bucket>/<prefix>/compaction/v1/topic=<topic>/partition=<p>/date=YYYY/MM/DD/<parquetId>.parquet`

**Parquet statistics:**
Compactor MUST populate file-level stats where possible:
* min/max `offset`
* min/max `timestamp_ms`

This accelerates `ListOffsets(timestamp)` and table queries.

---

## 6. Metadata engine abstraction

### 6.1 Required primitives

Ursa relies on a metadata service that supports:

* atomic increment / ordered offset assignment
* notification mechanism for readers
* transactional updates for index replacement during compaction 

Dray must implement a `MetadataStore` interface that minimally provides:

```go
type MetadataStore interface {
  // Basic KV
  Get(ctx, key) (value []byte, ver Version, err error)
  Put(ctx, key, value, opts...) (newVer Version, err error)
  Delete(ctx, key, opts...) error

  // Ordered listing / range query
  List(ctx, startKey, endKey, limit) ([]KV, err error)

  // Multi-op atomic transactions scoped to one “shard domain”
  Txn(ctx, scopeKey string, fn func(Txn) error) error

  // Notifications / watch feed
  Notifications(ctx) (NotificationStream, error)

  // Ephemeral keys / leases
  PutEphemeral(ctx, key, value, sessionOpts...) (ver Version, err error)
}
```

Key requirements:

* **Compare-then-set** semantics (OCC) via versions.
* **Atomic multi-step index updates** (remove old keys, put new key) in one transaction.
* **Efficient range queries** for offset index.

### 6.2 Oxia as the default metadata engine

Oxia provides:

* **Ephemeral records** whose lifecycle is tied to a client session; they are deleted if the session expires.
* **Notifications**: a change feed for a namespace; once created, a client is guaranteed to receive notifications for changes after that point even with failures.
* **Versioning**: versions for mutable operations enabling compare-then-set style atomic updates and optimistic concurrency control.
* **Key sorting** aware of `/` segments to enable efficient range queries.
* Namespaces with independent key-space and shards.
* A Go client SDK installable as `github.com/oxia-db/oxia/oxia`.

Dray will use one Oxia namespace per Dray cluster:

* `namespace = "dray/<cluster_id>"`

### 6.3 Keyspace layout (authoritative schema)

All keys begin with `/dray/v1`.

#### 6.3.1 Cluster and broker membership

* `/dray/v1/cluster/<clusterId>/brokers/<brokerId>` (ephemeral)

  * value: JSON/CBOR `{brokerId, zoneId, advertisedListeners, startedAt, buildInfo, rack, capacityHints}`
  * lease/session: Oxia client session tied to broker process

This enables discovery and liveness; ephemerals are intended for service discovery tasks.

#### 6.3.2 Topics and partitions

* `/dray/v1/topics/<topicName>`

  * value: `{topicId(UUID), partitions:int, config:map}`
* `/dray/v1/topics/<topicName>/partitions/<p>`

  * value: `{streamId(UUID), state, createdAt, configOverrides}`

#### 6.3.3 Offset index (core of the log)

Ursa offset index key is a composite:

* key: `(StreamID, OffsetEnd, CumulativeSize)`
* value: includes object location and local index into that object. 

Dray’s Oxia key encoding (string) must preserve lexicographic ordering for OffsetEnd then CumulativeSize.

Recommended encoding:

* `/dray/v1/streams/<streamId>/offset-index/<offsetEndZ>/<cumulativeSizeZ>`

  * `offsetEndZ`: zero-padded decimal width 20 (or fixed-width hex big-endian)
  * `cumulativeSizeZ`: zero-padded decimal width 20

**Value (IndexEntry)** (protobuf/flatbuffer):

The IndexEntry schema differs for WAL vs Parquet entries:

**Common fields:**

| Field | Type | Notes |
|-------|------|-------|
| `streamId` | u64 | Stream identifier |
| `startOffset` | i64 | First offset in this entry (inclusive) |
| `endOffset` | i64 | Last offset + 1 (exclusive), same as `offsetEnd` in key |
| `cumulativeSize` | i64 | Cumulative bytes up to `endOffset` |
| `createdAtMs` | i64 | When this entry was created |
| `fileType` | enum | `WAL` or `PARQUET` |
| `recordCount` | u32 | Number of Kafka records covered |
| `messageCount` | u32 | Same as recordCount for Kafka records |
| `minTimestampMs` | i64 | Min record timestamp in range |
| `maxTimestampMs` | i64 | Max record timestamp in range |

**WAL-specific fields:**

| Field | Type | Notes |
|-------|------|-------|
| `walId` | uuid | WAL object identifier |
| `walPath` | string | Full path to WAL object |
| `chunkOffset` | u64 | Byte offset to stream's chunk in WAL |
| `chunkLength` | u32 | Length of stream's chunk in bytes |
| `batchIndex` | repeated | Per-batch index for efficient offset lookup |

**batchIndex[] entry:**

| Field | Type | Notes |
|-------|------|-------|
| `batchStartOffsetDelta` | u32 | Offset delta from `startOffset` to batch start |
| `batchLastOffsetDelta` | u32 | Offset delta from `startOffset` to batch last record |
| `batchOffsetInChunk` | u32 | Byte offset within chunk |
| `batchLength` | u32 | Batch size in bytes |
| `minTimestampMs` | i64 | Min timestamp in batch |
| `maxTimestampMs` | i64 | Max timestamp in batch |

> The batchIndex enables efficient "start at offset x" by finding the first batch whose lastOffset >= x without scanning the whole chunk.

**Parquet-specific fields:**

| Field | Type | Notes |
|-------|------|-------|
| `parquetId` | uuid | Parquet file identifier |
| `parquetPath` | string | Full path to Parquet file |
| `parquetSizeBytes` | u64 | File size in bytes |
| `icebergDataFileId` | string (optional) | Iceberg data file reference (if table mode) |

The local index (batchIndex) exists in Ursa to reduce the number of offset-index entries needed.

**CumulativeSize definition (clarified):**

CumulativeSize MUST mean: Total **physical bytes attributed to this stream** up to `OffsetEnd`, where bytes are measured as:

* For WAL entries: exact `chunkLength` bytes in WAL object attributed to this stream chunk.
* For Parquet entries: Parquet file size in bytes (or the stream-specific range size if using multi-stream parquet, but v1.2 uses per-stream Parquet files).

CumulativeSize MUST be monotonic non-decreasing with OffsetEnd.

#### 6.3.4 High watermark / log end offset

* `/dray/v1/streams/<streamId>/hwm` (int64)

  * updated only after successful index commit

This supports fast `ListOffsets` and `Fetch` waiting.

#### 6.3.5 Compaction state (saga)

* `/dray/v1/streams/<streamId>/compaction/tasks/<taskId>`

  * state machine record: `CREATED → PARQUET_WRITTEN → ICEBERG_COMMITTED → INDEX_SWAPPED → WAL_GC_READY`

This is key for safe recovery (see §11).

#### 6.3.6 Consumer groups

* `/dray/v1/groups/<groupId>/state`
* `/dray/v1/groups/<groupId>/members/<memberId>`
* `/dray/v1/groups/<groupId>/offsets/<topic>/<partition>`

(Details in §12.)

---

## 7. Zone-aware routing and routing model

### 7.1 Ursa-compatible zone signaling

Ursa’s docs specify zone-aware routing by encoding the availability zone ID into the Kafka `client.id`:

* Format: `zone_id=<zone-id>,key1=value1,key2=value2`
* `zone_id` must match the availability zone ID for routing to work.

Dray will implement the same parsing rules and behavior:

* Parse `client.id` as a comma-separated list of `k=v` pairs.
* Extract `zone_id`.
* Route the client to brokers in that zone whenever possible.

### 7.2 Desired behavior

Ursa’s architecture eliminates cross-AZ replication by writing to object storage, and allows clients to connect to brokers in their same zone to reduce network costs/latency.

Dray must ensure:

1. **Metadata responses are zone-filtered**

   * When responding to `Metadata`, only return brokers in the client’s zone (if any exist).
   * Assign “leader” broker IDs for partitions from that same zone so that Kafka clients naturally connect only within-zone.

2. **Coordinator responses are zone-filtered**

   * `FindCoordinator` returns a coordinator broker in the client’s zone.
   * Since coordination state is stored in Oxia, any broker can act as “coordinator” (virtual coordinator model); all brokers serve coordinator APIs safely because state transitions are serialized in metadata.

3. **Fallback behavior**

   * If no broker exists in requested zone: return the full broker list.
   * If zone_id is missing: return the full broker list.

### 7.3 Partition affinity mapping (optional but recommended)

Ursa notes that although brokers are stateless and leaderless, “specific partitions may still be routed to designated brokers” for batch/fetch performance.

Dray will implement a deterministic mapping:

* For each `(zoneId, streamId)` pick an **affinity broker**:

  * `broker = RendezvousHash(zoneBrokers, streamId)`
* Use this broker as the “leader” in responses (purely for client connection affinity; any broker can still serve).

This improves cache locality and reduces hot-spotting.

### 7.4 Non-owner request handling policy

To avoid correctness hazards, **ownership is not correctness**.

* Default behavior (**recommended**): **serve anyway** (any broker can handle Produce/Fetch).
* Optional config `routing.enforce_owner=true`: if a request arrives at a non-owner broker, respond with the Kafka "not leader" style error to nudge client refresh (implementation detail: use the appropriate per-API error for "not leader/replica"). This mode is for cost/perf tuning, not correctness.

---

## 8. Kafka protocol layer

### 8.1 Protocol implementation strategy (correctness-first)

**Rule: do not hand-roll Kafka protocol structs.**
Use a generated protocol library.

Recommended: **`twmb/franz-go`’s `kmsg` package**:

* Provides Kafka request/response types and autogenerated serialization/deserialization functions. ([data.code.gouv.fr][2])
* Includes guidance that using `New`/`Default` initializers or pinning versions avoids unsafe zero defaults when new fields are added. Dray should follow this strictly. ([data.code.gouv.fr][2])

Dray will:

* Parse the Kafka frame header (length prefix, apiKey, apiVersion, correlationId, clientId).
* Decode requests with `kmsg` corresponding types.
* Encode responses with `kmsg`.

### 8.2 API version negotiation

* Implement `ApiVersions` early and correctly.
* Maintain a **single authoritative “supported API matrix”** in code, generated from:

  * Kafka 4.0 compatibility targets
  * Dray’s out-of-scope list

### 8.3 Request routing / handler design

Handlers should be pure functions over:

* request + connection context + auth context
* plus injected interfaces:

  * `MetadataStore`
  * `ObjectStore`
  * `Index`
  * `GroupCoordinator`
  * `IcebergCatalog` (mostly broker-side topic/table creation)

**No handler should depend on local disk state**.

---

## 9. Produce path (write pipeline)

### 9.1 Behavior summary

Ursa’s cost-optimized WAL:

* brokers batch produce requests and write them to object storage before acknowledging.

And Ursa provides:

* linearizable writes and durable commits by committing partition metadata (offset indices, etc.) to Oxia, acknowledging produce only after that commit. 

Dray will do the same.

### 9.2 Multi-stream atomic commit strategy (correctness-critical)

**Selected strategy: MetaDomain-scoped WAL objects + atomic multi-stream commit.**

To prevent data loss from partial commits:

* Each stream belongs to a deterministic **MetaDomain** (metadata shard domain), computed as:
  `metaDomain = Hash(streamId) % NumDomains` (exact function is part of metadata adapter).
* A single WAL object MAY contain streams **only from the same MetaDomain**.
* The metadata commit for a WAL object MUST be a single atomic transaction within that MetaDomain that:

  1. Allocates offsets for each stream chunk (by advancing `/hwm`)
  2. Writes corresponding index entries
  3. Marks WAL object as committed (removes staging marker)
  4. Writes any necessary refcounts/manifests

If the transaction fails, **no stream references** the WAL object → safe to delete after TTL. This avoids the "multi-stream orphan GC deletes live object" failure mode.

Ursa paper states metadata updates corresponding to a WAL object are committed atomically.

### 9.3 Produce pipeline steps

**Input**: `ProduceRequest` containing topic-partition record batches.

**Pipeline**:

1. **Validation**

   * Check topic exists, partition exists.
   * Enforce v1 out-of-scope constraints:

     * If request implies idempotence/transactions, return correct errors (see §14.3).

2. **Buffering / coalescing**

   * For each partition, append incoming record batches into an in-memory buffer:

     * bounded by `max_buffer_bytes`
     * flush triggered by size threshold or linger timeout
   * **Buffers are partitioned by `metaDomain`** (see §9.2).

3. **Flush to WAL object**

   * Create a WAL object containing entries from streams **in the same MetaDomain only**, sorted by `streamId`.
   * Write to object storage.
   * Write staging marker: `/wal/staging/<metaDomain>/<walId>` → `{ path, createdAt, sizeBytes }`
   * Must be durable before metadata commit.

4. **Commit offset index (atomic MetaDomain transaction)**
   In a **single atomic transaction** within the MetaDomain (see §9.2):

   For each stream chunk in the WAL object:
   * Read current `/hwm` (expected version)
   * Allocate `n = recordCount` offsets: `startOffset = hwm`, `endOffset = hwm + n`
   * Update `/hwm = endOffset`
   * Create offset-index entry referencing:
     * object location
     * offset in object
     * message count and local entry offsets

   Also in the same transaction:
   * Create `/wal/objects/<metaDomain>/<walId>` → `{ path, refCount, createdAt, sizeBytes }`
   * Delete staging key

   Ursa's pseudocode shows reading last key and putting a new key in a transaction. 

5. **Acknowledge**

   * Respond to client only after index commit.
   * This yields read-your-writes. 

### 9.4 Offset assignment model

Offsets are assigned by the metadata layer at commit time:

* For each stream, the commit transaction determines the next offset range.

This guarantees linearizable ordering because shard replication totally orders writes. 

### 9.5 Record batch offset patching (critical detail)

To keep the WAL immutable and still return correct offsets:

* WAL stores record batch bytes with **baseOffset = 0** (or any placeholder).
* When serving fetch, Dray patches the baseOffset in the batch header to the correct assigned base offset.

**Correctness requirement**:

* Validate that record offset deltas are consistent (0..n-1).
* Validate record batch CRC and compression correctness using franz-go batch parsing utilities (or equivalent).

**Test must exist**:

* round-trip: produce→write WAL→fetch→Kafka client reads offsets exactly and monotonically.

### 9.6 Failure handling

Cases and required behavior:

* WAL write fails → no metadata commit → return produce error; buffer may retry.

* WAL write succeeds, metadata commit fails → WAL object is orphaned (see §9.7 for safe cleanup).

* Metadata commit succeeds, broker crashes before responding → client may retry → duplicates possible (idempotence deferred).

### 9.7 WAL staging and orphan handling

To safely GC failed writes using the staging marker pattern:

1. After writing WAL object to storage, broker writes metadata key:
   `/wal/staging/<metaDomain>/<walId>` → `{ path, createdAt, sizeBytes }`

2. In the atomic commit txn (see §9.2), Dray:
   * Creates `/wal/objects/<metaDomain>/<walId>` → `{ path, refCount, createdAt, sizeBytes }`
   * Deletes staging key

**GC rules:**

* Periodically scan `/wal/staging/<metaDomain>/` and delete any WAL objects older than `wal.orphan_ttl` (e.g., 24h).
* Safe because staging implies "no committed references exist".

### 9.8 WAL object refcount (for GC)

Because WAL objects are multi-stream, Dray MUST maintain an object-level refcount:

* `/wal/objects/<metaDomain>/<walId>.refCount` initial = number of stream chunks referenced by index entries.
* Compaction that removes an index entry referencing this WAL object MUST decrement refCount in the *same txn* as the index deletion.
* When refCount reaches 0, mark object eligible for deletion:
  * `/wal/gc/<metaDomain>/<walId>` → `{ deleteAfterMs, path }`

**GC worker:**

* Scan `/wal/gc/<metaDomain>/` for entries past `deleteAfterMs`.
* Delete object from object storage.
* Delete GC marker from metadata.

---

## 10. Fetch path (read pipeline)

Ursa read path:

1. query offset index
2. range-read from object store
3. return WAL data directly or convert Parquet to row-based format for Kafka. 

### 10.1 Fetch pipeline steps

Given `(topic, partition, fetchOffset, maxBytes, maxWaitMs, minBytes)`:

1. **Resolve streamId**
2. **Ensure log end offset**

   * read `/hwm` (cached with revision)
3. **If `fetchOffset >= hwm`**:

   * if `maxWaitMs == 0`, return empty
   * else wait on metadata notifications until:

     * `/hwm` increases, or
     * timeout

Oxia notifications provide a feed of changes and are guaranteed after creation, making them suitable for long-poll waiting. 

4. **Find the offset index entry for fetchOffset**

   * Locate the smallest index key with `OffsetEnd > fetchOffset`.
   * Use `List(startKey, endKey, limit=1)`.

5. **Read from object store**

   * WAL:

     * range-read bytes covering the relevant record batches based on `offset_in_object + entry_offsets`.
   * Parquet:

     * read Parquet row groups covering offsets.
     * reconstruct Kafka record batches (uncompressed is acceptable v1).

6. **Return response**

   * Return batches until `maxBytes` satisfied or no more available.
   * Set high watermark fields equal to `hwm` (no replication/EOS).

### 10.2 Broker caches (ephemeral and revision-stamped)

Ursa stamps cache entries with latest Oxia revision to ensure monotonic session behavior. 

Dray will implement:

* `IndexCache`: streamId → recent index entries + revision
* `ObjectRangeCache`: recent WAL slices
* `ParquetRowGroupCache` (optional)

All caches are:

* bounded (memory)
* non-authoritative
* invalidated by Oxia notifications.

### 10.3 Consistency guarantees (must match Ursa)

Dray must provide the same guarantees described for Ursa:

* linearizable writes
* read-your-writes
* durability & atomicity (ack only after WAL+metadata)
* ephemeral caches only

### 10.4 ListOffsets (explicitly early + correct)

Dray MUST implement ListOffsets early because many clients call it at startup (earliest/latest).

Semantics:

* `LATEST`: return `/hwm` (LEO, end-exclusive)
* `EARLIEST`: return the smallest available offset (typically 0 unless retention deleted earlier offsets)
* `TIMESTAMP`: return the earliest offset whose record timestamp >= requested timestamp:
  * Use Parquet file stats / index entry min/max timestamps if available
  * Else fallback to WAL batchIndex min/max timestamps
  * Else (rare) scan

---

## 11. Compaction service and Iceberg integration

### 11.1 Motivation and behavior

Ursa compaction exists because interleaving streams in one WAL object hurts catch-up reads; lagging consumers would otherwise issue many non-contiguous range reads. 

Dray compaction:

* reads WAL objects
* writes per-stream Parquet files
* swaps index entries to point to Parquet (atomically)
* appends Parquet files into Iceberg tables

### 11.2 Stream–table duality mode

Dray operates in **duality mode** when Iceberg is enabled for the cluster:

* Each topic has an associated Iceberg table (created eagerly at topic creation or lazily at first compaction)
* Compaction commits Parquet data files to Iceberg
* There is no per-topic override; Iceberg enablement is cluster-wide

**Failure behavior (explicit):**

In duality mode:
* **Produce/Fetch MUST remain available even if Iceberg catalog is down**, because streaming correctness depends on WAL+metadata, not Iceberg.
* **Compaction publishing rule (correctness rail):**
  * Compactor MAY write Parquet files regardless.
  * Compactor MUST NOT swap the stream index from old objects to new Parquet objects until the Iceberg commit succeeds **if** the compaction operation is replacing existing Parquet files (to avoid table duplication).
  * For WAL→Parquet "first compaction" (no prior table files), compactor may:
    * commit Iceberg first, then index swap
    * or index swap first, then Iceberg commit
    * but MUST guarantee idempotent retries (see §11.7)

This creates a clean, explicit contract and avoids silent divergence that causes duplicates.

### 11.3 Compaction triggers (v1)

* Time-based: every `compaction.interval` per stream
* Size-based: after N MB WAL data for a stream
* Backlog-based: if consumer lag > threshold (optional future)

Config:
* `compaction.wal.max_age_ms` (convert old WAL to Parquet)
* `compaction.parquet.max_files_per_partition` (small-file compaction)
* `compaction.target_file_size_bytes` (e.g., 128–512 MiB)

**One active compactor per stream:**

Dray SHOULD enforce "one active compaction task per partition at a time" to simplify concurrency, as Ursa typically does.

Lock key:
* `/compaction/locks/<streamId>` (ephemeral)

### 11.4 Compaction unit of work

Define a **CompactionTask** as:

* input: a list of WAL index entries for a stream forming a contiguous offset range
* output:

  * one Parquet file (v1) per task (or multiple if too large)
  * one new offset-index entry referencing the Parquet file
  * removal of the old WAL index entries

### 11.5 Correct atomic index swap (must be transactional)

Ursa’s primitive for index updates removes old entries and inserts the new compacted entry within the same Oxia transaction so consumers never see partial state. 

Dray will do:

1. Write Parquet file(s) to object storage.
2. Commit Parquet file(s) to Iceberg table.
3. In a single metadata transaction:

   * remove old index keys
   * insert new index key referencing Parquet

### 11.6 Iceberg commit semantics and the "duality saga"

Because Iceberg commits are external to Oxia, Dray must use a saga:

**State machine** (persisted in Oxia):

* `CREATED`
* `PARQUET_WRITTEN` (parquet_path, offsets, stats)
* `ICEBERG_COMMITTED` (snapshot_id or commit token)
* `INDEX_SWAPPED`
* `WAL_GC_READY`

**Recovery rules**:

* If crash after `PARQUET_WRITTEN` but before Iceberg commit: retry Iceberg commit.
* If crash after Iceberg commit but before index swap: retry index swap (must ensure idempotency; use version checks).
* WAL objects are deleted only after index swap and after a retention safety window.

### 11.7 Compaction commit idempotency

Compactor MUST implement a saga with durable progress markers:

Metadata keys:
* `/compaction/<streamId>/jobs/<jobId>` → state: `WRITTEN_PARQUET`, `COMMITTED_ICEBERG`, `SWAPPED_INDEX`, `DONE`

Rules:
* If crash after Parquet write, re-run; reuse same Parquet or write new and mark old as garbage.
* If crash after Iceberg commit but before index swap, redo index swap only.
* If crash after index swap but before DONE, mark DONE and enqueue old objects for GC.

Ursa paper notes "rewrite into fresh Parquet then update index" enabling zero-downtime reads.

**Iceberg commit strategy (v1.2 minimal, correct):**

* Single writer per topic (or per partition) enforced via metadata lock:
  * `/iceberg/<topic>/lock` (ephemeral)
* Commits are append + (optional) remove:
  * For WAL→Parquet compaction: Add files
  * For Parquet→Parquet rewrite compaction: Remove old files, Add new file(s)
* Commit MUST be retried safely:
  * Use deterministic `commitId` and/or detect already-applied commits via snapshot properties.

### 11.8 Iceberg catalog integration strategy (v1)

**Primary target**: Iceberg REST catalog spec.

* Dray implements a `Catalog` interface:

  * `LoadTable`
  * `CreateTableIfMissing`
  * `AppendDataFiles`
  * `GetCurrentSnapshot`
* Provide adapters:

  * `RestCatalog` (standard)
  * Optional AWS Glue REST endpoint support (SigV4).

**Library usage**:

* Prefer `apache/iceberg-go` where it supports needed operations; it is a Go implementation of the Iceberg table spec.

### 11.9 Schema evolution

* Default schema (raw bytes) is stable.
* Add typed columns later via table schema evolution (Iceberg supports it by design; follow Iceberg spec).

---

## 12. Consumer offsets and group coordination

### 12.1 Kafka 4 group coordination targets

Kafka 4.0 includes:

* a brand-new group coordinator implementation
* KIP-848 consumer rebalance protocol GA ([Apache Kafka][1])

Dray targets Kafka v4, therefore must implement:

* classic group protocol (for broad compatibility)
* KIP-848 protocol (for Kafka 4 clients / GA behavior)

**API surface (explicit):**

Classic group APIs (supported):
* FindCoordinator (key 10)
* JoinGroup (11), SyncGroup (14), Heartbeat (12), LeaveGroup (13)
* DescribeGroups (15), ListGroups (16), DeleteGroups (42)
* OffsetCommit (8), OffsetFetch (9)

Consumer group APIs (supported):
* ConsumerGroupHeartbeat (key 68)
* ConsumerGroupDescribe (key 69)

v1.2 explicitly not supported:
* ShareGroup* APIs (keys 76+) — ignore/not advertised

### 12.2 Protocol selection and interop policy (clarified)

Dray v1.2 supports **offline conversion only** between Classic and Consumer:

* A group's protocol type is fixed once created and may switch only when the group is empty (no members).
* If a client attempts to join using the wrong protocol for an existing non-empty group:
  * return `MISMATCHED_ENDPOINT_TYPE` or protocol-appropriate error (see Kafka protocol error table).

Kafka supports online upgrade under constraints, but Dray v1.2 defers mixed-membership interop to later versions. (This is an explicit contract, not an ambiguity.)

### 12.3 Coordination architecture in Dray (virtual coordinator)

Because Dray brokers are stateless and share state via Oxia:

* any broker can serve “coordinator” APIs
* coordinator identity in protocol is a *routing hint* only

This also aligns with zone-aware routing: each client can talk to a local-zone broker, while shared state ensures correctness.

### 12.4 Group state model (persisted in Oxia)

Persist:

* group epoch (monotonic)
* protocol type
* member list and metadata
* assignment
* timers (session timeout, rebalance timeout)
* committed offsets

Use Oxia versioning (OCC) to serialize state transitions:

* read group state + version
* compute next state
* `Put` with ExpectedVersionId(version)

Storage model for group state:

* `/dray/v1/groups/<groupId>/type` → `classic|consumer`
* `/dray/v1/groups/<groupId>/members/<memberId>` → member metadata, epochs, subscriptions
* `/dray/v1/groups/<groupId>/assignment/<memberId>` → assignment blob
* `/dray/v1/groups/<groupId>/state` → state machine (generation/epoch, leader, etc.)
* `/dray/v1/groups/<groupId>/offsets/<topic>/<partition>` → committed offsets

All updates that must be atomic across keys MUST be executed as MetaDomain transactions (group keys MUST map to the same domain; groupId hashing ensures this).

### 12.5 Coordinator ownership and timers (correctness-critical)

Group state is stored in metadata (Oxia). Ursa does this and hashes `<groupId, zone>` to select coordinator.

**Problem:** Timers (session timeout expiry, rebalance completion, etc.) require an active owner.

**Dray solution: Lease-based execution with deterministic candidate.**

1. Candidate coordinator = `Hash(groupId, zone) → brokerId` (same as Ursa).
2. Candidate acquires an **ephemeral lease** key:
   * `/groups/<groupId>/lease` (ephemeral) = `{ brokerId, epoch }`
3. Only the lease-holder runs:
   * session timeout sweep
   * delayed rebalance timeouts
   * assignment transitions

If the broker dies, the ephemeral lease disappears; another broker acquires it. (Oxia ephemerals are designed for session-bound state.)

### 12.6 Classic group protocol (JoinGroup/SyncGroup/Heartbeat)

Implement the classic Kafka group semantics:

* JoinGroup: register member, determine leader
* SyncGroup: leader provides assignment (or broker performs assignment if configured)
* Heartbeat: update liveness; if missed beyond timeout, remove member and trigger rebalance
* LeaveGroup: remove member
* DescribeGroups/ListGroups: read from Oxia

**Assignors**:

* Provide at least: range, round-robin (v1)
* Later: sticky/cooperative-sticky (port from Java reference for correctness)

### 12.7 KIP-848 "consumer" protocol (Kafka 4)

Implement the newer rebalance protocol:

* server-driven incremental coordination
* async / incremental assignments as per KIP-848 description ([Confluent][3])

**Server-side assignors (Consumer groups):**

Kafka 4.0 server controls assignors; by default `uniform` and `range` are available, and default is first in the list unless consumer selects otherwise.

Dray v1.2 MUST implement:
* `uniform` assignor (default)
* `range` assignor

Config:
* `group.consumer.assignors = ["uniform", "range"]` (default)
* `group.consumer.session.timeout.ms`, `group.consumer.heartbeat.interval.ms` are server-driven in Kafka 4.0.

**Implementation approach**:

* Port Kafka's Java coordinator state machine behavior into Go with:

  * explicit finite state machines
  * golden tests comparing to Java broker outputs for key scenarios

### 12.8 Offset commit/fetch

Kafka v4 protocol uses `generation_id_or_member_epoch` field for both classic and consumer groups in offset commit/fetch schemas. Dray MUST handle this correctly:

* For Classic groups: interpret as `generationId`
* For Consumer groups: interpret as `memberEpoch`

Persist offsets in Oxia under:

* `/dray/v1/groups/<groupId>/offsets/<topic>/<partition>` → `{ offset, leaderEpoch?, metadata, commitTimestampMs, expireTimestampMs }`

Support:

* OffsetCommit
* OffsetFetch
* ListConsumerGroupOffsets (Admin)
* OffsetDelete (Admin)

Support `retention_time_ms` semantics best-effort (expire old commits) via coordinator sweeper.

---

## 13. Topic and cluster management

### 13.1 Topic lifecycle

Implement:

* CreateTopics
* DeleteTopics
* DescribeTopics / Metadata
* CreatePartitions (optional v1.1; must preserve streamId stability for existing partitions)

On topic create:

1. Write topic metadata to Oxia
2. Create stream entries for partitions
3. Create Iceberg table if configured as “dual” (default yes)

### 13.2 Configs

Maintain a minimal supported config set with strict validation:

* retention.ms / retention.bytes (Dray-enforced via compactor + GC)
* cleanup.policy = delete | compact (compact may be a later milestone, but should be modeled)
* min.insync.replicas, replication.factor: accept but ignore (or validate to 1)

Expose via:

* DescribeConfigs
* AlterConfigs / IncrementalAlterConfigs

### 13.3 ACLs and auth (operationally important)

Ursa Cloud uses SASL/PLAIN over TLS (SASL_SSL).

Dray v1:

* Support TLS (server cert)
* Support SASL/PLAIN
* Optional token-like password convention (not required)

ACL engine:

* simple allow/deny rules
* stored in Oxia under `/dray/v1/acls/...`

---

## 14. Kafka API coverage matrix (v1.2)

### 14.1 MUST implement (core clients)

* ApiVersions (18)
* Metadata (3)
* Produce (0), Fetch (1), ListOffsets (2)
* FindCoordinator (10)
* OffsetCommit (8), OffsetFetch (9)
* Classic group APIs (11/12/13/14/15/16/42)
* Consumer group APIs (68/69)

### 14.2 SHOULD implement (common admin)

* CreateTopics (19), DeleteTopics (20)
* DescribeConfigs (32), IncrementalAlterConfigs (44)
* DescribeCluster (60)

### 14.3 MUST NOT advertise / MUST reject (deferred)

**Transaction APIs:**
* AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26)
* TxnOffsetCommit (28), WriteTxnMarkers (27)
* DescribeTransactions (65), ListTransactions (66)

**Idempotence-related:**
* InitProducerId (22) (unless implementing partial non-idempotent behavior later)

Also, do not advertise idempotence/EOS capabilities.

Kafka 4.0 includes transactional protocol changes (KIP-890), but Dray v1.2 defers transactions entirely. ([Apache Kafka][1])

**Behavior:**

* Do not list these as supported in ApiVersions.
* If invoked anyway:
  * respond with `UNSUPPORTED_VERSION` where applicable or request-specific top-level error code `INVALID_REQUEST` with a clear log message.

### 14.4 Inter-broker / controller APIs (not applicable)

Requests that are Kafka internals (LeaderAndIsr, StopReplica, UpdateMetadata, etc.) should return `UNSUPPORTED_VERSION` or `INVALID_REQUEST`, and should not be advertised in ApiVersions.

### 14.5 Tiered storage APIs (Kafka-native) (deferred)

Kafka protocol includes tiered-storage-related error codes (e.g., `OFFSET_MOVED_TO_TIERED_STORAGE`).
Dray v1.2 does not implement tiered storage APIs and does not return those errors.

---

## 15. Retention and garbage collection

### 15.1 Safety rule

Dray MUST delete objects from object storage only when:

* They are not referenced by any stream index entry, AND
* (if Iceberg enabled) they have been removed from Iceberg table metadata (or the index swap is known to have happened after the table commit, per §11.2).

### 15.2 WAL GC

Triggered when WAL refCount reaches 0 (§9.8):

* delete object after `wal.gc.grace_ms` (default e.g. 10 minutes)

**GC worker behavior:**
* Scan `/wal/gc/<metaDomain>/` for entries past `deleteAfterMs`.
* Delete object from object storage.
* Delete GC marker from metadata.

### 15.3 WAL orphan GC

Orphaned WAL objects (staging marker exists but no commit) are cleaned up by:

* Periodically scanning `/wal/staging/<metaDomain>/`
* Deleting any WAL objects older than `wal.orphan_ttl` (default: 24h)
* Safe because staging implies "no committed references exist"

### 15.4 Parquet GC

When compaction rewrites Parquet:

* old Parquet objects are eligible for deletion after the compaction job reaches `DONE` and a grace period.

---

## 16. Notifications, caching, and correctness

### 16.1 Broker metadata cache

Brokers cache:

* stream metadata (streamId, configs)
* `/hwm`
* recent index entries

Caches MUST be invalidated by:

* Oxia notifications feed for the namespace.

### 16.2 Notification guarantees

Oxia states: after a notification object is created, client is guaranteed to receive notifications of all changes after that point, even with arbitrary failures.

Dray uses this to:

* wake fetch long-polls
* keep caches coherent

### 16.3 Resubscribe behavior

On notification stream restart, broker MUST re-check `/hwm` to avoid missing updates; missed notifications are tolerated because correctness is ensured by re-checking state.

---

## 17. Operational model

### 17.1 Deployment topology

Minimal production topology:

* `drayd` (N instances per zone)
* `dray-compactor` (M instances global or per zone)
* `oxia` cluster (3–5 nodes typical)
* object storage bucket/prefix
* Iceberg catalog endpoint (optional but recommended)

Ursa positions cost-optimized storage as object-storage-based with stateless brokers.

### 17.2 Single-binary operational simplicity

One container image, multiple modes:

* `drayd broker`
* `drayd compactor`
* `drayd admin` (CLI tools)

### 17.3 Observability

Expose:

* **Prometheus metrics:**
  * produce latency (p50, p99, p999)
  * fetch latency (p50, p99, p999)
  * WAL flush sizes, WAL flush latency
  * commit txn retries
  * compaction backlog (bytes, file count)
  * GC backlog (orphan count, pending deletes)
  * Oxia latency (get/put/txn)
  * object store latency (read/write)
  * active connections, requests/sec per API type

* **Structured logs** with request correlationId and traceId propagation

* **Health endpoints:**
  * `/healthz` liveness (broker process alive)
  * `/readyz` readiness (Oxia reachable, object store reachable, compactor healthy)

### 17.4 Upgrades and compatibility

* Broker is stateless → rolling upgrade is straightforward.
* Metadata schema is versioned (`/dray/v1/...`).
* WAL format is versioned.

---

## 18. Correctness-by-construction methodology (LLM-friendly)

This project will be implemented by LLMs; correctness must be “baked into the rails”.

### 18.1 Non-negotiable engineering rules

1. **Generated protocol types only** (kmsg).
2. **Every metadata mutation must be one of**:

   * single-key CAS (`ExpectedVersionId`)
   * scoped transaction
3. **No implicit ordering assumptions** beyond Oxia transaction/CAS semantics.
4. **All cross-system operations use sagas** (compaction + Iceberg).
5. **Every feature must ship with**:

   * unit tests
   * deterministic integration tests using Kafka clients
   * fault injection tests for critical paths

### 18.2 Core invariants as executable tests

Define a suite of invariants that run in CI:

* **I1**: produced offsets strictly increase per partition.
* **I2**: after produce ack, fetch from same client sees those records (read-your-writes). 
* **I3**: crash broker after WAL write but before metadata commit → records not visible.
* **I4**: crash after metadata commit but before response → duplicates possible but offsets remain monotonic.
* **I5**: compaction index swap atomicity: consumers never see partial index state. 
* **I6**: zone-aware metadata returns only zone brokers when zone_id is set.

### 18.3 Compatibility harness

Build a compatibility harness that runs:

* Kafka producer/consumer clients against Dray
* standard patterns:

  * produce/consume
  * consumer groups
  * rebalances (classic + KIP-848)
  * admin create/delete topics
* verify results against expected semantics.

### 18.4 Property testing & state-machine modeling

For metadata-driven state machines (offset index, group coordinator), implement:

* state machine model tests (randomized sequences)
* linearizability checks (Jepsen-style or model-based)

---

## 19. Go codebase structure (minimal but modular)

Recommended repo layout:

```
cmd/
  drayd/                 # main
internal/
  server/                # TCP server, connection lifecycle
  protocol/              # kmsg decoding/encoding, api routing
  auth/                  # TLS/SASL/ACL hooks
  metadata/              # MetadataStore interface + implementations
    oxia/
  objectstore/           # ObjectStore interface + s3/gcs/azure
  wal/                   # WAL format, writer, reader, validation
  index/                 # Offset index operations, caching
  topics/                # topic metadata + admin operations
  groups/                # group coordinator (classic + KIP-848)
  fetch/                 # fetch planner + record batch builder
  compaction/
    planner/
    worker/
  iceberg/
    catalog/
    writer/
pkg/                     # small exported SDK (optional)
tests/
  integration/
  compatibility/
```

---

## 20. Implementation plan: manageable chunks + recommended order

Below is an order that minimizes rework and maximizes correctness “early”.

### Phase 0 — Foundations (Week 0 equivalent)

**Deliverables**

* Repo scaffolding, build, lint, CI
* Deterministic config system (YAML + env overrides)
* Logging + metrics skeleton
* Interfaces: `MetadataStore`, `ObjectStore`, `Catalog`

**Correctness gates**

* Unit test baseline
* Static analysis gates

---

### Phase 1 — Oxia integration + core metadata schema

**Deliverables**

* Oxia client wrapper (sync API first)
* Namespace setup
* Keyspace helpers + encoders (zero-padded numeric keys)
* CAS + transaction primitives
* Notifications stream consumer

**Why first?** Every other correctness property depends on metadata semantics.

**Correctness gates**

* CAS semantics tests using Oxia versioning.
* Notification-driven invalidation test.

---

### Phase 2 — Object store + WAL format v1

**Deliverables**

* S3 object store adapter (PUT, GET range, HEAD, DELETE, multipart)
* WAL v1 encoder/decoder with checksum
* WAL writer that can batch multi-stream entries sorted by streamId
* **MetaDomain-aware buffering** (streams partitioned by MetaDomain)
* WAL staging marker + orphan detection

**Correctness gates**

* WAL round-trip test
* Fuzz test for WAL parser
* MetaDomain isolation test (different domains don't mix in same WAL object)

---

### Phase 3 — Offset index implementation (append + lookup)

**Deliverables**

* Create stream, maintain `/hwm`
* Append offset-index entry transaction: `get_last_key` → `put(new_key)` 
* Lookup by offset: “smallest OffsetEnd > x”
* Index cache with revision stamping

**Correctness gates**

* Concurrent append test (2 writers) → monotonic offsets, no overlap
* Crash simulation: WAL write ok, metadata commit fails → not visible

---

### Phase 4 — Kafka protocol skeleton + core IO APIs

**Deliverables**

* TCP server + request loop
* `ApiVersions`, `Metadata`, `Produce`, `Fetch`, **`ListOffsets`** (implement early!)
* Minimal topic management in Oxia
* Record batch patching during fetch
* Long-poll fetch via Oxia notifications

**Correctness gates**

* Kafka client integration test: produce+fetch single partition
* Read-your-writes test
* ListOffsets EARLIEST/LATEST returns correct values 

---

### Phase 5 — Zone-aware routing (metadata + coordinator)

**Deliverables**

* Parse `client.id` `zone_id=...` format
* Broker registration ephemerals
* Zone-filtered `MetadataResponse` and “affinity leader” mapping

**Correctness gates**

* Ensure clients only receive in-zone broker endpoints when zone exists

---

### Phase 6 — Offsets (non-transactional) and basic admin completeness

**Deliverables**

* ListOffsets TIMESTAMP support (using index entry min/max timestamps)
* OffsetCommit/OffsetFetch (simple, not transactional)
* DescribeCluster, DescribeConfigs/AlterConfigs (subset)
* CreateTopics, DeleteTopics

**Correctness gates**

* Consumer offset round-trip tests
* ListOffsets TIMESTAMP returns correct offset

---

### Phase 7 — Classic consumer groups

**Deliverables**

* FindCoordinator (virtual coordinator)
* JoinGroup/SyncGroup/Heartbeat/LeaveGroup
* Group state stored in Oxia with CAS transitions
* Basic assignors

**Correctness gates**

* Rebalance tests with 2–3 consumers
* Failure tests: broker crash mid-rebalance

---

### Phase 8 — KIP-848 consumer protocol (Kafka 4)

**Deliverables**

* Implement KIP-848 request set and coordinator state machine
* Ensure GA behavior compatibility expectations ([Apache Kafka][1])

**Correctness gates**

* Compatibility tests using Kafka 4 client libraries configured for new protocol

---

### Phase 9 — Compactor + Parquet + Iceberg duality

**Deliverables**

* Compaction planner: choose WAL index entries for a stream
* **One-compactor-per-stream lock** (ephemeral `/compaction/locks/<streamId>`)
* WAL reader → Parquet writer (with headers as LIST<STRUCT>)
* Iceberg catalog integration (REST-first)
* Index swap transaction (remove old + put new)
* **Saga state machine with durable progress markers** (§11.6, §11.7)
* WAL refcount decrement + GC scheduling

**Correctness gates**

* Verify: after compaction, fetch returns identical logical stream
* Verify: Parquet files appear in Iceberg table and match offsets
* Crash-recovery tests at each saga step
* Idempotent retry after crash at any stage

---

### Phase 10 — Hardening and "operational simplicity" polish

**Deliverables**

* TLS + SASL/PLAIN + ACLs
* Helm charts / docker-compose
* Rate limits / quotas (optional)
* Retention and GC policies:
  * WAL orphan GC (staging marker scan)
  * WAL refcount GC (refCount=0 → delete)
  * Parquet GC after compaction rewrite
* Full observability (metrics, structured logs, health endpoints)

**Correctness gates**

* Soak tests, chaos tests, performance regressions
* GC safety: no live data deleted
* Retention: old data correctly expired

---

## 21. Notes on "copying from Java reference implementation"

For parts where Kafka semantics are famously subtle (group coordination, assignors, edge-case error codes), Dray should:

1. Treat Apache Kafka Java broker behavior as the reference model.
2. Port the *state machine and algorithms* (not the entire codebase).
3. Build **golden tests** from Kafka’s expected behaviors:

   * given a sequence of requests, assert exact response fields and errors.

This is the most reliable way to “bake correctness in” when the implementers are LLMs.

---

## 22. Summary of the minimalistic beauty

Dray’s “beautiful minimal core” is:

* **Kafka protocol in, Kafka protocol out**
* **WAL objects in object storage**
* **Offset index + coordination in Oxia**
* **Compaction produces Parquet and commits to Iceberg**
* **No broker state is authoritative**

This matches the key Ursa pillars (Kafka API compatibility, object-storage WAL, stateless leaderless brokers, Oxia metadata, and cost-optimized storage). 

[1]: https://kafka.apache.org/40/getting-started/upgrade/ "Upgrading | Apache Kafka"
[2]: https://data.code.gouv.fr/usage/go/github.com%2Ftwmb%2Ffranz-go%2Fpkg%2Fkmsg?utm_source=chatgpt.com "github.com/twmb/franz-go/pkg/kmsg | go | Ecosyste.ms: Repos"
[3]: https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/ "KIP-848: A New Consumer Rebalance Protocol for Apache Kafka® 4.0"
