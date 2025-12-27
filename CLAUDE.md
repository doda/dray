# Dray - Kafka v4 Compatible Streaming System

## Overview
Dray is a clean-room, open-source Go implementation of StreamNative's Ursa Engine architecture:
- **Kafka v4 wire protocol compatibility** (including KIP-848 consumer groups)
- **Leaderless, stateless brokers** - no inter-broker replication
- **Object-storage WAL** (S3/GCS/Azure) as sole durability layer
- **Oxia metadata engine** for offset index, coordination, and discovery
- **Stream-table duality** with Iceberg/Parquet integration
- **Zone-aware routing** for multi-AZ cost optimization

## Project Status
**NOT YET IMPLEMENTED** - This repo contains planning artifacts only.

## Key Requirements

### Must Have (v1)
- Kafka v4 client compatibility (produce, fetch, consumer groups)
- Classic group protocol (JoinGroup/SyncGroup/Heartbeat)
- KIP-848 consumer protocol (Kafka 4 GA)
- WAL on object storage with Oxia offset index
- Compaction to Parquet with Iceberg table commits
- Zone-aware routing via client.id zone_id parsing

### Explicitly Deferred
- Transactions (Kafka transactions)
- Idempotent producers
- Exactly-once semantics
- Kafka-native tiered storage APIs

### Architectural Invariants
1. Produce ack only after WAL + metadata commit
2. Linearizable write ordering per partition
3. Read-your-writes consistency
4. Brokers never authoritative (caches only)
5. Atomic compaction index swap

## Recommended Stack
- **Language**: Go 1.21+
- **Protocol**: twmb/franz-go (kmsg package for wire protocol)
- **Metadata**: github.com/oxia-db/oxia Go SDK
- **Object Store**: AWS SDK v2 (S3-compatible)
- **Parquet**: parquet-go or apache/parquet-go
- **Iceberg**: apache/iceberg-go

## Directory Structure (target)
```
cmd/
  drayd/                 # main binary (broker/compactor/admin modes)
internal/
  server/                # TCP server, connection lifecycle
  protocol/              # kmsg decoding/encoding, API routing
  auth/                  # TLS/SASL/ACL hooks
  metadata/              # MetadataStore interface + Oxia implementation
  objectstore/           # ObjectStore interface + S3/GCS adapters
  wal/                   # WAL format v1 encoder/decoder
  index/                 # Offset index operations and caching
  topics/                # Topic metadata and admin operations
  groups/                # Group coordinator (classic + KIP-848)
  fetch/                 # Fetch planner and record batch builder
  produce/               # Produce buffer and commit logic
  compaction/            # Compaction planner and worker
  iceberg/               # Catalog and writer integration
  gc/                    # WAL and Parquet garbage collection
  routing/               # Zone-aware routing and affinity
tests/
  integration/           # End-to-end tests with Kafka clients
  compatibility/         # Golden tests against Kafka broker
  property/              # Property-based testing
```

## Getting Started
1. Run the `project-setup` task first
2. Follow task dependencies in task_list.json
3. See SPEC.md for full technical specification

## Key Files
- `task_list.json` - Comprehensive task list with dependencies
- `agent-progress.txt` - Progress log for agents
- `SPEC.md` - Full technical specification
- `init.sh` - Project initialization script (to be implemented)
