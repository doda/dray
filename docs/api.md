# Dray Public APIs

This document describes Dray's public-facing APIs: Kafka wire protocol coverage, the `drayd` admin CLI, and observability endpoints.

## Kafka wire protocol coverage

Dray implements a Kafka v4-compatible subset of the protocol. The authoritative API/version matrix comes from `internal/protocol/api_versions.go` and is exposed via ApiVersions.

### Supported APIs and versions

| API key | Name | Versions |
| --- | --- | --- |
| 0 | Produce | v0-v11 |
| 1 | Fetch | v0-v16 |
| 2 | ListOffsets | v0-v8 |
| 3 | Metadata | v0-v12 |
| 8 | OffsetCommit | v0-v9 |
| 9 | OffsetFetch | v0-v9 |
| 10 | FindCoordinator | v0-v6 |
| 11 | JoinGroup | v0-v9 |
| 12 | Heartbeat | v0-v5 |
| 13 | LeaveGroup | v0-v5 |
| 14 | SyncGroup | v0-v7 |
| 15 | DescribeGroups | v0-v5 |
| 16 | ListGroups | v0-v4 |
| 18 | ApiVersions | v0-v4 |
| 19 | CreateTopics | v0-v7 |
| 20 | DeleteTopics | v0-v6 |
| 32 | DescribeConfigs | v0-v4 |
| 42 | DeleteGroups | v0-v2 |
| 44 | IncrementalAlterConfigs | v0-v1 |
| 60 | DescribeCluster | v0-v1 |
| 68 | ConsumerGroupHeartbeat (KIP-848) | v0 |
| 69 | ConsumerGroupDescribe (KIP-848) | v0 |

### Conditional SASL APIs

When SASL is enabled, ApiVersions also advertises:

| API key | Name | Versions |
| --- | --- | --- |
| 17 | SaslHandshake | v0-v1 |
| 36 | SaslAuthenticate | v0-v2 |

### Explicitly rejected APIs

Dray does not advertise or support transactional/idempotent or inter-broker APIs. If invoked directly, these are rejected with appropriate Kafka error codes (typically `UNSUPPORTED_VERSION`) and logged.

- Transactional APIs: AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26), WriteTxnMarkers (27), TxnOffsetCommit (28), DescribeTransactions (65), ListTransactions (66).
- Idempotence: InitProducerId (22).
- Inter-broker/controller APIs: LeaderAndIsr (4), StopReplica (5), UpdateMetadata (6), ControlledShutdown (7), and other Kafka-internal controller APIs.

## Admin CLI (`drayd admin`)

The `drayd` binary includes an admin CLI that talks to the metadata store and does not require a running broker connection.

### Top-level commands

```
Usage: drayd <command> [options]

Commands:
  broker      Start the Kafka protocol server (broker mode)
  compactor   Start the compaction worker
  admin       Administrative commands (topics, groups, configs, status)
  version     Print version information
```

### Broker mode

```
Usage: drayd broker [options]

Options:
  -config <path>        Path to configuration file
  -listen <addr>        Override listen address (e.g., :9092)
  -health-addr <addr>   Override health/metrics address (e.g., :9090)
  -broker-id <id>       Override broker ID (default: auto-generated UUID)
  -node-id <id>         Override Kafka node ID (default: 1)
  -zone-id <zone>       Override availability zone ID
  -cluster-id <id>      Override cluster ID (default: from config)
```

### Compactor mode

```
Usage: drayd compactor [options]

Options:
  -config <path>        Path to configuration file
  -health-addr <addr>   Override health/metrics address (e.g., :9090)
  -compactor-id <id>    Override compactor ID (default: auto-generated UUID)
```

### Admin commands

```
Usage: drayd admin <command> [options]

Commands:
  topics     Topic management (list, describe, create, delete)
  groups     Consumer group management (list, describe, delete)
  configs    Configuration management (describe, alter)
  status     Cluster status and diagnostics
```

#### Topics

```
Usage: drayd admin topics list [options]
  -config <path>   Path to configuration file
  -json            Output in JSON format

Usage: drayd admin topics describe [options] <topic>
  -config <path>   Path to configuration file
  -json            Output in JSON format

Usage: drayd admin topics create [options] <topic>
  -config <path>         Path to configuration file
  -partitions <n>        Number of partitions (default: 1)
  -config-values <k=v>   Comma-separated config key=value pairs
  -json                  Output in JSON format

Usage: drayd admin topics delete [options] <topic>
  -config <path>   Path to configuration file
  -force           Skip confirmation prompt
```

Examples:

```
# Create a topic with 10 partitions and custom configs
./drayd admin topics create --partitions 10 --config-values "retention.ms=86400000,cleanup.policy=delete" my-topic

# List topics in JSON
./drayd admin topics list --json
```

#### Consumer groups

```
Usage: drayd admin groups list [options]
  -config <path>   Path to configuration file
  -json            Output in JSON format
  -state <state>   Filter by state (Empty, Stable, PreparingRebalance, etc.)

Usage: drayd admin groups describe [options] <group-id>
  -config <path>   Path to configuration file
  -json            Output in JSON format
  -members         Show group members
  -offsets         Show committed offsets

Usage: drayd admin groups delete [options] <group-id>
  -config <path>   Path to configuration file
  -force           Skip confirmation prompt (group must be empty)
```

Examples:

```
# Describe a group and include members + offsets
./drayd admin groups describe --members --offsets my-group

# Delete an empty group without confirmation
./drayd admin groups delete --force my-group
```

#### Topic configs

```
Usage: drayd admin configs describe [options] <topic>
  -config <path>   Path to configuration file
  -json            Output in JSON format
  -defaults        Include default values

Usage: drayd admin configs alter [options] <topic>
  -config <path>   Path to configuration file
  -set <k=v>       Comma-separated key=value pairs to set
  -delete <keys>   Comma-separated config keys to delete
```

Examples:

```
# Set and delete topic config values
./drayd admin configs alter --set "retention.ms=86400000" --delete "retention.bytes" my-topic
```

#### Status

```
Usage: drayd admin status [options]
  -config <path>   Path to configuration file
  -json            Output in JSON format
```

## Observability endpoints

Dray exposes health and metrics endpoints on the health/metrics address (default is configured via `observability.metrics_addr`).

### `GET /healthz`

- Purpose: liveness probe for the broker/compactor process.
- Success: `200 OK` with JSON body.
- Failure: `503 Service Unavailable` when shutting down or when critical goroutines stop reporting.
- Methods: `GET`, `HEAD` (other methods return `405`).

Example response:

```json
{
  "status": "ok",
  "goroutines": {
    "protocol-server": true
  },
  "checks": {
    "shutdown": {"healthy": true, "message": "broker is running"},
    "goroutines": {"healthy": true, "message": "all critical goroutines are running"}
  }
}
```

### `GET /readyz`

- Purpose: readiness probe for dependencies (metadata store, object store, compactor, etc.).
- Success: `200 OK` when all readiness checks succeed.
- Failure: `503 Service Unavailable` when shutting down or any readiness check fails.
- Methods: `GET`, `HEAD` (other methods return `405`).

Example response:

```json
{
  "status": "ok",
  "checks": {
    "shutdown": {"healthy": true, "message": "broker is running"},
    "metadata": {"healthy": true, "message": "healthy"},
    "objectstore": {"healthy": true, "message": "healthy"}
  }
}
```

### `GET /metrics`

- Purpose: Prometheus scrape endpoint.
- Format: Prometheus exposition format.
- Methods: `GET` only.
- The exported metric families include:
  - Produce/fetch latency histograms and request counters
  - Connection counts and per-API error counters
  - WAL flush sizes and latency histograms
  - Object store operation metrics
  - Compaction and GC backlog metrics
  - Oxia metadata operation metrics

