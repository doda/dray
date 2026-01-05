# Configuration Reference

This document describes Dray's YAML configuration structure, defaults, and environment variable overrides. The source of truth is `internal/config/config.go`.

## Basics

- Default config path: `/etc/dray/config.yaml`
- Load order: defaults -> YAML file -> environment variable overrides
- Env override parsing:
  - strings: raw value
  - ints: base-10 integers
  - bools: `true` or `1` enable, everything else disables

## Required vs Optional

Required for a working broker/compactor deployment:
- `clusterId` (defaults to `local`)
- `broker.listenAddr`
- `metadata.oxiaEndpoint`
- `metadata.numDomains`
- `wal.flushSizeBytes`, `wal.flushIntervalMs`, `wal.orphanTTLMs`
- Object store settings (`objectStore.*`) are not validated but are required for WAL and compaction to function in practice.

Conditional requirements:
- TLS enabled: `broker.tls.certFile` and `broker.tls.keyFile`
- SASL enabled: valid `sasl.mechanism` (`PLAIN`), valid `sasl.credentialsSource` (`file` or `env`), and `sasl.credentialsFile` when `file` is selected
- Iceberg enabled: `iceberg.catalogType`

Optional settings:
- `metadata.oxiaNamespace` (defaults to `dray/<clusterId>`)
- All remaining fields have defaults and are optional unless you need to override behavior.

## Top-Level Configuration

```yaml
clusterId: local
broker:
  listenAddr: ":9092"
  zoneId: "us-east-1a"
  tls:
    enabled: false
    certFile: ""
    keyFile: ""
sasl:
  enabled: false
  mechanism: "PLAIN"
  credentialsSource: "env"
  credentialsFile: ""
  users: ""
metadata:
  oxiaEndpoint: "localhost:6648"
  oxiaNamespace: ""
  numDomains: 16
objectStore:
  endpoint: ""
  bucket: ""
  region: "us-east-1"
  accessKey: ""
  secretKey: ""
wal:
  flushSizeBytes: 16777216
  flushIntervalMs: 100
  orphanTTLMs: 60000
compaction:
  enabled: true
  maxFilesToMerge: 10
  minAgeMs: 300000
  parquetSmallFileThresholdBytes: 67108864
  parquetTargetFileSizeBytes: 268435456
  parquetMaxMergeBytes: 536870912
  parquetMinFiles: 4
  parquetMaxFiles: 50
  parquetMinAgeMs: 600000
  valueProjections: []
iceberg:
  enabled: true
  catalogType: "rest"
  catalogUri: ""
  warehouse: ""
routing:
  enforceOwner: false
observability:
  metricsAddr: ":9090"
  logLevel: "info"
  logFormat: "json"
```

## Environment Variable Overrides

These environment variables override values from YAML and defaults.

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `clusterId` | string | `local` | `DRAY_CLUSTER_ID` | Required (non-empty) |

### Broker

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `broker.listenAddr` | string | `:9092` | `DRAY_LISTEN_ADDR` | Required |
| `broker.zoneId` | string | `""` | `DRAY_ZONE_ID` | Zone-aware routing hint; optional |
| `broker.tls.enabled` | bool | `false` | `DRAY_TLS_ENABLED` | When true, cert/key required |
| `broker.tls.certFile` | string | `""` | `DRAY_TLS_CERT_FILE` | Required if TLS enabled |
| `broker.tls.keyFile` | string | `""` | `DRAY_TLS_KEY_FILE` | Required if TLS enabled |

### SASL

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `sasl.enabled` | bool | `false` | `DRAY_SASL_ENABLED` | When true, SASL is enforced |
| `sasl.mechanism` | string | `PLAIN` | `DRAY_SASL_MECHANISM` | Only `PLAIN` supported |
| `sasl.credentialsSource` | string | `env` | `DRAY_SASL_CREDENTIALS_SOURCE` | `env` or `file` |
| `sasl.credentialsFile` | string | `""` | `DRAY_SASL_CREDENTIALS_FILE` | Required when source is `file` |
| `sasl.users` | string | `""` | `DRAY_SASL_USERS` | Format: `user1:pass1,user2:pass2` |

### Metadata

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `metadata.oxiaEndpoint` | string | `localhost:6648` | `DRAY_OXIA_ENDPOINT` | Required |
| `metadata.oxiaNamespace` | string | `""` | `DRAY_OXIA_NAMESPACE` | Defaults to `dray/<clusterId>` when unset |
| `metadata.numDomains` | int | `16` | `DRAY_METADATA_NUM_DOMAINS` | Must be positive |

### Object Store

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `objectStore.endpoint` | string | `""` | `DRAY_S3_ENDPOINT` | S3-compatible endpoint URL |
| `objectStore.bucket` | string | `""` | `DRAY_S3_BUCKET` | Bucket name for WAL/Parquet |
| `objectStore.region` | string | `us-east-1` | `DRAY_S3_REGION` | Region for S3 client |
| `objectStore.accessKey` | string | `""` | `DRAY_S3_ACCESS_KEY` | Access key ID |
| `objectStore.secretKey` | string | `""` | `DRAY_S3_SECRET_KEY` | Secret access key |

### WAL

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `wal.flushSizeBytes` | int64 | `16777216` | `DRAY_WAL_FLUSH_SIZE` | Must be positive |
| `wal.flushIntervalMs` | int64 | `100` | `DRAY_WAL_FLUSH_INTERVAL_MS` | Must be positive |
| `wal.orphanTTLMs` | int64 | `60000` | `DRAY_WAL_ORPHAN_TTL_MS` | Must be positive |

### Compaction

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `compaction.enabled` | bool | `true` | `DRAY_COMPACTION_ENABLED` | Enables background compaction |
| `compaction.maxFilesToMerge` | int | `10` | `DRAY_COMPACTION_MAX_FILES` | Must be positive when enabled |
| `compaction.minAgeMs` | int64 | `300000` | `DRAY_COMPACTION_MIN_AGE_MS` | Must be >= 0 |
| `compaction.parquetSmallFileThresholdBytes` | int64 | `67108864` | `DRAY_COMPACTION_PARQUET_SMALL_FILE_THRESHOLD_BYTES` | Must be >= 0 |
| `compaction.parquetTargetFileSizeBytes` | int64 | `268435456` | `DRAY_COMPACTION_PARQUET_TARGET_FILE_SIZE_BYTES` | Must be >= 0 |
| `compaction.parquetMaxMergeBytes` | int64 | `536870912` | `DRAY_COMPACTION_PARQUET_MAX_MERGE_BYTES` | Must be >= 0 |
| `compaction.parquetMinFiles` | int | `4` | `DRAY_COMPACTION_PARQUET_MIN_FILES` | Must be >= 0 |
| `compaction.parquetMaxFiles` | int | `50` | `DRAY_COMPACTION_PARQUET_MAX_FILES` | Must be >= 0 |
| `compaction.parquetMinAgeMs` | int64 | `600000` | `DRAY_COMPACTION_PARQUET_MIN_AGE_MS` | Must be >= 0 |
| `compaction.valueProjections` | list | `[]` | (none) | Optional value-projection rules |

#### Value Projections

`compaction.valueProjections` lets you extract columns from JSON record values during compaction.

```yaml
compaction:
  valueProjections:
    - topic: "orders"
      format: "json"
      fields:
        - name: "order_id"
          path: "orderId"
          type: "string"
        - name: "amount"
          path: "total.amount"
          type: "int64"
```

Rules and defaults:
- `format`: defaults to `json` when empty
- `fields[].path`: defaults to `fields[].name` when empty
- Supported field types: `string`, `int32`, `int64`, `bool`, `timestamp_ms`, `string_list`, `int64_list`, `bool_list`
- Reserved names that cannot be used as projection field names: `partition`, `offset`, `timestamp_ms`, `key`, `value`, `headers`, `producer_id`, `producer_epoch`, `base_sequence`, `attributes`

### Iceberg

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `iceberg.enabled` | bool | `true` | `DRAY_ICEBERG_ENABLED` | Enables Iceberg commits |
| `iceberg.catalogType` | string | `rest` | `DRAY_ICEBERG_CATALOG_TYPE` | `rest`, `glue`, or `sql` |
| `iceberg.catalogUri` | string | `""` | `DRAY_ICEBERG_CATALOG_URI` | Catalog connection string |
| `iceberg.warehouse` | string | `""` | `DRAY_ICEBERG_WAREHOUSE` | Warehouse URI |

### Routing

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `routing.enforceOwner` | bool | `false` | `DRAY_ROUTING_ENFORCE_OWNER` | Reject non-owner writes when true |

### Observability

| Field | Type | Default | Env Var | Notes |
| --- | --- | --- | --- | --- |
| `observability.metricsAddr` | string | `:9090` | `DRAY_METRICS_ADDR` | Metrics HTTP bind address |
| `observability.logLevel` | string | `info` | `DRAY_LOG_LEVEL` | `debug`, `info`, `warn`, `error` |
| `observability.logFormat` | string | `json` | `DRAY_LOG_FORMAT` | `json` or `text` |

## Minimal Quickstart Configuration

A minimal config suitable for local development with a local Oxia and MinIO setup.

```yaml
metadata:
  oxiaEndpoint: "localhost:6648"
objectStore:
  endpoint: "http://localhost:9000"
  bucket: "dray"
  region: "us-east-1"
  accessKey: "minioadmin"
  secretKey: "minioadmin"
```

## Full Example Configuration

```yaml
clusterId: "prod-us-east-1"
broker:
  listenAddr: ":9092"
  zoneId: "use1-az1"
  tls:
    enabled: true
    certFile: "/etc/dray/tls/server.crt"
    keyFile: "/etc/dray/tls/server.key"
sasl:
  enabled: true
  mechanism: "PLAIN"
  credentialsSource: "file"
  credentialsFile: "/etc/dray/sasl/users.txt"
metadata:
  oxiaEndpoint: "oxia.prod.internal:6648"
  oxiaNamespace: "dray/prod-us-east-1"
  numDomains: 32
objectStore:
  endpoint: "https://s3.amazonaws.com"
  bucket: "dray-prod"
  region: "us-east-1"
  accessKey: "${DRAY_S3_ACCESS_KEY}"
  secretKey: "${DRAY_S3_SECRET_KEY}"
wal:
  flushSizeBytes: 33554432
  flushIntervalMs: 250
  orphanTTLMs: 120000
compaction:
  enabled: true
  maxFilesToMerge: 20
  minAgeMs: 900000
  parquetSmallFileThresholdBytes: 134217728
  parquetTargetFileSizeBytes: 536870912
  parquetMaxMergeBytes: 1073741824
  parquetMinFiles: 6
  parquetMaxFiles: 80
  parquetMinAgeMs: 1200000
  valueProjections:
    - topic: "orders"
      format: "json"
      fields:
        - name: "order_id"
          path: "orderId"
          type: "string"
        - name: "line_items"
          path: "items"
          type: "int64_list"
iceberg:
  enabled: true
  catalogType: "rest"
  catalogUri: "https://iceberg.prod.internal"
  warehouse: "s3://dray-warehouse"
routing:
  enforceOwner: true
observability:
  metricsAddr: ":9090"
  logLevel: "info"
  logFormat: "json"
```
