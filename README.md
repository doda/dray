# Dray

Dray is a clean-room, open-source Go implementation of StreamNative's Ursa
Engine architecture: a Kafka v4-compatible streaming system with stateless,
leaderless brokers, object-storage durability, and Oxia-backed metadata.

## Highlights

- Kafka v4 wire-protocol compatibility (produce, fetch, admin, groups).
- Leaderless, stateless brokers backed by object-storage WAL.
- Oxia metadata for offset index, coordination, and discovery.
- Stream-table duality with Parquet compaction and Iceberg commits.
- Zone-aware routing via `client.id` zone hints.
- Single binary with broker, compactor, and admin modes.

Deferred by design: Kafka transactions, idempotent producers, and EOS.

## Build

Prerequisites:
- Go 1.21+
- Oxia cluster reachable from the broker/compactor
- S3-compatible object storage (MinIO, AWS S3, etc.)
- Optional: Iceberg REST catalog endpoint

Build with Makefile:

```bash
make build
```

Or build directly:

```bash
go build -o bin/drayd ./cmd/drayd
```

## Run

Dray runs from a single binary:

```
drayd <command> [options]
```

Commands:
- `broker` - Kafka protocol server
- `compactor` - WAL to Parquet compaction worker
- `admin` - metadata-aware admin CLI

Example (config file path is optional; default is `/etc/dray/config.yaml`):

```bash
./bin/drayd broker -config ./config.yaml
./bin/drayd compactor -config ./config.yaml
./bin/drayd admin status -config ./config.yaml
```

See `cmd/drayd/main.go` for CLI flags and `docs/api.md` for full usage.

## Quickstart

Minimal `config.yaml` (edit endpoints, bucket, and credentials):

```yaml
clusterId: local
broker:
  listenAddr: ":9092"
metadata:
  oxiaEndpoint: "localhost:6648"
  numDomains: 16
objectStore:
  endpoint: "http://localhost:9000"
  bucket: "dray"
  region: "us-east-1"
  accessKey: "minioadmin"
  secretKey: "minioadmin"
wal:
  flushSizeBytes: 16777216
  flushIntervalMs: 100
  orphanTTLMs: 60000
compaction:
  enabled: true
iceberg:
  enabled: true
  catalogType: "rest"
  catalogUri: "http://localhost:8181"
  warehouse: "s3://dray-warehouse"
observability:
  metricsAddr: ":9090"
  logLevel: "info"
  logFormat: "json"
```

Start broker and compactor:

```bash
./bin/drayd broker -config ./config.yaml
./bin/drayd compactor -config ./config.yaml
```

Admin examples:

```bash
./bin/drayd admin topics create -config ./config.yaml --partitions 3 my-topic
./bin/drayd admin topics list -config ./config.yaml
./bin/drayd admin status -config ./config.yaml
```

## Development and verification

Initialize and run the full local checks (downloads deps, vet, build, tests):

```bash
./init.sh
```

Quick sanity check:

```bash
./init.sh --quick
```

## Documentation

- Architecture overview: `docs/architecture.md`
- API reference: `docs/api.md`
- Configuration reference: `docs/configuration.md`
- Full specification: `SPEC.md`
