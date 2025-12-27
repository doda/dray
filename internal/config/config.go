// Package config provides configuration loading and validation for Dray.
// Supports YAML files with environment variable overrides.
package config

// Config holds all configuration for a Dray broker.
type Config struct {
	Broker       BrokerConfig       `yaml:"broker"`
	Metadata     MetadataConfig     `yaml:"metadata"`
	ObjectStore  ObjectStoreConfig  `yaml:"objectStore"`
	WAL          WALConfig          `yaml:"wal"`
	Compaction   CompactionConfig   `yaml:"compaction"`
	Iceberg      IcebergConfig      `yaml:"iceberg"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type BrokerConfig struct {
	ListenAddr string `yaml:"listenAddr" env:"DRAY_LISTEN_ADDR"`
	ZoneID     string `yaml:"zoneId" env:"DRAY_ZONE_ID"`
}

type MetadataConfig struct {
	OxiaEndpoint string `yaml:"oxiaEndpoint" env:"DRAY_OXIA_ENDPOINT"`
	Namespace    string `yaml:"namespace" env:"DRAY_OXIA_NAMESPACE"`
}

type ObjectStoreConfig struct {
	Endpoint  string `yaml:"endpoint" env:"DRAY_S3_ENDPOINT"`
	Bucket    string `yaml:"bucket" env:"DRAY_S3_BUCKET"`
	Region    string `yaml:"region" env:"DRAY_S3_REGION"`
	AccessKey string `yaml:"accessKey" env:"DRAY_S3_ACCESS_KEY"`
	SecretKey string `yaml:"secretKey" env:"DRAY_S3_SECRET_KEY"`
}

type WALConfig struct {
	FlushSizeBytes int64 `yaml:"flushSizeBytes" env:"DRAY_WAL_FLUSH_SIZE"`
	FlushIntervalMs int64 `yaml:"flushIntervalMs" env:"DRAY_WAL_FLUSH_INTERVAL_MS"`
	OrphanTTLMs     int64 `yaml:"orphanTTLMs" env:"DRAY_WAL_ORPHAN_TTL_MS"`
}

type CompactionConfig struct {
	Enabled         bool  `yaml:"enabled" env:"DRAY_COMPACTION_ENABLED"`
	MaxFilesToMerge int   `yaml:"maxFilesToMerge" env:"DRAY_COMPACTION_MAX_FILES"`
	MinAgeMs        int64 `yaml:"minAgeMs" env:"DRAY_COMPACTION_MIN_AGE_MS"`
}

type IcebergConfig struct {
	Enabled      bool   `yaml:"enabled" env:"DRAY_ICEBERG_ENABLED"`
	CatalogType  string `yaml:"catalogType" env:"DRAY_ICEBERG_CATALOG_TYPE"`
	CatalogURI   string `yaml:"catalogUri" env:"DRAY_ICEBERG_CATALOG_URI"`
	Warehouse    string `yaml:"warehouse" env:"DRAY_ICEBERG_WAREHOUSE"`
}

type ObservabilityConfig struct {
	MetricsAddr string `yaml:"metricsAddr" env:"DRAY_METRICS_ADDR"`
	LogLevel    string `yaml:"logLevel" env:"DRAY_LOG_LEVEL"`
	LogFormat   string `yaml:"logFormat" env:"DRAY_LOG_FORMAT"`
}

// Default returns a Config with sensible defaults.
func Default() *Config {
	return &Config{
		Broker: BrokerConfig{
			ListenAddr: ":9092",
		},
		Metadata: MetadataConfig{
			OxiaEndpoint: "localhost:6648",
			Namespace:    "dray",
		},
		ObjectStore: ObjectStoreConfig{
			Region: "us-east-1",
		},
		WAL: WALConfig{
			FlushSizeBytes:  16 * 1024 * 1024, // 16MB
			FlushIntervalMs: 100,
			OrphanTTLMs:     60000, // 1 minute
		},
		Compaction: CompactionConfig{
			Enabled:         true,
			MaxFilesToMerge: 10,
			MinAgeMs:        300000, // 5 minutes
		},
		Iceberg: IcebergConfig{
			Enabled:     true,
			CatalogType: "rest",
		},
		Observability: ObservabilityConfig{
			MetricsAddr: ":9090",
			LogLevel:    "info",
			LogFormat:   "json",
		},
	}
}
