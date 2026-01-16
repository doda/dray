// Package config provides configuration loading and validation for Dray.
// Supports YAML files with environment variable overrides.
package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/dray-io/dray/internal/projection"
	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the default path for the Dray configuration file.
const DefaultConfigPath = "/etc/dray/config.yaml"

// DefaultClusterID is the default cluster identifier used for Oxia namespaces.
const DefaultClusterID = "local"

// Config holds all configuration for a Dray broker.
type Config struct {
	ClusterID     string              `yaml:"clusterId" env:"DRAY_CLUSTER_ID"`
	Broker        BrokerConfig        `yaml:"broker"`
	SASL          SASLConfig          `yaml:"sasl"`
	Metadata      MetadataConfig      `yaml:"metadata"`
	ObjectStore   ObjectStoreConfig   `yaml:"objectStore"`
	WAL           WALConfig           `yaml:"wal"`
	Compaction    CompactionConfig    `yaml:"compaction"`
	Iceberg       IcebergConfig       `yaml:"iceberg"`
	Routing       RoutingConfig       `yaml:"routing"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type BrokerConfig struct {
	ListenAddr    string    `yaml:"listenAddr" env:"DRAY_LISTEN_ADDR"`
	AdvertiseAddr string    `yaml:"advertiseAddr" env:"DRAY_ADVERTISE_ADDR"`
	ZoneID        string    `yaml:"zoneId" env:"DRAY_ZONE_ID"`
	TLS           TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled" env:"DRAY_TLS_ENABLED"`
	CertFile string `yaml:"certFile" env:"DRAY_TLS_CERT_FILE"`
	KeyFile  string `yaml:"keyFile" env:"DRAY_TLS_KEY_FILE"`
}

// SASLConfig holds SASL authentication configuration.
type SASLConfig struct {
	Enabled bool `yaml:"enabled" env:"DRAY_SASL_ENABLED"`
	// Mechanism is the SASL mechanism to use ("PLAIN" only for now).
	Mechanism string `yaml:"mechanism" env:"DRAY_SASL_MECHANISM"`
	// CredentialsSource specifies where to load credentials from ("file" or "env").
	CredentialsSource string `yaml:"credentialsSource" env:"DRAY_SASL_CREDENTIALS_SOURCE"`
	// CredentialsFile path to file containing credentials (when source is "file").
	// Format: one "username:password" pair per line.
	CredentialsFile string `yaml:"credentialsFile" env:"DRAY_SASL_CREDENTIALS_FILE"`
	// Credentials loaded from environment (when source is "env").
	// Format: "DRAY_SASL_USERS=user1:pass1,user2:pass2"
	Users string `yaml:"users" env:"DRAY_SASL_USERS"`
}

type MetadataConfig struct {
	OxiaEndpoint  string `yaml:"oxiaEndpoint" env:"DRAY_OXIA_ENDPOINT"`
	OxiaNamespace string `yaml:"oxiaNamespace" env:"DRAY_OXIA_NAMESPACE"`
	NumDomains    int    `yaml:"numDomains" env:"DRAY_METADATA_NUM_DOMAINS"`
}

type ObjectStoreConfig struct {
	Endpoint  string `yaml:"endpoint" env:"DRAY_S3_ENDPOINT"`
	Bucket    string `yaml:"bucket" env:"DRAY_S3_BUCKET"`
	Region    string `yaml:"region" env:"DRAY_S3_REGION"`
	AccessKey string `yaml:"accessKey" env:"DRAY_S3_ACCESS_KEY"`
	SecretKey string `yaml:"secretKey" env:"DRAY_S3_SECRET_KEY"`
}

type WALConfig struct {
	FlushSizeBytes  int64 `yaml:"flushSizeBytes" env:"DRAY_WAL_FLUSH_SIZE"`
	FlushIntervalMs int64 `yaml:"flushIntervalMs" env:"DRAY_WAL_FLUSH_INTERVAL_MS"`
	OrphanTTLMs     int64 `yaml:"orphanTTLMs" env:"DRAY_WAL_ORPHAN_TTL_MS"`
}

type CompactionConfig struct {
	Enabled                        bool                         `yaml:"enabled" env:"DRAY_COMPACTION_ENABLED"`
	MaxConcurrentJobs              int                          `yaml:"maxConcurrentJobs" env:"DRAY_COMPACTION_MAX_CONCURRENT_JOBS"`
	MaxFilesToMerge                int                          `yaml:"maxFilesToMerge" env:"DRAY_COMPACTION_MAX_FILES"`
	MinAgeMs                       int64                        `yaml:"minAgeMs" env:"DRAY_COMPACTION_MIN_AGE_MS"`
	ParquetSmallFileThresholdBytes int64                        `yaml:"parquetSmallFileThresholdBytes" env:"DRAY_COMPACTION_PARQUET_SMALL_FILE_THRESHOLD_BYTES"`
	ParquetTargetFileSizeBytes     int64                        `yaml:"parquetTargetFileSizeBytes" env:"DRAY_COMPACTION_PARQUET_TARGET_FILE_SIZE_BYTES"`
	ParquetMaxMergeBytes           int64                        `yaml:"parquetMaxMergeBytes" env:"DRAY_COMPACTION_PARQUET_MAX_MERGE_BYTES"`
	ParquetMinFiles                int                          `yaml:"parquetMinFiles" env:"DRAY_COMPACTION_PARQUET_MIN_FILES"`
	ParquetMaxFiles                int                          `yaml:"parquetMaxFiles" env:"DRAY_COMPACTION_PARQUET_MAX_FILES"`
	ParquetMinAgeMs                int64                        `yaml:"parquetMinAgeMs" env:"DRAY_COMPACTION_PARQUET_MIN_AGE_MS"`
	ValueProjections               []projection.TopicProjection `yaml:"valueProjections"`
}

type IcebergConfig struct {
	Enabled      bool     `yaml:"enabled" env:"DRAY_ICEBERG_ENABLED"`
	CatalogType  string   `yaml:"catalogType" env:"DRAY_ICEBERG_CATALOG_TYPE"`
	CatalogURI   string   `yaml:"catalogUri" env:"DRAY_ICEBERG_CATALOG_URI"`
	Warehouse    string   `yaml:"warehouse" env:"DRAY_ICEBERG_WAREHOUSE"`
	Partitioning []string `yaml:"partitioning"` // e.g., ["partition", "day(created_at)"]

	// Maintenance settings
	SnapshotRetentionAgeMs    int64 `yaml:"snapshotRetentionAgeMs" env:"DRAY_ICEBERG_SNAPSHOT_RETENTION_AGE_MS"`
	SnapshotRetentionMinCount int   `yaml:"snapshotRetentionMinCount" env:"DRAY_ICEBERG_SNAPSHOT_RETENTION_MIN_COUNT"`
	ManifestRewriteEnabled    bool  `yaml:"manifestRewriteEnabled" env:"DRAY_ICEBERG_MANIFEST_REWRITE_ENABLED"`
	ManifestRewriteTargetSize int64 `yaml:"manifestRewriteTargetSize" env:"DRAY_ICEBERG_MANIFEST_REWRITE_TARGET_SIZE"`
}

type RoutingConfig struct {
	EnforceOwner bool `yaml:"enforceOwner" env:"DRAY_ROUTING_ENFORCE_OWNER"`
}

type ObservabilityConfig struct {
	MetricsAddr string `yaml:"metricsAddr" env:"DRAY_METRICS_ADDR"`
	LogLevel    string `yaml:"logLevel" env:"DRAY_LOG_LEVEL"`
	LogFormat   string `yaml:"logFormat" env:"DRAY_LOG_FORMAT"`
}

// Default returns a Config with sensible defaults.
func Default() *Config {
	return &Config{
		ClusterID: DefaultClusterID,
		Broker: BrokerConfig{
			ListenAddr: ":9092",
		},
		SASL: SASLConfig{
			Enabled:           false,
			Mechanism:         "PLAIN",
			CredentialsSource: "env",
		},
		Metadata: MetadataConfig{
			OxiaEndpoint: "localhost:6648",
			NumDomains:   16,
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
			Enabled:                        true,
			MaxConcurrentJobs:              0,
			MaxFilesToMerge:                10,
			MinAgeMs:                       300000, // 5 minutes
			ParquetSmallFileThresholdBytes: 64 * 1024 * 1024,
			ParquetTargetFileSizeBytes:     256 * 1024 * 1024,
			ParquetMaxMergeBytes:           512 * 1024 * 1024,
			ParquetMinFiles:                4,
			ParquetMaxFiles:                50,
			ParquetMinAgeMs:                10 * 60 * 1000,
		},
		Iceberg: IcebergConfig{
			Enabled:                   true,
			CatalogType:               "rest",
			Partitioning:              []string{"partition"},   // default: partition by Kafka partition only
			SnapshotRetentionAgeMs:    7 * 24 * 60 * 60 * 1000, // 7 days
			SnapshotRetentionMinCount: 1,
			ManifestRewriteEnabled:    true,
			ManifestRewriteTargetSize: 10 * 1024 * 1024, // 10MB
		},
		Routing: RoutingConfig{
			EnforceOwner: false,
		},
		Observability: ObservabilityConfig{
			MetricsAddr: ":9090",
			LogLevel:    "info",
			LogFormat:   "json",
		},
	}
}

// Load loads configuration from the default path with environment variable overrides.
func Load() (*Config, error) {
	return LoadFromPath(DefaultConfigPath)
}

// LoadFromPath loads configuration from a file path with environment variable overrides.
// If the file does not exist, returns defaults with environment overrides applied.
func LoadFromPath(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist - use defaults with env overrides
			applyEnvOverrides(cfg)
			if err := cfg.Validate(); err != nil {
				return nil, fmt.Errorf("config validation failed: %w", err)
			}
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	applyEnvOverrides(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// LoadFromBytes loads configuration from YAML bytes with environment variable overrides.
func LoadFromBytes(data []byte) (*Config, error) {
	cfg := Default()

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	applyEnvOverrides(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// LoadNoValidate loads configuration from the default path without validation.
// This is useful for admin CLI commands that may not need full configuration.
func LoadNoValidate() (*Config, error) {
	return LoadFromPathNoValidate(DefaultConfigPath)
}

// LoadFromPathNoValidate loads configuration from a file path without validation.
// This is useful for admin CLI commands that may not need full configuration.
func LoadFromPathNoValidate(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist - use defaults with env overrides
			applyEnvOverrides(cfg)
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	applyEnvOverrides(cfg)

	return cfg, nil
}

// Validate validates the configuration and returns an error if invalid.
func (c *Config) Validate() error {
	var errs []error

	c.Compaction.ValueProjections = projection.Normalize(c.Compaction.ValueProjections)

	if strings.TrimSpace(c.ClusterID) == "" {
		errs = append(errs, errors.New("clusterId is required"))
	}

	// Broker validation
	if c.Broker.ListenAddr == "" {
		errs = append(errs, errors.New("broker.listenAddr is required"))
	}

	// TLS validation
	if c.Broker.TLS.Enabled {
		if c.Broker.TLS.CertFile == "" {
			errs = append(errs, errors.New("broker.tls.certFile is required when TLS is enabled"))
		}
		if c.Broker.TLS.KeyFile == "" {
			errs = append(errs, errors.New("broker.tls.keyFile is required when TLS is enabled"))
		}
	}

	// SASL validation
	if c.SASL.Enabled {
		validMechanisms := map[string]bool{"PLAIN": true}
		if !validMechanisms[c.SASL.Mechanism] {
			errs = append(errs, fmt.Errorf("sasl.mechanism must be one of: PLAIN; got %q", c.SASL.Mechanism))
		}
		validSources := map[string]bool{"file": true, "env": true}
		if !validSources[c.SASL.CredentialsSource] {
			errs = append(errs, fmt.Errorf("sasl.credentialsSource must be one of: file, env; got %q", c.SASL.CredentialsSource))
		}
		if c.SASL.CredentialsSource == "file" && c.SASL.CredentialsFile == "" {
			errs = append(errs, errors.New("sasl.credentialsFile is required when sasl.credentialsSource is 'file'"))
		}
	}

	// Metadata validation
	if c.Metadata.OxiaEndpoint == "" {
		errs = append(errs, errors.New("metadata.oxiaEndpoint is required"))
	}
	if c.Metadata.NumDomains <= 0 {
		errs = append(errs, errors.New("metadata.numDomains must be positive"))
	}

	// WAL validation
	if c.WAL.FlushSizeBytes <= 0 {
		errs = append(errs, errors.New("wal.flushSizeBytes must be positive"))
	}
	if c.WAL.FlushIntervalMs <= 0 {
		errs = append(errs, errors.New("wal.flushIntervalMs must be positive"))
	}
	if c.WAL.OrphanTTLMs <= 0 {
		errs = append(errs, errors.New("wal.orphanTTLMs must be positive"))
	}

	// Compaction validation
	if c.Compaction.Enabled {
		if c.Compaction.MaxConcurrentJobs < 0 {
			errs = append(errs, errors.New("compaction.maxConcurrentJobs cannot be negative"))
		}
		if c.Compaction.MaxFilesToMerge <= 0 {
			errs = append(errs, errors.New("compaction.maxFilesToMerge must be positive when compaction is enabled"))
		}
		if c.Compaction.MinAgeMs < 0 {
			errs = append(errs, errors.New("compaction.minAgeMs cannot be negative"))
		}
		if c.Compaction.ParquetSmallFileThresholdBytes < 0 {
			errs = append(errs, errors.New("compaction.parquetSmallFileThresholdBytes cannot be negative"))
		}
		if c.Compaction.ParquetTargetFileSizeBytes < 0 {
			errs = append(errs, errors.New("compaction.parquetTargetFileSizeBytes cannot be negative"))
		}
		if c.Compaction.ParquetMaxMergeBytes < 0 {
			errs = append(errs, errors.New("compaction.parquetMaxMergeBytes cannot be negative"))
		}
		if c.Compaction.ParquetMinFiles < 0 {
			errs = append(errs, errors.New("compaction.parquetMinFiles cannot be negative"))
		}
		if c.Compaction.ParquetMaxFiles < 0 {
			errs = append(errs, errors.New("compaction.parquetMaxFiles cannot be negative"))
		}
		if c.Compaction.ParquetMinAgeMs < 0 {
			errs = append(errs, errors.New("compaction.parquetMinAgeMs cannot be negative"))
		}
		if len(c.Compaction.ValueProjections) > 0 {
			topics := make(map[string]struct{}, len(c.Compaction.ValueProjections))
			reserved := map[string]struct{}{
				"partition":      {},
				"offset":         {},
				"timestamp":      {},
				"key":            {},
				"value":          {},
				"headers":        {},
				"producer_id":    {},
				"producer_epoch": {},
				"base_sequence":  {},
				"attributes":     {},
			}
			for _, proj := range c.Compaction.ValueProjections {
				if proj.Topic == "" {
					errs = append(errs, errors.New("compaction.valueProjections.topic is required"))
					continue
				}
				if _, exists := topics[proj.Topic]; exists {
					errs = append(errs, fmt.Errorf("compaction.valueProjections has duplicate topic %q", proj.Topic))
				}
				topics[proj.Topic] = struct{}{}

				if !projection.IsValidFormat(proj.Format) {
					errs = append(errs, fmt.Errorf("compaction.valueProjections[%s].format must be json; got %q", proj.Topic, proj.Format))
				}

				fields := make(map[string]struct{}, len(proj.Fields))
				for _, field := range proj.Fields {
					if field.Name == "" {
						errs = append(errs, fmt.Errorf("compaction.valueProjections[%s].fields.name is required", proj.Topic))
						continue
					}
					if _, exists := reserved[field.Name]; exists {
						errs = append(errs, fmt.Errorf("compaction.valueProjections[%s].fields.name %q conflicts with base schema", proj.Topic, field.Name))
					}
					if _, exists := fields[field.Name]; exists {
						errs = append(errs, fmt.Errorf("compaction.valueProjections[%s].fields has duplicate name %q", proj.Topic, field.Name))
					}
					fields[field.Name] = struct{}{}
					if !projection.IsValidFieldType(field.Type) {
						errs = append(errs, fmt.Errorf("compaction.valueProjections[%s].fields[%s].type is invalid: %q", proj.Topic, field.Name, field.Type))
					}
				}
			}
		}
	}

	// Iceberg validation
	if c.Iceberg.Enabled {
		if c.Iceberg.CatalogType == "" {
			errs = append(errs, errors.New("iceberg.catalogType is required when iceberg is enabled"))
		}
		validTypes := map[string]bool{"rest": true, "glue": true, "sql": true}
		if !validTypes[c.Iceberg.CatalogType] {
			errs = append(errs, fmt.Errorf("iceberg.catalogType must be one of: rest, glue, sql; got %q", c.Iceberg.CatalogType))
		}
	}

	// Observability validation
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[c.Observability.LogLevel] {
		errs = append(errs, fmt.Errorf("observability.logLevel must be one of: debug, info, warn, error; got %q", c.Observability.LogLevel))
	}
	validLogFormats := map[string]bool{"json": true, "text": true}
	if !validLogFormats[c.Observability.LogFormat] {
		errs = append(errs, fmt.Errorf("observability.logFormat must be one of: json, text; got %q", c.Observability.LogFormat))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// OxiaNamespace returns the Oxia namespace for the given cluster ID.
func OxiaNamespace(clusterID string) string {
	return fmt.Sprintf("dray/%s", clusterID)
}

// OxiaNamespace returns the Oxia namespace for this config.
// If metadata.oxiaNamespace is set, it is used directly.
// Otherwise, returns "dray/<clusterId>".
func (c *Config) OxiaNamespace() string {
	if ns := strings.TrimSpace(c.Metadata.OxiaNamespace); ns != "" {
		return ns
	}
	clusterID := strings.TrimSpace(c.ClusterID)
	if clusterID == "" {
		clusterID = DefaultClusterID
	}
	return OxiaNamespace(clusterID)
}

// applyEnvOverrides applies environment variable overrides to the config.
// Environment variables take precedence over file values.
func applyEnvOverrides(cfg *Config) {
	applyEnvToStruct(reflect.ValueOf(cfg).Elem())
}

func applyEnvToStruct(v reflect.Value) {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		if field.Kind() == reflect.Struct {
			applyEnvToStruct(field)
			continue
		}

		envTag := fieldType.Tag.Get("env")
		if envTag == "" {
			continue
		}

		envVal := os.Getenv(envTag)
		if envVal == "" {
			continue
		}

		switch field.Kind() {
		case reflect.String:
			field.SetString(envVal)
		case reflect.Int, reflect.Int64:
			if n, err := strconv.ParseInt(envVal, 10, 64); err == nil {
				field.SetInt(n)
			}
		case reflect.Bool:
			field.SetBool(strings.ToLower(envVal) == "true" || envVal == "1")
		}
	}
}
