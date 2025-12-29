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

	"gopkg.in/yaml.v3"
)

// DefaultConfigPath is the default path for the Dray configuration file.
const DefaultConfigPath = "/etc/dray/config.yaml"

// Config holds all configuration for a Dray broker.
type Config struct {
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
	ListenAddr string    `yaml:"listenAddr" env:"DRAY_LISTEN_ADDR"`
	ZoneID     string    `yaml:"zoneId" env:"DRAY_ZONE_ID"`
	TLS        TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled" env:"DRAY_TLS_ENABLED"`
	CertFile string `yaml:"certFile" env:"DRAY_TLS_CERT_FILE"`
	KeyFile  string `yaml:"keyFile" env:"DRAY_TLS_KEY_FILE"`
}

// SASLConfig holds SASL authentication configuration.
type SASLConfig struct {
	Enabled bool   `yaml:"enabled" env:"DRAY_SASL_ENABLED"`
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
	OxiaEndpoint string `yaml:"oxiaEndpoint" env:"DRAY_OXIA_ENDPOINT"`
	Namespace    string `yaml:"namespace" env:"DRAY_OXIA_NAMESPACE"`
	NumDomains   int    `yaml:"numDomains" env:"DRAY_METADATA_NUM_DOMAINS"`
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
	Enabled         bool  `yaml:"enabled" env:"DRAY_COMPACTION_ENABLED"`
	MaxFilesToMerge int   `yaml:"maxFilesToMerge" env:"DRAY_COMPACTION_MAX_FILES"`
	MinAgeMs        int64 `yaml:"minAgeMs" env:"DRAY_COMPACTION_MIN_AGE_MS"`
}

type IcebergConfig struct {
	Enabled     bool   `yaml:"enabled" env:"DRAY_ICEBERG_ENABLED"`
	CatalogType string `yaml:"catalogType" env:"DRAY_ICEBERG_CATALOG_TYPE"`
	CatalogURI  string `yaml:"catalogUri" env:"DRAY_ICEBERG_CATALOG_URI"`
	Warehouse   string `yaml:"warehouse" env:"DRAY_ICEBERG_WAREHOUSE"`
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
			Namespace:    "dray",
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
			Enabled:         true,
			MaxFilesToMerge: 10,
			MinAgeMs:        300000, // 5 minutes
		},
		Iceberg: IcebergConfig{
			Enabled:     true,
			CatalogType: "rest",
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
	if c.Metadata.Namespace == "" {
		errs = append(errs, errors.New("metadata.namespace is required"))
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
		if c.Compaction.MaxFilesToMerge <= 0 {
			errs = append(errs, errors.New("compaction.maxFilesToMerge must be positive when compaction is enabled"))
		}
		if c.Compaction.MinAgeMs < 0 {
			errs = append(errs, errors.New("compaction.minAgeMs cannot be negative"))
		}
	}

	// Iceberg validation
	if c.Iceberg.Enabled {
		if c.Iceberg.CatalogType == "" {
			errs = append(errs, errors.New("iceberg.catalogType is required when iceberg is enabled"))
		}
		validTypes := map[string]bool{"rest": true, "hive": true, "glue": true}
		if !validTypes[c.Iceberg.CatalogType] {
			errs = append(errs, fmt.Errorf("iceberg.catalogType must be one of: rest, hive, glue; got %q", c.Iceberg.CatalogType))
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
