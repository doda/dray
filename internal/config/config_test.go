package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := Default()

	if cfg.ClusterID != DefaultClusterID {
		t.Errorf("expected default cluster ID %s, got %s", DefaultClusterID, cfg.ClusterID)
	}

	if cfg.Broker.ListenAddr != ":9092" {
		t.Errorf("expected default listen addr :9092, got %s", cfg.Broker.ListenAddr)
	}

	if cfg.Metadata.OxiaEndpoint != "localhost:6648" {
		t.Errorf("expected default oxia endpoint localhost:6648, got %s", cfg.Metadata.OxiaEndpoint)
	}

	if cfg.Metadata.NumDomains != 16 {
		t.Errorf("expected default numDomains 16, got %d", cfg.Metadata.NumDomains)
	}

	if cfg.WAL.FlushSizeBytes != 16*1024*1024 {
		t.Errorf("expected default flush size 16MB, got %d", cfg.WAL.FlushSizeBytes)
	}

	if !cfg.Compaction.Enabled {
		t.Error("expected compaction to be enabled by default")
	}

	if !cfg.Iceberg.Enabled {
		t.Error("expected iceberg to be enabled by default")
	}

	if cfg.Observability.LogLevel != "info" {
		t.Errorf("expected default log level info, got %s", cfg.Observability.LogLevel)
	}

	if cfg.Observability.LogFormat != "json" {
		t.Errorf("expected default log format json, got %s", cfg.Observability.LogFormat)
	}
}

func TestDefaultConfigValidates(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should validate: %v", err)
	}
}

func TestOxiaNamespaceFormatting(t *testing.T) {
	tests := []struct {
		clusterID string
		want      string
	}{
		{"cluster-1", "dray/cluster-1"},
		{"prod/us-east-1", "dray/prod/us-east-1"},
	}

	for _, tt := range tests {
		t.Run(tt.clusterID, func(t *testing.T) {
			cfg := Default()
			cfg.ClusterID = tt.clusterID
			if got := cfg.OxiaNamespace(); got != tt.want {
				t.Errorf("OxiaNamespace() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoadFromPath(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
clusterId: "prod-cluster"
broker:
  listenAddr: ":9093"
  zoneId: "us-east-1a"
metadata:
  oxiaEndpoint: "oxia.example.com:6648"
  numDomains: 32
objectStore:
  endpoint: "s3.example.com"
  bucket: "test-bucket"
  region: "us-west-2"
wal:
  flushSizeBytes: 8388608
  flushIntervalMs: 200
  orphanTTLMs: 120000
compaction:
  enabled: true
  maxFilesToMerge: 20
  minAgeMs: 600000
  parquetSmallFileThresholdBytes: 33554432
  parquetTargetFileSizeBytes: 268435456
  parquetMaxMergeBytes: 536870912
  parquetMinFiles: 3
  parquetMaxFiles: 25
  parquetMinAgeMs: 900000
iceberg:
  enabled: true
  catalogType: "rest"
  catalogUri: "http://iceberg.example.com"
  warehouse: "s3://warehouse"
observability:
  metricsAddr: ":9091"
  logLevel: "debug"
  logFormat: "text"
routing:
  enforceOwner: true
`
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test config file: %v", err)
	}

	cfg, err := LoadFromPath(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ClusterID != "prod-cluster" {
		t.Errorf("expected cluster ID prod-cluster, got %s", cfg.ClusterID)
	}
	if cfg.Broker.ListenAddr != ":9093" {
		t.Errorf("expected listen addr :9093, got %s", cfg.Broker.ListenAddr)
	}
	if cfg.Broker.ZoneID != "us-east-1a" {
		t.Errorf("expected zone id us-east-1a, got %s", cfg.Broker.ZoneID)
	}
	if cfg.Metadata.OxiaEndpoint != "oxia.example.com:6648" {
		t.Errorf("expected oxia endpoint oxia.example.com:6648, got %s", cfg.Metadata.OxiaEndpoint)
	}
	if cfg.Metadata.NumDomains != 32 {
		t.Errorf("expected numDomains 32, got %d", cfg.Metadata.NumDomains)
	}
	if cfg.WAL.FlushSizeBytes != 8388608 {
		t.Errorf("expected flush size 8388608, got %d", cfg.WAL.FlushSizeBytes)
	}
	if cfg.Compaction.MaxFilesToMerge != 20 {
		t.Errorf("expected max files 20, got %d", cfg.Compaction.MaxFilesToMerge)
	}
	if cfg.Compaction.ParquetSmallFileThresholdBytes != 33554432 {
		t.Errorf("expected parquet small file threshold 33554432, got %d", cfg.Compaction.ParquetSmallFileThresholdBytes)
	}
	if cfg.Compaction.ParquetTargetFileSizeBytes != 268435456 {
		t.Errorf("expected parquet target size 268435456, got %d", cfg.Compaction.ParquetTargetFileSizeBytes)
	}
	if cfg.Compaction.ParquetMaxMergeBytes != 536870912 {
		t.Errorf("expected parquet max merge 536870912, got %d", cfg.Compaction.ParquetMaxMergeBytes)
	}
	if cfg.Compaction.ParquetMinFiles != 3 {
		t.Errorf("expected parquet min files 3, got %d", cfg.Compaction.ParquetMinFiles)
	}
	if cfg.Compaction.ParquetMaxFiles != 25 {
		t.Errorf("expected parquet max files 25, got %d", cfg.Compaction.ParquetMaxFiles)
	}
	if cfg.Compaction.ParquetMinAgeMs != 900000 {
		t.Errorf("expected parquet min age 900000, got %d", cfg.Compaction.ParquetMinAgeMs)
	}
	if cfg.Observability.LogLevel != "debug" {
		t.Errorf("expected log level debug, got %s", cfg.Observability.LogLevel)
	}
	if cfg.Observability.LogFormat != "text" {
		t.Errorf("expected log format text, got %s", cfg.Observability.LogFormat)
	}
	if !cfg.Routing.EnforceOwner {
		t.Error("expected enforceOwner to be enabled")
	}
}

func TestLoadFromPathMissing(t *testing.T) {
	cfg, err := LoadFromPath("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("missing config should return defaults, not error: %v", err)
	}

	// Should have defaults
	if cfg.Broker.ListenAddr != ":9092" {
		t.Errorf("expected default listen addr :9092, got %s", cfg.Broker.ListenAddr)
	}
}

func TestLoadFromBytes(t *testing.T) {
	yamlContent := `
clusterId: "test-cluster"
broker:
  listenAddr: ":8080"
metadata:
  oxiaEndpoint: "localhost:6648"
  numDomains: 8
`
	cfg, err := LoadFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ClusterID != "test-cluster" {
		t.Errorf("expected cluster ID test-cluster, got %s", cfg.ClusterID)
	}
	if cfg.Broker.ListenAddr != ":8080" {
		t.Errorf("expected listen addr :8080, got %s", cfg.Broker.ListenAddr)
	}
	if cfg.Metadata.NumDomains != 8 {
		t.Errorf("expected numDomains 8, got %d", cfg.Metadata.NumDomains)
	}
}

func TestEnvironmentVariableOverrides(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
clusterId: "file-cluster"
broker:
  listenAddr: ":9092"
metadata:
  oxiaEndpoint: "localhost:6648"
  numDomains: 16
`
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test config file: %v", err)
	}

	// Set environment variables
	os.Setenv("DRAY_CLUSTER_ID", "env-cluster")
	os.Setenv("DRAY_LISTEN_ADDR", ":9999")
	os.Setenv("DRAY_ZONE_ID", "test-zone")
	os.Setenv("DRAY_WAL_FLUSH_SIZE", "33554432")
	os.Setenv("DRAY_COMPACTION_ENABLED", "false")
	os.Setenv("DRAY_LOG_LEVEL", "warn")
	os.Setenv("DRAY_ROUTING_ENFORCE_OWNER", "true")
	defer func() {
		os.Unsetenv("DRAY_CLUSTER_ID")
		os.Unsetenv("DRAY_LISTEN_ADDR")
		os.Unsetenv("DRAY_ZONE_ID")
		os.Unsetenv("DRAY_WAL_FLUSH_SIZE")
		os.Unsetenv("DRAY_COMPACTION_ENABLED")
		os.Unsetenv("DRAY_LOG_LEVEL")
		os.Unsetenv("DRAY_ROUTING_ENFORCE_OWNER")
	}()

	cfg, err := LoadFromPath(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Environment variables should override file values
	if cfg.ClusterID != "env-cluster" {
		t.Errorf("expected cluster ID env-cluster, got %s", cfg.ClusterID)
	}
	if cfg.Broker.ListenAddr != ":9999" {
		t.Errorf("expected env override :9999, got %s", cfg.Broker.ListenAddr)
	}
	if cfg.Broker.ZoneID != "test-zone" {
		t.Errorf("expected env zone test-zone, got %s", cfg.Broker.ZoneID)
	}
	if cfg.WAL.FlushSizeBytes != 33554432 {
		t.Errorf("expected env flush size 33554432, got %d", cfg.WAL.FlushSizeBytes)
	}
	if cfg.Compaction.Enabled {
		t.Error("expected compaction to be disabled via env")
	}
	if cfg.Observability.LogLevel != "warn" {
		t.Errorf("expected log level warn, got %s", cfg.Observability.LogLevel)
	}
	if !cfg.Routing.EnforceOwner {
		t.Error("expected routing enforceOwner to be enabled via env")
	}
}

func TestEnvironmentVariableOverridesDefaults(t *testing.T) {
	// Test that env vars override defaults when no config file exists
	os.Setenv("DRAY_S3_ENDPOINT", "http://localhost:9000")
	os.Setenv("DRAY_S3_BUCKET", "my-bucket")
	os.Setenv("DRAY_ICEBERG_ENABLED", "true")
	os.Setenv("DRAY_ICEBERG_CATALOG_URI", "http://catalog:8181")
	defer func() {
		os.Unsetenv("DRAY_S3_ENDPOINT")
		os.Unsetenv("DRAY_S3_BUCKET")
		os.Unsetenv("DRAY_ICEBERG_ENABLED")
		os.Unsetenv("DRAY_ICEBERG_CATALOG_URI")
	}()

	cfg, err := LoadFromPath("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.ObjectStore.Endpoint != "http://localhost:9000" {
		t.Errorf("expected endpoint http://localhost:9000, got %s", cfg.ObjectStore.Endpoint)
	}
	if cfg.ObjectStore.Bucket != "my-bucket" {
		t.Errorf("expected bucket my-bucket, got %s", cfg.ObjectStore.Bucket)
	}
	if cfg.Iceberg.CatalogURI != "http://catalog:8181" {
		t.Errorf("expected catalog URI http://catalog:8181, got %s", cfg.Iceberg.CatalogURI)
	}
}

func TestEnvironmentVariableBoolParsing(t *testing.T) {
	tests := []struct {
		envValue string
		expected bool
	}{
		{"true", true},
		{"True", true},
		{"TRUE", true},
		{"1", true},
		{"false", false},
		{"False", false},
		{"0", false},
		{"", false}, // Empty string should not override
	}

	for _, tt := range tests {
		t.Run(tt.envValue, func(t *testing.T) {
			if tt.envValue == "" {
				os.Unsetenv("DRAY_COMPACTION_ENABLED")
			} else {
				os.Setenv("DRAY_COMPACTION_ENABLED", tt.envValue)
			}
			defer os.Unsetenv("DRAY_COMPACTION_ENABLED")

			cfg, err := LoadFromPath("/nonexistent/path/config.yaml")
			if err != nil {
				t.Fatalf("failed to load config: %v", err)
			}

			if tt.envValue == "" {
				// Default should be true
				if !cfg.Compaction.Enabled {
					t.Error("expected default true when env is unset")
				}
			} else if cfg.Compaction.Enabled != tt.expected {
				t.Errorf("expected %v for %q, got %v", tt.expected, tt.envValue, cfg.Compaction.Enabled)
			}
		})
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name     string
		modifier func(*Config)
		errCount int
	}{
		{
			name: "empty cluster id",
			modifier: func(c *Config) {
				c.ClusterID = ""
			},
			errCount: 1,
		},
		{
			name: "empty listen addr",
			modifier: func(c *Config) {
				c.Broker.ListenAddr = ""
			},
			errCount: 1,
		},
		{
			name: "empty oxia endpoint",
			modifier: func(c *Config) {
				c.Metadata.OxiaEndpoint = ""
			},
			errCount: 1,
		},
		{
			name: "zero numDomains",
			modifier: func(c *Config) {
				c.Metadata.NumDomains = 0
			},
			errCount: 1,
		},
		{
			name: "negative numDomains",
			modifier: func(c *Config) {
				c.Metadata.NumDomains = -1
			},
			errCount: 1,
		},
		{
			name: "zero flush size",
			modifier: func(c *Config) {
				c.WAL.FlushSizeBytes = 0
			},
			errCount: 1,
		},
		{
			name: "negative flush interval",
			modifier: func(c *Config) {
				c.WAL.FlushIntervalMs = -1
			},
			errCount: 1,
		},
		{
			name: "zero orphan TTL",
			modifier: func(c *Config) {
				c.WAL.OrphanTTLMs = 0
			},
			errCount: 1,
		},
		{
			name: "compaction enabled with zero max files",
			modifier: func(c *Config) {
				c.Compaction.Enabled = true
				c.Compaction.MaxFilesToMerge = 0
			},
			errCount: 1,
		},
		{
			name: "compaction disabled with zero max files is OK",
			modifier: func(c *Config) {
				c.Compaction.Enabled = false
				c.Compaction.MaxFilesToMerge = 0
			},
			errCount: 0,
		},
		{
			name: "invalid log level",
			modifier: func(c *Config) {
				c.Observability.LogLevel = "invalid"
			},
			errCount: 1,
		},
		{
			name: "invalid log format",
			modifier: func(c *Config) {
				c.Observability.LogFormat = "xml"
			},
			errCount: 1,
		},
		{
			name: "invalid iceberg catalog type",
			modifier: func(c *Config) {
				c.Iceberg.Enabled = true
				c.Iceberg.CatalogType = "invalid"
			},
			errCount: 1,
		},
		{
			name: "iceberg disabled with empty catalog type is OK",
			modifier: func(c *Config) {
				c.Iceberg.Enabled = false
				c.Iceberg.CatalogType = ""
			},
			errCount: 0,
		},
		{
			name: "multiple errors",
			modifier: func(c *Config) {
				c.Broker.ListenAddr = ""
				c.Metadata.OxiaEndpoint = ""
				c.WAL.FlushSizeBytes = 0
			},
			errCount: 3,
		},
		{
			name: "negative parquet rewrite config",
			modifier: func(c *Config) {
				c.Compaction.ParquetSmallFileThresholdBytes = -1
				c.Compaction.ParquetTargetFileSizeBytes = -1
				c.Compaction.ParquetMaxMergeBytes = -1
				c.Compaction.ParquetMinFiles = -1
				c.Compaction.ParquetMaxFiles = -1
				c.Compaction.ParquetMinAgeMs = -1
			},
			errCount: 6,
		},
		{
			name: "negative max concurrent compaction jobs",
			modifier: func(c *Config) {
				c.Compaction.MaxConcurrentJobs = -1
			},
			errCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.modifier(cfg)
			err := cfg.Validate()

			if tt.errCount == 0 {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}
			} else {
				if err == nil {
					t.Error("expected error, got nil")
				}
			}
		})
	}
}

func TestValidIcebergCatalogTypes(t *testing.T) {
	validTypes := []string{"rest", "glue", "sql"}

	for _, catalogType := range validTypes {
		t.Run(catalogType, func(t *testing.T) {
			cfg := Default()
			cfg.Iceberg.Enabled = true
			cfg.Iceberg.CatalogType = catalogType

			if err := cfg.Validate(); err != nil {
				t.Errorf("expected %s to be valid, got error: %v", catalogType, err)
			}
		})
	}
}

func TestValidLogLevels(t *testing.T) {
	validLevels := []string{"debug", "info", "warn", "error"}

	for _, level := range validLevels {
		t.Run(level, func(t *testing.T) {
			cfg := Default()
			cfg.Observability.LogLevel = level

			if err := cfg.Validate(); err != nil {
				t.Errorf("expected %s to be valid, got error: %v", level, err)
			}
		})
	}
}

func TestValidLogFormats(t *testing.T) {
	validFormats := []string{"json", "text"}

	for _, format := range validFormats {
		t.Run(format, func(t *testing.T) {
			cfg := Default()
			cfg.Observability.LogFormat = format

			if err := cfg.Validate(); err != nil {
				t.Errorf("expected %s to be valid, got error: %v", format, err)
			}
		})
	}
}

func TestInvalidYAMLParsing(t *testing.T) {
	invalidYAML := `
broker:
  listenAddr: ":9092"
  this is not valid yaml
`
	_, err := LoadFromBytes([]byte(invalidYAML))
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestLoadFromBytesAppliesDefaults(t *testing.T) {
	// Partial config - should merge with defaults
	yamlContent := `
broker:
  listenAddr: ":8080"
`
	cfg, err := LoadFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Custom value
	if cfg.Broker.ListenAddr != ":8080" {
		t.Errorf("expected listen addr :8080, got %s", cfg.Broker.ListenAddr)
	}

	// Default values should still be present
	if cfg.Metadata.OxiaEndpoint != "localhost:6648" {
		t.Errorf("expected default oxia endpoint, got %s", cfg.Metadata.OxiaEndpoint)
	}
	if cfg.WAL.FlushSizeBytes != 16*1024*1024 {
		t.Errorf("expected default flush size, got %d", cfg.WAL.FlushSizeBytes)
	}
}

func TestLoadWithInvalidConfigReturnsValidationError(t *testing.T) {
	yamlContent := `
broker:
  listenAddr: ""
metadata:
  oxiaEndpoint: ""
`
	_, err := LoadFromBytes([]byte(yamlContent))
	if err == nil {
		t.Error("expected validation error")
	}
}

func TestTLSConfigValidation(t *testing.T) {
	tests := []struct {
		name     string
		modifier func(*Config)
		wantErr  bool
	}{
		{
			name: "TLS disabled is valid without cert files",
			modifier: func(c *Config) {
				c.Broker.TLS.Enabled = false
				c.Broker.TLS.CertFile = ""
				c.Broker.TLS.KeyFile = ""
			},
			wantErr: false,
		},
		{
			name: "TLS enabled with both files is valid",
			modifier: func(c *Config) {
				c.Broker.TLS.Enabled = true
				c.Broker.TLS.CertFile = "/path/to/cert.pem"
				c.Broker.TLS.KeyFile = "/path/to/key.pem"
			},
			wantErr: false,
		},
		{
			name: "TLS enabled without cert file is invalid",
			modifier: func(c *Config) {
				c.Broker.TLS.Enabled = true
				c.Broker.TLS.CertFile = ""
				c.Broker.TLS.KeyFile = "/path/to/key.pem"
			},
			wantErr: true,
		},
		{
			name: "TLS enabled without key file is invalid",
			modifier: func(c *Config) {
				c.Broker.TLS.Enabled = true
				c.Broker.TLS.CertFile = "/path/to/cert.pem"
				c.Broker.TLS.KeyFile = ""
			},
			wantErr: true,
		},
		{
			name: "TLS enabled without both files is invalid",
			modifier: func(c *Config) {
				c.Broker.TLS.Enabled = true
				c.Broker.TLS.CertFile = ""
				c.Broker.TLS.KeyFile = ""
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.modifier(cfg)
			err := cfg.Validate()

			if tt.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestTLSConfigFromYAML(t *testing.T) {
	yamlContent := `
clusterId: "tls-cluster"
broker:
  listenAddr: ":9093"
  tls:
    enabled: true
    certFile: "/etc/dray/certs/server.crt"
    keyFile: "/etc/dray/certs/server.key"
metadata:
  oxiaEndpoint: "localhost:6648"
  numDomains: 16
`
	cfg, err := LoadFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !cfg.Broker.TLS.Enabled {
		t.Error("expected TLS to be enabled")
	}
	if cfg.Broker.TLS.CertFile != "/etc/dray/certs/server.crt" {
		t.Errorf("expected cert file /etc/dray/certs/server.crt, got %s", cfg.Broker.TLS.CertFile)
	}
	if cfg.Broker.TLS.KeyFile != "/etc/dray/certs/server.key" {
		t.Errorf("expected key file /etc/dray/certs/server.key, got %s", cfg.Broker.TLS.KeyFile)
	}
}

func TestTLSConfigEnvOverrides(t *testing.T) {
	os.Setenv("DRAY_TLS_ENABLED", "true")
	os.Setenv("DRAY_TLS_CERT_FILE", "/env/cert.pem")
	os.Setenv("DRAY_TLS_KEY_FILE", "/env/key.pem")
	defer func() {
		os.Unsetenv("DRAY_TLS_ENABLED")
		os.Unsetenv("DRAY_TLS_CERT_FILE")
		os.Unsetenv("DRAY_TLS_KEY_FILE")
	}()

	cfg, err := LoadFromPath("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !cfg.Broker.TLS.Enabled {
		t.Error("expected TLS to be enabled via env")
	}
	if cfg.Broker.TLS.CertFile != "/env/cert.pem" {
		t.Errorf("expected cert file /env/cert.pem, got %s", cfg.Broker.TLS.CertFile)
	}
	if cfg.Broker.TLS.KeyFile != "/env/key.pem" {
		t.Errorf("expected key file /env/key.pem, got %s", cfg.Broker.TLS.KeyFile)
	}
}

func TestSASLConfigDefaults(t *testing.T) {
	cfg := Default()

	if cfg.SASL.Enabled {
		t.Error("expected SASL to be disabled by default")
	}
	if cfg.SASL.Mechanism != "PLAIN" {
		t.Errorf("expected default mechanism PLAIN, got %s", cfg.SASL.Mechanism)
	}
	if cfg.SASL.CredentialsSource != "env" {
		t.Errorf("expected default credentials source env, got %s", cfg.SASL.CredentialsSource)
	}
}

func TestSASLConfigValidation(t *testing.T) {
	tests := []struct {
		name     string
		modifier func(*Config)
		wantErr  bool
	}{
		{
			name: "SASL disabled is valid without configuration",
			modifier: func(c *Config) {
				c.SASL.Enabled = false
			},
			wantErr: false,
		},
		{
			name: "SASL enabled with PLAIN mechanism is valid",
			modifier: func(c *Config) {
				c.SASL.Enabled = true
				c.SASL.Mechanism = "PLAIN"
				c.SASL.CredentialsSource = "env"
			},
			wantErr: false,
		},
		{
			name: "SASL enabled with invalid mechanism is invalid",
			modifier: func(c *Config) {
				c.SASL.Enabled = true
				c.SASL.Mechanism = "SCRAM-SHA-256"
				c.SASL.CredentialsSource = "env"
			},
			wantErr: true,
		},
		{
			name: "SASL enabled with invalid credentials source is invalid",
			modifier: func(c *Config) {
				c.SASL.Enabled = true
				c.SASL.Mechanism = "PLAIN"
				c.SASL.CredentialsSource = "database"
			},
			wantErr: true,
		},
		{
			name: "SASL enabled with file source but no file path is invalid",
			modifier: func(c *Config) {
				c.SASL.Enabled = true
				c.SASL.Mechanism = "PLAIN"
				c.SASL.CredentialsSource = "file"
				c.SASL.CredentialsFile = ""
			},
			wantErr: true,
		},
		{
			name: "SASL enabled with file source and file path is valid",
			modifier: func(c *Config) {
				c.SASL.Enabled = true
				c.SASL.Mechanism = "PLAIN"
				c.SASL.CredentialsSource = "file"
				c.SASL.CredentialsFile = "/etc/dray/sasl_users.txt"
			},
			wantErr: false,
		},
		{
			name: "SASL enabled with env source is valid",
			modifier: func(c *Config) {
				c.SASL.Enabled = true
				c.SASL.Mechanism = "PLAIN"
				c.SASL.CredentialsSource = "env"
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.modifier(cfg)
			err := cfg.Validate()

			if tt.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestSASLConfigFromYAML(t *testing.T) {
	yamlContent := `
clusterId: "sasl-cluster"
broker:
  listenAddr: ":9093"
sasl:
  enabled: true
  mechanism: "PLAIN"
  credentialsSource: "file"
  credentialsFile: "/etc/dray/sasl_users.txt"
metadata:
  oxiaEndpoint: "localhost:6648"
  numDomains: 16
`
	cfg, err := LoadFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !cfg.SASL.Enabled {
		t.Error("expected SASL to be enabled")
	}
	if cfg.SASL.Mechanism != "PLAIN" {
		t.Errorf("expected mechanism PLAIN, got %s", cfg.SASL.Mechanism)
	}
	if cfg.SASL.CredentialsSource != "file" {
		t.Errorf("expected credentials source file, got %s", cfg.SASL.CredentialsSource)
	}
	if cfg.SASL.CredentialsFile != "/etc/dray/sasl_users.txt" {
		t.Errorf("expected credentials file /etc/dray/sasl_users.txt, got %s", cfg.SASL.CredentialsFile)
	}
}

func TestSASLConfigEnvOverrides(t *testing.T) {
	os.Setenv("DRAY_SASL_ENABLED", "true")
	os.Setenv("DRAY_SASL_MECHANISM", "PLAIN")
	os.Setenv("DRAY_SASL_CREDENTIALS_SOURCE", "env")
	os.Setenv("DRAY_SASL_USERS", "alice:secret,bob:password")
	defer func() {
		os.Unsetenv("DRAY_SASL_ENABLED")
		os.Unsetenv("DRAY_SASL_MECHANISM")
		os.Unsetenv("DRAY_SASL_CREDENTIALS_SOURCE")
		os.Unsetenv("DRAY_SASL_USERS")
	}()

	cfg, err := LoadFromPath("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !cfg.SASL.Enabled {
		t.Error("expected SASL to be enabled via env")
	}
	if cfg.SASL.Mechanism != "PLAIN" {
		t.Errorf("expected mechanism PLAIN, got %s", cfg.SASL.Mechanism)
	}
	if cfg.SASL.CredentialsSource != "env" {
		t.Errorf("expected credentials source env, got %s", cfg.SASL.CredentialsSource)
	}
	if cfg.SASL.Users != "alice:secret,bob:password" {
		t.Errorf("expected users alice:secret,bob:password, got %s", cfg.SASL.Users)
	}
}
