package topics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

func TestValidateRetentionMs(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid positive", "604800000", false},
		{"valid zero", "0", false},
		{"valid unlimited", "-1", false},
		{"invalid negative", "-2", true},
		{"invalid non-numeric", "abc", true},
		{"valid large value", "9223372036854775807", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(ConfigRetentionMs, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig(%s, %s) error = %v, wantErr %v", ConfigRetentionMs, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestValidateRetentionBytes(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid positive", "1073741824", false},
		{"valid zero", "0", false},
		{"valid unlimited", "-1", false},
		{"invalid negative", "-2", true},
		{"invalid non-numeric", "xyz", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(ConfigRetentionBytes, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig(%s, %s) error = %v, wantErr %v", ConfigRetentionBytes, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestValidateCleanupPolicy(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid delete", "delete", false},
		{"valid compact", "compact", false},
		{"invalid mixed", "delete,compact", true},
		{"invalid empty", "", true},
		{"invalid other", "unknown", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(ConfigCleanupPolicy, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig(%s, %s) error = %v, wantErr %v", ConfigCleanupPolicy, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestValidateMinInsyncReplicas(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid 1", "1", false},
		{"valid 3", "3", false},
		{"invalid 0", "0", true},
		{"invalid negative", "-1", true},
		{"invalid non-numeric", "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(ConfigMinInsyncReplicas, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig(%s, %s) error = %v, wantErr %v", ConfigMinInsyncReplicas, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestValidateReplicationFactor(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid 1", "1", false},
		{"valid 3", "3", false},
		{"valid broker default", "-1", false},
		{"invalid 0", "0", true},
		{"invalid -2", "-2", true},
		{"invalid non-numeric", "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(ConfigReplicationFactor, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig(%s, %s) error = %v, wantErr %v", ConfigReplicationFactor, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestValidateUnknownConfig(t *testing.T) {
	err := ValidateConfig("unknown.config", "value")
	if err == nil {
		t.Error("expected error for unknown config key")
	}
	var cfgErr *ConfigValidationError
	if !errors.As(err, &cfgErr) {
		t.Errorf("expected ConfigValidationError, got %T", err)
	}
}

func TestIsSupportedConfig(t *testing.T) {
	supported := []string{
		ConfigRetentionMs,
		ConfigRetentionBytes,
		ConfigCleanupPolicy,
		ConfigMinInsyncReplicas,
		ConfigReplicationFactor,
	}
	for _, key := range supported {
		if !IsSupportedConfig(key) {
			t.Errorf("expected %s to be supported", key)
		}
	}
	if IsSupportedConfig("unknown.key") {
		t.Error("expected unknown.key to not be supported")
	}
}

func TestIsIgnoredConfig(t *testing.T) {
	ignoredConfigs := []string{ConfigMinInsyncReplicas, ConfigReplicationFactor}
	for _, key := range ignoredConfigs {
		if !IsIgnoredConfig(key) {
			t.Errorf("expected %s to be ignored", key)
		}
	}
	notIgnored := []string{ConfigRetentionMs, ConfigRetentionBytes, ConfigCleanupPolicy}
	for _, key := range notIgnored {
		if IsIgnoredConfig(key) {
			t.Errorf("expected %s to not be ignored", key)
		}
	}
}

func TestValidateConfigs(t *testing.T) {
	validConfigs := map[string]string{
		ConfigRetentionMs:       "86400000",
		ConfigRetentionBytes:    "1073741824",
		ConfigCleanupPolicy:     "delete",
		ConfigMinInsyncReplicas: "1",
		ConfigReplicationFactor: "3",
	}
	if err := ValidateConfigs(validConfigs); err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	invalidConfigs := map[string]string{
		ConfigRetentionMs:   "86400000",
		ConfigCleanupPolicy: "invalid",
	}
	if err := ValidateConfigs(invalidConfigs); err == nil {
		t.Error("expected error for invalid config")
	}
}

func TestNormalizeConfigs(t *testing.T) {
	input := map[string]string{
		ConfigRetentionMs:       "86400000",
		ConfigMinInsyncReplicas: "1",
	}
	normalized, err := NormalizeConfigs(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if normalized[ConfigRetentionMs] != "86400000" {
		t.Errorf("expected retention.ms=86400000, got %s", normalized[ConfigRetentionMs])
	}
	if normalized[ConfigMinInsyncReplicas] != "1" {
		t.Errorf("expected min.insync.replicas=1, got %s", normalized[ConfigMinInsyncReplicas])
	}

	// Test nil input
	nilNormalized, err := NormalizeConfigs(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil: %v", err)
	}
	if nilNormalized != nil {
		t.Errorf("expected nil for nil input, got %v", nilNormalized)
	}

	// Test invalid input
	_, err = NormalizeConfigs(map[string]string{"invalid.key": "value"})
	if err == nil {
		t.Error("expected error for invalid config key")
	}
}

func TestDefaultConfigs(t *testing.T) {
	defaults := DefaultConfigs()
	if defaults[ConfigRetentionMs] != "604800000" {
		t.Errorf("expected default retention.ms=604800000, got %s", defaults[ConfigRetentionMs])
	}
	if defaults[ConfigRetentionBytes] != "-1" {
		t.Errorf("expected default retention.bytes=-1, got %s", defaults[ConfigRetentionBytes])
	}
	if defaults[ConfigCleanupPolicy] != CleanupPolicyDelete {
		t.Errorf("expected default cleanup.policy=delete, got %s", defaults[ConfigCleanupPolicy])
	}
}

func TestMergeWithDefaults(t *testing.T) {
	userConfigs := map[string]string{
		ConfigRetentionMs: "86400000",
	}
	merged := MergeWithDefaults(userConfigs)

	if merged[ConfigRetentionMs] != "86400000" {
		t.Errorf("expected user value for retention.ms, got %s", merged[ConfigRetentionMs])
	}
	if merged[ConfigRetentionBytes] != "-1" {
		t.Errorf("expected default retention.bytes, got %s", merged[ConfigRetentionBytes])
	}
	if merged[ConfigCleanupPolicy] != CleanupPolicyDelete {
		t.Errorf("expected default cleanup.policy, got %s", merged[ConfigCleanupPolicy])
	}
}

func TestGetRetentionMs(t *testing.T) {
	configs := map[string]string{ConfigRetentionMs: "86400000"}
	v, err := GetRetentionMs(configs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 86400000 {
		t.Errorf("expected 86400000, got %d", v)
	}

	// Test default
	v, err = GetRetentionMs(map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != DefaultRetentionMs {
		t.Errorf("expected default %d, got %d", DefaultRetentionMs, v)
	}
}

func TestGetRetentionBytes(t *testing.T) {
	configs := map[string]string{ConfigRetentionBytes: "1073741824"}
	v, err := GetRetentionBytes(configs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 1073741824 {
		t.Errorf("expected 1073741824, got %d", v)
	}

	// Test default
	v, err = GetRetentionBytes(map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != DefaultRetentionBytes {
		t.Errorf("expected default %d, got %d", DefaultRetentionBytes, v)
	}
}

func TestGetCleanupPolicy(t *testing.T) {
	configs := map[string]string{ConfigCleanupPolicy: CleanupPolicyCompact}
	v := GetCleanupPolicy(configs)
	if v != CleanupPolicyCompact {
		t.Errorf("expected compact, got %s", v)
	}

	// Test default
	v = GetCleanupPolicy(map[string]string{})
	if v != DefaultCleanupPolicy {
		t.Errorf("expected default %s, got %s", DefaultCleanupPolicy, v)
	}
}

func TestConfigValidationError(t *testing.T) {
	err := &ConfigValidationError{
		Key:     "test.key",
		Value:   "bad-value",
		Message: "invalid format",
	}
	expected := `topics: config "test.key"="bad-value": invalid format`
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}

func TestSupportedConfigs(t *testing.T) {
	configs := SupportedConfigs()
	if len(configs) != 6 {
		t.Errorf("expected 6 supported configs, got %d", len(configs))
	}
	expected := map[string]bool{
		ConfigRetentionMs:       true,
		ConfigRetentionBytes:    true,
		ConfigCleanupPolicy:     true,
		ConfigMinInsyncReplicas: true,
		ConfigReplicationFactor: true,
		ConfigIcebergEnabled:    true,
	}
	for _, c := range configs {
		if !expected[c] {
			t.Errorf("unexpected config: %s", c)
		}
	}
}

// Store integration tests for config operations

func TestStore_CreateTopicWithValidConfigs(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigRetentionMs:       "86400000",
			ConfigRetentionBytes:    "1073741824",
			ConfigCleanupPolicy:     "compact",
			ConfigMinInsyncReplicas: "1",
			ConfigReplicationFactor: "3",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if result.Topic.Config[ConfigRetentionMs] != "86400000" {
		t.Errorf("retention.ms not stored correctly: %v", result.Topic.Config)
	}
	if result.Topic.Config[ConfigCleanupPolicy] != "compact" {
		t.Errorf("cleanup.policy not stored correctly: %v", result.Topic.Config)
	}
}

func TestStore_CreateTopicWithInvalidConfigs(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "invalid-config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigRetentionMs: "not-a-number",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
	var cfgErr *ConfigValidationError
	if !errors.As(err, &cfgErr) {
		t.Errorf("expected ConfigValidationError, got %T: %v", err, err)
	}
}

func TestStore_CreateTopicWithUnknownConfig(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "unknown-config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			"unknown.key": "value",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err == nil {
		t.Fatal("expected error for unknown config key")
	}
	var cfgErr *ConfigValidationError
	if !errors.As(err, &cfgErr) {
		t.Errorf("expected ConfigValidationError, got %T: %v", err, err)
	}
}

func TestStore_GetTopicConfig(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "get-config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigRetentionMs:   "86400000",
			ConfigCleanupPolicy: "delete",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	config, err := store.GetTopicConfig(ctx, "get-config-topic")
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if config[ConfigRetentionMs] != "86400000" {
		t.Errorf("expected retention.ms=86400000, got %s", config[ConfigRetentionMs])
	}
	if config[ConfigCleanupPolicy] != "delete" {
		t.Errorf("expected cleanup.policy=delete, got %s", config[ConfigCleanupPolicy])
	}
}

func TestStore_GetTopicConfigNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.GetTopicConfig(ctx, "nonexistent")
	if err != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestStore_GetTopicConfigEmpty(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "no-config-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	config, err := store.GetTopicConfig(ctx, "no-config-topic")
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if len(config) != 0 {
		t.Errorf("expected empty config, got %v", config)
	}
}

func TestStore_UpdateTopicConfig(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "update-config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigRetentionMs: "86400000",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = store.UpdateTopicConfig(ctx, UpdateTopicConfigRequest{
		TopicName: "update-config-topic",
		Configs: map[string]string{
			ConfigRetentionMs:    "172800000",
			ConfigRetentionBytes: "2147483648",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to update config: %v", err)
	}

	config, err := store.GetTopicConfig(ctx, "update-config-topic")
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if config[ConfigRetentionMs] != "172800000" {
		t.Errorf("expected retention.ms=172800000, got %s", config[ConfigRetentionMs])
	}
	if config[ConfigRetentionBytes] != "2147483648" {
		t.Errorf("expected retention.bytes=2147483648, got %s", config[ConfigRetentionBytes])
	}
}

func TestStore_UpdateTopicConfigNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	err := store.UpdateTopicConfig(ctx, UpdateTopicConfigRequest{
		TopicName: "nonexistent",
		Configs: map[string]string{
			ConfigRetentionMs: "86400000",
		},
	})
	if err != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestStore_UpdateTopicConfigInvalid(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "update-invalid-config-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = store.UpdateTopicConfig(ctx, UpdateTopicConfigRequest{
		TopicName: "update-invalid-config-topic",
		Configs: map[string]string{
			ConfigCleanupPolicy: "invalid-policy",
		},
	})
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
	var cfgErr *ConfigValidationError
	if !errors.As(err, &cfgErr) {
		t.Errorf("expected ConfigValidationError, got %T: %v", err, err)
	}
}

func TestStore_UpdateTopicConfigEmptyName(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	err := store.UpdateTopicConfig(ctx, UpdateTopicConfigRequest{
		TopicName: "",
		Configs: map[string]string{
			ConfigRetentionMs: "86400000",
		},
	})
	if err != ErrInvalidTopicName {
		t.Errorf("expected ErrInvalidTopicName, got %v", err)
	}
}

func TestStore_SetTopicConfig(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "set-config-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = store.SetTopicConfig(ctx, "set-config-topic", ConfigRetentionMs, "86400000")
	if err != nil {
		t.Fatalf("failed to set config: %v", err)
	}

	config, err := store.GetTopicConfig(ctx, "set-config-topic")
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if config[ConfigRetentionMs] != "86400000" {
		t.Errorf("expected retention.ms=86400000, got %s", config[ConfigRetentionMs])
	}
}

func TestStore_SetTopicConfigInvalid(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "set-invalid-config-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = store.SetTopicConfig(ctx, "set-invalid-config-topic", ConfigCleanupPolicy, "bad-value")
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
	var cfgErr *ConfigValidationError
	if !errors.As(err, &cfgErr) {
		t.Errorf("expected ConfigValidationError, got %T: %v", err, err)
	}
}

func TestStore_SetTopicConfigNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	err := store.SetTopicConfig(ctx, "nonexistent", ConfigRetentionMs, "86400000")
	if err != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestStore_DeleteTopicConfig(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "delete-config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigRetentionMs:   "86400000",
			ConfigCleanupPolicy: "delete",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	err = store.DeleteTopicConfig(ctx, "delete-config-topic", ConfigRetentionMs)
	if err != nil {
		t.Fatalf("failed to delete config: %v", err)
	}

	config, err := store.GetTopicConfig(ctx, "delete-config-topic")
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if _, ok := config[ConfigRetentionMs]; ok {
		t.Error("retention.ms should have been deleted")
	}
	if config[ConfigCleanupPolicy] != "delete" {
		t.Errorf("cleanup.policy should still exist, got %v", config)
	}
}

func TestStore_DeleteTopicConfigNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	err := store.DeleteTopicConfig(ctx, "nonexistent", ConfigRetentionMs)
	if err != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestStore_IgnoredConfigsStored(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "ignored-configs-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigMinInsyncReplicas: "2",
			ConfigReplicationFactor: "3",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if result.Topic.Config[ConfigMinInsyncReplicas] != "2" {
		t.Errorf("expected min.insync.replicas=2, got %s", result.Topic.Config[ConfigMinInsyncReplicas])
	}
	if result.Topic.Config[ConfigReplicationFactor] != "3" {
		t.Errorf("expected replication.factor=3, got %s", result.Topic.Config[ConfigReplicationFactor])
	}

	topic, err := store.GetTopic(ctx, "ignored-configs-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topic.Config[ConfigMinInsyncReplicas] != "2" {
		t.Errorf("expected min.insync.replicas=2, got %s", topic.Config[ConfigMinInsyncReplicas])
	}
	if topic.Config[ConfigReplicationFactor] != "3" {
		t.Errorf("expected replication.factor=3, got %s", topic.Config[ConfigReplicationFactor])
	}
}

// Tests for table.iceberg.enabled config (duality mode)

func TestValidateIcebergEnabled(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid true", "true", false},
		{"valid false", "false", false},
		{"invalid empty", "", true},
		{"invalid uppercase", "TRUE", true},
		{"invalid yes", "yes", true},
		{"invalid 1", "1", true},
		{"invalid 0", "0", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(ConfigIcebergEnabled, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig(%s, %s) error = %v, wantErr %v", ConfigIcebergEnabled, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestGetIcebergEnabled(t *testing.T) {
	tests := []struct {
		name          string
		configs       map[string]string
		globalDefault bool
		expected      bool
	}{
		{
			name:          "not set, global true",
			configs:       map[string]string{},
			globalDefault: true,
			expected:      true,
		},
		{
			name:          "not set, global false",
			configs:       map[string]string{},
			globalDefault: false,
			expected:      false,
		},
		{
			name:          "explicitly true, global true",
			configs:       map[string]string{ConfigIcebergEnabled: "true"},
			globalDefault: true,
			expected:      true,
		},
		{
			name:          "explicitly true, global false",
			configs:       map[string]string{ConfigIcebergEnabled: "true"},
			globalDefault: false,
			expected:      true,
		},
		{
			name:          "explicitly false, global true",
			configs:       map[string]string{ConfigIcebergEnabled: "false"},
			globalDefault: true,
			expected:      false,
		},
		{
			name:          "explicitly false, global false",
			configs:       map[string]string{ConfigIcebergEnabled: "false"},
			globalDefault: false,
			expected:      false,
		},
		{
			name:          "nil configs, global true",
			configs:       nil,
			globalDefault: true,
			expected:      true,
		},
		{
			name:          "other configs, global true",
			configs:       map[string]string{ConfigRetentionMs: "86400000"},
			globalDefault: true,
			expected:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetIcebergEnabled(tt.configs, tt.globalDefault)
			if result != tt.expected {
				t.Errorf("GetIcebergEnabled() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestStore_CreateTopicWithIcebergEnabled(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "iceberg-enabled-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigIcebergEnabled: "true",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if result.Topic.Config[ConfigIcebergEnabled] != "true" {
		t.Errorf("expected table.iceberg.enabled=true, got %s", result.Topic.Config[ConfigIcebergEnabled])
	}
}

func TestStore_CreateTopicWithIcebergDisabled(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "iceberg-disabled-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigIcebergEnabled: "false",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if result.Topic.Config[ConfigIcebergEnabled] != "false" {
		t.Errorf("expected table.iceberg.enabled=false, got %s", result.Topic.Config[ConfigIcebergEnabled])
	}
}

func TestStore_CreateTopicWithInvalidIcebergEnabled(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "iceberg-invalid-topic",
		PartitionCount: 1,
		Config: map[string]string{
			ConfigIcebergEnabled: "invalid",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err == nil {
		t.Fatal("expected error for invalid table.iceberg.enabled value")
	}
	var cfgErr *ConfigValidationError
	if !errors.As(err, &cfgErr) {
		t.Errorf("expected ConfigValidationError, got %T: %v", err, err)
	}
}
