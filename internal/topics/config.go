package topics

import (
	"errors"
	"fmt"
	"strconv"
)

// Supported config keys.
const (
	ConfigRetentionMs        = "retention.ms"
	ConfigRetentionBytes     = "retention.bytes"
	ConfigCleanupPolicy      = "cleanup.policy"
	ConfigMinInsyncReplicas  = "min.insync.replicas"
	ConfigReplicationFactor  = "replication.factor"
)

// Cleanup policy values.
const (
	CleanupPolicyDelete  = "delete"
	CleanupPolicyCompact = "compact"
)

// Default config values.
const (
	DefaultRetentionMs    = 604800000 // 7 days in ms
	DefaultRetentionBytes = -1        // unlimited
	DefaultCleanupPolicy  = CleanupPolicyDelete
)

// Config validation errors.
var (
	ErrInvalidConfigKey   = errors.New("topics: invalid config key")
	ErrInvalidConfigValue = errors.New("topics: invalid config value")
)

// ConfigValidationError provides detailed error information for config validation failures.
type ConfigValidationError struct {
	Key     string
	Value   string
	Message string
}

func (e *ConfigValidationError) Error() string {
	return fmt.Sprintf("topics: config %q=%q: %s", e.Key, e.Value, e.Message)
}

// SupportedConfigs returns the set of config keys that Dray supports.
func SupportedConfigs() []string {
	return []string{
		ConfigRetentionMs,
		ConfigRetentionBytes,
		ConfigCleanupPolicy,
		ConfigMinInsyncReplicas,
		ConfigReplicationFactor,
	}
}

// IsSupportedConfig checks if a config key is supported.
func IsSupportedConfig(key string) bool {
	switch key {
	case ConfigRetentionMs, ConfigRetentionBytes, ConfigCleanupPolicy,
		ConfigMinInsyncReplicas, ConfigReplicationFactor:
		return true
	default:
		return false
	}
}

// IsIgnoredConfig returns true if the config key is accepted but ignored.
// Per spec section 13.2: min.insync.replicas and replication.factor are accepted but ignored.
func IsIgnoredConfig(key string) bool {
	switch key {
	case ConfigMinInsyncReplicas, ConfigReplicationFactor:
		return true
	default:
		return false
	}
}

// ValidateConfig validates a single config key-value pair.
// Returns nil if valid, or a ConfigValidationError if invalid.
// For ignored configs (min.insync.replicas, replication.factor), basic validation is still performed.
func ValidateConfig(key, value string) error {
	switch key {
	case ConfigRetentionMs:
		return validateRetentionMs(value)
	case ConfigRetentionBytes:
		return validateRetentionBytes(value)
	case ConfigCleanupPolicy:
		return validateCleanupPolicy(value)
	case ConfigMinInsyncReplicas:
		return validateMinInsyncReplicas(value)
	case ConfigReplicationFactor:
		return validateReplicationFactor(value)
	default:
		return &ConfigValidationError{
			Key:     key,
			Value:   value,
			Message: "unknown config key",
		}
	}
}

// ValidateConfigs validates a map of config key-value pairs.
// Returns the first validation error encountered, or nil if all configs are valid.
func ValidateConfigs(configs map[string]string) error {
	for key, value := range configs {
		if err := ValidateConfig(key, value); err != nil {
			return err
		}
	}
	return nil
}

// NormalizeConfigs processes configs and returns a normalized map.
// Ignored configs are still stored for compatibility but have no effect.
// Unknown configs result in an error.
func NormalizeConfigs(configs map[string]string) (map[string]string, error) {
	if configs == nil {
		return nil, nil
	}

	if err := ValidateConfigs(configs); err != nil {
		return nil, err
	}

	normalized := make(map[string]string, len(configs))
	for key, value := range configs {
		normalized[key] = value
	}

	return normalized, nil
}

// DefaultConfigs returns the default topic configuration.
func DefaultConfigs() map[string]string {
	return map[string]string{
		ConfigRetentionMs:    strconv.FormatInt(DefaultRetentionMs, 10),
		ConfigRetentionBytes: strconv.FormatInt(DefaultRetentionBytes, 10),
		ConfigCleanupPolicy:  DefaultCleanupPolicy,
	}
}

// MergeWithDefaults merges user-provided configs with defaults.
// User-provided values override defaults.
func MergeWithDefaults(configs map[string]string) map[string]string {
	result := DefaultConfigs()
	for key, value := range configs {
		result[key] = value
	}
	return result
}

// GetRetentionMs extracts retention.ms from config, returning default if not set.
func GetRetentionMs(configs map[string]string) (int64, error) {
	if value, ok := configs[ConfigRetentionMs]; ok {
		return strconv.ParseInt(value, 10, 64)
	}
	return DefaultRetentionMs, nil
}

// GetRetentionBytes extracts retention.bytes from config, returning default if not set.
func GetRetentionBytes(configs map[string]string) (int64, error) {
	if value, ok := configs[ConfigRetentionBytes]; ok {
		return strconv.ParseInt(value, 10, 64)
	}
	return DefaultRetentionBytes, nil
}

// GetCleanupPolicy extracts cleanup.policy from config, returning default if not set.
func GetCleanupPolicy(configs map[string]string) string {
	if value, ok := configs[ConfigCleanupPolicy]; ok {
		return value
	}
	return DefaultCleanupPolicy
}

func validateRetentionMs(value string) error {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return &ConfigValidationError{
			Key:     ConfigRetentionMs,
			Value:   value,
			Message: "must be a valid integer",
		}
	}
	if v < -1 {
		return &ConfigValidationError{
			Key:     ConfigRetentionMs,
			Value:   value,
			Message: "must be >= -1 (use -1 for unlimited)",
		}
	}
	return nil
}

func validateRetentionBytes(value string) error {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return &ConfigValidationError{
			Key:     ConfigRetentionBytes,
			Value:   value,
			Message: "must be a valid integer",
		}
	}
	if v < -1 {
		return &ConfigValidationError{
			Key:     ConfigRetentionBytes,
			Value:   value,
			Message: "must be >= -1 (use -1 for unlimited)",
		}
	}
	return nil
}

func validateCleanupPolicy(value string) error {
	switch value {
	case CleanupPolicyDelete, CleanupPolicyCompact:
		return nil
	default:
		return &ConfigValidationError{
			Key:     ConfigCleanupPolicy,
			Value:   value,
			Message: fmt.Sprintf("must be one of: %s, %s", CleanupPolicyDelete, CleanupPolicyCompact),
		}
	}
}

func validateMinInsyncReplicas(value string) error {
	v, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return &ConfigValidationError{
			Key:     ConfigMinInsyncReplicas,
			Value:   value,
			Message: "must be a valid integer",
		}
	}
	if v < 1 {
		return &ConfigValidationError{
			Key:     ConfigMinInsyncReplicas,
			Value:   value,
			Message: "must be >= 1",
		}
	}
	return nil
}

func validateReplicationFactor(value string) error {
	v, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return &ConfigValidationError{
			Key:     ConfigReplicationFactor,
			Value:   value,
			Message: "must be a valid integer",
		}
	}
	if v < 1 && v != -1 {
		return &ConfigValidationError{
			Key:     ConfigReplicationFactor,
			Value:   value,
			Message: "must be >= 1 (or -1 for broker default)",
		}
	}
	return nil
}
