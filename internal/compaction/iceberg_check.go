// Package compaction implements stream compaction for the Dray broker.
// This file provides helpers to check whether Iceberg commits should be made
// for a given stream based on the topic's table.iceberg.enabled config.

package compaction

import (
	"context"
	"fmt"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/topics"
)

// TopicConfigProvider provides access to topic configuration.
// This interface allows the compaction package to check per-topic settings
// without depending directly on the topics.Store implementation.
type TopicConfigProvider interface {
	// GetTopicConfig returns the configuration for a topic.
	// Returns nil, nil if the topic does not exist.
	GetTopicConfig(ctx context.Context, topicName string) (map[string]string, error)
}

// StreamMetaProvider provides access to stream metadata.
// This interface allows the compaction package to look up the topic name
// for a given stream ID.
type StreamMetaProvider interface {
	// GetStreamMeta returns the metadata for a stream.
	GetStreamMeta(ctx context.Context, streamID string) (*index.StreamMeta, error)
}

// IcebergChecker determines whether Iceberg commits should be made for a stream
// based on the topic's table.iceberg.enabled configuration.
//
// This encapsulates the logic per SPEC.md section 11.2:
//   - If table.iceberg.enabled is set per-topic, use that value
//   - Otherwise, use the global default (typically true for duality mode)
type IcebergChecker struct {
	topicConfigs  TopicConfigProvider
	streamMetas   StreamMetaProvider
	globalDefault bool
}

// NewIcebergChecker creates a new IcebergChecker.
//
// The globalDefault parameter should be the cluster-wide Iceberg enable setting.
// Per-topic config (table.iceberg.enabled) overrides this default.
func NewIcebergChecker(topicConfigs TopicConfigProvider, streamMetas StreamMetaProvider, globalDefault bool) *IcebergChecker {
	return &IcebergChecker{
		topicConfigs:  topicConfigs,
		streamMetas:   streamMetas,
		globalDefault: globalDefault,
	}
}

// IsIcebergEnabled checks whether Iceberg commits should be made for a stream.
//
// It returns true if:
//   - The topic's table.iceberg.enabled is set to "true", OR
//   - The topic's table.iceberg.enabled is not set AND globalDefault is true
//
// It returns false if:
//   - The topic's table.iceberg.enabled is set to "false", OR
//   - The topic's table.iceberg.enabled is not set AND globalDefault is false
func (ic *IcebergChecker) IsIcebergEnabled(ctx context.Context, streamID string) (bool, error) {
	if ic.streamMetas == nil || ic.topicConfigs == nil {
		// If providers are not configured, fall back to global default
		return ic.globalDefault, nil
	}

	// Look up stream metadata to get the topic name
	meta, err := ic.streamMetas.GetStreamMeta(ctx, streamID)
	if err != nil {
		return false, fmt.Errorf("compaction: get stream meta for iceberg check: %w", err)
	}

	// Look up topic configuration
	config, err := ic.topicConfigs.GetTopicConfig(ctx, meta.TopicName)
	if err != nil {
		return false, fmt.Errorf("compaction: get topic config for iceberg check: %w", err)
	}

	// Use the helper from topics package to resolve the value
	// This handles the per-topic override vs global default logic
	return topics.GetIcebergEnabled(config, ic.globalDefault), nil
}

// ShouldSkipIcebergCommit is a convenience method that returns true if Iceberg
// commits should be skipped for a stream. This is the inverse of IsIcebergEnabled.
func (ic *IcebergChecker) ShouldSkipIcebergCommit(ctx context.Context, streamID string) (bool, error) {
	enabled, err := ic.IsIcebergEnabled(ctx, streamID)
	if err != nil {
		return false, err
	}
	return !enabled, nil
}

// TopicConfigProviderFunc is an adapter to allow ordinary functions to be used
// as TopicConfigProvider.
type TopicConfigProviderFunc func(ctx context.Context, topicName string) (map[string]string, error)

func (f TopicConfigProviderFunc) GetTopicConfig(ctx context.Context, topicName string) (map[string]string, error) {
	return f(ctx, topicName)
}

// StreamMetaProviderFunc is an adapter to allow ordinary functions to be used
// as StreamMetaProvider.
type StreamMetaProviderFunc func(ctx context.Context, streamID string) (*index.StreamMeta, error)

func (f StreamMetaProviderFunc) GetStreamMeta(ctx context.Context, streamID string) (*index.StreamMeta, error) {
	return f(ctx, streamID)
}
