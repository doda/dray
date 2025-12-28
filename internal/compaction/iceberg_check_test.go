package compaction

import (
	"context"
	"errors"
	"testing"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/topics"
)

// mockStreamMetaProvider implements StreamMetaProvider for testing.
type mockStreamMetaProvider struct {
	metas map[string]*index.StreamMeta
	err   error
}

func (m *mockStreamMetaProvider) GetStreamMeta(_ context.Context, streamID string) (*index.StreamMeta, error) {
	if m.err != nil {
		return nil, m.err
	}
	meta, ok := m.metas[streamID]
	if !ok {
		return nil, errors.New("stream not found")
	}
	return meta, nil
}

// mockTopicConfigProvider implements TopicConfigProvider for testing.
type mockTopicConfigProvider struct {
	configs map[string]map[string]string
	err     error
}

func (m *mockTopicConfigProvider) GetTopicConfig(_ context.Context, topicName string) (map[string]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.configs[topicName], nil
}

func TestIcebergChecker_IsIcebergEnabled(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		streamID       string
		topicName      string
		topicConfig    map[string]string
		globalDefault  bool
		expectedResult bool
		expectedError  bool
	}{
		{
			name:           "global default true, no per-topic config",
			streamID:       "stream-1",
			topicName:      "my-topic",
			topicConfig:    nil,
			globalDefault:  true,
			expectedResult: true,
		},
		{
			name:           "global default false, no per-topic config",
			streamID:       "stream-1",
			topicName:      "my-topic",
			topicConfig:    nil,
			globalDefault:  false,
			expectedResult: false,
		},
		{
			name:           "global default true, per-topic config false",
			streamID:       "stream-1",
			topicName:      "my-topic",
			topicConfig:    map[string]string{topics.ConfigIcebergEnabled: "false"},
			globalDefault:  true,
			expectedResult: false,
		},
		{
			name:           "global default false, per-topic config true",
			streamID:       "stream-1",
			topicName:      "my-topic",
			topicConfig:    map[string]string{topics.ConfigIcebergEnabled: "true"},
			globalDefault:  false,
			expectedResult: true,
		},
		{
			name:           "global default true, per-topic config true (explicit)",
			streamID:       "stream-1",
			topicName:      "my-topic",
			topicConfig:    map[string]string{topics.ConfigIcebergEnabled: "true"},
			globalDefault:  true,
			expectedResult: true,
		},
		{
			name:      "config with other keys but not iceberg enabled",
			streamID:  "stream-1",
			topicName: "my-topic",
			topicConfig: map[string]string{
				topics.ConfigRetentionMs: "86400000",
			},
			globalDefault:  true,
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamMetas := &mockStreamMetaProvider{
				metas: map[string]*index.StreamMeta{
					tt.streamID: {
						StreamID:  tt.streamID,
						TopicName: tt.topicName,
						Partition: 0,
					},
				},
			}

			topicConfigs := &mockTopicConfigProvider{
				configs: map[string]map[string]string{
					tt.topicName: tt.topicConfig,
				},
			}

			checker := NewIcebergChecker(topicConfigs, streamMetas, tt.globalDefault)

			result, err := checker.IsIcebergEnabled(ctx, tt.streamID)
			if (err != nil) != tt.expectedError {
				t.Errorf("IsIcebergEnabled() error = %v, expectedError %v", err, tt.expectedError)
				return
			}
			if result != tt.expectedResult {
				t.Errorf("IsIcebergEnabled() = %v, want %v", result, tt.expectedResult)
			}
		})
	}
}

func TestIcebergChecker_ShouldSkipIcebergCommit(t *testing.T) {
	ctx := context.Background()

	streamMetas := &mockStreamMetaProvider{
		metas: map[string]*index.StreamMeta{
			"stream-1": {
				StreamID:  "stream-1",
				TopicName: "enabled-topic",
				Partition: 0,
			},
			"stream-2": {
				StreamID:  "stream-2",
				TopicName: "disabled-topic",
				Partition: 0,
			},
		},
	}

	topicConfigs := &mockTopicConfigProvider{
		configs: map[string]map[string]string{
			"enabled-topic":  {topics.ConfigIcebergEnabled: "true"},
			"disabled-topic": {topics.ConfigIcebergEnabled: "false"},
		},
	}

	checker := NewIcebergChecker(topicConfigs, streamMetas, true)

	t.Run("enabled topic should not skip", func(t *testing.T) {
		skip, err := checker.ShouldSkipIcebergCommit(ctx, "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if skip {
			t.Error("expected ShouldSkipIcebergCommit=false for enabled topic")
		}
	})

	t.Run("disabled topic should skip", func(t *testing.T) {
		skip, err := checker.ShouldSkipIcebergCommit(ctx, "stream-2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !skip {
			t.Error("expected ShouldSkipIcebergCommit=true for disabled topic")
		}
	})
}

func TestIcebergChecker_NilProviders(t *testing.T) {
	ctx := context.Background()

	t.Run("nil providers default to global true", func(t *testing.T) {
		checker := NewIcebergChecker(nil, nil, true)
		result, err := checker.IsIcebergEnabled(ctx, "any-stream")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result {
			t.Error("expected IsIcebergEnabled=true when providers are nil and global default is true")
		}
	})

	t.Run("nil providers default to global false", func(t *testing.T) {
		checker := NewIcebergChecker(nil, nil, false)
		result, err := checker.IsIcebergEnabled(ctx, "any-stream")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result {
			t.Error("expected IsIcebergEnabled=false when providers are nil and global default is false")
		}
	})
}

func TestIcebergChecker_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("stream meta error", func(t *testing.T) {
		streamMetas := &mockStreamMetaProvider{
			err: errors.New("stream lookup failed"),
		}
		topicConfigs := &mockTopicConfigProvider{
			configs: make(map[string]map[string]string),
		}

		checker := NewIcebergChecker(topicConfigs, streamMetas, true)
		_, err := checker.IsIcebergEnabled(ctx, "stream-1")
		if err == nil {
			t.Error("expected error when stream meta lookup fails")
		}
	})

	t.Run("topic config error", func(t *testing.T) {
		streamMetas := &mockStreamMetaProvider{
			metas: map[string]*index.StreamMeta{
				"stream-1": {StreamID: "stream-1", TopicName: "my-topic"},
			},
		}
		topicConfigs := &mockTopicConfigProvider{
			err: errors.New("topic config lookup failed"),
		}

		checker := NewIcebergChecker(topicConfigs, streamMetas, true)
		_, err := checker.IsIcebergEnabled(ctx, "stream-1")
		if err == nil {
			t.Error("expected error when topic config lookup fails")
		}
	})
}

func TestProviderFuncs(t *testing.T) {
	ctx := context.Background()

	t.Run("TopicConfigProviderFunc", func(t *testing.T) {
		provider := TopicConfigProviderFunc(func(_ context.Context, topicName string) (map[string]string, error) {
			if topicName == "test-topic" {
				return map[string]string{"key": "value"}, nil
			}
			return nil, nil
		})

		config, err := provider.GetTopicConfig(ctx, "test-topic")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if config["key"] != "value" {
			t.Errorf("expected config[key]=value, got %v", config)
		}
	})

	t.Run("StreamMetaProviderFunc", func(t *testing.T) {
		provider := StreamMetaProviderFunc(func(_ context.Context, streamID string) (*index.StreamMeta, error) {
			return &index.StreamMeta{
				StreamID:  streamID,
				TopicName: "test-topic",
			}, nil
		})

		meta, err := provider.GetStreamMeta(ctx, "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if meta.StreamID != "stream-1" || meta.TopicName != "test-topic" {
			t.Errorf("unexpected meta: %+v", meta)
		}
	})
}
