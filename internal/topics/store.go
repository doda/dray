// Package topics handles topic metadata and admin operations.
package topics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/google/uuid"
)

// Common errors.
var (
	ErrTopicNotFound      = errors.New("topics: topic not found")
	ErrTopicExists        = errors.New("topics: topic already exists")
	ErrPartitionNotFound  = errors.New("topics: partition not found")
	ErrInvalidTopicName   = errors.New("topics: invalid topic name")
	ErrInvalidPartitions  = errors.New("topics: invalid partition count")
)

// TopicMeta holds metadata for a topic.
type TopicMeta struct {
	Name           string            `json:"name"`
	TopicID        string            `json:"topicId"`
	PartitionCount int32             `json:"partitionCount"`
	Config         map[string]string `json:"config,omitempty"`
	CreatedAtMs    int64             `json:"createdAtMs"`
}

// PartitionMeta holds metadata for a partition.
type PartitionMeta struct {
	Partition   int32  `json:"partition"`
	StreamID    string `json:"streamId"`
	State       string `json:"state"` // active, pending, deleting
	CreatedAtMs int64  `json:"createdAtMs"`
}

// Store provides topic metadata operations backed by MetadataStore.
type Store struct {
	meta metadata.MetadataStore
}

// NewStore creates a new topic metadata store.
func NewStore(meta metadata.MetadataStore) *Store {
	return &Store{meta: meta}
}

// GetTopic retrieves topic metadata by name.
func (s *Store) GetTopic(ctx context.Context, name string) (*TopicMeta, error) {
	key := keys.TopicKeyPath(name)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("topics: get topic: %w", err)
	}
	if !result.Exists {
		return nil, ErrTopicNotFound
	}

	var topic TopicMeta
	if err := json.Unmarshal(result.Value, &topic); err != nil {
		return nil, fmt.Errorf("topics: unmarshal topic: %w", err)
	}
	return &topic, nil
}

// GetPartition retrieves partition metadata.
func (s *Store) GetPartition(ctx context.Context, topicName string, partition int32) (*PartitionMeta, error) {
	key := keys.TopicPartitionKeyPath(topicName, partition)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("topics: get partition: %w", err)
	}
	if !result.Exists {
		return nil, ErrPartitionNotFound
	}

	var p PartitionMeta
	if err := json.Unmarshal(result.Value, &p); err != nil {
		return nil, fmt.Errorf("topics: unmarshal partition: %w", err)
	}
	return &p, nil
}

// ListPartitions returns all partitions for a topic.
func (s *Store) ListPartitions(ctx context.Context, topicName string) ([]PartitionMeta, error) {
	prefix := keys.TopicPartitionsPrefix(topicName)
	kvs, err := s.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("topics: list partitions: %w", err)
	}

	partitions := make([]PartitionMeta, 0, len(kvs))
	for _, kv := range kvs {
		var p PartitionMeta
		if err := json.Unmarshal(kv.Value, &p); err != nil {
			return nil, fmt.Errorf("topics: unmarshal partition: %w", err)
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}

// ListTopics returns all topics (metadata only, not partitions).
func (s *Store) ListTopics(ctx context.Context) ([]TopicMeta, error) {
	kvs, err := s.meta.List(ctx, keys.TopicsPrefix+"/", "", 0)
	if err != nil {
		return nil, fmt.Errorf("topics: list topics: %w", err)
	}

	var topics []TopicMeta
	for _, kv := range kvs {
		// Only include topic metadata keys (not partition keys)
		if len(kv.Key) > len(keys.TopicsPrefix)+1 {
			rest := kv.Key[len(keys.TopicsPrefix)+1:]
			// Skip if this is a partition key (contains /partitions/)
			if contains(rest, "/") {
				continue
			}
		}

		var t TopicMeta
		if err := json.Unmarshal(kv.Value, &t); err != nil {
			continue // Skip malformed entries
		}
		topics = append(topics, t)
	}
	return topics, nil
}

// CreateTopicRequest holds parameters for topic creation.
type CreateTopicRequest struct {
	Name           string
	PartitionCount int32
	Config         map[string]string
	NowMs          int64
}

// CreateTopicResult holds the result of topic creation.
type CreateTopicResult struct {
	Topic      TopicMeta
	Partitions []PartitionMeta
}

// CreateTopic creates a new topic with partitions.
// Uses a transaction to atomically create topic metadata and all partitions.
func (s *Store) CreateTopic(ctx context.Context, req CreateTopicRequest) (*CreateTopicResult, error) {
	if req.Name == "" {
		return nil, ErrInvalidTopicName
	}
	if req.PartitionCount <= 0 {
		return nil, ErrInvalidPartitions
	}

	topicID := uuid.New().String()
	topic := TopicMeta{
		Name:           req.Name,
		TopicID:        topicID,
		PartitionCount: req.PartitionCount,
		Config:         req.Config,
		CreatedAtMs:    req.NowMs,
	}

	topicData, err := json.Marshal(topic)
	if err != nil {
		return nil, fmt.Errorf("topics: marshal topic: %w", err)
	}

	partitions := make([]PartitionMeta, req.PartitionCount)
	for i := int32(0); i < req.PartitionCount; i++ {
		partitions[i] = PartitionMeta{
			Partition:   i,
			StreamID:    uuid.New().String(),
			State:       "active",
			CreatedAtMs: req.NowMs,
		}
	}

	topicKey := keys.TopicKeyPath(req.Name)

	err = s.meta.Txn(ctx, topicKey, func(txn metadata.Txn) error {
		// Check if topic already exists
		_, _, err := txn.Get(topicKey)
		if err == nil {
			return ErrTopicExists
		}
		if !errors.Is(err, metadata.ErrKeyNotFound) {
			return err
		}

		// Create topic metadata
		txn.Put(topicKey, topicData)

		// Create partition entries
		for _, p := range partitions {
			partData, err := json.Marshal(p)
			if err != nil {
				return fmt.Errorf("marshal partition: %w", err)
			}
			partKey := keys.TopicPartitionKeyPath(req.Name, p.Partition)
			txn.Put(partKey, partData)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &CreateTopicResult{
		Topic:      topic,
		Partitions: partitions,
	}, nil
}

// GetTopicByID retrieves topic metadata by topic ID.
// This performs a linear scan of all topics since we don't have an index by ID.
func (s *Store) GetTopicByID(ctx context.Context, topicID string) (*TopicMeta, error) {
	topics, err := s.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	for i := range topics {
		if topics[i].TopicID == topicID {
			return &topics[i], nil
		}
	}
	return nil, ErrTopicNotFound
}

// TopicExists checks if a topic exists.
func (s *Store) TopicExists(ctx context.Context, name string) (bool, error) {
	key := keys.TopicKeyPath(name)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return result.Exists, nil
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
