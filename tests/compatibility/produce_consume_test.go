package compatibility

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestProduceAndConsume_BasicFlow tests basic produce and consume operations.
func TestProduceAndConsume_BasicFlow(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "produce-consume-basic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	messages := []string{
		"Hello, World!",
		"This is a test message",
		"Kafka compatibility test",
	}

	for i, msg := range messages {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Errorf("produce failed for message %d: %v", i, result.FirstErr())
		}
	}

	t.Logf("Produced %d messages successfully", len(messages))

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var fetched []string
	for len(fetched) < len(messages) {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetched = append(fetched, string(r.Value))
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if len(fetched) != len(messages) {
		t.Errorf("expected %d messages, got %d", len(messages), len(fetched))
	}

	for i, msg := range messages {
		if i < len(fetched) && fetched[i] != msg {
			t.Errorf("message %d mismatch: expected %q, got %q", i, msg, fetched[i])
		}
	}

	t.Logf("Consumed %d messages successfully", len(fetched))
}

// TestProduceAndConsume_OffsetMonotonicity verifies offsets are strictly increasing.
func TestProduceAndConsume_OffsetMonotonicity(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "offset-monotonicity"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	numMessages := 20
	var producedOffsets []int64

	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
		for _, r := range result {
			producedOffsets = append(producedOffsets, r.Record.Offset)
		}
	}

	for i := 1; i < len(producedOffsets); i++ {
		if producedOffsets[i] <= producedOffsets[i-1] {
			t.Errorf("produced offsets not monotonic: offset[%d]=%d <= offset[%d]=%d",
				i, producedOffsets[i], i-1, producedOffsets[i-1])
		}
	}

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var fetchedOffsets []int64
	for len(fetchedOffsets) < numMessages {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedOffsets = append(fetchedOffsets, r.Offset)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	for i := 1; i < len(fetchedOffsets); i++ {
		if fetchedOffsets[i] <= fetchedOffsets[i-1] {
			t.Errorf("fetched offsets not monotonic: offset[%d]=%d <= offset[%d]=%d",
				i, fetchedOffsets[i], i-1, fetchedOffsets[i-1])
		}
	}

	t.Logf("Verified %d offsets are monotonically increasing", len(fetchedOffsets))
}

// TestProduceAndConsume_MultiplePartitions tests produce/consume with multiple partitions.
func TestProduceAndConsume_MultiplePartitions(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "multi-partition-test"
	numPartitions := int32(4)

	if err := suite.Broker().CreateTopic(ctx, topicName, numPartitions); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	messagesPerPartition := 5
	for partition := int32(0); partition < numPartitions; partition++ {
		for i := 0; i < messagesPerPartition; i++ {
			record := &kgo.Record{
				Topic:     topicName,
				Partition: partition,
				Key:       []byte(fmt.Sprintf("p%d-key-%d", partition, i)),
				Value:     []byte(fmt.Sprintf("partition-%d-msg-%d", partition, i)),
			}
			result := client.ProduceSync(ctx, record)
			if result.FirstErr() != nil {
				t.Errorf("produce to partition %d failed: %v", partition, result.FirstErr())
			}
		}
	}

	t.Logf("Produced %d messages to %d partitions", int(numPartitions)*messagesPerPartition, numPartitions)

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	partitionCounts := make(map[int32]int)
	totalExpected := int(numPartitions) * messagesPerPartition

	for sum := 0; sum < totalExpected; {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			partitionCounts[r.Partition]++
			sum++
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	for partition := int32(0); partition < numPartitions; partition++ {
		if partitionCounts[partition] != messagesPerPartition {
			t.Errorf("partition %d: expected %d messages, got %d",
				partition, messagesPerPartition, partitionCounts[partition])
		}
	}

	t.Logf("Consumed messages from %d partitions successfully", len(partitionCounts))
}

// TestProduceAndConsume_ContentIntegrity verifies message content is preserved.
func TestProduceAndConsume_ContentIntegrity(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "content-integrity"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	testCases := []struct {
		key   string
		value string
	}{
		{"key-normal", "Normal message content"},
		{"key-empty-value", ""},
		{"", "Empty key message"},
		{"key-special", "Special chars: !@#$%^&*()"},
		{"key-numbers", "12345678901234567890"},
		{"key-spaces", "   leading and trailing spaces   "},
		{"key-unicode", "Unicode: 你好世界 🎉"},
	}

	for _, tc := range testCases {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(tc.key),
			Value: []byte(tc.value),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Errorf("produce failed for key=%q: %v", tc.key, result.FirstErr())
		}
	}

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fetched := make(map[string]string)
	for len(fetched) < len(testCases) {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetched[string(r.Key)] = string(r.Value)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	for _, tc := range testCases {
		if v, ok := fetched[tc.key]; !ok {
			t.Errorf("key %q not found in consumed messages", tc.key)
		} else if v != tc.value {
			t.Errorf("key %q: expected value %q, got %q", tc.key, tc.value, v)
		}
	}

	t.Logf("Verified content integrity for %d message types", len(testCases))
}

// TestProduceAndConsume_FetchFromOffset tests fetching from specific offsets.
func TestProduceAndConsume_FetchFromOffset(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "fetch-from-offset"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	numMessages := 10
	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
	}

	startOffset := int64(5)
	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(startOffset)),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var firstOffset int64 = -1
	var msgCount int

	for msgCount < numMessages-int(startOffset) {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if firstOffset == -1 {
				firstOffset = r.Offset
			}
			msgCount++
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if firstOffset != startOffset {
		t.Errorf("expected first fetched offset to be %d, got %d", startOffset, firstOffset)
	}

	expectedCount := numMessages - int(startOffset)
	if msgCount != expectedCount {
		t.Errorf("expected %d messages from offset %d, got %d", expectedCount, startOffset, msgCount)
	}

	t.Logf("Fetched %d messages starting from offset %d", msgCount, firstOffset)
}

// TestProduceAndConsume_BatchProduce tests producing multiple records in a batch.
func TestProduceAndConsume_BatchProduce(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "batch-produce"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	batchSize := 10
	var records []*kgo.Record
	for i := 0; i < batchSize; i++ {
		records = append(records, &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("batch-key-%d", i)),
			Value: []byte(fmt.Sprintf("batch-value-%d", i)),
		})
	}

	results := client.ProduceSync(ctx, records...)
	if results.FirstErr() != nil {
		t.Fatalf("batch produce failed: %v", results.FirstErr())
	}

	successCount := 0
	for _, r := range results {
		if r.Err == nil {
			successCount++
		}
	}

	if successCount != batchSize {
		t.Errorf("expected %d successful produces, got %d", batchSize, successCount)
	}

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var fetchedCount int
	for fetchedCount < batchSize {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedCount++
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if fetchedCount != batchSize {
		t.Errorf("expected %d messages, got %d", batchSize, fetchedCount)
	}

	t.Logf("Batch produced and consumed %d messages successfully", batchSize)
}

// TestProduceAndConsume_EmptyTopic tests consuming from an empty topic.
func TestProduceAndConsume_EmptyTopic(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "empty-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	var fetchedCount int
	fetches := consumer.PollFetches(fetchCtx)
	fetches.EachRecord(func(r *kgo.Record) {
		fetchedCount++
	})

	if fetchedCount != 0 {
		t.Errorf("expected 0 messages from empty topic, got %d", fetchedCount)
	}

	t.Log("Empty topic returns no messages as expected")
}

// TestProduceAndConsume_LargeMessage tests handling of large messages.
func TestProduceAndConsume_LargeMessage(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "large-message"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(2*1024*1024),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	largeValue := make([]byte, 100*1024)
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	record := &kgo.Record{
		Topic: topicName,
		Key:   []byte("large-key"),
		Value: largeValue,
	}

	result := client.ProduceSync(ctx, record)
	if result.FirstErr() != nil {
		t.Fatalf("produce of large message failed: %v", result.FirstErr())
	}

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(2*1024*1024),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var fetchedValue []byte
	for fetchedValue == nil {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedValue = r.Value
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if len(fetchedValue) != len(largeValue) {
		t.Errorf("expected %d bytes, got %d bytes", len(largeValue), len(fetchedValue))
	}

	for i := range largeValue {
		if fetchedValue[i] != largeValue[i] {
			t.Errorf("mismatch at byte %d: expected %c, got %c", i, largeValue[i], fetchedValue[i])
			break
		}
	}

	t.Logf("Large message (%d KB) produced and consumed successfully", len(largeValue)/1024)
}
