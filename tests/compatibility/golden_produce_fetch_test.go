// Package compatibility provides golden tests for produce/fetch operations.
// Golden tests compare Dray responses against expected Kafka broker behavior,
// validating that response fields match Kafka protocol expectations.
package compatibility

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestGolden_ProduceResponse_SingleRecord validates produce response fields for single record.
func TestGolden_ProduceResponse_SingleRecord(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-produce-single"

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

	record := &kgo.Record{
		Topic: topicName,
		Key:   []byte("golden-key"),
		Value: []byte("golden-value"),
	}

	result := client.ProduceSync(ctx, record)

	// Golden expectations: Kafka-compatible produce response
	if result.FirstErr() != nil {
		t.Fatalf("produce failed: %v", result.FirstErr())
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}

	r := result[0]

	// Golden check: Error should be nil
	if r.Err != nil {
		t.Errorf("[GOLDEN] expected no error, got: %v", r.Err)
	}

	// Golden check: Offset should be valid (>= 0)
	if r.Record.Offset < 0 {
		t.Errorf("[GOLDEN] expected offset >= 0, got: %d", r.Record.Offset)
	}

	// Golden check: Partition should match
	if r.Record.Partition != 0 {
		t.Errorf("[GOLDEN] expected partition 0, got: %d", r.Record.Partition)
	}

	// Golden check: Topic should match
	if r.Record.Topic != topicName {
		t.Errorf("[GOLDEN] expected topic %q, got: %q", topicName, r.Record.Topic)
	}

	t.Logf("[GOLDEN] Single record produce: offset=%d, partition=%d", r.Record.Offset, r.Record.Partition)
}

// TestGolden_ProduceResponse_Batch validates produce response fields for batch produce.
func TestGolden_ProduceResponse_Batch(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-produce-batch"

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

	batchSize := 5
	var records []*kgo.Record
	for i := 0; i < batchSize; i++ {
		records = append(records, &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("batch-key-%d", i)),
			Value: []byte(fmt.Sprintf("batch-value-%d", i)),
		})
	}

	results := client.ProduceSync(ctx, records...)

	// Golden expectations
	if results.FirstErr() != nil {
		t.Fatalf("batch produce failed: %v", results.FirstErr())
	}

	if len(results) != batchSize {
		t.Fatalf("expected %d results, got %d", batchSize, len(results))
	}

	var offsets []int64
	for i, r := range results {
		// Golden check: Each record should succeed
		if r.Err != nil {
			t.Errorf("[GOLDEN] record %d: expected no error, got: %v", i, r.Err)
		}

		// Golden check: Offset should be valid
		if r.Record.Offset < 0 {
			t.Errorf("[GOLDEN] record %d: expected offset >= 0, got: %d", i, r.Record.Offset)
		}

		offsets = append(offsets, r.Record.Offset)
	}

	// Golden check: Offsets should be monotonically increasing
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Errorf("[GOLDEN] offsets not monotonic: offset[%d]=%d <= offset[%d]=%d",
				i, offsets[i], i-1, offsets[i-1])
		}
	}

	// Golden check: Offsets should be contiguous
	for i := 1; i < len(offsets); i++ {
		if offsets[i] != offsets[i-1]+1 {
			t.Errorf("[GOLDEN] offsets not contiguous: offset[%d]=%d, expected %d",
				i, offsets[i], offsets[i-1]+1)
		}
	}

	t.Logf("[GOLDEN] Batch produce: %d records with offsets %v", batchSize, offsets)
}

// TestGolden_ProduceResponse_MultiPartition validates produce across multiple partitions.
func TestGolden_ProduceResponse_MultiPartition(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-produce-multipart"
	numPartitions := int32(3)

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

	partitionOffsets := make(map[int32][]int64)

	for partition := int32(0); partition < numPartitions; partition++ {
		for i := 0; i < 3; i++ {
			record := &kgo.Record{
				Topic:     topicName,
				Partition: partition,
				Key:       []byte(fmt.Sprintf("p%d-key-%d", partition, i)),
				Value:     []byte(fmt.Sprintf("p%d-value-%d", partition, i)),
			}

			result := client.ProduceSync(ctx, record)
			if result.FirstErr() != nil {
				t.Fatalf("produce to partition %d failed: %v", partition, result.FirstErr())
			}

			for _, r := range result {
				// Golden check: Partition should match requested
				if r.Record.Partition != partition {
					t.Errorf("[GOLDEN] partition mismatch: expected %d, got %d", partition, r.Record.Partition)
				}

				partitionOffsets[partition] = append(partitionOffsets[partition], r.Record.Offset)
			}
		}
	}

	// Golden check: Each partition should have independent offset sequences starting at 0
	for partition, offsets := range partitionOffsets {
		// First offset should be 0
		if offsets[0] != 0 {
			t.Errorf("[GOLDEN] partition %d: first offset should be 0, got %d", partition, offsets[0])
		}

		// Offsets should be contiguous within partition
		for i := 1; i < len(offsets); i++ {
			if offsets[i] != offsets[i-1]+1 {
				t.Errorf("[GOLDEN] partition %d: offsets not contiguous at position %d", partition, i)
			}
		}
	}

	t.Logf("[GOLDEN] Multi-partition produce: %d partitions with offsets %v", numPartitions, partitionOffsets)
}

// TestGolden_FetchResponse_SingleRecord validates fetch response fields for single record.
func TestGolden_FetchResponse_SingleRecord(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-fetch-single"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	originalKey := []byte("fetch-key")
	originalValue := []byte("fetch-value")

	result := producer.ProduceSync(ctx, &kgo.Record{
		Topic: topicName,
		Key:   originalKey,
		Value: originalValue,
	})
	if result.FirstErr() != nil {
		t.Fatalf("produce failed: %v", result.FirstErr())
	}
	producedOffset := result[0].Record.Offset
	producer.Close()

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

	var fetchedRecords []*kgo.Record
	for len(fetchedRecords) < 1 {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecords = append(fetchedRecords, r)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if len(fetchedRecords) != 1 {
		t.Fatalf("[GOLDEN] expected 1 record, got %d", len(fetchedRecords))
	}

	r := fetchedRecords[0]

	// Golden check: Offset should match produced offset
	if r.Offset != producedOffset {
		t.Errorf("[GOLDEN] offset mismatch: expected %d, got %d", producedOffset, r.Offset)
	}

	// Golden check: Key should match
	if string(r.Key) != string(originalKey) {
		t.Errorf("[GOLDEN] key mismatch: expected %q, got %q", originalKey, r.Key)
	}

	// Golden check: Value should match
	if string(r.Value) != string(originalValue) {
		t.Errorf("[GOLDEN] value mismatch: expected %q, got %q", originalValue, r.Value)
	}

	// Golden check: Partition should be 0
	if r.Partition != 0 {
		t.Errorf("[GOLDEN] partition mismatch: expected 0, got %d", r.Partition)
	}

	// Golden check: Topic should match
	if r.Topic != topicName {
		t.Errorf("[GOLDEN] topic mismatch: expected %q, got %q", topicName, r.Topic)
	}

	t.Logf("[GOLDEN] Single record fetch: offset=%d, key=%q", r.Offset, r.Key)
}

// TestGolden_FetchResponse_OffsetSequence validates fetched record offsets match produced.
func TestGolden_FetchResponse_OffsetSequence(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-fetch-sequence"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	numRecords := 10
	var producedOffsets []int64

	for i := 0; i < numRecords; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("sequence-value-%d", i)),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce %d failed: %v", i, result.FirstErr())
		}
		producedOffsets = append(producedOffsets, result[0].Record.Offset)
	}
	producer.Close()

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
	for len(fetchedOffsets) < numRecords {
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

	if len(fetchedOffsets) != numRecords {
		t.Fatalf("[GOLDEN] expected %d records, got %d", numRecords, len(fetchedOffsets))
	}

	// Golden check: Fetched offsets should exactly match produced offsets
	for i, expected := range producedOffsets {
		if fetchedOffsets[i] != expected {
			t.Errorf("[GOLDEN] offset %d mismatch: expected %d, got %d", i, expected, fetchedOffsets[i])
		}
	}

	// Golden check: Offsets should be strictly increasing
	for i := 1; i < len(fetchedOffsets); i++ {
		if fetchedOffsets[i] <= fetchedOffsets[i-1] {
			t.Errorf("[GOLDEN] offsets not strictly increasing: %d <= %d at position %d",
				fetchedOffsets[i], fetchedOffsets[i-1], i)
		}
	}

	t.Logf("[GOLDEN] Offset sequence: produced=%v, fetched=%v", producedOffsets, fetchedOffsets)
}

// TestGolden_FetchResponse_FromOffset validates fetch from specific offset.
func TestGolden_FetchResponse_FromOffset(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-fetch-offset"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	numRecords := 10
	for i := 0; i < numRecords; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("offset-value-%d", i)),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
	}
	producer.Close()

	// Fetch from offset 5 - should get records 5, 6, 7, 8, 9
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

	expectedCount := numRecords - int(startOffset)
	var fetchedOffsets []int64

	for len(fetchedOffsets) < expectedCount {
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

	if len(fetchedOffsets) != expectedCount {
		t.Fatalf("[GOLDEN] expected %d records, got %d", expectedCount, len(fetchedOffsets))
	}

	// Golden check: First fetched offset should equal start offset
	if fetchedOffsets[0] != startOffset {
		t.Errorf("[GOLDEN] first offset mismatch: expected %d, got %d", startOffset, fetchedOffsets[0])
	}

	// Golden check: All offsets should be >= startOffset
	for i, offset := range fetchedOffsets {
		if offset < startOffset {
			t.Errorf("[GOLDEN] offset %d at position %d is < startOffset %d", offset, i, startOffset)
		}
	}

	t.Logf("[GOLDEN] Fetch from offset %d: got offsets %v", startOffset, fetchedOffsets)
}

// TestGolden_FetchResponse_EmptyPartition validates fetch from empty partition.
func TestGolden_FetchResponse_EmptyPartition(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-fetch-empty"

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

	// Golden check: Empty partition should return no records
	if fetchedCount != 0 {
		t.Errorf("[GOLDEN] expected 0 records from empty partition, got %d", fetchedCount)
	}

	// Golden check: Should not return errors for empty partition
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, e := range errs {
			// Timeout is expected, other errors are not
			if e.Err != context.DeadlineExceeded {
				t.Errorf("[GOLDEN] unexpected error on empty partition: %v", e.Err)
			}
		}
	}

	t.Log("[GOLDEN] Empty partition fetch: correctly returned no records")
}

// TestGolden_ProduceResponse_WithHeaders validates produce with headers.
func TestGolden_ProduceResponse_WithHeaders(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-produce-headers"

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

	headers := []kgo.RecordHeader{
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "correlation-id", Value: []byte(uuid.New().String())},
		{Key: "empty-header", Value: []byte{}},
	}

	record := &kgo.Record{
		Topic:   topicName,
		Key:     []byte("header-key"),
		Value:   []byte(`{"message":"test"}`),
		Headers: headers,
	}

	result := client.ProduceSync(ctx, record)
	if result.FirstErr() != nil {
		t.Fatalf("produce failed: %v", result.FirstErr())
	}

	// Golden check: Produce should succeed with headers
	if len(result) != 1 || result[0].Err != nil {
		t.Errorf("[GOLDEN] produce with headers should succeed")
	}

	producedOffset := result[0].Record.Offset

	// Consume and verify headers are preserved
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

	var fetchedRecord *kgo.Record
	for fetchedRecord == nil {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecord = r
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if fetchedRecord == nil {
		t.Fatal("[GOLDEN] failed to fetch record with headers")
	}

	// Golden check: Headers should be preserved
	if len(fetchedRecord.Headers) != len(headers) {
		t.Errorf("[GOLDEN] header count mismatch: expected %d, got %d", len(headers), len(fetchedRecord.Headers))
	}

	for i, expected := range headers {
		if i >= len(fetchedRecord.Headers) {
			break
		}
		got := fetchedRecord.Headers[i]
		if got.Key != expected.Key {
			t.Errorf("[GOLDEN] header %d key mismatch: expected %q, got %q", i, expected.Key, got.Key)
		}
		if string(got.Value) != string(expected.Value) {
			t.Errorf("[GOLDEN] header %d value mismatch: expected %q, got %q", i, expected.Value, got.Value)
		}
	}

	t.Logf("[GOLDEN] Produce/fetch with headers: offset=%d, headers=%d", producedOffset, len(fetchedRecord.Headers))
}

// TestGolden_ProduceResponse_Timestamps validates timestamp handling.
func TestGolden_ProduceResponse_Timestamps(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-produce-timestamp"

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

	// Produce with client-side timestamp
	now := time.Now()
	record := &kgo.Record{
		Topic:     topicName,
		Key:       []byte("timestamp-key"),
		Value:     []byte("timestamp-value"),
		Timestamp: now,
	}

	result := client.ProduceSync(ctx, record)
	if result.FirstErr() != nil {
		t.Fatalf("produce failed: %v", result.FirstErr())
	}

	producedOffset := result[0].Record.Offset

	// Consume and verify timestamp
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

	var fetchedRecord *kgo.Record
	for fetchedRecord == nil {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecord = r
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if fetchedRecord == nil {
		t.Fatal("[GOLDEN] failed to fetch record with timestamp")
	}

	// Golden check: Timestamp should be preserved (within tolerance)
	timeDiff := fetchedRecord.Timestamp.Sub(now).Abs()
	if timeDiff > time.Second {
		t.Errorf("[GOLDEN] timestamp drift: expected ~%v, got %v (diff: %v)",
			now.Unix(), fetchedRecord.Timestamp.Unix(), timeDiff)
	}

	t.Logf("[GOLDEN] Timestamp test: offset=%d, timestamp=%v", producedOffset, fetchedRecord.Timestamp)
}

// TestGolden_FetchResponse_RecordBatchFields validates record batch fields in fetch response.
func TestGolden_FetchResponse_RecordBatchFields(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-fetch-batch-fields"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// Produce multiple records to create a batch
	numRecords := 5
	for i := 0; i < numRecords; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("batch-field-key-%d", i)),
			Value: []byte(fmt.Sprintf("batch-field-value-%d", i)),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce %d failed: %v", i, result.FirstErr())
		}
	}
	producer.Close()

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

	var fetchedRecords []*kgo.Record
	for len(fetchedRecords) < numRecords {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecords = append(fetchedRecords, r)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if len(fetchedRecords) != numRecords {
		t.Fatalf("[GOLDEN] expected %d records, got %d", numRecords, len(fetchedRecords))
	}

	// Golden check: Verify each record has valid fields
	for i, r := range fetchedRecords {
		// Offset should be correct sequence
		if r.Offset != int64(i) {
			t.Errorf("[GOLDEN] record %d: offset mismatch, expected %d, got %d", i, i, r.Offset)
		}

		// Key should match expected
		expectedKey := fmt.Sprintf("batch-field-key-%d", i)
		if string(r.Key) != expectedKey {
			t.Errorf("[GOLDEN] record %d: key mismatch, expected %q, got %q", i, expectedKey, r.Key)
		}

		// Value should match expected
		expectedValue := fmt.Sprintf("batch-field-value-%d", i)
		if string(r.Value) != expectedValue {
			t.Errorf("[GOLDEN] record %d: value mismatch, expected %q, got %q", i, expectedValue, r.Value)
		}

		// Timestamp should be valid (non-zero)
		if r.Timestamp.IsZero() {
			t.Errorf("[GOLDEN] record %d: timestamp should not be zero", i)
		}
	}

	t.Logf("[GOLDEN] Batch field verification: %d records validated", len(fetchedRecords))
}

// TestGolden_ProduceResponse_LargePayload validates produce/fetch with large payload.
func TestGolden_ProduceResponse_LargePayload(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-large-payload"

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

	// Create 50KB payload
	payloadSize := 50 * 1024
	largeValue := make([]byte, payloadSize)
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	record := &kgo.Record{
		Topic: topicName,
		Key:   []byte("large-payload-key"),
		Value: largeValue,
	}

	result := client.ProduceSync(ctx, record)
	if result.FirstErr() != nil {
		t.Fatalf("produce of large payload failed: %v", result.FirstErr())
	}

	// Golden check: Large payload produce should succeed
	if len(result) != 1 {
		t.Fatalf("[GOLDEN] expected 1 result, got %d", len(result))
	}

	producedOffset := result[0].Record.Offset

	// Consume and verify
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

	// Golden check: Payload size should match
	if len(fetchedValue) != payloadSize {
		t.Errorf("[GOLDEN] payload size mismatch: expected %d, got %d", payloadSize, len(fetchedValue))
	}

	// Golden check: Payload content should match
	for i := 0; i < len(largeValue); i++ {
		if fetchedValue[i] != largeValue[i] {
			t.Errorf("[GOLDEN] payload byte mismatch at position %d", i)
			break
		}
	}

	t.Logf("[GOLDEN] Large payload: offset=%d, size=%d bytes", producedOffset, len(fetchedValue))
}

// TestGolden_ProduceResponse_NullKey validates null key handling.
func TestGolden_ProduceResponse_NullKey(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-null-key"

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

	record := &kgo.Record{
		Topic: topicName,
		Key:   nil, // Null key
		Value: []byte("value-with-null-key"),
	}

	result := client.ProduceSync(ctx, record)
	if result.FirstErr() != nil {
		t.Fatalf("produce with null key failed: %v", result.FirstErr())
	}

	// Golden check: Produce with null key should succeed
	if len(result) != 1 || result[0].Err != nil {
		t.Errorf("[GOLDEN] produce with null key should succeed")
	}

	// Consume and verify null key preserved
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

	var fetchedRecord *kgo.Record
	for fetchedRecord == nil {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecord = r
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if fetchedRecord == nil {
		t.Fatal("[GOLDEN] failed to fetch record with null key")
	}

	// Golden check: Null key should be preserved (nil or empty)
	if len(fetchedRecord.Key) != 0 {
		t.Errorf("[GOLDEN] null key should be preserved as empty, got: %v", fetchedRecord.Key)
	}

	t.Logf("[GOLDEN] Null key test: offset=%d, key=%v", fetchedRecord.Offset, fetchedRecord.Key)
}

// TestGolden_FetchResponse_MultiPartition validates fetch across multiple partitions.
func TestGolden_FetchResponse_MultiPartition(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-fetch-multipart"
	numPartitions := int32(3)

	if err := suite.Broker().CreateTopic(ctx, topicName, numPartitions); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	recordsPerPartition := 3
	for partition := int32(0); partition < numPartitions; partition++ {
		for i := 0; i < recordsPerPartition; i++ {
			result := producer.ProduceSync(ctx, &kgo.Record{
				Topic:     topicName,
				Partition: partition,
				Key:       []byte(fmt.Sprintf("p%d-key-%d", partition, i)),
				Value:     []byte(fmt.Sprintf("p%d-value-%d", partition, i)),
			})
			if result.FirstErr() != nil {
				t.Fatalf("produce to partition %d failed: %v", partition, result.FirstErr())
			}
		}
	}
	producer.Close()

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

	totalExpected := int(numPartitions) * recordsPerPartition
	partitionRecords := make(map[int32][]*kgo.Record)

	for len(partitionRecords[0])+len(partitionRecords[1])+len(partitionRecords[2]) < totalExpected {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			partitionRecords[r.Partition] = append(partitionRecords[r.Partition], r)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	// Golden check: Each partition should have the correct number of records
	for partition := int32(0); partition < numPartitions; partition++ {
		records := partitionRecords[partition]
		if len(records) != recordsPerPartition {
			t.Errorf("[GOLDEN] partition %d: expected %d records, got %d",
				partition, recordsPerPartition, len(records))
		}

		// Golden check: Offsets within partition should be sequential
		for i, r := range records {
			if r.Offset != int64(i) {
				t.Errorf("[GOLDEN] partition %d: record %d has offset %d, expected %d",
					partition, i, r.Offset, i)
			}
		}
	}

	t.Logf("[GOLDEN] Multi-partition fetch: %d partitions, %d records each", numPartitions, recordsPerPartition)
}

// TestGolden_ProduceFetch_ReadYourWrites validates read-your-writes consistency.
func TestGolden_ProduceFetch_ReadYourWrites(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-read-your-writes"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create a single client for both produce and consume
	client, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ConsumeTopics(topicName),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Produce a record
	uniqueValue := fmt.Sprintf("read-your-writes-%d", time.Now().UnixNano())
	result := client.ProduceSync(ctx, &kgo.Record{
		Topic: topicName,
		Value: []byte(uniqueValue),
	})
	if result.FirstErr() != nil {
		t.Fatalf("produce failed: %v", result.FirstErr())
	}

	producedOffset := result[0].Record.Offset

	// Immediately fetch and verify the record is visible
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topicName: {0: kgo.NewOffset().At(producedOffset)},
	})

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var fetchedValue string
	for fetchedValue == "" {
		fetches := client.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedValue = string(r.Value)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	// Golden check: Just-written record should be immediately readable
	if fetchedValue != uniqueValue {
		t.Errorf("[GOLDEN] read-your-writes violation: expected %q, got %q", uniqueValue, fetchedValue)
	}

	t.Logf("[GOLDEN] Read-your-writes: offset=%d, value=%q", producedOffset, fetchedValue)
}

// TestGolden_ProduceResponse_BaseOffsetInResponse validates base offset field in produce response.
func TestGolden_ProduceResponse_BaseOffsetInResponse(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-base-offset"

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

	// First produce - should get base offset 0
	result1 := client.ProduceSync(ctx, &kgo.Record{
		Topic: topicName,
		Value: []byte("first"),
	})
	if result1.FirstErr() != nil {
		t.Fatalf("first produce failed: %v", result1.FirstErr())
	}

	// Golden check: First produce base offset should be 0
	if result1[0].Record.Offset != 0 {
		t.Errorf("[GOLDEN] first produce: expected base offset 0, got %d", result1[0].Record.Offset)
	}

	// Second produce - should get base offset 1
	result2 := client.ProduceSync(ctx, &kgo.Record{
		Topic: topicName,
		Value: []byte("second"),
	})
	if result2.FirstErr() != nil {
		t.Fatalf("second produce failed: %v", result2.FirstErr())
	}

	// Golden check: Second produce base offset should be 1
	if result2[0].Record.Offset != 1 {
		t.Errorf("[GOLDEN] second produce: expected base offset 1, got %d", result2[0].Record.Offset)
	}

	// Batch produce - should get base offset 2 for first record
	batchResults := client.ProduceSync(ctx,
		&kgo.Record{Topic: topicName, Value: []byte("third")},
		&kgo.Record{Topic: topicName, Value: []byte("fourth")},
	)
	if batchResults.FirstErr() != nil {
		t.Fatalf("batch produce failed: %v", batchResults.FirstErr())
	}

	// Golden check: Batch offsets should continue sequence
	if batchResults[0].Record.Offset != 2 {
		t.Errorf("[GOLDEN] batch first record: expected offset 2, got %d", batchResults[0].Record.Offset)
	}
	if batchResults[1].Record.Offset != 3 {
		t.Errorf("[GOLDEN] batch second record: expected offset 3, got %d", batchResults[1].Record.Offset)
	}

	t.Logf("[GOLDEN] Base offset sequence: 0, 1, 2, 3 verified")
}

// TestGolden_FetchResponse_HighWatermark validates high watermark in fetch response.
func TestGolden_FetchResponse_HighWatermark(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-hwm"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	// Produce 5 records (HWM should be 5)
	for i := 0; i < 5; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("hwm-value-%d", i)),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce %d failed: %v", i, result.FirstErr())
		}
	}
	producer.Close()

	// Use low-level fetch request to check HWM
	// Since franz-go abstracts this, we verify indirectly via offset behavior
	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), // Start at end
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Produce more records
	producer2, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create second producer: %v", err)
	}

	result := producer2.ProduceSync(ctx, &kgo.Record{
		Topic: topicName,
		Value: []byte("hwm-after-consumer-started"),
	})
	producer2.Close()

	if result.FirstErr() != nil {
		t.Fatalf("produce after consumer started failed: %v", result.FirstErr())
	}

	// Consumer should see the new record (starting from end at offset 5)
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var lastOffset int64 = -1
	for lastOffset < 5 {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			lastOffset = r.Offset
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	// Golden check: Consumer starting at end should have gotten the 6th record (offset 5)
	if lastOffset != 5 {
		t.Errorf("[GOLDEN] expected last offset 5, got %d", lastOffset)
	}

	t.Logf("[GOLDEN] High watermark verified: consumer at end received offset %d", lastOffset)
}
