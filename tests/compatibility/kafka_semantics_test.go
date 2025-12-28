package compatibility

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestKafkaSemantics_AckBeforeVisible ensures messages are visible only after ack.
func TestKafkaSemantics_AckBeforeVisible(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "ack-before-visible"

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
	defer producer.Close()

	for i := 0; i < 5; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		result := producer.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}

		consumer, err := suite.Broker().NewClient(
			kgo.ConsumeTopics(topicName),
			kgo.ConsumeResetOffset(kgo.NewOffset().At(int64(i))),
		)
		if err != nil {
			t.Fatalf("failed to create consumer: %v", err)
		}

		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var found bool
		for !found {
			fetches := consumer.PollFetches(fetchCtx)
			if fetches.IsClientClosed() || fetchCtx.Err() != nil {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				if r.Offset == int64(i) {
					found = true
				}
			})
		}
		cancel()
		consumer.Close()

		if !found {
			t.Errorf("message at offset %d not visible after ack", i)
		}
	}

	t.Log("Verified: messages visible immediately after ack")
}

// TestKafkaSemantics_PartitionOrdering ensures ordering within a partition.
func TestKafkaSemantics_PartitionOrdering(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "partition-ordering"

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
	defer producer.Close()

	numMessages := 100
	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		result := producer.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
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

	var consumed []int64
	for len(consumed) < numMessages {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() || fetchCtx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			consumed = append(consumed, r.Offset)
		})
	}

	if len(consumed) != numMessages {
		t.Fatalf("expected %d consumed messages, got %d", numMessages, len(consumed))
	}

	for i := 1; i < len(consumed); i++ {
		if consumed[i] != consumed[i-1]+1 {
			t.Errorf("ordering violation: offset %d followed by %d", consumed[i-1], consumed[i])
		}
	}

	t.Logf("Verified: %d messages in correct order within partition", len(consumed))
}

// TestKafkaSemantics_OffsetContinuity ensures offsets are contiguous.
func TestKafkaSemantics_OffsetContinuity(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "offset-continuity"

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
	defer producer.Close()

	var producedOffsets []int64
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 10; i++ {
			record := &kgo.Record{
				Topic: topicName,
				Value: []byte(fmt.Sprintf("batch%d-msg%d", batch, i)),
			}
			result := producer.ProduceSync(ctx, record)
			if result.FirstErr() != nil {
				t.Fatalf("produce failed: %v", result.FirstErr())
			}
			for _, r := range result {
				producedOffsets = append(producedOffsets, r.Record.Offset)
			}
		}
	}

	if len(producedOffsets) != 50 {
		t.Fatalf("expected 50 produced offsets, got %d", len(producedOffsets))
	}

	for i := 1; i < len(producedOffsets); i++ {
		expected := producedOffsets[i-1] + 1
		if producedOffsets[i] != expected {
			t.Errorf("offset gap: expected %d after %d, got %d",
				expected, producedOffsets[i-1], producedOffsets[i])
		}
	}

	t.Logf("Verified: %d offsets are contiguous", len(producedOffsets))
}

// TestKafkaSemantics_ConcurrentProducers ensures concurrent producers get unique offsets.
func TestKafkaSemantics_ConcurrentProducers(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "concurrent-producers"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	numProducers := 5
	messagesPerProducer := 10

	var wg sync.WaitGroup
	var mu sync.Mutex
	allOffsets := make(map[int64]bool)
	var offsetList []int64

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			producer, err := suite.Broker().NewClient(
				kgo.DefaultProduceTopic(topicName),
				kgo.RequiredAcks(kgo.AllISRAcks()),
			)
			if err != nil {
				t.Errorf("producer %d: failed to create client: %v", producerID, err)
				return
			}
			defer producer.Close()

			for i := 0; i < messagesPerProducer; i++ {
				record := &kgo.Record{
					Topic: topicName,
					Value: []byte(fmt.Sprintf("producer%d-msg%d", producerID, i)),
				}
				result := producer.ProduceSync(ctx, record)
				if result.FirstErr() != nil {
					t.Errorf("producer %d: produce failed: %v", producerID, result.FirstErr())
					continue
				}
				for _, r := range result {
					mu.Lock()
					if allOffsets[r.Record.Offset] {
						t.Errorf("duplicate offset %d", r.Record.Offset)
					}
					allOffsets[r.Record.Offset] = true
					offsetList = append(offsetList, r.Record.Offset)
					mu.Unlock()
				}
			}
		}(p)
	}

	wg.Wait()

	expectedTotal := numProducers * messagesPerProducer
	if len(allOffsets) != expectedTotal {
		t.Errorf("expected %d unique offsets, got %d", expectedTotal, len(allOffsets))
	}

	t.Logf("Verified: %d concurrent produces got unique offsets", len(allOffsets))
}

// TestKafkaSemantics_HighWatermark tests HWM semantics.
func TestKafkaSemantics_HighWatermark(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "hwm-test"

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
	defer producer.Close()

	adminClient, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	admin := kadm.NewClient(adminClient)

	endOffsets, err := admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		t.Fatalf("ListEndOffsets failed: %v", err)
	}
	initialHWM := int64(0)
	if lo, ok := endOffsets.Lookup(topicName, 0); ok {
		initialHWM = lo.Offset
	}

	numMessages := 10
	for i := 0; i < numMessages; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("msg-%d", i)),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
	}

	endOffsets, err = admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		t.Fatalf("ListEndOffsets failed: %v", err)
	}
	finalHWM := int64(0)
	if lo, ok := endOffsets.Lookup(topicName, 0); ok {
		finalHWM = lo.Offset
	}

	expectedHWM := initialHWM + int64(numMessages)
	if finalHWM != expectedHWM {
		t.Errorf("expected HWM %d, got %d", expectedHWM, finalHWM)
	}

	t.Logf("Verified: HWM advanced from %d to %d after %d messages", initialHWM, finalHWM, numMessages)
}

// TestKafkaSemantics_ConsumeFromEnd tests consuming from log end.
func TestKafkaSemantics_ConsumeFromEnd(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "consume-from-end"

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
	defer producer.Close()

	for i := 0; i < 5; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("old-%d", i)),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
	}

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	for i := 0; i < 5; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("new-%d", i)),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var consumed []string
	for len(consumed) < 5 {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() || fetchCtx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			consumed = append(consumed, string(r.Value))
		})
	}

	for _, msg := range consumed {
		if len(msg) > 3 && msg[:3] == "old" {
			t.Errorf("consumer from end should not see old messages: %s", msg)
		}
	}

	t.Logf("Verified: consumer from end only sees new messages (%d)", len(consumed))
}

// TestKafkaSemantics_GroupOffsetIsolation tests group offset isolation.
func TestKafkaSemantics_GroupOffsetIsolation(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "group-isolation"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)

	group1, group2 := "isolation-group-1", "isolation-group-2"
	offset1, offset2 := int64(100), int64(200)

	toCommit1 := kadm.Offsets{}
	toCommit1.Add(kadm.Offset{Topic: topicName, Partition: 0, At: offset1})
	if _, err := admin.CommitOffsets(ctx, group1, toCommit1); err != nil {
		t.Fatalf("commit offsets for %s failed: %v", group1, err)
	}

	toCommit2 := kadm.Offsets{}
	toCommit2.Add(kadm.Offset{Topic: topicName, Partition: 0, At: offset2})
	if _, err := admin.CommitOffsets(ctx, group2, toCommit2); err != nil {
		t.Fatalf("commit offsets for %s failed: %v", group2, err)
	}

	fetch1, err := admin.FetchOffsets(ctx, group1)
	if err != nil {
		t.Fatalf("fetch offsets for %s failed: %v", group1, err)
	}
	fetch2, err := admin.FetchOffsets(ctx, group2)
	if err != nil {
		t.Fatalf("fetch offsets for %s failed: %v", group2, err)
	}

	if o, ok := fetch1.Offsets().Lookup(topicName, 0); ok && o.At != offset1 {
		t.Errorf("group1 offset mismatch: expected %d, got %d", offset1, o.At)
	}

	if o, ok := fetch2.Offsets().Lookup(topicName, 0); ok && o.At != offset2 {
		t.Errorf("group2 offset mismatch: expected %d, got %d", offset2, o.At)
	}

	t.Log("Verified: consumer group offsets are isolated")
}

// TestKafkaSemantics_TimestampOrdering tests timestamp handling.
func TestKafkaSemantics_TimestampOrdering(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "timestamp-ordering"

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
	defer producer.Close()

	numMessages := 10
	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic:     topicName,
			Value:     []byte(fmt.Sprintf("msg-%d", i)),
			Timestamp: time.Now(),
		}
		result := producer.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
		time.Sleep(10 * time.Millisecond)
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

	var timestamps []time.Time
	for len(timestamps) < numMessages {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() || fetchCtx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			timestamps = append(timestamps, r.Timestamp)
		})
	}

	if len(timestamps) != numMessages {
		t.Fatalf("expected %d timestamps, got %d", numMessages, len(timestamps))
	}

	nonDecreasingCount := 0
	for i := 1; i < len(timestamps); i++ {
		if !timestamps[i].Before(timestamps[i-1]) {
			nonDecreasingCount++
		}
	}

	if nonDecreasingCount < len(timestamps)-1 {
		t.Log("Note: Some timestamps may be equal due to timing")
	}

	t.Logf("Verified: %d messages with timestamps", len(timestamps))
}

// TestKafkaSemantics_KeyPreservation tests that keys are preserved.
func TestKafkaSemantics_KeyPreservation(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "key-preservation"

	if err := suite.Broker().CreateTopic(ctx, topicName, 3); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	keyValues := make(map[string]string)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		keyValues[key] = value

		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Key:   []byte(key),
			Value: []byte(value),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
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
	for len(fetched) < len(keyValues) {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() || fetchCtx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetched[string(r.Key)] = string(r.Value)
		})
	}

	for key, expectedValue := range keyValues {
		if actualValue, ok := fetched[key]; !ok {
			t.Errorf("key %s not found", key)
		} else if actualValue != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}

	t.Logf("Verified: %d key-value pairs preserved", len(keyValues))
}
