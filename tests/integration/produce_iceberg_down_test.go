package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
)

// TestProduceAndFetchWithIcebergDown verifies that produce and fetch operations
// remain available when the Iceberg catalog is unreachable. This is a critical
// requirement per SPEC.md section 11.2: catalog unavailability must not block
// produce/fetch operations.
func TestProduceAndFetchWithIcebergDown(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	topicName := "iceberg-down-test-topic"

	// Step 1: Configure duality mode by creating topic metadata
	// (In a real system, this would be via CreateTopics with Iceberg enabled)
	t.Log("Step 1: Creating topic with duality mode configured")
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 1,
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, topicName, 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	t.Logf("  Created topic %s with streamID %s", topicName, streamID)

	// Step 2: Make Iceberg catalog unreachable
	// We simulate this by creating a catalog that always returns ErrCatalogUnavailable
	t.Log("Step 2: Simulating Iceberg catalog being unreachable")
	unavailableCatalog := newUnavailableIcebergCatalog()

	// Verify the catalog is indeed unavailable
	_, err = unavailableCatalog.LoadTable(ctx, catalog.NewTableIdentifier([]string{"dray"}, topicName))
	if err != catalog.ErrCatalogUnavailable {
		t.Fatalf("expected ErrCatalogUnavailable, got: %v", err)
	}
	t.Log("  Iceberg catalog is confirmed unreachable")

	// Step 3: Produce records successfully
	// This should work because produce does NOT touch Iceberg - only WAL + Oxia
	t.Log("Step 3: Producing records with Iceberg catalog down")

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Produce 10 records
	recordCount := 10
	produceReq := buildCompactionTestProduceRequest(topicName, 0, recordCount, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)

	if len(produceResp.Topics) == 0 || len(produceResp.Topics[0].Partitions) == 0 {
		t.Fatal("produce response missing topic/partition data")
	}

	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", partResp.ErrorCode)
	}
	t.Logf("  Produced %d records successfully, BaseOffset=%d", recordCount, partResp.BaseOffset)

	// Step 4: Fetch records successfully
	// This should also work because fetch reads from WAL/Parquet in object storage
	t.Log("Step 4: Fetching records with Iceberg catalog down")

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	fetchedRecords := fetchAllRecords(t, fetchHandler, ctx, topicName, 0, 0)
	if len(fetchedRecords) != recordCount {
		t.Fatalf("expected %d records, got %d", recordCount, len(fetchedRecords))
	}
	t.Logf("  Fetched %d records successfully", len(fetchedRecords))

	// Verify record content
	for i, rec := range fetchedRecords {
		if rec.Offset != int64(i) {
			t.Errorf("record %d: expected offset %d, got %d", i, i, rec.Offset)
		}
	}
	t.Log("  Verified record content and offsets")

	// Step 5: Verify compaction fails gracefully
	// Compaction should not block produce/fetch even if Iceberg commit fails
	t.Log("Step 5: Verifying compaction fails gracefully when Iceberg is down")

	// Create the appender with our unavailable catalog
	appender := catalog.NewAppender(catalog.AppenderConfig{
		Catalog:   unavailableCatalog,
		Namespace: []string{"dray"},
	})

	// Attempt to append files to Iceberg (this should fail)
	_, err = appender.AppendFiles(ctx, topicName, []catalog.DataFile{
		{Path: "dummy/path.parquet"},
	}, nil)
	if err == nil {
		t.Fatal("expected Iceberg append to fail, but it succeeded")
	}
	if !errors.Is(err, catalog.ErrCatalogUnavailable) && err.Error() != "catalog unavailable" {
		t.Fatalf("unexpected Iceberg append error: %v", err)
	}
	t.Logf("  Iceberg append failed with expected error: %v", err)

	// Step 6: Verify produce and fetch still work after compaction failure
	t.Log("Step 6: Verifying produce/fetch still work after compaction failure")

	// Produce more records
	produceReq2 := buildCompactionTestProduceRequest(topicName, 0, 5, 1)
	produceResp2 := produceHandler.Handle(ctx, 9, produceReq2)
	partResp2 := produceResp2.Topics[0].Partitions[0]
	if partResp2.ErrorCode != 0 {
		t.Fatalf("second produce failed with error code %d", partResp2.ErrorCode)
	}
	t.Logf("  Produced 5 more records, BaseOffset=%d", partResp2.BaseOffset)

	// Fetch all records
	allRecords := fetchAllRecords(t, fetchHandler, ctx, topicName, 0, 0)
	expectedTotal := recordCount + 5
	if len(allRecords) != expectedTotal {
		t.Fatalf("expected %d total records, got %d", expectedTotal, len(allRecords))
	}
	t.Logf("  Fetched all %d records successfully", len(allRecords))

	t.Log("All verifications passed - produce/fetch remain available when Iceberg is down")
}

// TestTopicCreationWithIcebergDown verifies that topic creation succeeds
// even when the Iceberg catalog is unreachable.
func TestTopicCreationWithIcebergDown(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	ctx := context.Background()

	// Create unavailable Iceberg catalog
	unavailableCatalog := newUnavailableIcebergCatalog()

	// Create table creator that will fail on Iceberg operations
	tableCreator := catalog.NewTableCreator(catalog.TableCreatorConfig{
		Catalog:   unavailableCatalog,
		Namespace: []string{"dray"},
		ClusterID: "test-cluster",
	})

	topicName := "topic-with-iceberg-down"

	// Step 1: Create topic via TopicStore (this always succeeds)
	t.Log("Step 1: Creating topic metadata")
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 3,
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	t.Logf("  Created topic with %d partitions", len(result.Partitions))

	// Create streams for partitions
	for i, p := range result.Partitions {
		if err := streamManager.CreateStreamWithID(ctx, p.StreamID, topicName, int32(i)); err != nil {
			t.Fatalf("failed to create stream for partition %d: %v", i, err)
		}
	}

	// Step 2: Attempt to create Iceberg table (this should fail)
	t.Log("Step 2: Attempting Iceberg table creation (expected to fail)")
	_, err = tableCreator.CreateTableForTopic(ctx, topicName)
	if err == nil {
		t.Fatal("expected Iceberg table creation to fail")
	}
	t.Logf("  Iceberg table creation failed as expected: %v", err)

	// Step 3: Verify topic metadata exists and is usable
	t.Log("Step 3: Verifying topic is usable despite Iceberg failure")
	topic, err := topicStore.GetTopic(ctx, topicName)
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topic.PartitionCount != 3 {
		t.Errorf("expected 3 partitions, got %d", topic.PartitionCount)
	}
	t.Logf("  Topic %s exists with %d partitions", topicName, topic.PartitionCount)

	// Step 4: Verify we can produce to the topic
	t.Log("Step 4: Verifying produce works despite Iceberg failure")
	objStore := newCompactionTestObjectStore()
	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	produceReq := buildCompactionTestProduceRequest(topicName, 0, 5, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", produceResp.Topics[0].Partitions[0].ErrorCode)
	}
	t.Log("  Produce succeeded despite Iceberg catalog being down")

	t.Log("Topic creation and usage works correctly when Iceberg is unavailable")
}

// unavailableIcebergCatalog is a mock catalog that always returns ErrCatalogUnavailable.
// This simulates an Iceberg catalog that is unreachable (network failure, service down, etc.).
type unavailableIcebergCatalog struct{}

func newUnavailableIcebergCatalog() *unavailableIcebergCatalog {
	return &unavailableIcebergCatalog{}
}

func (c *unavailableIcebergCatalog) LoadTable(ctx context.Context, identifier catalog.TableIdentifier) (catalog.Table, error) {
	return nil, catalog.ErrCatalogUnavailable
}

func (c *unavailableIcebergCatalog) CreateTableIfMissing(ctx context.Context, identifier catalog.TableIdentifier, opts catalog.CreateTableOptions) (catalog.Table, error) {
	return nil, catalog.ErrCatalogUnavailable
}

func (c *unavailableIcebergCatalog) GetCurrentSnapshot(ctx context.Context, identifier catalog.TableIdentifier) (*table.Snapshot, error) {
	return nil, catalog.ErrCatalogUnavailable
}

func (c *unavailableIcebergCatalog) AppendDataFiles(ctx context.Context, identifier catalog.TableIdentifier, files []catalog.DataFile, opts *catalog.AppendFilesOptions) (*table.Snapshot, error) {
	return nil, catalog.ErrCatalogUnavailable
}

func (c *unavailableIcebergCatalog) DropTable(ctx context.Context, identifier catalog.TableIdentifier) error {
	return catalog.ErrCatalogUnavailable
}

func (c *unavailableIcebergCatalog) ListTables(ctx context.Context, namespace []string) ([]catalog.TableIdentifier, error) {
	return nil, catalog.ErrCatalogUnavailable
}

func (c *unavailableIcebergCatalog) TableExists(ctx context.Context, identifier catalog.TableIdentifier) (bool, error) {
	return false, catalog.ErrCatalogUnavailable
}

func (c *unavailableIcebergCatalog) Close() error {
	return nil
}

var _ catalog.Catalog = (*unavailableIcebergCatalog)(nil)
