package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/planner"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	metaoxia "github.com/dray-io/dray/internal/metadata/oxia"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/objectstore/s3"
	"github.com/dray-io/dray/internal/server"
	"github.com/dray-io/dray/internal/topics"
)

// CompactorOptions contains the configuration for creating a compactor.
type CompactorOptions struct {
	Config      *config.Config
	Logger      *logging.Logger
	CompactorID string
	Version     string
	GitCommit   string
	BuildTime   string
}

// Compactor represents a running Dray compactor instance.
type Compactor struct {
	opts                CompactorOptions
	logger              *logging.Logger
	metaStore           metadata.MetadataStore
	objectStore         objectstore.Store
	topicStore          *topics.Store
	streamManager       *index.StreamManager
	planner             walPlanner
	lockManager         *compaction.LockManager
	sagaManager         *compaction.SagaManager
	indexSwapper        *compaction.IndexSwapper
	icebergCatalog      catalog.Catalog
	icebergAppender     *catalog.Appender
	icebergTableCreator *catalog.TableCreator
	converter           *worker.Converter
	healthServer        *server.HealthServer
	parquetPlanner      parquetRewritePlanner

	mu        sync.Mutex
	started   bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

type walPlanner interface {
	Plan(ctx context.Context, streamID string) (*planner.Result, error)
}

type parquetRewritePlanner interface {
	Plan(ctx context.Context, streamID string) (*planner.ParquetRewriteResult, error)
}

type planKind int

const (
	planKindWAL planKind = iota
	planKindParquetRewrite
)

func (k planKind) String() string {
	switch k {
	case planKindParquetRewrite:
		return "parquet-rewrite"
	default:
		return "wal"
	}
}

type compactionPlan struct {
	Kind        planKind
	WALPlan     *planner.Result
	RewritePlan *planner.ParquetRewriteResult
}

func (p *compactionPlan) StartOffset() int64 {
	if p == nil {
		return 0
	}
	if p.Kind == planKindParquetRewrite {
		return p.RewritePlan.StartOffset
	}
	return p.WALPlan.StartOffset
}

func (p *compactionPlan) EndOffset() int64 {
	if p == nil {
		return 0
	}
	if p.Kind == planKindParquetRewrite {
		return p.RewritePlan.EndOffset
	}
	return p.WALPlan.EndOffset
}

func (p *compactionPlan) EntryCount() int {
	if p == nil {
		return 0
	}
	if p.Kind == planKindParquetRewrite {
		return len(p.RewritePlan.Entries)
	}
	return len(p.WALPlan.Entries)
}

func (p *compactionPlan) TotalSizeBytes() int64 {
	if p == nil {
		return 0
	}
	if p.Kind == planKindParquetRewrite {
		return p.RewritePlan.TotalSizeBytes
	}
	return p.WALPlan.TotalSizeBytes
}

func (c *Compactor) planCompaction(ctx context.Context, streamID string) (*compactionPlan, error) {
	if c.parquetPlanner != nil {
		rewritePlan, err := c.parquetPlanner.Plan(ctx, streamID)
		if err != nil {
			return nil, err
		}
		if rewritePlan != nil {
			return &compactionPlan{
				Kind:        planKindParquetRewrite,
				RewritePlan: rewritePlan,
			}, nil
		}
	}

	if c.planner == nil {
		return nil, nil
	}

	walPlan, err := c.planner.Plan(ctx, streamID)
	if err != nil {
		return nil, err
	}
	if walPlan == nil {
		return nil, nil
	}

	return &compactionPlan{
		Kind:    planKindWAL,
		WALPlan: walPlan,
	}, nil
}

func (c *Compactor) executePlannedCompaction(ctx context.Context, plan *compactionPlan, topicName string, partition int32, icebergEnabled bool) error {
	if plan == nil {
		return nil
	}
	switch plan.Kind {
	case planKindParquetRewrite:
		return c.executeParquetRewriteCompaction(ctx, plan.RewritePlan, topicName, partition, icebergEnabled)
	default:
		return c.executeCompaction(ctx, plan.WALPlan, topicName, partition, icebergEnabled)
	}
}

// NewCompactor creates a new Compactor instance but does not start it.
func NewCompactor(opts CompactorOptions) (*Compactor, error) {
	if opts.Logger == nil {
		opts.Logger = logging.DefaultLogger()
	}

	c := &Compactor{
		opts:      opts,
		logger:    opts.Logger,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}

	return c, nil
}

// Start initializes and starts all compactor components.
func (c *Compactor) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return fmt.Errorf("compactor already started")
	}
	c.started = true
	c.mu.Unlock()

	cfg := c.opts.Config

	c.logger.Infof("starting compactor", map[string]any{
		"compactorId": c.opts.CompactorID,
		"version":     c.opts.Version,
	})

	// Initialize metadata store (Oxia)
	oxiaStore, err := metaoxia.New(ctx, metaoxia.Config{
		ServiceAddress: cfg.Metadata.OxiaEndpoint,
		Namespace:      cfg.OxiaNamespace(),
		RequestTimeout: 30 * time.Second,
		SessionTimeout: 15 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to create Oxia metadata store: %w", err)
	}
	c.metaStore = oxiaStore

	if cfg.Compaction.Enabled {
		if cfg.ObjectStore.Bucket == "" {
			c.logger.Warn("object store bucket not configured; compaction disabled")
			cfg.Compaction.Enabled = false
		} else {
			store, err := s3.New(ctx, s3.Config{
				Bucket:          cfg.ObjectStore.Bucket,
				Region:          cfg.ObjectStore.Region,
				Endpoint:        cfg.ObjectStore.Endpoint,
				AccessKeyID:     cfg.ObjectStore.AccessKey,
				SecretAccessKey: cfg.ObjectStore.SecretKey,
			})
			if err != nil {
				return fmt.Errorf("failed to initialize object store: %w", err)
			}
			c.objectStore = store
		}
	}

	// Initialize components
	c.topicStore = topics.NewStore(c.metaStore)
	c.streamManager = index.NewStreamManager(c.metaStore)

	// Initialize Iceberg if enabled
	if cfg.Iceberg.Enabled {
		c.logger.Infof("initializing Iceberg catalog", map[string]any{
			"uri":  cfg.Iceberg.CatalogURI,
			"type": cfg.Iceberg.CatalogType,
		})

		catalogType := strings.ToLower(cfg.Iceberg.CatalogType)
		cat, err := catalog.LoadCatalog(ctx, catalog.CatalogConfig{
			Type:      catalogType,
			URI:       cfg.Iceberg.CatalogURI,
			Warehouse: cfg.Iceberg.Warehouse,
		})
		if err != nil {
			return fmt.Errorf("failed to load Iceberg catalog: %w", err)
		}
		c.icebergCatalog = cat
		c.icebergAppender = catalog.NewAppender(catalog.DefaultAppenderConfig(cat))
		c.icebergTableCreator = catalog.NewTableCreator(catalog.TableCreatorConfig{
			Catalog:   cat,
			ClusterID: c.opts.Config.ClusterID,
		})
	}

	// Create lock manager for one-compactor-per-stream locking
	c.lockManager = compaction.NewLockManager(c.metaStore, c.opts.CompactorID)

	// Create saga manager for durable job state
	c.sagaManager = compaction.NewSagaManager(c.metaStore, c.opts.CompactorID)

	// Create index swapper for atomic index updates
	c.indexSwapper = compaction.NewIndexSwapper(c.metaStore)

	// Create converter (nil object store for now - to be replaced with real S3)
	c.converter = worker.NewConverter(c.objectStore)

	// Create planner with configuration
	plannerCfg := planner.Config{
		MaxAgeMs:        cfg.Compaction.MinAgeMs,
		MaxFilesToMerge: cfg.Compaction.MaxFilesToMerge,
		MinEntries:      2,
		MaxSizeBytes:    512 * 1024 * 1024, // 512 MiB
	}
	c.planner = planner.New(plannerCfg, c.streamManager)
	rewriteCfg := planner.ParquetRewriteConfig{
		MinAgeMs:                cfg.Compaction.ParquetMinAgeMs,
		SmallFileThresholdBytes: cfg.Compaction.ParquetSmallFileThresholdBytes,
		TargetFileSizeBytes:     cfg.Compaction.ParquetTargetFileSizeBytes,
		MaxMergeBytes:           cfg.Compaction.ParquetMaxMergeBytes,
		MinFiles:                cfg.Compaction.ParquetMinFiles,
		MaxFiles:                cfg.Compaction.ParquetMaxFiles,
	}
	c.parquetPlanner = planner.NewParquetRewritePlanner(rewriteCfg, c.streamManager)
	// Start health server
	c.healthServer = server.NewHealthServer(cfg.Observability.MetricsAddr, c.logger)
	if err := c.healthServer.Start(); err != nil {
		return fmt.Errorf("failed to start health server: %w", err)
	}
	c.logger.Infof("health server started", map[string]any{
		"addr": c.healthServer.Addr(),
	})

	// Register compactor goroutine with health server
	c.healthServer.RegisterGoroutine("compaction-worker")

	// Start the compaction worker loop
	go c.runWorkerLoop(ctx)

	c.logger.Info("compactor started")

	// Block until stopped
	<-c.stopCh
	close(c.stoppedCh)

	return nil
}

// runWorkerLoop is the main compaction worker loop.
func (c *Compactor) runWorkerLoop(ctx context.Context) {
	cfg := c.opts.Config

	// Default scan interval of 10 seconds
	scanInterval := 10 * time.Second

	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	c.logger.Info("compaction worker loop started")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("compaction worker context cancelled")
			return
		case <-c.stopCh:
			c.logger.Info("compaction worker stop signal received")
			return
		case <-ticker.C:
			if !cfg.Compaction.Enabled {
				continue
			}
			c.healthServer.UpdateGoroutine("compaction-worker")
			c.scanAndCompact(ctx)
		}
	}
}

// scanAndCompact scans for streams needing compaction and processes them.
func (c *Compactor) scanAndCompact(ctx context.Context) {
	// List all topics to find streams that may need compaction
	topicList, err := c.topicStore.ListTopics(ctx)
	if err != nil {
		c.logger.Warnf("failed to list topics", map[string]any{"error": err.Error()})
		return
	}

	for _, topicMeta := range topicList {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		// Check each partition
		for p := int32(0); p < topicMeta.PartitionCount; p++ {
			partMeta, err := c.topicStore.GetPartition(ctx, topicMeta.Name, p)
			if err != nil {
				continue
			}

			c.processStream(ctx, partMeta.StreamID, topicMeta.Name, p)
		}
	}
}

// processStream attempts to compact a single stream.
func (c *Compactor) processStream(ctx context.Context, streamID, topicName string, partition int32) {
	// Try to acquire lock for this stream
	result, err := c.lockManager.AcquireLock(ctx, streamID)
	if err != nil {
		c.logger.Warnf("failed to acquire compaction lock", map[string]any{
			"streamId": streamID,
			"error":    err.Error(),
		})
		return
	}

	if !result.Acquired {
		// Another compactor is working on this stream
		return
	}

	defer func() {
		if err := c.lockManager.ReleaseLock(ctx, streamID); err != nil {
			c.logger.Warnf("failed to release compaction lock", map[string]any{
				"streamId": streamID,
				"error":    err.Error(),
			})
		}
	}()

	// Check for incomplete jobs that need recovery
	incompleteJobs, err := c.sagaManager.ListIncompleteJobs(ctx, streamID)
	if err != nil {
		c.logger.Warnf("failed to list incomplete jobs", map[string]any{
			"streamId": streamID,
			"error":    err.Error(),
		})
		return
	}

	// Recover incomplete jobs first
	for _, job := range incompleteJobs {
		c.logger.Infof("recovering incomplete compaction job", map[string]any{
			"streamId": streamID,
			"jobId":    job.JobID,
			"state":    job.State,
		})
		c.recoverJob(ctx, job)
	}

	icebergEnabled := c.opts.Config.Iceberg.Enabled && c.icebergAppender != nil
	plan, err := c.planCompaction(ctx, streamID)
	if err != nil {
		c.logger.Warnf("compaction planning failed", map[string]any{
			"streamId": streamID,
			"error":    err.Error(),
		})
		return
	}

	if plan == nil {
		// No compaction needed
		return
	}

	c.logger.Infof("starting compaction job", map[string]any{
		"streamId":    streamID,
		"topic":       topicName,
		"partition":   partition,
		"startOffset": plan.StartOffset(),
		"endOffset":   plan.EndOffset(),
		"entryCount":  plan.EntryCount(),
		"totalBytes":  plan.TotalSizeBytes(),
		"kind":        plan.Kind.String(),
	})

	// Execute compaction
	if err := c.executePlannedCompaction(ctx, plan, topicName, partition, icebergEnabled); err != nil {
		c.logger.Errorf("compaction failed", map[string]any{
			"streamId": streamID,
			"error":    err.Error(),
		})
	}
}

// executeCompaction runs the full compaction saga for a planned WAL job.
func (c *Compactor) executeCompaction(ctx context.Context, plan *planner.Result, topicName string, partition int32, icebergEnabled bool) error {
	// Create compaction job
	job, err := c.sagaManager.CreateJob(ctx, plan.StreamID,
		compaction.WithSourceRange(plan.StartOffset, plan.EndOffset),
		compaction.WithSourceWALCount(len(plan.Entries)),
		compaction.WithSourceSizeBytes(plan.TotalSizeBytes),
	)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}

	c.logger.Infof("created compaction job", map[string]any{
		"streamId": plan.StreamID,
		"jobId":    job.JobID,
	})

	// Convert WAL entries to Parquet
	entries := make([]*index.IndexEntry, len(plan.Entries))
	for i := range plan.Entries {
		entries[i] = &plan.Entries[i]
	}

	convertResult, err := c.converter.Convert(ctx, entries, partition)
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
		return fmt.Errorf("convert to parquet: %w", err)
	}

	// Generate Parquet paths - relative for object store, full S3 URI for Iceberg manifests
	parquetRelPath := fmt.Sprintf("parquet/%s/%s.parquet", plan.StreamID, job.JobID)
	parquetPath := fmt.Sprintf("s3://%s/%s", c.opts.Config.ObjectStore.Bucket, parquetRelPath)

	// Write Parquet to object storage using relative path
	if c.objectStore != nil {
		if err := c.converter.WriteParquetToStorage(ctx, parquetRelPath, convertResult.ParquetData); err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
			return fmt.Errorf("write parquet: %w", err)
		}
	}

	// Mark Parquet as written
	job, err = c.sagaManager.MarkParquetWritten(ctx, plan.StreamID, job.JobID,
		parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount,
		convertResult.Stats.MinTimestamp, convertResult.Stats.MaxTimestamp)
	if err != nil {
		return fmt.Errorf("mark parquet written: %w", err)
	}

	if icebergEnabled && c.icebergAppender != nil {
		// Ensure table exists (lazy creation)
		if _, err := c.icebergTableCreator.CreateTableForTopic(ctx, topicName); err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("ensure iceberg table exists: %v", err))
			return fmt.Errorf("ensure iceberg table exists: %w", err)
		}

		// Commit to Iceberg
		c.logger.Infof("committing to Iceberg", map[string]any{
			"topic": topicName,
			"jobId": job.JobID,
		})

		stats := catalog.DefaultDataFileStats(partition, plan.StartOffset, plan.EndOffset-1,
			convertResult.Stats.MinTimestamp, convertResult.Stats.MaxTimestamp, convertResult.RecordCount)
		dataFile := catalog.BuildDataFileFromStats(parquetPath, partition,
			convertResult.RecordCount, int64(len(convertResult.ParquetData)), stats)

		appendResult, err := c.icebergAppender.AppendFilesForStream(ctx, topicName, job.JobID, []catalog.DataFile{dataFile})
		if err != nil {
			// Per SPEC.md 11.2, Produce/Fetch must remain available even if Iceberg is down.
			// We fail the compaction job but it can be retried.
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("iceberg commit: %v", err))
			return fmt.Errorf("iceberg commit: %w", err)
		}

		// Mark Iceberg as committed
		job, err = c.sagaManager.MarkIcebergCommitted(ctx, plan.StreamID, job.JobID, appendResult.Snapshot.SnapshotID)
		if err != nil {
			return fmt.Errorf("mark iceberg committed: %w", err)
		}
	} else {
		job, err = c.sagaManager.MarkIcebergCommitted(ctx, plan.StreamID, job.JobID, 0)
		if err != nil {
			return fmt.Errorf("mark iceberg committed: %w", err)
		}
	}

	// Perform index swap
	c.logger.Infof("swapping index entries", map[string]any{
		"streamId": plan.StreamID,
		"jobId":    job.JobID,
	})

	metaDomain := int(metadata.CalculateMetaDomain(plan.StreamID, c.opts.Config.Metadata.NumDomains))
	swapResult, err := c.indexSwapper.SwapFromJob(ctx, job, parquetPath,
		int64(len(convertResult.ParquetData)), convertResult.RecordCount, metaDomain)
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("index swap: %v", err))
		return fmt.Errorf("index swap: %w", err)
	}

	// Mark index as swapped
	job, err = c.sagaManager.MarkIndexSwapped(ctx, plan.StreamID, job.JobID, swapResult.DecrementedWALObjects)
	if err != nil {
		return fmt.Errorf("mark index swapped: %w", err)
	}

	// Mark job as done
	_, err = c.sagaManager.MarkDone(ctx, plan.StreamID, job.JobID)
	if err != nil {
		return fmt.Errorf("mark done: %w", err)
	}

	c.logger.Infof("compaction job completed", map[string]any{
		"streamId":    plan.StreamID,
		"jobId":       job.JobID,
		"parquetPath": parquetPath,
		"recordCount": convertResult.RecordCount,
	})

	return nil
}

// executeParquetRewriteCompaction rewrites small Parquet files into a larger one.
func (c *Compactor) executeParquetRewriteCompaction(ctx context.Context, plan *planner.ParquetRewriteResult, topicName string, partition int32, icebergEnabled bool) error {
	if plan == nil {
		return nil
	}
	if c.objectStore == nil {
		return fmt.Errorf("object store unavailable for parquet rewrite")
	}

	job, err := c.sagaManager.CreateJob(ctx, plan.StreamID,
		compaction.WithSourceRange(plan.StartOffset, plan.EndOffset),
		compaction.WithSourceWALCount(len(plan.Entries)),
		compaction.WithSourceSizeBytes(plan.TotalSizeBytes),
	)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}

	c.logger.Infof("created parquet rewrite job", map[string]any{
		"streamId": plan.StreamID,
		"jobId":    job.JobID,
	})

	records, err := c.readParquetRecords(ctx, plan.Entries, partition)
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
		return fmt.Errorf("read parquet records: %w", err)
	}
	if len(records) == 0 {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, "no records in parquet rewrite plan")
		return fmt.Errorf("no records in parquet rewrite plan")
	}

	parquetData, fileStats, err := worker.WriteToBuffer(records)
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
		return fmt.Errorf("write parquet: %w", err)
	}

	parquetRelPath := fmt.Sprintf("parquet/%s/%s.parquet", plan.StreamID, job.JobID)
	parquetPath := fmt.Sprintf("s3://%s/%s", c.opts.Config.ObjectStore.Bucket, parquetRelPath)

	if err := c.objectStore.Put(ctx, parquetRelPath, bytes.NewReader(parquetData), int64(len(parquetData)), "application/octet-stream"); err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
		return fmt.Errorf("write parquet: %w", err)
	}

	job, err = c.sagaManager.MarkParquetWritten(ctx, plan.StreamID, job.JobID,
		parquetPath, int64(len(parquetData)), fileStats.RecordCount, fileStats.MinTimestamp, fileStats.MaxTimestamp)
	if err != nil {
		return fmt.Errorf("mark parquet written: %w", err)
	}

	minTimestamp, maxTimestamp := fileStats.MinTimestamp, fileStats.MaxTimestamp
	if fileStats.RecordCount > math.MaxUint32 {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, "parquet record count exceeds uint32")
		return fmt.Errorf("parquet record count exceeds uint32")
	}

	parquetEntry := index.IndexEntry{
		StreamID:         plan.StreamID,
		StartOffset:      plan.StartOffset,
		EndOffset:        plan.EndOffset,
		FileType:         index.FileTypeParquet,
		RecordCount:      uint32(fileStats.RecordCount),
		MessageCount:     uint32(fileStats.RecordCount),
		MinTimestampMs:   minTimestamp,
		MaxTimestampMs:   maxTimestamp,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      parquetPath,
		ParquetSizeBytes: uint64(len(parquetData)),
	}

	parquetKeys, err := parquetIndexKeys(plan.Entries)
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
		return fmt.Errorf("build parquet index keys: %w", err)
	}

	if icebergEnabled && c.icebergAppender != nil {
		if _, err := c.icebergTableCreator.CreateTableForTopic(ctx, topicName); err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("ensure iceberg table exists: %v", err))
			return fmt.Errorf("ensure iceberg table exists: %w", err)
		}

		dfStats := catalog.DefaultDataFileStats(partition, plan.StartOffset, plan.EndOffset-1,
			fileStats.MinTimestamp, fileStats.MaxTimestamp, fileStats.RecordCount)
		addedFile := catalog.BuildDataFileFromStats(parquetPath, partition,
			fileStats.RecordCount, int64(len(parquetData)), dfStats)

		removedFiles := make([]catalog.DataFile, 0, len(plan.Entries))
		for _, entry := range plan.Entries {
			removedFiles = append(removedFiles, catalog.DataFile{
				Path: entry.ParquetPath,
			})
		}

		replaceResult, err := c.icebergAppender.ReplaceFilesForStream(ctx, topicName, job.JobID, []catalog.DataFile{addedFile}, removedFiles)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("iceberg replace: %v", err))
			return fmt.Errorf("iceberg replace: %w", err)
		}

		job, err = c.sagaManager.MarkIcebergCommitted(ctx, plan.StreamID, job.JobID, replaceResult.Snapshot.SnapshotID)
		if err != nil {
			return fmt.Errorf("mark iceberg committed: %w", err)
		}
	} else {
		job, err = c.sagaManager.MarkIcebergCommitted(ctx, plan.StreamID, job.JobID, 0)
		if err != nil {
			return fmt.Errorf("mark iceberg committed: %w", err)
		}
	}

	metaDomain := int(metadata.CalculateMetaDomain(plan.StreamID, c.opts.Config.Metadata.NumDomains))
	swapResult, err := c.indexSwapper.Swap(ctx, compaction.SwapRequest{
		StreamID:         plan.StreamID,
		ParquetIndexKeys: parquetKeys,
		ParquetEntry:     parquetEntry,
		MetaDomain:       metaDomain,
	})
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
		return fmt.Errorf("index swap: %w", err)
	}

	for i := range swapResult.ParquetGCCandidates {
		swapResult.ParquetGCCandidates[i].IcebergEnabled = icebergEnabled
		swapResult.ParquetGCCandidates[i].IcebergRemovalConfirmed = true
	}

	job, err = c.sagaManager.MarkIndexSwappedWithGC(ctx, plan.StreamID, job.JobID, nil, swapResult.ParquetGCCandidates, swapResult.ParquetGCGracePeriodMs)
	if err != nil {
		return fmt.Errorf("mark index swapped: %w", err)
	}

	if _, err := c.sagaManager.MarkDone(ctx, plan.StreamID, job.JobID); err != nil {
		return fmt.Errorf("mark done: %w", err)
	}

	c.logger.Infof("parquet rewrite job completed", map[string]any{
		"streamId":    plan.StreamID,
		"jobId":       job.JobID,
		"parquetPath": parquetPath,
		"recordCount": fileStats.RecordCount,
	})

	return nil
}

func (c *Compactor) readParquetRecords(ctx context.Context, entries []index.IndexEntry, partition int32) ([]worker.Record, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].StartOffset < entries[j].StartOffset
	})

	var records []worker.Record
	for _, entry := range entries {
		key := objectKeyFromPath(entry.ParquetPath)
		rc, err := c.objectStore.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}

		reader, err := worker.NewReader(data)
		if err != nil {
			return nil, err
		}
		parquetRecords, err := reader.ReadAll()
		reader.Close()
		if err != nil {
			return nil, err
		}

		// Ensure records have expected partition value.
		for i := range parquetRecords {
			parquetRecords[i].Partition = partition
		}

		records = append(records, parquetRecords...)
	}

	return records, nil
}

func parquetIndexKeys(entries []index.IndexEntry) ([]string, error) {
	keysList := make([]string, 0, len(entries))
	for _, entry := range entries {
		key, err := keys.OffsetIndexKeyPath(entry.StreamID, entry.EndOffset, entry.CumulativeSize)
		if err != nil {
			return nil, err
		}
		keysList = append(keysList, key)
	}
	return keysList, nil
}

func objectKeyFromPath(path string) string {
	if strings.HasPrefix(path, "s3://") {
		parts := strings.SplitN(path[5:], "/", 2)
		if len(parts) > 1 {
			return parts[1]
		}
	}
	return path
}

// recoverJob resumes an incomplete compaction job from its current state.
func (c *Compactor) recoverJob(ctx context.Context, job *compaction.Job) {
	streamMeta, err := c.streamManager.GetStreamMeta(ctx, job.StreamID)
	if err != nil {
		c.logger.Warnf("failed to get stream meta for recovery", map[string]any{
			"streamId": job.StreamID,
			"error":    err.Error(),
		})
		return
	}

	switch job.State {
	case compaction.JobStateCreated:
		// Need to restart from the beginning - mark as failed and let next scan create new job
		c.sagaManager.MarkFailed(ctx, job.StreamID, job.JobID, "recovery: restarting from CREATED state")

	case compaction.JobStateParquetWritten:
		// Parquet file exists, proceed to Iceberg/index swap
		icebergEnabled := c.opts.Config.Iceberg.Enabled && c.icebergAppender != nil
		if icebergEnabled {
			// Ensure table exists (lazy creation)
			if _, err := c.icebergTableCreator.CreateTableForTopic(ctx, streamMeta.TopicName); err != nil {
				c.logger.Warnf("recovery: ensure iceberg table exists failed", map[string]any{"error": err.Error()})
				return
			}

			stats := catalog.DefaultDataFileStats(streamMeta.Partition, job.SourceStartOffset, job.SourceEndOffset-1,
				job.ParquetMinTimestampMs, job.ParquetMaxTimestampMs, job.ParquetRecordCount)
			dataFile := catalog.BuildDataFileFromStats(job.ParquetPath, streamMeta.Partition,
				job.ParquetRecordCount, job.ParquetSizeBytes, stats)

			appendResult, err := c.icebergAppender.AppendFilesForStream(ctx, streamMeta.TopicName, job.JobID, []catalog.DataFile{dataFile})
			if err != nil {
				c.logger.Warnf("recovery: iceberg commit failed", map[string]any{"error": err.Error()})
				return
			}
			job, err = c.sagaManager.MarkIcebergCommitted(ctx, job.StreamID, job.JobID, appendResult.Snapshot.SnapshotID)
			if err != nil {
				return
			}
		} else {
			job, err = c.sagaManager.MarkIcebergCommitted(ctx, job.StreamID, job.JobID, 0)
			if err != nil {
				return
			}
		}
		// Fallthrough to next states
		c.recoverJob(ctx, job)

	case compaction.JobStateIcebergCommitted:
		// Iceberg committed, perform index swap
		metaDomain := int(metadata.CalculateMetaDomain(job.StreamID, c.opts.Config.Metadata.NumDomains))
		swapResult, err := c.indexSwapper.SwapFromJob(ctx, job, job.ParquetPath,
			job.ParquetSizeBytes, job.ParquetRecordCount, metaDomain)
		if err != nil {
			c.logger.Warnf("recovery: index swap failed", map[string]any{"error": err.Error()})
			return
		}
		job, err = c.sagaManager.MarkIndexSwapped(ctx, job.StreamID, job.JobID, swapResult.DecrementedWALObjects)
		if err != nil {
			return
		}
		// Fallthrough to next states
		c.recoverJob(ctx, job)

	case compaction.JobStateIndexSwapped:
		// Index swapped, just need to mark done
		c.sagaManager.MarkDone(ctx, job.StreamID, job.JobID)

	case compaction.JobStateDone, compaction.JobStateFailed:
		// Terminal states
	}
}

// Shutdown gracefully stops the compactor.
func (c *Compactor) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	c.logger.Info("shutting down compactor")

	// Mark health server as shutting down
	if c.healthServer != nil {
		c.healthServer.SetShuttingDown()
	}

	// Signal worker loop to stop
	close(c.stopCh)

	// Wait for worker loop to finish with timeout
	select {
	case <-c.stoppedCh:
		// Worker stopped cleanly
	case <-ctx.Done():
		c.logger.Warn("shutdown context cancelled, forcing stop")
	}

	// Release all held locks
	if c.lockManager != nil {
		if err := c.lockManager.ReleaseAllLocks(ctx); err != nil {
			c.logger.Warnf("failed to release all locks", map[string]any{
				"error": err.Error(),
			})
		}
	}

	// Close health server
	if c.healthServer != nil {
		if err := c.healthServer.Close(); err != nil {
			c.logger.Warnf("error closing health server", map[string]any{
				"error": err.Error(),
			})
		}
	}

	// Close metadata store
	if c.metaStore != nil {
		if err := c.metaStore.Close(); err != nil {
			c.logger.Warnf("error closing metadata store", map[string]any{
				"error": err.Error(),
			})
		}
	}

	if c.objectStore != nil {
		if err := c.objectStore.Close(); err != nil {
			c.logger.Warnf("error closing object store", map[string]any{
				"error": err.Error(),
			})
		}
	}

	if c.icebergCatalog != nil {
		if err := c.icebergCatalog.Close(); err != nil {
			c.logger.Warnf("error closing Iceberg catalog", map[string]any{
				"error": err.Error(),
			})
		}
	}

	c.logger.Info("compactor shutdown complete")
	return nil
}
