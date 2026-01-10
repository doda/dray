package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/planner"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/gc"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/dray-io/dray/internal/metadata/keys"
	metaoxia "github.com/dray-io/dray/internal/metadata/oxia"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/objectstore/s3"
	"github.com/dray-io/dray/internal/server"
	"github.com/dray-io/dray/internal/topics"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	walGCWorker         *gc.WALGCWorker
	walOrphanGCWorker   *gc.WALOrphanGCWorker
	parquetGCWorker     *gc.ParquetGCWorker
	compactionMetrics   *metrics.CompactionMetrics
	metricsServer       *metrics.Server
	rewriteNext         bool

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

const parquetTimestampScale = int64(1000)

func icebergS3Props(store config.ObjectStoreConfig) iceberg.Properties {
	props := iceberg.Properties{}

	if endpoint := strings.TrimSpace(store.Endpoint); endpoint != "" {
		props[iceio.S3EndpointURL] = endpoint
	}

	region := strings.TrimSpace(store.Region)
	if region == "" || strings.EqualFold(region, "auto") {
		region = "us-east-1"
	}
	props[iceio.S3Region] = region

	if store.AccessKey != "" {
		props[iceio.S3AccessKeyID] = store.AccessKey
	}
	if store.SecretKey != "" {
		props[iceio.S3SecretAccessKey] = store.SecretKey
	}

	// Tigris expects path-style addressing; keep virtual addressing disabled.
	props[iceio.S3ForceVirtualAddressing] = "false"

	return props
}

func (k planKind) String() string {
	switch k {
	case planKindParquetRewrite:
		return "parquet-rewrite"
	default:
		return "wal"
	}
}

// hasHourPartitioning checks if the configured partitioning includes hour-based time partitioning.
func hasHourPartitioning(partitioning []string) bool {
	for _, expr := range partitioning {
		cleanExpr := strings.ToLower(strings.TrimSpace(expr))
		if strings.HasPrefix(cleanExpr, "hour(") {
			return true
		}
	}
	return false
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
	// Alternate between WAL compaction and parquet rewrites when both are available.
	if c.rewriteNext {
		if c.parquetPlanner != nil {
			rewritePlan, err := c.parquetPlanner.Plan(ctx, streamID)
			if err != nil {
				return nil, err
			}
			if rewritePlan != nil {
				c.rewriteNext = false
				return &compactionPlan{
					Kind:        planKindParquetRewrite,
					RewritePlan: rewritePlan,
				}, nil
			}
		}
	}

	if c.planner != nil {
		walPlan, err := c.planner.Plan(ctx, streamID)
		if err != nil {
			return nil, err
		}
		if walPlan != nil {
			c.rewriteNext = true
			return &compactionPlan{
				Kind:    planKindWAL,
				WALPlan: walPlan,
			}, nil
		}
	}

	if c.parquetPlanner != nil {
		rewritePlan, err := c.parquetPlanner.Plan(ctx, streamID)
		if err != nil {
			return nil, err
		}
		if rewritePlan != nil {
			c.rewriteNext = false
			return &compactionPlan{
				Kind:        planKindParquetRewrite,
				RewritePlan: rewritePlan,
			}, nil
		}
	}

	return nil, nil
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
		opts:        opts,
		logger:      opts.Logger,
		rewriteNext: true,
		stopCh:      make(chan struct{}),
		stoppedCh:   make(chan struct{}),
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
		icebergProps := icebergS3Props(cfg.ObjectStore)
		cat, err := catalog.LoadCatalog(ctx, catalog.CatalogConfig{
			Type:      catalogType,
			URI:       cfg.Iceberg.CatalogURI,
			Warehouse: cfg.Iceberg.Warehouse,
			Props:     icebergProps,
		})
		if err != nil {
			return fmt.Errorf("failed to load Iceberg catalog: %w", err)
		}
		c.icebergCatalog = cat
		c.icebergAppender = catalog.NewAppender(catalog.DefaultAppenderConfig(cat))
		c.icebergTableCreator = catalog.NewTableCreator(catalog.TableCreatorConfig{
			Catalog:          cat,
			ClusterID:        c.opts.Config.ClusterID,
			ValueProjections: cfg.Compaction.ValueProjections,
			Partitioning:     cfg.Iceberg.Partitioning,
		})
	}

	// Create lock manager for one-compactor-per-stream locking
	c.lockManager = compaction.NewLockManager(c.metaStore, c.opts.CompactorID)

	// Create saga manager for durable job state
	c.sagaManager = compaction.NewSagaManager(c.metaStore, c.opts.CompactorID)

	// Create index swapper for atomic index updates
	c.indexSwapper = compaction.NewIndexSwapper(c.metaStore)

	// Create converter (nil object store for now - to be replaced with real S3)
	c.converter = worker.NewConverter(c.objectStore, cfg.Compaction.ValueProjections)

	if cfg.Compaction.Enabled && c.objectStore != nil {
		walCfg := gc.DefaultWALGCWorkerConfig()
		walCfg.NumDomains = cfg.Metadata.NumDomains
		c.walGCWorker = gc.NewWALGCWorker(c.metaStore, c.objectStore, walCfg)
		c.walGCWorker.Start()

		orphanCfg := gc.DefaultWALOrphanGCWorkerConfig()
		orphanCfg.NumDomains = cfg.Metadata.NumDomains
		if cfg.WAL.OrphanTTLMs > 0 {
			orphanCfg.OrphanTTLMs = cfg.WAL.OrphanTTLMs
		}
		c.walOrphanGCWorker = gc.NewWALOrphanGCWorker(c.metaStore, c.objectStore, orphanCfg)
		c.walOrphanGCWorker.Start()

		parquetCfg := gc.DefaultParquetGCWorkerConfig()
		c.parquetGCWorker = gc.NewParquetGCWorker(c.metaStore, c.objectStore, parquetCfg)
		c.parquetGCWorker.Start()
	}

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

	// Initialize compaction metrics
	c.compactionMetrics = metrics.NewCompactionMetrics()

	// Start health server with metrics endpoint
	healthServer := server.NewHealthServer(cfg.Observability.MetricsAddr, c.logger)
	healthServer.RegisterHandler("/metrics", promhttp.Handler())
	c.mu.Lock()
	c.healthServer = healthServer
	c.mu.Unlock()
	if err := c.healthServer.Start(); err != nil {
		return fmt.Errorf("failed to start health server: %w", err)
	}
	c.logger.Infof("health server started", map[string]any{
		"addr": c.healthServer.Addr(),
	})

	// Register compactor goroutine with health server
	c.healthServer.RegisterGoroutine("compaction-worker")
	c.healthServer.RegisterGoroutine("iceberg-maintenance")

	// Start the compaction worker loop
	go c.runWorkerLoop(ctx)

	// Start the Iceberg maintenance loop if enabled
	if cfg.Iceberg.Enabled {
		go c.runIcebergMaintenanceLoop(ctx)
	}

	c.logger.Info("compactor started")

	// Block until stopped
	<-c.stopCh
	close(c.stoppedCh)

	return nil
}

// runIcebergMaintenanceLoop periodically runs Iceberg table maintenance.
func (c *Compactor) runIcebergMaintenanceLoop(ctx context.Context) {
	// Run maintenance every hour
	maintenanceInterval := time.Hour
	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()

	c.logger.Info("iceberg maintenance loop started")
	c.runIcebergMaintenance(ctx)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("iceberg maintenance loop context cancelled")
			return
		case <-c.stopCh:
			c.logger.Info("iceberg maintenance loop stop signal received")
			return
		case <-ticker.C:
			c.healthServer.UpdateGoroutine("iceberg-maintenance")
			c.runIcebergMaintenance(ctx)
		}
	}
}

func (c *Compactor) runIcebergMaintenance(ctx context.Context) {
	if c.icebergCatalog == nil || c.topicStore == nil || c.icebergTableCreator == nil {
		return
	}

	topics, err := c.topicStore.ListTopics(ctx)
	if err != nil {
		c.logger.Warnf("failed to list topics for iceberg maintenance", map[string]any{"error": err.Error()})
		return
	}

	for _, topicMeta := range topics {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		c.maintainTopicTable(ctx, topicMeta.Name)
	}
}

func (c *Compactor) maintainTopicTable(ctx context.Context, topicName string) {
	cfg := c.opts.Config
	namespace := c.icebergTableCreator.Namespace()
	if len(namespace) == 0 {
		namespace = []string{"dray"}
	}
	tableID := catalog.NewTableIdentifier(namespace, topicName)

	tbl, err := c.icebergTableCreator.LoadTableForTopic(ctx, topicName)
	if err != nil {
		if err == catalog.ErrTableNotFound {
			return
		}
		c.logger.Warnf("failed to load table for maintenance", map[string]any{
			"table": catalog.TableIdentifierString(tableID),
			"error": err.Error(),
		})
		return
	}

	// Expire snapshots
	olderThan := time.Duration(cfg.Iceberg.SnapshotRetentionAgeMs) * time.Millisecond
	retainLast := cfg.Iceberg.SnapshotRetentionMinCount
	if retainLast <= 0 {
		retainLast = 1
	}

	if err := tbl.ExpireSnapshots(ctx, olderThan, retainLast); err != nil {
		c.logger.Warnf("failed to expire snapshots", map[string]any{
			"table": catalog.TableIdentifierString(tableID),
			"error": err.Error(),
		})
	} else {
		c.logger.Infof("expired snapshots for table", map[string]any{
			"table": catalog.TableIdentifierString(tableID),
		})
	}

	// NOTE: RewriteManifests is not yet supported in the Iceberg library
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
func (c *Compactor) executeCompaction(ctx context.Context, plan *planner.Result, topicName string, partition int32, icebergEnabled bool) (retErr error) {
	// Start tracking compaction job metrics
	var jobTracker *metrics.JobTracker
	if c.compactionMetrics != nil {
		jobTracker = c.compactionMetrics.StartJob(metrics.JobTypeWAL, plan.StreamID, plan.TotalSizeBytes)
	}
	var jobBytes, jobRecords int64
	defer func() {
		if jobTracker != nil {
			if retErr != nil {
				jobTracker.Failed()
			} else {
				jobTracker.Complete(jobBytes, jobRecords)
			}
		}
	}()

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

	projectionFields := c.converter.ProjectionFields(topicName)
	var parquetSchema *parquet.Schema
	var icebergSchema *iceberg.Schema
	if icebergEnabled && c.icebergTableCreator != nil {
		table, err := c.icebergTableCreator.CreateTableForTopic(ctx, topicName)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("ensure iceberg table exists: %v", err))
			return fmt.Errorf("ensure iceberg table exists: %w", err)
		}
		icebergSchema = table.Schema()
		parquetSchema, err = worker.BuildParquetSchemaFromIceberg(icebergSchema, projectionFields)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
			return fmt.Errorf("build parquet schema: %w", err)
		}
	}

	// Check if we need to split by hour for Iceberg partitioning
	useHourPartitioning := icebergEnabled && hasHourPartitioning(c.opts.Config.Iceberg.Partitioning)

	c.logger.Infof("compaction partitioning check", map[string]any{
		"icebergEnabled":      icebergEnabled,
		"partitioning":        c.opts.Config.Iceberg.Partitioning,
		"useHourPartitioning": useHourPartitioning,
	})

	var parquetPaths []string
	var parquetPath string
	var totalBytes int64
	var totalRecords int64
	var minTimestamp, maxTimestamp int64
	var dataFiles []catalog.DataFile

	if useHourPartitioning {
		// Use partitioned conversion - splits records by hour
		partResult, err := c.converter.ConvertPartitioned(ctx, entries, partition, topicName, parquetSchema)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
			return fmt.Errorf("convert to parquet (partitioned): %w", err)
		}

		totalRecords = partResult.TotalRecords

		// Write each partitioned file
		for i, pf := range partResult.Results {
			// Generate path with hour suffix
			relPath := fmt.Sprintf("parquet/%s/%s-h%d.parquet", plan.StreamID, job.JobID, pf.HourValue)
			fullPath := fmt.Sprintf("s3://%s/%s", c.opts.Config.ObjectStore.Bucket, relPath)

			if c.objectStore != nil {
				if err := c.converter.WriteParquetToStorage(ctx, relPath, pf.ParquetData); err != nil {
					c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
					return fmt.Errorf("write parquet for hour %d: %w", pf.HourValue, err)
				}
			}

			totalBytes += int64(len(pf.ParquetData))
			parquetPaths = append(parquetPaths, fullPath)

			// Use first file path as primary (for logging/backwards compatibility)
			if i == 0 {
				parquetPath = fullPath
			}

			// Track min/max timestamps across all files
			if i == 0 || pf.Stats.MinTimestamp < minTimestamp {
				minTimestamp = pf.Stats.MinTimestamp
			}
			if i == 0 || pf.Stats.MaxTimestamp > maxTimestamp {
				maxTimestamp = pf.Stats.MaxTimestamp
			}

			// Build data file for Iceberg
			minTimestampMicros := scaleParquetTimestamp(pf.Stats.MinTimestamp)
			maxTimestampMicros := scaleParquetTimestamp(pf.Stats.MaxTimestamp)
			stats := buildDataFileStats(partition, pf.Stats.MinOffset, pf.Stats.MaxOffset,
				minTimestampMicros, maxTimestampMicros, pf.RecordCount,
				icebergSchema, pf.Stats.ProjectedBounds)
			dataFile := catalog.BuildDataFileFromStats(fullPath, partition,
				pf.RecordCount, int64(len(pf.ParquetData)), stats)
			dataFiles = append(dataFiles, dataFile)
		}
	} else {
		// Standard single-file conversion
		convertResult, err := c.converter.Convert(ctx, entries, partition, topicName, parquetSchema)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
			return fmt.Errorf("convert to parquet: %w", err)
		}

		// Generate Parquet paths - relative for object store, full S3 URI for Iceberg manifests
		parquetRelPath := fmt.Sprintf("parquet/%s/%s.parquet", plan.StreamID, job.JobID)
		parquetPath = fmt.Sprintf("s3://%s/%s", c.opts.Config.ObjectStore.Bucket, parquetRelPath)
		parquetPaths = []string{parquetPath}

		// Write Parquet to object storage using relative path
		if c.objectStore != nil {
			if err := c.converter.WriteParquetToStorage(ctx, parquetRelPath, convertResult.ParquetData); err != nil {
				c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
				return fmt.Errorf("write parquet: %w", err)
			}
		}

		totalBytes = int64(len(convertResult.ParquetData))
		totalRecords = convertResult.RecordCount
		minTimestamp = convertResult.Stats.MinTimestamp
		maxTimestamp = convertResult.Stats.MaxTimestamp

		// Build single data file for Iceberg
		minTimestampMicros := scaleParquetTimestamp(convertResult.Stats.MinTimestamp)
		maxTimestampMicros := scaleParquetTimestamp(convertResult.Stats.MaxTimestamp)
		stats := buildDataFileStats(partition, plan.StartOffset, plan.EndOffset-1,
			minTimestampMicros, maxTimestampMicros, convertResult.RecordCount,
			icebergSchema, convertResult.Stats.ProjectedBounds)
		dataFile := catalog.BuildDataFileFromStats(parquetPath, partition,
			convertResult.RecordCount, int64(len(convertResult.ParquetData)), stats)
		dataFiles = append(dataFiles, dataFile)
	}

	// Mark Parquet as written
	job, err = c.sagaManager.MarkParquetWritten(ctx, plan.StreamID, job.JobID,
		parquetPaths, totalBytes, totalRecords,
		minTimestamp, maxTimestamp)
	if err != nil {
		return fmt.Errorf("mark parquet written: %w", err)
	}

	if icebergEnabled && c.icebergAppender != nil {
		// Commit to Iceberg
		c.logger.Infof("committing to Iceberg", map[string]any{
			"topic": topicName,
			"jobId": job.JobID,
		})

		appendResult, err := c.icebergAppender.AppendFilesForStream(ctx, topicName, job.JobID, dataFiles)
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
	swapResult, err := c.indexSwapper.SwapFromJob(ctx, job, metaDomain)
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("index swap: %v", err))
		return fmt.Errorf("index swap: %w", err)
	}

	// Mark index as swapped
	job, err = c.sagaManager.MarkIndexSwapped(ctx, plan.StreamID, job.JobID, swapResult.DecrementedWALObjects)
	if err != nil {
		return fmt.Errorf("mark index swapped: %w", err)
	}

	// Mark WAL GC as ready (required before DONE per saga state machine)
	job, err = c.sagaManager.MarkWALGCReady(ctx, plan.StreamID, job.JobID)
	if err != nil {
		return fmt.Errorf("mark wal gc ready: %w", err)
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
		"recordCount": totalRecords,
	})

	// Set metrics for successful completion
	jobBytes = totalBytes
	jobRecords = totalRecords

	return nil
}

// executeParquetRewriteCompaction rewrites small Parquet files into a larger one.
func (c *Compactor) executeParquetRewriteCompaction(ctx context.Context, plan *planner.ParquetRewriteResult, topicName string, partition int32, icebergEnabled bool) (retErr error) {
	if plan == nil {
		return nil
	}
	if c.objectStore == nil {
		return fmt.Errorf("object store unavailable for parquet rewrite")
	}

	// Start tracking parquet rewrite job metrics
	var jobTracker *metrics.JobTracker
	if c.compactionMetrics != nil {
		jobTracker = c.compactionMetrics.StartJob(metrics.JobTypeParquetRewrite, plan.StreamID, plan.TotalSizeBytes)
	}
	var jobBytes, jobRecords int64
	defer func() {
		if jobTracker != nil {
			if retErr != nil {
				jobTracker.Failed()
			} else {
				jobTracker.Complete(jobBytes, jobRecords)
			}
		}
	}()

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

	projectionFields := c.converter.ProjectionFields(topicName)
	parquetSchema := worker.BuildParquetSchema(projectionFields)
	var icebergSchema *iceberg.Schema
	if icebergEnabled && c.icebergTableCreator != nil {
		table, err := c.icebergTableCreator.CreateTableForTopic(ctx, topicName)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, fmt.Sprintf("ensure iceberg table exists: %v", err))
			return fmt.Errorf("ensure iceberg table exists: %w", err)
		}
		icebergSchema = table.Schema()
		parquetSchema, err = worker.BuildParquetSchemaFromIceberg(icebergSchema, projectionFields)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
			return fmt.Errorf("build parquet schema: %w", err)
		}
	}
	// Check if we need to split by hour for Iceberg partitioning
	useHourPartitioning := icebergEnabled && hasHourPartitioning(c.opts.Config.Iceberg.Partitioning)

	var parquetPaths []string
	var parquetPath string
	var totalBytes int64
	var totalRecords int64
	var minTimestamp, maxTimestamp int64
	var dataFiles []catalog.DataFile

	if useHourPartitioning {
		// Group records by hour
		recordsByHour := make(map[int64][]worker.Record)
		for _, rec := range records {
			var hourValue int64
			if createdAt, ok := rec.Projected["created_at"]; ok {
				switch v := createdAt.(type) {
				case int64:
					hourValue = v / (3600 * 1000)
				case float64:
					hourValue = int64(v) / (3600 * 1000)
				default:
					hourValue = rec.Timestamp / (3600 * 1000)
				}
			} else {
				hourValue = rec.Timestamp / (3600 * 1000)
			}
			recordsByHour[hourValue] = append(recordsByHour[hourValue], rec)
		}

		for hourValue, hourRecords := range recordsByHour {
			pData, stats, err := worker.WriteToBufferWithProjections(parquetSchema, hourRecords, projectionFields)
			if err != nil {
				c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
				return fmt.Errorf("write parquet for hour %d: %w", hourValue, err)
			}

			relPath := fmt.Sprintf("parquet/%s/%s-h%d.parquet", plan.StreamID, job.JobID, hourValue)
			fullPath := fmt.Sprintf("s3://%s/%s", c.opts.Config.ObjectStore.Bucket, relPath)

			if err := c.objectStore.Put(ctx, relPath, bytes.NewReader(pData), int64(len(pData)), "application/octet-stream"); err != nil {
				c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
				return fmt.Errorf("write parquet for hour %d: %w", hourValue, err)
			}

			totalBytes += int64(len(pData))
			totalRecords += stats.RecordCount
			parquetPaths = append(parquetPaths, fullPath)

			if len(parquetPaths) == 1 {
				parquetPath = fullPath
			}

			if len(parquetPaths) == 1 || stats.MinTimestamp < minTimestamp {
				minTimestamp = stats.MinTimestamp
			}
			if len(parquetPaths) == 1 || stats.MaxTimestamp > maxTimestamp {
				maxTimestamp = stats.MaxTimestamp
			}

			minTimestampMicros := scaleParquetTimestamp(stats.MinTimestamp)
			maxTimestampMicros := scaleParquetTimestamp(stats.MaxTimestamp)
			dfStats := buildDataFileStats(partition, stats.MinOffset, stats.MaxOffset,
				minTimestampMicros, maxTimestampMicros, stats.RecordCount,
				icebergSchema, stats.ProjectedBounds)
			dataFile := catalog.BuildDataFileFromStats(fullPath, partition,
				stats.RecordCount, int64(len(pData)), dfStats)
			dataFiles = append(dataFiles, dataFile)
		}
	} else {
		parquetData, fileStats, err := worker.WriteToBufferWithProjections(parquetSchema, records, projectionFields)
		if err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
			return fmt.Errorf("write parquet: %w", err)
		}

		parquetRelPath := fmt.Sprintf("parquet/%s/%s.parquet", plan.StreamID, job.JobID)
		parquetPath = fmt.Sprintf("s3://%s/%s", c.opts.Config.ObjectStore.Bucket, parquetRelPath)
		parquetPaths = []string{parquetPath}

		if err := c.objectStore.Put(ctx, parquetRelPath, bytes.NewReader(parquetData), int64(len(parquetData)), "application/octet-stream"); err != nil {
			c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
			return fmt.Errorf("write parquet: %w", err)
		}

		totalBytes = int64(len(parquetData))
		totalRecords = fileStats.RecordCount
		minTimestamp = fileStats.MinTimestamp
		maxTimestamp = fileStats.MaxTimestamp

		minTimestampMicros := scaleParquetTimestamp(fileStats.MinTimestamp)
		maxTimestampMicros := scaleParquetTimestamp(fileStats.MaxTimestamp)
		dfStats := buildDataFileStats(partition, plan.StartOffset, plan.EndOffset-1,
			minTimestampMicros, maxTimestampMicros, fileStats.RecordCount,
			icebergSchema, fileStats.ProjectedBounds)
		dataFile := catalog.BuildDataFileFromStats(parquetPath, partition,
			fileStats.RecordCount, int64(len(parquetData)), dfStats)
		dataFiles = []catalog.DataFile{dataFile}
	}

	job, err = c.sagaManager.MarkParquetWritten(ctx, plan.StreamID, job.JobID,
		parquetPaths, totalBytes, totalRecords, minTimestamp, maxTimestamp)
	if err != nil {
		return fmt.Errorf("mark parquet written: %w", err)
	}

	if totalRecords > math.MaxUint32 {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, "parquet record count exceeds uint32")
		return fmt.Errorf("parquet record count exceeds uint32")
	}

	parquetKeys, err := parquetIndexKeys(plan.Entries)
	if err != nil {
		c.sagaManager.MarkFailed(ctx, plan.StreamID, job.JobID, err.Error())
		return fmt.Errorf("build parquet index keys: %w", err)
	}

	if icebergEnabled && c.icebergAppender != nil {
		removedFiles := make([]catalog.DataFile, 0, len(plan.Entries))
		for _, entry := range plan.Entries {
			paths := entry.ParquetPaths
			if len(paths) == 0 && entry.ParquetPath != "" {
				paths = []string{entry.ParquetPath}
			}
			for _, path := range paths {
				removedFiles = append(removedFiles, catalog.DataFile{
					Path: path,
				})
			}
		}

		replaceResult, err := c.icebergAppender.ReplaceFilesForStream(ctx, topicName, job.JobID, dataFiles, removedFiles)
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
		ParquetEntry: index.IndexEntry{
			StreamID:         plan.StreamID,
			StartOffset:      plan.StartOffset,
			EndOffset:        plan.EndOffset,
			FileType:         index.FileTypeParquet,
			RecordCount:      uint32(job.ParquetRecordCount),
			MessageCount:     uint32(job.ParquetRecordCount),
			MinTimestampMs:   job.ParquetMinTimestampMs,
			MaxTimestampMs:   job.ParquetMaxTimestampMs,
			CreatedAtMs:      time.Now().UnixMilli(),
			ParquetPath:      job.ParquetPath,
			ParquetPaths:     job.ParquetPaths,
			ParquetSizeBytes: uint64(job.ParquetSizeBytes),
		},
		MetaDomain:     metaDomain,
		IcebergEnabled: icebergEnabled,
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

	// Mark WAL GC as ready (required before DONE per saga state machine)
	job, err = c.sagaManager.MarkWALGCReady(ctx, plan.StreamID, job.JobID)
	if err != nil {
		return fmt.Errorf("mark wal gc ready: %w", err)
	}

	if _, err := c.sagaManager.MarkDone(ctx, plan.StreamID, job.JobID); err != nil {
		return fmt.Errorf("mark done: %w", err)
	}

	c.logger.Infof("parquet rewrite job completed", map[string]any{
		"streamId":     plan.StreamID,
		"jobId":        job.JobID,
		"parquetPath":  parquetPath,
		"parquetFiles": len(parquetPaths),
		"recordCount":  totalRecords,
		"totalBytes":   totalBytes,
		"minTimestamp": minTimestamp,
		"maxTimestamp": maxTimestamp,
	})

	// Set metrics for successful completion
	jobBytes = totalBytes
	jobRecords = totalRecords

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
		paths := entry.ParquetPaths
		if len(paths) == 0 {
			if entry.ParquetPath == "" {
				return nil, fmt.Errorf("parquet entry missing path for stream %s", entry.StreamID)
			}
			paths = []string{entry.ParquetPath}
		}

		for _, path := range paths {
			key := objectstore.NormalizeKey(path)
			rc, err := c.objectStore.Get(ctx, key)
			if err != nil {
				return nil, err
			}

			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return nil, err
			}

			reader := parquet.NewReader(bytes.NewReader(data))
			schema := reader.Schema()
			rowBuf := make([]parquet.Row, 1)
			for {
				n, readErr := reader.ReadRows(rowBuf)
				if n > 0 {
					rowMap := make(map[string]any)
					if err := schema.Reconstruct(&rowMap, rowBuf[0]); err != nil {
						reader.Close()
						return nil, err
					}
					rec, err := recordFromMap(rowMap, partition)
					if err != nil {
						reader.Close()
						return nil, err
					}
					records = append(records, rec)
				}
				if readErr != nil {
					if readErr == io.EOF {
						break
					}
					reader.Close()
					return nil, readErr
				}
			}
			reader.Close()
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Offset < records[j].Offset
	})

	return records, nil
}

var baseRecordFields = map[string]struct{}{
	"partition":      {},
	"offset":         {},
	"timestamp":      {},
	"key":            {},
	"value":          {},
	"headers":        {},
	"producer_id":    {},
	"producer_epoch": {},
	"base_sequence":  {},
	"attributes":     {},
	"record_crc":     {},
}

func recordFromMap(row map[string]any, partition int32) (worker.Record, error) {
	rec := worker.Record{
		Partition: partition,
	}

	if value, ok := row["offset"]; ok {
		offset, ok := coerceInt64(value)
		if !ok {
			return worker.Record{}, fmt.Errorf("invalid offset value")
		}
		rec.Offset = offset
	} else {
		return worker.Record{}, fmt.Errorf("missing offset value")
	}

	if value, ok := row["timestamp"]; ok {
		ts, ok := coerceTimestampMs(value)
		if !ok {
			return worker.Record{}, fmt.Errorf("invalid timestamp value")
		}
		rec.Timestamp = ts
	} else {
		return worker.Record{}, fmt.Errorf("missing timestamp value")
	}

	if value, ok := row["partition"]; ok {
		if parsed, ok := coerceInt32(value); ok {
			rec.Partition = parsed
		}
	}

	rec.Key = coerceBytes(row["key"])
	rec.Value = coerceBytes(row["value"])
	rec.Headers = coerceHeaders(row["headers"])
	rec.ProducerID = coerceOptionalInt64(row["producer_id"])
	rec.ProducerEpoch = coerceOptionalInt32(row["producer_epoch"])
	rec.BaseSequence = coerceOptionalInt32(row["base_sequence"])
	rec.Attributes = coerceInt32Default(row["attributes"])
	rec.RecordCRC = coerceOptionalInt32(row["record_crc"])

	projected := make(map[string]any)
	for key, value := range row {
		if _, ok := baseRecordFields[key]; ok {
			continue
		}
		projected[key] = normalizeProjectedValue(key, value)
	}
	if len(projected) > 0 {
		rec.Projected = projected
	}

	return rec, nil
}

func normalizeProjectedValue(key string, value any) any {
	if !strings.HasSuffix(key, "_at") {
		return value
	}
	switch v := value.(type) {
	case time.Time:
		return v.UnixMilli()
	case *time.Time:
		if v == nil {
			return nil
		}
		return v.UnixMilli()
	case int64:
		return normalizeParquetTimestamp(v)
	case float64:
		return normalizeParquetTimestamp(int64(v))
	default:
		return value
	}
}

func coerceInt64(value any) (int64, bool) {
	const maxInt64 = int64(^uint64(0) >> 1)
	switch v := value.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case uint64:
		if v > uint64(maxInt64) {
			return 0, false
		}
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		return 0, false
	}
}

func coerceInt32(value any) (int32, bool) {
	parsed, ok := coerceInt64(value)
	if !ok {
		return 0, false
	}
	if parsed > int64(int32(^uint32(0)>>1)) || parsed < int64(-int32(^uint32(0)>>1)-1) {
		return 0, false
	}
	return int32(parsed), true
}

func coerceInt32Default(value any) int32 {
	if parsed, ok := coerceInt32(value); ok {
		return parsed
	}
	return 0
}

func coerceOptionalInt64(value any) *int64 {
	if value == nil {
		return nil
	}
	if parsed, ok := coerceInt64(value); ok {
		return &parsed
	}
	return nil
}

func coerceOptionalInt32(value any) *int32 {
	if value == nil {
		return nil
	}
	if parsed, ok := coerceInt32(value); ok {
		return &parsed
	}
	return nil
}

func coerceTimestampMs(value any) (int64, bool) {
	switch v := value.(type) {
	case time.Time:
		return v.UnixMilli(), true
	case *time.Time:
		if v == nil {
			return 0, false
		}
		return v.UnixMilli(), true
	default:
		parsed, ok := coerceInt64(value)
		if !ok {
			return 0, false
		}
		return normalizeParquetTimestamp(parsed), true
	}
}

func normalizeParquetTimestamp(value int64) int64 {
	return value / parquetTimestampScale
}

func scaleParquetTimestamp(value int64) int64 {
	return value * parquetTimestampScale
}

// buildDataFileStats creates DataFileStats, including projected field bounds if available.
func buildDataFileStats(
	partition int32,
	minOffset, maxOffset int64,
	minTs, maxTs int64,
	recordCount int64,
	schema *iceberg.Schema,
	projectedBounds map[string]worker.FieldBounds,
) *catalog.DataFileStats {
	// Convert worker.FieldBounds to catalog.ProjectedFieldBounds
	if schema != nil && len(projectedBounds) > 0 {
		catalogBounds := make(map[string]catalog.ProjectedFieldBounds, len(projectedBounds))
		for name, bounds := range projectedBounds {
			catalogBounds[name] = catalog.ProjectedFieldBounds{
				Min: bounds.Min,
				Max: bounds.Max,
			}
		}
		return catalog.DataFileStatsWithProjections(
			partition, minOffset, maxOffset, minTs, maxTs, recordCount,
			schema, catalogBounds,
		)
	}
	return catalog.DefaultDataFileStats(partition, minOffset, maxOffset, minTs, maxTs, recordCount)
}

func coerceBytes(value any) []byte {
	switch v := value.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return nil
	}
}

func coerceHeaders(value any) []worker.Header {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []worker.Header:
		return v
	case []map[string]any:
		headers := make([]worker.Header, 0, len(v))
		for _, item := range v {
			headers = append(headers, headerFromMap(item))
		}
		return headers
	case []any:
		headers := make([]worker.Header, 0, len(v))
		for _, item := range v {
			switch typed := item.(type) {
			case map[string]any:
				headers = append(headers, headerFromMap(typed))
			case map[string]string:
				headers = append(headers, worker.Header{
					Key:   typed["key"],
					Value: []byte(typed["value"]),
				})
			}
		}
		return headers
	default:
		return nil
	}
}

func headerFromMap(item map[string]any) worker.Header {
	return worker.Header{
		Key:   coerceString(item["key"]),
		Value: coerceBytes(item["value"]),
	}
}

func coerceString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return ""
	}
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

			paths := job.ParquetPaths
			if len(paths) == 0 && job.ParquetPath != "" {
				paths = []string{job.ParquetPath}
			}
			if len(paths) == 0 {
				c.logger.Warnf("recovery: missing parquet paths", map[string]any{"streamId": job.StreamID, "jobId": job.JobID})
				return
			}

			dataFiles := make([]catalog.DataFile, 0, len(paths))
			if len(paths) == 1 {
				minTimestampMicros := scaleParquetTimestamp(job.ParquetMinTimestampMs)
				maxTimestampMicros := scaleParquetTimestamp(job.ParquetMaxTimestampMs)
				stats := catalog.DefaultDataFileStats(streamMeta.Partition, job.SourceStartOffset, job.SourceEndOffset-1,
					minTimestampMicros, maxTimestampMicros, job.ParquetRecordCount)
				dataFiles = append(dataFiles, catalog.BuildDataFileFromStats(paths[0], streamMeta.Partition,
					job.ParquetRecordCount, job.ParquetSizeBytes, stats))
			} else {
				for _, path := range paths {
					key := objectstore.NormalizeKey(path)
					rc, err := c.objectStore.Get(ctx, key)
					if err != nil {
						c.logger.Warnf("recovery: parquet read failed", map[string]any{"error": err.Error(), "path": path})
						return
					}

					data, err := io.ReadAll(rc)
					rc.Close()
					if err != nil {
						c.logger.Warnf("recovery: parquet read failed", map[string]any{"error": err.Error(), "path": path})
						return
					}

					parquetFile, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
					if err != nil {
						c.logger.Warnf("recovery: parquet metadata read failed", map[string]any{"error": err.Error(), "path": path})
						return
					}

					recordCount := parquetFile.NumRows()
					dataFiles = append(dataFiles, catalog.BuildDataFileFromStats(path, streamMeta.Partition,
						recordCount, int64(len(data)), nil))
				}
			}

			removedFiles, err := c.rewriteRemovedParquetFiles(ctx, job)
			if err != nil {
				c.logger.Warnf("recovery: list parquet rewrite sources failed", map[string]any{"error": err.Error()})
				return
			}

			if len(removedFiles) > 0 {
				replaceResult, err := c.icebergAppender.ReplaceFilesForStream(ctx, streamMeta.TopicName, job.JobID, dataFiles, removedFiles)
				if err != nil {
					c.logger.Warnf("recovery: iceberg replace failed", map[string]any{"error": err.Error()})
					return
				}
				job, err = c.sagaManager.MarkIcebergCommitted(ctx, job.StreamID, job.JobID, replaceResult.Snapshot.SnapshotID)
				if err != nil {
					return
				}
			} else {
				appendResult, err := c.icebergAppender.AppendFilesForStream(ctx, streamMeta.TopicName, job.JobID, dataFiles)
				if err != nil {
					c.logger.Warnf("recovery: iceberg commit failed", map[string]any{"error": err.Error()})
					return
				}
				job, err = c.sagaManager.MarkIcebergCommitted(ctx, job.StreamID, job.JobID, appendResult.Snapshot.SnapshotID)
				if err != nil {
					return
				}
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
		swapResult, err := c.indexSwapper.SwapFromJob(ctx, job, metaDomain)
		if err != nil {
			if errors.Is(err, compaction.ErrNoEntriesToSwap) {
				c.logger.Warnf("recovery: no index entries to swap, marking job failed", map[string]any{
					"streamId": job.StreamID,
					"jobId":    job.JobID,
				})
				c.sagaManager.MarkFailed(ctx, job.StreamID, job.JobID, "recovery: no index entries to swap")
				return
			}
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
		// Index swapped, need to mark WAL GC ready then done
		job, err := c.sagaManager.MarkWALGCReady(ctx, job.StreamID, job.JobID)
		if err != nil {
			return
		}
		c.sagaManager.MarkDone(ctx, job.StreamID, job.JobID)

	case compaction.JobStateWALGCReady:
		// WAL GC ready, just need to mark done
		c.sagaManager.MarkDone(ctx, job.StreamID, job.JobID)

	case compaction.JobStateDone, compaction.JobStateFailed:
		// Terminal states
	}
}

func (c *Compactor) rewriteRemovedParquetFiles(ctx context.Context, job *compaction.Job) ([]catalog.DataFile, error) {
	startKey, err := keys.OffsetIndexStartKey(job.StreamID, job.SourceStartOffset+1)
	if err != nil {
		return nil, fmt.Errorf("build start key: %w", err)
	}
	endKey, err := keys.OffsetIndexStartKey(job.StreamID, job.SourceEndOffset+1)
	if err != nil {
		return nil, fmt.Errorf("build end key: %w", err)
	}

	kvs, err := c.metaStore.List(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, fmt.Errorf("list index entries: %w", err)
	}

	removed := make([]catalog.DataFile, 0)
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			return nil, fmt.Errorf("unmarshal index entry: %w", err)
		}
		if entry.FileType != index.FileTypeParquet {
			continue
		}
		if entry.StartOffset < job.SourceStartOffset || entry.EndOffset > job.SourceEndOffset {
			continue
		}

		paths := entry.ParquetPaths
		if len(paths) == 0 && entry.ParquetPath != "" {
			paths = []string{entry.ParquetPath}
		}
		for _, path := range paths {
			removed = append(removed, catalog.DataFile{Path: path})
		}
	}

	return removed, nil
}

// HealthServerAddr returns the address the health server is listening on, or empty if not yet listening.
// This method is thread-safe.
func (c *Compactor) HealthServerAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.healthServer == nil {
		return ""
	}
	return c.healthServer.Addr()
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

	if c.parquetGCWorker != nil {
		c.parquetGCWorker.Stop()
	}
	if c.walOrphanGCWorker != nil {
		c.walOrphanGCWorker.Stop()
	}
	if c.walGCWorker != nil {
		c.walGCWorker.Stop()
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
