package main

import (
	"context"
	"fmt"
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
        opts           CompactorOptions
        logger         *logging.Logger
        metaStore      metadata.MetadataStore
        objectStore    objectstore.Store
        topicStore     *topics.Store
        streamManager  *index.StreamManager
        planner        *planner.Planner
        lockManager    *compaction.LockManager
        sagaManager    *compaction.SagaManager
        indexSwapper   *compaction.IndexSwapper
        icebergCatalog catalog.Catalog
        icebergAppender *catalog.Appender
        icebergChecker *compaction.IcebergChecker
        converter      *worker.Converter
        healthServer   *server.HealthServer

        mu        sync.Mutex
        started   bool
        stopCh    chan struct{}
        stoppedCh chan struct{}
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

	

	                cat, err := catalog.NewRestCatalog(catalog.RestCatalogConfig{

	                        URI:       cfg.Iceberg.CatalogURI,

	                        Warehouse: cfg.Iceberg.Warehouse,

	                })

	                if err != nil {

	                        return fmt.Errorf("failed to create Iceberg catalog: %w", err)

	                }

	                c.icebergCatalog = cat

	                c.icebergAppender = catalog.NewAppender(catalog.DefaultAppenderConfig(cat))

	        }

	

	        // Create Iceberg checker

	        c.icebergChecker = compaction.NewIcebergChecker(

	                compaction.TopicConfigProviderFunc(c.topicStore.GetTopicConfig),

	                compaction.StreamMetaProviderFunc(c.streamManager.GetStreamMeta),

	                cfg.Iceberg.Enabled,

	        )

	

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

	// Plan new compaction
	plan, err := c.planner.Plan(ctx, streamID)
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
		"startOffset": plan.StartOffset,
		"endOffset":   plan.EndOffset,
		"entryCount":  len(plan.Entries),
		"totalBytes":  plan.TotalSizeBytes,
	})

	// Execute compaction
	if err := c.executeCompaction(ctx, plan, topicName, partition); err != nil {
		c.logger.Errorf("compaction failed", map[string]any{
			"streamId": streamID,
			"error":    err.Error(),
		})
	}
}

// executeCompaction runs the full compaction saga for a planned job.
func (c *Compactor) executeCompaction(ctx context.Context, plan *planner.Result, topicName string, partition int32) error {
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

	// Generate Parquet path
	parquetPath := fmt.Sprintf("parquet/%s/%s.parquet", plan.StreamID, job.JobID)

	// Write Parquet to object storage
	if c.objectStore != nil {
		if err := c.converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
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

	                

	                        // Check if Iceberg is enabled for this stream

	                        icebergEnabled, err := c.icebergChecker.IsIcebergEnabled(ctx, plan.StreamID)

	                        if err != nil {

	                                return fmt.Errorf("check iceberg enabled: %w", err)

	                        }

	                

	                        if icebergEnabled && c.icebergAppender != nil {

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
                icebergEnabled, err := c.icebergChecker.IsIcebergEnabled(ctx, job.StreamID)
                if err != nil {
                        c.logger.Warnf("recovery: check iceberg enabled failed", map[string]any{"error": err.Error()})
                        return
                }

                if icebergEnabled && c.icebergAppender != nil {
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
                        // Skip Iceberg
                        job, err = c.sagaManager.SkipIcebergCommit(ctx, job.StreamID, job.JobID, nil)
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
