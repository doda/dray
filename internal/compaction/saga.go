// Package compaction implements stream compaction for the Dray broker.
// This file implements the compaction saga state machine with durable progress markers.
//
// The saga pattern ensures that compaction jobs can be recovered and completed
// from any intermediate state after a crash. Each state transition is persisted
// atomically using CAS operations before proceeding to the next step.
//
// Key format: /dray/v1/compaction/<streamId>/jobs/<jobId>
//
// State machine:
//
//	CREATED -> PARQUET_WRITTEN -> ICEBERG_COMMITTED -> INDEX_SWAPPED -> DONE
//
// Recovery:
//   - CREATED: Restart from the beginning (re-read WAL, re-write Parquet)
//   - PARQUET_WRITTEN: Resume at Iceberg commit (Parquet file exists)
//   - ICEBERG_COMMITTED: Resume at index swap (Iceberg table updated)
//   - INDEX_SWAPPED: Resume at marking done (index updated)
//   - DONE: Job complete, cleanup
package compaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dray-io/dray/internal/gc"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/google/uuid"
)

// JobState represents the current state of a compaction job in the saga.
type JobState string

const (
	// JobStateCreated indicates the job has been created but Parquet file
	// has not yet been written to object storage.
	JobStateCreated JobState = "CREATED"

	// JobStateParquetWritten indicates the Parquet file has been written
	// to object storage but not yet committed to Iceberg catalog.
	JobStateParquetWritten JobState = "PARQUET_WRITTEN"

	// JobStateIcebergCommitted indicates the Parquet file has been committed
	// to the Iceberg catalog but the offset index has not yet been swapped.
	JobStateIcebergCommitted JobState = "ICEBERG_COMMITTED"

	// JobStateIndexSwapped indicates the offset index has been swapped from
	// WAL entries to the Parquet entry. WAL refcounts may be decremented.
	JobStateIndexSwapped JobState = "INDEX_SWAPPED"

	// JobStateDone indicates the compaction job is complete.
	// The job record can be cleaned up after a grace period.
	JobStateDone JobState = "DONE"

	// JobStateFailed indicates the job failed and cannot be retried.
	// This is a terminal state for jobs that encounter unrecoverable errors.
	JobStateFailed JobState = "FAILED"
)

// Job-related errors.
var (
	// ErrJobNotFound is returned when a job does not exist.
	ErrJobNotFound = errors.New("compaction: job not found")

	// ErrInvalidJobID is returned when a job ID is empty or invalid.
	ErrInvalidJobID = errors.New("compaction: invalid job ID")

	// ErrInvalidTransition is returned when a state transition is not allowed.
	ErrInvalidTransition = errors.New("compaction: invalid state transition")

	// ErrJobAlreadyExists is returned when trying to create a job that already exists.
	ErrJobAlreadyExists = errors.New("compaction: job already exists")

	// ErrConcurrentModification is returned when a job was modified by another process.
	ErrConcurrentModification = errors.New("compaction: concurrent modification")
)

// Job represents a compaction job with its current state and progress markers.
// Jobs are persisted to the metadata store and can be recovered after crashes.
type Job struct {
	// JobID is the unique identifier for this compaction job.
	JobID string `json:"jobId"`

	// StreamID is the stream being compacted.
	StreamID string `json:"streamId"`

	// State is the current saga state.
	State JobState `json:"state"`

	// CreatedAtMs is when the job was created (unix milliseconds).
	CreatedAtMs int64 `json:"createdAtMs"`

	// UpdatedAtMs is when the job was last updated (unix milliseconds).
	UpdatedAtMs int64 `json:"updatedAtMs"`

	// CompactorID is the compactor that owns this job.
	CompactorID string `json:"compactorId"`

	// ---- Planning Phase Fields ----

	// SourceStartOffset is the starting offset of WAL entries being compacted.
	SourceStartOffset int64 `json:"sourceStartOffset"`

	// SourceEndOffset is the ending offset (exclusive) of WAL entries being compacted.
	SourceEndOffset int64 `json:"sourceEndOffset"`

	// SourceWALCount is the number of WAL entries being compacted.
	SourceWALCount int `json:"sourceWALCount"`

	// SourceSizeBytes is the total size of source WAL entries.
	SourceSizeBytes int64 `json:"sourceSizeBytes"`

	// ---- Parquet Write Phase Fields ----

	// ParquetPath is the object storage path of the written Parquet file.
	// Set when state transitions to PARQUET_WRITTEN.
	ParquetPath string `json:"parquetPath,omitempty"`

	// ParquetSizeBytes is the size of the Parquet file.
	ParquetSizeBytes int64 `json:"parquetSizeBytes,omitempty"`

	// ParquetRecordCount is the number of records in the Parquet file.
	ParquetRecordCount int64 `json:"parquetRecordCount,omitempty"`

	// ParquetMinTimestampMs is the minimum timestamp in the Parquet file.
	ParquetMinTimestampMs int64 `json:"parquetMinTimestampMs,omitempty"`

	// ParquetMaxTimestampMs is the maximum timestamp in the Parquet file.
	ParquetMaxTimestampMs int64 `json:"parquetMaxTimestampMs,omitempty"`

	// ---- Iceberg Commit Phase Fields ----

	// IcebergSnapshotID is the Iceberg snapshot ID after commit.
	// Set when state transitions to ICEBERG_COMMITTED.
	IcebergSnapshotID int64 `json:"icebergSnapshotId,omitempty"`

	// IcebergDataFileID is the Iceberg data file reference.
	// Set when Iceberg commit succeeds in duality mode per SPEC 6.3.3.
	// This is typically the ParquetPath registered with Iceberg.
	IcebergDataFileID string `json:"icebergDataFileId,omitempty"`

	// ---- Index Swap Phase Fields ----

	// WALObjectsToDecrement contains WAL object IDs whose refcounts need decrementing.
	// Populated during planning, used during INDEX_SWAPPED cleanup.
	WALObjectsToDecrement []string `json:"walObjectsToDecrement,omitempty"`

	// ParquetGCCandidates are old Parquet files eligible for GC after the job reaches DONE.
	ParquetGCCandidates []ParquetGCCandidate `json:"parquetGcCandidates,omitempty"`

	// ParquetGCGracePeriodMs is the grace period applied when scheduling Parquet GC at DONE.
	ParquetGCGracePeriodMs int64 `json:"parquetGcGracePeriodMs,omitempty"`

	// ---- Error Handling Fields ----

	// ErrorMessage contains the error message if the job failed.
	ErrorMessage string `json:"errorMessage,omitempty"`

	// RetryCount is the number of times this job has been retried.
	RetryCount int `json:"retryCount,omitempty"`
}

// ParquetGCCandidate describes a Parquet file eligible for GC once compaction is DONE.
type ParquetGCCandidate struct {
	Path                    string `json:"path"`
	CreatedAtMs             int64  `json:"createdAtMs"`
	SizeBytes               int64  `json:"sizeBytes"`
	IcebergEnabled          bool   `json:"icebergEnabled,omitempty"`
	IcebergRemovalConfirmed bool   `json:"icebergRemovalConfirmed,omitempty"`
}

// NewJobID generates a new unique job ID.
func NewJobID() string {
	return uuid.New().String()
}

// IsTerminal returns true if the job is in a terminal state (DONE or FAILED).
func (j *Job) IsTerminal() bool {
	return j.State == JobStateDone || j.State == JobStateFailed
}

// IsRecoverable returns true if the job can be recovered and resumed.
func (j *Job) IsRecoverable() bool {
	switch j.State {
	case JobStateCreated, JobStateParquetWritten, JobStateIcebergCommitted, JobStateIndexSwapped:
		return true
	default:
		return false
	}
}

// validTransitions defines allowed state transitions.
var validTransitions = map[JobState][]JobState{
	JobStateCreated:          {JobStateParquetWritten, JobStateFailed},
	JobStateParquetWritten:   {JobStateIcebergCommitted, JobStateFailed},
	JobStateIcebergCommitted: {JobStateIndexSwapped, JobStateFailed},
	JobStateIndexSwapped:     {JobStateDone, JobStateFailed},
	JobStateDone:             {}, // Terminal state
	JobStateFailed:           {}, // Terminal state
}

// CanTransitionTo checks if transitioning from current state to newState is valid.
func (j *Job) CanTransitionTo(newState JobState) bool {
	allowed, ok := validTransitions[j.State]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == newState {
			return true
		}
	}
	return false
}

// SagaManager manages compaction job lifecycle with durable state persistence.
// It ensures that jobs can be recovered and completed from any intermediate state.
type SagaManager struct {
	meta        metadata.MetadataStore
	compactorID string
}

// NewSagaManager creates a new saga manager for compaction jobs.
func NewSagaManager(meta metadata.MetadataStore, compactorID string) *SagaManager {
	return &SagaManager{
		meta:        meta,
		compactorID: compactorID,
	}
}

// CreateJob creates a new compaction job in CREATED state.
// The job is persisted atomically with ExpectNotExists to prevent duplicates.
func (sm *SagaManager) CreateJob(ctx context.Context, streamID string, opts ...CreateJobOption) (*Job, error) {
	if streamID == "" {
		return nil, ErrInvalidStreamID
	}

	var options createJobOptions
	for _, opt := range opts {
		opt(&options)
	}

	jobID := options.jobID
	if jobID == "" {
		jobID = NewJobID()
	}

	now := time.Now().UnixMilli()
	job := &Job{
		JobID:             jobID,
		StreamID:          streamID,
		State:             JobStateCreated,
		CreatedAtMs:       now,
		UpdatedAtMs:       now,
		CompactorID:       sm.compactorID,
		SourceStartOffset: options.sourceStartOffset,
		SourceEndOffset:   options.sourceEndOffset,
		SourceWALCount:    options.sourceWALCount,
		SourceSizeBytes:   options.sourceSizeBytes,
	}

	jobData, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("compaction: marshal job: %w", err)
	}

	key := keys.CompactionJobKeyPath(streamID, jobID)

	// Use a transaction to check for existence and create atomically
	err = sm.meta.Txn(ctx, key, func(txn metadata.Txn) error {
		_, _, getErr := txn.Get(key)
		if getErr == nil {
			// Key exists
			return ErrJobAlreadyExists
		}
		if !errors.Is(getErr, metadata.ErrKeyNotFound) {
			return fmt.Errorf("compaction: check job exists: %w", getErr)
		}
		txn.Put(key, jobData)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrJobAlreadyExists) {
			return nil, ErrJobAlreadyExists
		}
		return nil, fmt.Errorf("compaction: create job: %w", err)
	}

	return job, nil
}

// createJobOptions holds options for CreateJob.
type createJobOptions struct {
	jobID             string
	sourceStartOffset int64
	sourceEndOffset   int64
	sourceWALCount    int
	sourceSizeBytes   int64
}

// CreateJobOption configures job creation.
type CreateJobOption func(*createJobOptions)

// WithJobID sets a specific job ID instead of generating one.
func WithJobID(jobID string) CreateJobOption {
	return func(o *createJobOptions) {
		o.jobID = jobID
	}
}

// WithSourceRange sets the source offset range for the job.
func WithSourceRange(startOffset, endOffset int64) CreateJobOption {
	return func(o *createJobOptions) {
		o.sourceStartOffset = startOffset
		o.sourceEndOffset = endOffset
	}
}

// WithSourceWALCount sets the number of WAL entries being compacted.
func WithSourceWALCount(count int) CreateJobOption {
	return func(o *createJobOptions) {
		o.sourceWALCount = count
	}
}

// WithSourceSizeBytes sets the total source size.
func WithSourceSizeBytes(size int64) CreateJobOption {
	return func(o *createJobOptions) {
		o.sourceSizeBytes = size
	}
}

// GetJob retrieves a compaction job by ID.
func (sm *SagaManager) GetJob(ctx context.Context, streamID, jobID string) (*Job, metadata.Version, error) {
	if streamID == "" {
		return nil, 0, ErrInvalidStreamID
	}
	if jobID == "" {
		return nil, 0, ErrInvalidJobID
	}

	key := keys.CompactionJobKeyPath(streamID, jobID)
	result, err := sm.meta.Get(ctx, key)
	if err != nil {
		return nil, 0, fmt.Errorf("compaction: get job: %w", err)
	}
	if !result.Exists {
		return nil, 0, ErrJobNotFound
	}

	var job Job
	if err := json.Unmarshal(result.Value, &job); err != nil {
		return nil, 0, fmt.Errorf("compaction: unmarshal job: %w", err)
	}

	return &job, result.Version, nil
}

// TransitionState atomically transitions a job to a new state.
// This is the core saga operation - it persists state changes with CAS semantics
// to ensure exactly-once progress even across crashes.
//
// The updateFn is called with the current job to allow setting state-specific fields
// before the transition is persisted. If updateFn is nil, only the state is changed.
func (sm *SagaManager) TransitionState(ctx context.Context, streamID, jobID string, newState JobState, updateFn func(*Job) error) (*Job, error) {
	if streamID == "" {
		return nil, ErrInvalidStreamID
	}
	if jobID == "" {
		return nil, ErrInvalidJobID
	}

	key := keys.CompactionJobKeyPath(streamID, jobID)

	// Read current state
	result, err := sm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("compaction: get job for transition: %w", err)
	}
	if !result.Exists {
		return nil, ErrJobNotFound
	}

	var job Job
	if err := json.Unmarshal(result.Value, &job); err != nil {
		return nil, fmt.Errorf("compaction: unmarshal job: %w", err)
	}

	// Validate transition
	if !job.CanTransitionTo(newState) {
		return nil, fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidTransition, job.State, newState)
	}

	// Apply updates
	job.State = newState
	job.UpdatedAtMs = time.Now().UnixMilli()

	if updateFn != nil {
		if err := updateFn(&job); err != nil {
			return nil, fmt.Errorf("compaction: update job fields: %w", err)
		}
	}

	// Persist atomically with version check
	jobData, err := json.Marshal(&job)
	if err != nil {
		return nil, fmt.Errorf("compaction: marshal updated job: %w", err)
	}

	_, err = sm.meta.Put(ctx, key, jobData, metadata.WithExpectedVersion(result.Version))
	if err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) {
			return nil, ErrConcurrentModification
		}
		return nil, fmt.Errorf("compaction: persist transition: %w", err)
	}

	return &job, nil
}

// MarkParquetWritten transitions a job to PARQUET_WRITTEN state.
func (sm *SagaManager) MarkParquetWritten(ctx context.Context, streamID, jobID, parquetPath string, sizeBytes, recordCount, minTimestamp, maxTimestamp int64) (*Job, error) {
	return sm.TransitionState(ctx, streamID, jobID, JobStateParquetWritten, func(j *Job) error {
		j.ParquetPath = parquetPath
		j.ParquetSizeBytes = sizeBytes
		j.ParquetRecordCount = recordCount
		j.ParquetMinTimestampMs = minTimestamp
		j.ParquetMaxTimestampMs = maxTimestamp
		return nil
	})
}

// MarkIcebergCommitted transitions a job to ICEBERG_COMMITTED state.
func (sm *SagaManager) MarkIcebergCommitted(ctx context.Context, streamID, jobID string, snapshotID int64) (*Job, error) {
	return sm.MarkIcebergCommittedWithDataFileID(ctx, streamID, jobID, snapshotID, "")
}

// MarkIcebergCommittedWithDataFileID transitions a job to ICEBERG_COMMITTED state
// and records the Iceberg data file ID per SPEC 6.3.3.
func (sm *SagaManager) MarkIcebergCommittedWithDataFileID(ctx context.Context, streamID, jobID string, snapshotID int64, dataFileID string) (*Job, error) {
	return sm.TransitionState(ctx, streamID, jobID, JobStateIcebergCommitted, func(j *Job) error {
		j.IcebergSnapshotID = snapshotID
		if dataFileID != "" {
			j.IcebergDataFileID = dataFileID
		}
		return nil
	})
}

// MarkIndexSwapped transitions a job to INDEX_SWAPPED state.
func (sm *SagaManager) MarkIndexSwapped(ctx context.Context, streamID, jobID string, walObjectsToDecrement []string) (*Job, error) {
	return sm.MarkIndexSwappedWithGC(ctx, streamID, jobID, walObjectsToDecrement, nil, 0)
}

// MarkIndexSwappedWithGC transitions a job to INDEX_SWAPPED and records Parquet GC candidates.
func (sm *SagaManager) MarkIndexSwappedWithGC(ctx context.Context, streamID, jobID string, walObjectsToDecrement []string, parquetGCCandidates []ParquetGCCandidate, parquetGCGracePeriodMs int64) (*Job, error) {
	return sm.TransitionState(ctx, streamID, jobID, JobStateIndexSwapped, func(j *Job) error {
		j.WALObjectsToDecrement = walObjectsToDecrement
		if len(parquetGCCandidates) > 0 {
			j.ParquetGCCandidates = parquetGCCandidates
		}
		if parquetGCGracePeriodMs > 0 {
			j.ParquetGCGracePeriodMs = parquetGCGracePeriodMs
		}
		return nil
	})
}

// MarkDone transitions a job to DONE state.
func (sm *SagaManager) MarkDone(ctx context.Context, streamID, jobID string) (*Job, error) {
	job, err := sm.TransitionState(ctx, streamID, jobID, JobStateDone, nil)
	if err != nil {
		return nil, err
	}

	if len(job.ParquetGCCandidates) == 0 {
		return job, nil
	}

	gracePeriodMs := job.ParquetGCGracePeriodMs
	if gracePeriodMs <= 0 {
		gracePeriodMs = defaultParquetGCGracePeriodMs
	}
	deleteAfterMs := time.Now().UnixMilli() + gracePeriodMs

	for _, candidate := range job.ParquetGCCandidates {
		gcRecord := gc.ParquetGCRecord{
			Path:                    candidate.Path,
			DeleteAfterMs:           deleteAfterMs,
			CreatedAt:               candidate.CreatedAtMs,
			SizeBytes:               candidate.SizeBytes,
			StreamID:                job.StreamID,
			IcebergEnabled:          candidate.IcebergEnabled,
			IcebergRemovalConfirmed: candidate.IcebergRemovalConfirmed,
		}
		if err := gc.ScheduleParquetGC(ctx, sm.meta, gcRecord); err != nil {
			continue
		}
	}

	return job, nil
}

// MarkFailed transitions a job to FAILED state with an error message.
func (sm *SagaManager) MarkFailed(ctx context.Context, streamID, jobID, errorMessage string) (*Job, error) {
	return sm.TransitionState(ctx, streamID, jobID, JobStateFailed, func(j *Job) error {
		j.ErrorMessage = errorMessage
		return nil
	})
}

// IncrementRetryCount increments the retry count for a job without changing state.
func (sm *SagaManager) IncrementRetryCount(ctx context.Context, streamID, jobID string) (*Job, error) {
	if streamID == "" {
		return nil, ErrInvalidStreamID
	}
	if jobID == "" {
		return nil, ErrInvalidJobID
	}

	key := keys.CompactionJobKeyPath(streamID, jobID)

	result, err := sm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("compaction: get job: %w", err)
	}
	if !result.Exists {
		return nil, ErrJobNotFound
	}

	var job Job
	if err := json.Unmarshal(result.Value, &job); err != nil {
		return nil, fmt.Errorf("compaction: unmarshal job: %w", err)
	}

	job.RetryCount++
	job.UpdatedAtMs = time.Now().UnixMilli()

	jobData, err := json.Marshal(&job)
	if err != nil {
		return nil, fmt.Errorf("compaction: marshal job: %w", err)
	}

	_, err = sm.meta.Put(ctx, key, jobData, metadata.WithExpectedVersion(result.Version))
	if err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) {
			return nil, ErrConcurrentModification
		}
		return nil, fmt.Errorf("compaction: persist retry count: %w", err)
	}

	return &job, nil
}

// ListJobs returns all jobs for a stream.
func (sm *SagaManager) ListJobs(ctx context.Context, streamID string) ([]*Job, error) {
	if streamID == "" {
		return nil, ErrInvalidStreamID
	}

	prefix := keys.CompactionJobsForStreamPrefix(streamID)
	entries, err := sm.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("compaction: list jobs: %w", err)
	}

	jobs := make([]*Job, 0, len(entries))
	for _, kv := range entries {
		var job Job
		if err := json.Unmarshal(kv.Value, &job); err != nil {
			return nil, fmt.Errorf("compaction: unmarshal job at %s: %w", kv.Key, err)
		}
		jobs = append(jobs, &job)
	}

	return jobs, nil
}

// ListIncompleteJobs returns all non-terminal jobs for a stream.
// Use this for recovery after restart.
func (sm *SagaManager) ListIncompleteJobs(ctx context.Context, streamID string) ([]*Job, error) {
	jobs, err := sm.ListJobs(ctx, streamID)
	if err != nil {
		return nil, err
	}

	incomplete := make([]*Job, 0)
	for _, job := range jobs {
		if !job.IsTerminal() {
			incomplete = append(incomplete, job)
		}
	}

	return incomplete, nil
}

// DeleteJob removes a job from the metadata store.
// This should only be called for terminal jobs after a grace period.
func (sm *SagaManager) DeleteJob(ctx context.Context, streamID, jobID string) error {
	if streamID == "" {
		return ErrInvalidStreamID
	}
	if jobID == "" {
		return ErrInvalidJobID
	}

	key := keys.CompactionJobKeyPath(streamID, jobID)
	if err := sm.meta.Delete(ctx, key); err != nil {
		return fmt.Errorf("compaction: delete job: %w", err)
	}

	return nil
}

// CleanupCompletedJobs removes all DONE jobs for a stream that are older than maxAge.
func (sm *SagaManager) CleanupCompletedJobs(ctx context.Context, streamID string, maxAge time.Duration) (int, error) {
	jobs, err := sm.ListJobs(ctx, streamID)
	if err != nil {
		return 0, err
	}

	cutoff := time.Now().Add(-maxAge).UnixMilli()
	deleted := 0

	for _, job := range jobs {
		if job.State == JobStateDone && job.UpdatedAtMs < cutoff {
			if err := sm.DeleteJob(ctx, streamID, job.JobID); err != nil {
				return deleted, err
			}
			deleted++
		}
	}

	return deleted, nil
}

// CompactorID returns the compactor ID of this saga manager.
func (sm *SagaManager) CompactorID() string {
	return sm.compactorID
}

// ResumeJob attempts to resume an incomplete job from its current state.
// Returns the job if it can be resumed, or an error if the job is terminal or not found.
func (sm *SagaManager) ResumeJob(ctx context.Context, streamID, jobID string) (*Job, error) {
	job, _, err := sm.GetJob(ctx, streamID, jobID)
	if err != nil {
		return nil, err
	}

	if job.IsTerminal() {
		return nil, fmt.Errorf("%w: job is in terminal state %s", ErrInvalidTransition, job.State)
	}

	// Increment retry count to track resumption
	return sm.IncrementRetryCount(ctx, streamID, jobID)
}
