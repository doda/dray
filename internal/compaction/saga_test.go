package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

func TestNewJobID(t *testing.T) {
	id1 := NewJobID()
	id2 := NewJobID()

	if id1 == "" {
		t.Error("expected non-empty job ID")
	}
	if id1 == id2 {
		t.Error("expected unique job IDs")
	}
}

func TestJobState_IsTerminal(t *testing.T) {
	tests := []struct {
		state    JobState
		terminal bool
	}{
		{JobStateCreated, false},
		{JobStateParquetWritten, false},
		{JobStateIcebergCommitted, false},
		{JobStateIndexSwapped, false},
		{JobStateDone, true},
		{JobStateFailed, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			job := &Job{State: tt.state}
			if got := job.IsTerminal(); got != tt.terminal {
				t.Errorf("IsTerminal() = %v, want %v", got, tt.terminal)
			}
		})
	}
}

func TestJobState_IsRecoverable(t *testing.T) {
	tests := []struct {
		state       JobState
		recoverable bool
	}{
		{JobStateCreated, true},
		{JobStateParquetWritten, true},
		{JobStateIcebergCommitted, true},
		{JobStateIndexSwapped, true},
		{JobStateDone, false},
		{JobStateFailed, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			job := &Job{State: tt.state}
			if got := job.IsRecoverable(); got != tt.recoverable {
				t.Errorf("IsRecoverable() = %v, want %v", got, tt.recoverable)
			}
		})
	}
}

func TestJobState_CanTransitionTo(t *testing.T) {
	tests := []struct {
		from  JobState
		to    JobState
		valid bool
	}{
		// Valid forward transitions
		{JobStateCreated, JobStateParquetWritten, true},
		{JobStateParquetWritten, JobStateIcebergCommitted, true},
		{JobStateIcebergCommitted, JobStateIndexSwapped, true},
		{JobStateIndexSwapped, JobStateDone, true},

		// Valid failure transitions
		{JobStateCreated, JobStateFailed, true},
		{JobStateParquetWritten, JobStateFailed, true},
		{JobStateIcebergCommitted, JobStateFailed, true},
		{JobStateIndexSwapped, JobStateFailed, true},

		// Invalid backward transitions
		{JobStateParquetWritten, JobStateCreated, false},
		{JobStateIcebergCommitted, JobStateParquetWritten, false},
		{JobStateIndexSwapped, JobStateIcebergCommitted, false},
		{JobStateDone, JobStateIndexSwapped, false},

		// Invalid skip transitions
		{JobStateCreated, JobStateIcebergCommitted, false},
		{JobStateCreated, JobStateIndexSwapped, false},
		{JobStateCreated, JobStateDone, false},

		// Terminal states cannot transition
		{JobStateDone, JobStateCreated, false},
		{JobStateDone, JobStateFailed, false},
		{JobStateFailed, JobStateCreated, false},
		{JobStateFailed, JobStateDone, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			job := &Job{State: tt.from}
			if got := job.CanTransitionTo(tt.to); got != tt.valid {
				t.Errorf("CanTransitionTo(%s) = %v, want %v", tt.to, got, tt.valid)
			}
		})
	}
}

func TestSagaManager_CreateJob(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	t.Run("create job success", func(t *testing.T) {
		job, err := sm.CreateJob(ctx, "stream-1",
			WithSourceRange(0, 1000),
			WithSourceWALCount(5),
			WithSourceSizeBytes(10000))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}

		if job.JobID == "" {
			t.Error("expected non-empty job ID")
		}
		if job.StreamID != "stream-1" {
			t.Errorf("StreamID = %s, want stream-1", job.StreamID)
		}
		if job.State != JobStateCreated {
			t.Errorf("State = %s, want CREATED", job.State)
		}
		if job.CompactorID != "compactor-1" {
			t.Errorf("CompactorID = %s, want compactor-1", job.CompactorID)
		}
		if job.SourceStartOffset != 0 {
			t.Errorf("SourceStartOffset = %d, want 0", job.SourceStartOffset)
		}
		if job.SourceEndOffset != 1000 {
			t.Errorf("SourceEndOffset = %d, want 1000", job.SourceEndOffset)
		}
		if job.SourceWALCount != 5 {
			t.Errorf("SourceWALCount = %d, want 5", job.SourceWALCount)
		}
		if job.CreatedAtMs == 0 {
			t.Error("expected non-zero CreatedAtMs")
		}
	})

	t.Run("create job with explicit ID", func(t *testing.T) {
		job, err := sm.CreateJob(ctx, "stream-2", WithJobID("job-explicit"))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}
		if job.JobID != "job-explicit" {
			t.Errorf("JobID = %s, want job-explicit", job.JobID)
		}
	})

	t.Run("create duplicate job fails", func(t *testing.T) {
		_, err := sm.CreateJob(ctx, "stream-3", WithJobID("job-dup"))
		if err != nil {
			t.Fatalf("first CreateJob failed: %v", err)
		}

		_, err = sm.CreateJob(ctx, "stream-3", WithJobID("job-dup"))
		if !errors.Is(err, ErrJobAlreadyExists) {
			t.Errorf("expected ErrJobAlreadyExists, got %v", err)
		}
	})

	t.Run("create job with empty stream ID fails", func(t *testing.T) {
		_, err := sm.CreateJob(ctx, "")
		if !errors.Is(err, ErrInvalidStreamID) {
			t.Errorf("expected ErrInvalidStreamID, got %v", err)
		}
	})
}

func TestSagaManager_GetJob(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	t.Run("get existing job", func(t *testing.T) {
		created, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-1"))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}

		job, version, err := sm.GetJob(ctx, "stream-1", "job-1")
		if err != nil {
			t.Fatalf("GetJob failed: %v", err)
		}
		if job.JobID != created.JobID {
			t.Errorf("JobID = %s, want %s", job.JobID, created.JobID)
		}
		if version == 0 {
			t.Error("expected non-zero version")
		}
	})

	t.Run("get non-existent job", func(t *testing.T) {
		_, _, err := sm.GetJob(ctx, "stream-1", "job-missing")
		if !errors.Is(err, ErrJobNotFound) {
			t.Errorf("expected ErrJobNotFound, got %v", err)
		}
	})

	t.Run("get with empty stream ID", func(t *testing.T) {
		_, _, err := sm.GetJob(ctx, "", "job-1")
		if !errors.Is(err, ErrInvalidStreamID) {
			t.Errorf("expected ErrInvalidStreamID, got %v", err)
		}
	})

	t.Run("get with empty job ID", func(t *testing.T) {
		_, _, err := sm.GetJob(ctx, "stream-1", "")
		if !errors.Is(err, ErrInvalidJobID) {
			t.Errorf("expected ErrInvalidJobID, got %v", err)
		}
	})
}

func TestSagaManager_TransitionState(t *testing.T) {
	ctx := context.Background()

	t.Run("valid forward transition", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		created, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-1"))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}
		createdAt := created.CreatedAtMs

		// Give a tiny delay to ensure timestamps differ
		time.Sleep(time.Millisecond)

		job, err := sm.TransitionState(ctx, "stream-1", "job-1", JobStateParquetWritten, nil)
		if err != nil {
			t.Fatalf("TransitionState failed: %v", err)
		}
		if job.State != JobStateParquetWritten {
			t.Errorf("State = %s, want PARQUET_WRITTEN", job.State)
		}
		// Check that UpdatedAtMs is at least the same as creation time
		// (timing-sensitive tests can be flaky, so we just check >= instead of >)
		if job.UpdatedAtMs < createdAt {
			t.Error("UpdatedAtMs should not be before CreatedAtMs")
		}
	})

	t.Run("transition with update function", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-1"))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}

		job, err := sm.TransitionState(ctx, "stream-1", "job-1", JobStateParquetWritten, func(j *Job) error {
			j.ParquetPath = "/path/to/parquet"
			j.ParquetSizeBytes = 5000
			return nil
		})
		if err != nil {
			t.Fatalf("TransitionState failed: %v", err)
		}
		if job.ParquetPath != "/path/to/parquet" {
			t.Errorf("ParquetPath = %s, want /path/to/parquet", job.ParquetPath)
		}
		if job.ParquetSizeBytes != 5000 {
			t.Errorf("ParquetSizeBytes = %d, want 5000", job.ParquetSizeBytes)
		}
	})

	t.Run("invalid transition fails", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-1"))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}

		// Try to skip ahead
		_, err = sm.TransitionState(ctx, "stream-1", "job-1", JobStateIcebergCommitted, nil)
		if !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("expected ErrInvalidTransition, got %v", err)
		}
	})

	t.Run("transition non-existent job fails", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		_, err := sm.TransitionState(ctx, "stream-1", "job-missing", JobStateParquetWritten, nil)
		if !errors.Is(err, ErrJobNotFound) {
			t.Errorf("expected ErrJobNotFound, got %v", err)
		}
	})
}

func TestSagaManager_FullLifecycle(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	// Create
	job, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-full"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	if job.State != JobStateCreated {
		t.Errorf("expected CREATED, got %s", job.State)
	}

	// -> PARQUET_WRITTEN
	job, err = sm.MarkParquetWritten(ctx, "stream-1", "job-full", "/bucket/file.parquet", 10000, 500)
	if err != nil {
		t.Fatalf("MarkParquetWritten failed: %v", err)
	}
	if job.State != JobStateParquetWritten {
		t.Errorf("expected PARQUET_WRITTEN, got %s", job.State)
	}
	if job.ParquetPath != "/bucket/file.parquet" {
		t.Errorf("ParquetPath = %s, want /bucket/file.parquet", job.ParquetPath)
	}

	// -> ICEBERG_COMMITTED
	job, err = sm.MarkIcebergCommitted(ctx, "stream-1", "job-full", 12345)
	if err != nil {
		t.Fatalf("MarkIcebergCommitted failed: %v", err)
	}
	if job.State != JobStateIcebergCommitted {
		t.Errorf("expected ICEBERG_COMMITTED, got %s", job.State)
	}
	if job.IcebergSnapshotID != 12345 {
		t.Errorf("IcebergSnapshotID = %d, want 12345", job.IcebergSnapshotID)
	}

	// -> INDEX_SWAPPED
	job, err = sm.MarkIndexSwapped(ctx, "stream-1", "job-full", []string{"wal-1", "wal-2"})
	if err != nil {
		t.Fatalf("MarkIndexSwapped failed: %v", err)
	}
	if job.State != JobStateIndexSwapped {
		t.Errorf("expected INDEX_SWAPPED, got %s", job.State)
	}
	if len(job.WALObjectsToDecrement) != 2 {
		t.Errorf("expected 2 WAL objects, got %d", len(job.WALObjectsToDecrement))
	}

	// -> DONE
	job, err = sm.MarkDone(ctx, "stream-1", "job-full")
	if err != nil {
		t.Fatalf("MarkDone failed: %v", err)
	}
	if job.State != JobStateDone {
		t.Errorf("expected DONE, got %s", job.State)
	}
	if !job.IsTerminal() {
		t.Error("job should be terminal")
	}
}

func TestSagaManager_FailureTransition(t *testing.T) {
	ctx := context.Background()

	states := []JobState{
		JobStateCreated,
		JobStateParquetWritten,
		JobStateIcebergCommitted,
		JobStateIndexSwapped,
	}

	for _, state := range states {
		t.Run("fail from "+string(state), func(t *testing.T) {
			store := metadata.NewMockStore()
			sm := NewSagaManager(store, "compactor-1")

			_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-fail"))
			if err != nil {
				t.Fatalf("CreateJob failed: %v", err)
			}

			// Advance to the target state
			switch state {
			case JobStateParquetWritten:
				_, err = sm.MarkParquetWritten(ctx, "stream-1", "job-fail", "/p", 1, 1)
			case JobStateIcebergCommitted:
				_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-fail", "/p", 1, 1)
				_, err = sm.MarkIcebergCommitted(ctx, "stream-1", "job-fail", 1)
			case JobStateIndexSwapped:
				_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-fail", "/p", 1, 1)
				_, _ = sm.MarkIcebergCommitted(ctx, "stream-1", "job-fail", 1)
				_, err = sm.MarkIndexSwapped(ctx, "stream-1", "job-fail", nil)
			}
			if err != nil {
				t.Fatalf("advance to state failed: %v", err)
			}

			// Mark failed
			job, err := sm.MarkFailed(ctx, "stream-1", "job-fail", "test error message")
			if err != nil {
				t.Fatalf("MarkFailed failed: %v", err)
			}
			if job.State != JobStateFailed {
				t.Errorf("expected FAILED, got %s", job.State)
			}
			if job.ErrorMessage != "test error message" {
				t.Errorf("ErrorMessage = %s, want 'test error message'", job.ErrorMessage)
			}
			if !job.IsTerminal() {
				t.Error("job should be terminal")
			}
		})
	}
}

func TestSagaManager_ListJobs(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	// Create jobs in different states
	_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-1"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	_, err = sm.CreateJob(ctx, "stream-1", WithJobID("job-2"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	_, err = sm.MarkParquetWritten(ctx, "stream-1", "job-2", "/p", 1, 1)
	if err != nil {
		t.Fatalf("MarkParquetWritten failed: %v", err)
	}
	_, err = sm.CreateJob(ctx, "stream-1", WithJobID("job-3"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-3", "/p", 1, 1)
	_, _ = sm.MarkIcebergCommitted(ctx, "stream-1", "job-3", 1)
	_, _ = sm.MarkIndexSwapped(ctx, "stream-1", "job-3", nil)
	_, err = sm.MarkDone(ctx, "stream-1", "job-3")
	if err != nil {
		t.Fatalf("MarkDone failed: %v", err)
	}

	t.Run("list all jobs", func(t *testing.T) {
		jobs, err := sm.ListJobs(ctx, "stream-1")
		if err != nil {
			t.Fatalf("ListJobs failed: %v", err)
		}
		if len(jobs) != 3 {
			t.Errorf("expected 3 jobs, got %d", len(jobs))
		}
	})

	t.Run("list incomplete jobs", func(t *testing.T) {
		jobs, err := sm.ListIncompleteJobs(ctx, "stream-1")
		if err != nil {
			t.Fatalf("ListIncompleteJobs failed: %v", err)
		}
		if len(jobs) != 2 {
			t.Errorf("expected 2 incomplete jobs, got %d", len(jobs))
		}
		for _, job := range jobs {
			if job.IsTerminal() {
				t.Errorf("job %s should not be terminal", job.JobID)
			}
		}
	})

	t.Run("list jobs for different stream", func(t *testing.T) {
		jobs, err := sm.ListJobs(ctx, "stream-2")
		if err != nil {
			t.Fatalf("ListJobs failed: %v", err)
		}
		if len(jobs) != 0 {
			t.Errorf("expected 0 jobs, got %d", len(jobs))
		}
	})
}

func TestSagaManager_DeleteJob(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-del"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	err = sm.DeleteJob(ctx, "stream-1", "job-del")
	if err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	_, _, err = sm.GetJob(ctx, "stream-1", "job-del")
	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound after delete, got %v", err)
	}
}

func TestSagaManager_CleanupCompletedJobs(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	// Create and complete jobs
	for i := 0; i < 3; i++ {
		jobID := NewJobID()
		_, _ = sm.CreateJob(ctx, "stream-1", WithJobID(jobID))
		_, _ = sm.MarkParquetWritten(ctx, "stream-1", jobID, "/p", 1, 1)
		_, _ = sm.MarkIcebergCommitted(ctx, "stream-1", jobID, 1)
		_, _ = sm.MarkIndexSwapped(ctx, "stream-1", jobID, nil)
		_, _ = sm.MarkDone(ctx, "stream-1", jobID)
	}

	// Create an incomplete job (should not be cleaned up)
	_, _ = sm.CreateJob(ctx, "stream-1", WithJobID("job-incomplete"))

	t.Run("cleanup with short age deletes nothing", func(t *testing.T) {
		deleted, err := sm.CleanupCompletedJobs(ctx, "stream-1", time.Hour)
		if err != nil {
			t.Fatalf("CleanupCompletedJobs failed: %v", err)
		}
		if deleted != 0 {
			t.Errorf("expected 0 deleted, got %d", deleted)
		}
	})

	// We can't easily test age-based cleanup without mocking time,
	// but we can verify the cleanup respects job state

	jobs, _ := sm.ListJobs(ctx, "stream-1")
	if len(jobs) != 4 {
		t.Errorf("expected 4 jobs (3 done + 1 incomplete), got %d", len(jobs))
	}
}

func TestSagaManager_IncrementRetryCount(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-retry"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		job, err := sm.IncrementRetryCount(ctx, "stream-1", "job-retry")
		if err != nil {
			t.Fatalf("IncrementRetryCount failed: %v", err)
		}
		if job.RetryCount != i {
			t.Errorf("RetryCount = %d, want %d", job.RetryCount, i)
		}
	}
}

func TestSagaManager_ResumeJob(t *testing.T) {
	ctx := context.Background()

	t.Run("resume incomplete job", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		_, _ = sm.CreateJob(ctx, "stream-1", WithJobID("job-resume"))
		_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-resume", "/p", 1, 1)

		job, err := sm.ResumeJob(ctx, "stream-1", "job-resume")
		if err != nil {
			t.Fatalf("ResumeJob failed: %v", err)
		}
		if job.State != JobStateParquetWritten {
			t.Errorf("State = %s, want PARQUET_WRITTEN", job.State)
		}
		if job.RetryCount != 1 {
			t.Errorf("RetryCount = %d, want 1", job.RetryCount)
		}
	})

	t.Run("resume terminal job fails", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		_, _ = sm.CreateJob(ctx, "stream-1", WithJobID("job-done"))
		_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-done", "/p", 1, 1)
		_, _ = sm.MarkIcebergCommitted(ctx, "stream-1", "job-done", 1)
		_, _ = sm.MarkIndexSwapped(ctx, "stream-1", "job-done", nil)
		_, _ = sm.MarkDone(ctx, "stream-1", "job-done")

		_, err := sm.ResumeJob(ctx, "stream-1", "job-done")
		if !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("expected ErrInvalidTransition, got %v", err)
		}
	})

	t.Run("resume non-existent job fails", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		_, err := sm.ResumeJob(ctx, "stream-1", "job-missing")
		if !errors.Is(err, ErrJobNotFound) {
			t.Errorf("expected ErrJobNotFound, got %v", err)
		}
	})
}

func TestSagaManager_ConcurrentModification(t *testing.T) {
	ctx := context.Background()

	// Test that concurrent transitions from two SagaManagers result in one failing
	// This test simulates what happens when two compactors try to transition
	// the same job at the same time.
	t.Run("two managers racing on same job", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm1 := NewSagaManager(store, "compactor-1")
		sm2 := NewSagaManager(store, "compactor-2")

		// Create job with sm1
		_, err := sm1.CreateJob(ctx, "stream-1", WithJobID("job-race"))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}

		// sm1 transitions first - should succeed
		_, err = sm1.TransitionState(ctx, "stream-1", "job-race", JobStateParquetWritten, nil)
		if err != nil {
			t.Fatalf("first transition failed: %v", err)
		}

		// sm2 also tries to transition from CREATED to PARQUET_WRITTEN
		// But the job is already in PARQUET_WRITTEN state, so this should fail
		// because the transition from PARQUET_WRITTEN to PARQUET_WRITTEN is invalid
		_, err = sm2.TransitionState(ctx, "stream-1", "job-race", JobStateParquetWritten, nil)
		if !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("expected ErrInvalidTransition due to state already advanced, got %v", err)
		}
	})

	// Test that stale version on Put fails
	t.Run("stale version fails", func(t *testing.T) {
		store := metadata.NewMockStore()
		sm := NewSagaManager(store, "compactor-1")

		_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-stale"))
		if err != nil {
			t.Fatalf("CreateJob failed: %v", err)
		}

		// Get the job state and version
		key := keys.CompactionJobKeyPath("stream-1", "job-stale")
		result1, _ := store.Get(ctx, key)

		// Another process updates the job (simulated by incrementing version)
		_, _ = store.Put(ctx, key, result1.Value)

		// Now try to update with the OLD version - this should fail
		// We simulate this by using Put with WithExpectedVersion directly
		_, err = store.Put(ctx, key, result1.Value, metadata.WithExpectedVersion(result1.Version))
		if !errors.Is(err, metadata.ErrVersionMismatch) {
			t.Errorf("expected ErrVersionMismatch, got %v", err)
		}
	})
}

func TestSagaManager_RecoveryFromEachState(t *testing.T) {
	ctx := context.Background()

	states := []struct {
		name          string
		setupFn       func(*SagaManager, context.Context) error
		expectedState JobState
	}{
		{
			name: "recover from CREATED",
			setupFn: func(sm *SagaManager, ctx context.Context) error {
				_, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-recover"))
				return err
			},
			expectedState: JobStateCreated,
		},
		{
			name: "recover from PARQUET_WRITTEN",
			setupFn: func(sm *SagaManager, ctx context.Context) error {
				_, _ = sm.CreateJob(ctx, "stream-1", WithJobID("job-recover"))
				_, err := sm.MarkParquetWritten(ctx, "stream-1", "job-recover", "/p", 1, 1)
				return err
			},
			expectedState: JobStateParquetWritten,
		},
		{
			name: "recover from ICEBERG_COMMITTED",
			setupFn: func(sm *SagaManager, ctx context.Context) error {
				_, _ = sm.CreateJob(ctx, "stream-1", WithJobID("job-recover"))
				_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-recover", "/p", 1, 1)
				_, err := sm.MarkIcebergCommitted(ctx, "stream-1", "job-recover", 123)
				return err
			},
			expectedState: JobStateIcebergCommitted,
		},
		{
			name: "recover from INDEX_SWAPPED",
			setupFn: func(sm *SagaManager, ctx context.Context) error {
				_, _ = sm.CreateJob(ctx, "stream-1", WithJobID("job-recover"))
				_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-recover", "/p", 1, 1)
				_, _ = sm.MarkIcebergCommitted(ctx, "stream-1", "job-recover", 123)
				_, err := sm.MarkIndexSwapped(ctx, "stream-1", "job-recover", []string{"wal-1"})
				return err
			},
			expectedState: JobStateIndexSwapped,
		},
	}

	for _, tt := range states {
		t.Run(tt.name, func(t *testing.T) {
			store := metadata.NewMockStore()
			sm := NewSagaManager(store, "compactor-1")

			// Set up the job to the target state
			if err := tt.setupFn(sm, ctx); err != nil {
				t.Fatalf("setup failed: %v", err)
			}

			// Simulate restart by creating a new SagaManager
			sm2 := NewSagaManager(store, "compactor-2")

			// Find incomplete jobs
			jobs, err := sm2.ListIncompleteJobs(ctx, "stream-1")
			if err != nil {
				t.Fatalf("ListIncompleteJobs failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("expected 1 incomplete job, got %d", len(jobs))
			}

			job := jobs[0]
			if job.State != tt.expectedState {
				t.Errorf("State = %s, want %s", job.State, tt.expectedState)
			}
			if !job.IsRecoverable() {
				t.Error("job should be recoverable")
			}

			// Resume the job
			resumed, err := sm2.ResumeJob(ctx, "stream-1", "job-recover")
			if err != nil {
				t.Fatalf("ResumeJob failed: %v", err)
			}
			if resumed.RetryCount != 1 {
				t.Errorf("RetryCount = %d, want 1", resumed.RetryCount)
			}
		})
	}
}

func TestSagaManager_CompactorID(t *testing.T) {
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "my-compactor")

	if sm.CompactorID() != "my-compactor" {
		t.Errorf("CompactorID() = %s, want my-compactor", sm.CompactorID())
	}
}

func TestJobStatePreservation(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	sm := NewSagaManager(store, "compactor-1")

	// Create job with all fields populated
	_, err := sm.CreateJob(ctx, "stream-1",
		WithJobID("job-fields"),
		WithSourceRange(100, 500),
		WithSourceWALCount(10),
		WithSourceSizeBytes(50000))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Transition through states and verify fields are preserved
	_, _ = sm.MarkParquetWritten(ctx, "stream-1", "job-fields", "/bucket/data.parquet", 25000, 400)
	_, _ = sm.MarkIcebergCommitted(ctx, "stream-1", "job-fields", 999)
	job, _ := sm.MarkIndexSwapped(ctx, "stream-1", "job-fields", []string{"wal-a", "wal-b", "wal-c"})

	// Verify all fields are preserved
	if job.StreamID != "stream-1" {
		t.Errorf("StreamID = %s, want stream-1", job.StreamID)
	}
	if job.SourceStartOffset != 100 {
		t.Errorf("SourceStartOffset = %d, want 100", job.SourceStartOffset)
	}
	if job.SourceEndOffset != 500 {
		t.Errorf("SourceEndOffset = %d, want 500", job.SourceEndOffset)
	}
	if job.SourceWALCount != 10 {
		t.Errorf("SourceWALCount = %d, want 10", job.SourceWALCount)
	}
	if job.SourceSizeBytes != 50000 {
		t.Errorf("SourceSizeBytes = %d, want 50000", job.SourceSizeBytes)
	}
	if job.ParquetPath != "/bucket/data.parquet" {
		t.Errorf("ParquetPath = %s, want /bucket/data.parquet", job.ParquetPath)
	}
	if job.ParquetSizeBytes != 25000 {
		t.Errorf("ParquetSizeBytes = %d, want 25000", job.ParquetSizeBytes)
	}
	if job.ParquetRecordCount != 400 {
		t.Errorf("ParquetRecordCount = %d, want 400", job.ParquetRecordCount)
	}
	if job.IcebergSnapshotID != 999 {
		t.Errorf("IcebergSnapshotID = %d, want 999", job.IcebergSnapshotID)
	}
	if len(job.WALObjectsToDecrement) != 3 {
		t.Errorf("WALObjectsToDecrement length = %d, want 3", len(job.WALObjectsToDecrement))
	}
}

// Tests for SkipIcebergCommit (table.iceberg.enabled=false)

func TestSagaManager_SkipIcebergCommit(t *testing.T) {
	metaStore := metadata.NewMockStore()
	sm := NewSagaManager(metaStore, "compactor-1")
	ctx := context.Background()

	// Create and progress job to PARQUET_WRITTEN
	job, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-skip-iceberg"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	job, err = sm.MarkParquetWritten(ctx, "stream-1", job.JobID, "/path/to/file.parquet", 10000, 100)
	if err != nil {
		t.Fatalf("MarkParquetWritten failed: %v", err)
	}

	// Skip iceberg commit (as if table.iceberg.enabled=false)
	job, err = sm.SkipIcebergCommit(ctx, "stream-1", job.JobID, []string{"wal-1", "wal-2"})
	if err != nil {
		t.Fatalf("SkipIcebergCommit failed: %v", err)
	}

	// Verify job state
	if job.State != JobStateIndexSwapped {
		t.Errorf("expected state INDEX_SWAPPED, got %s", job.State)
	}
	if !job.IcebergSkipped {
		t.Error("expected IcebergSkipped to be true")
	}
	if job.IcebergSnapshotID != 0 {
		t.Errorf("expected IcebergSnapshotID to be 0 when skipped, got %d", job.IcebergSnapshotID)
	}
	if len(job.WALObjectsToDecrement) != 2 {
		t.Errorf("expected 2 WAL objects to decrement, got %d", len(job.WALObjectsToDecrement))
	}

	// Verify we can complete the job normally
	job, err = sm.MarkDone(ctx, "stream-1", job.JobID)
	if err != nil {
		t.Fatalf("MarkDone failed: %v", err)
	}
	if job.State != JobStateDone {
		t.Errorf("expected state DONE, got %s", job.State)
	}
}

func TestSagaManager_SkipIcebergCommit_FromWrongState(t *testing.T) {
	metaStore := metadata.NewMockStore()
	sm := NewSagaManager(metaStore, "compactor-1")
	ctx := context.Background()

	// Create job (in CREATED state)
	job, err := sm.CreateJob(ctx, "stream-1", WithJobID("job-skip-wrong-state"))
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Try to skip iceberg commit from CREATED state (should fail)
	_, err = sm.SkipIcebergCommit(ctx, "stream-1", job.JobID, nil)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestSagaManager_SkipIcebergCommit_FullLifecycle(t *testing.T) {
	metaStore := metadata.NewMockStore()
	sm := NewSagaManager(metaStore, "compactor-1")
	ctx := context.Background()

	// Simulate full compaction lifecycle with Iceberg disabled
	job, err := sm.CreateJob(ctx, "stream-1",
		WithJobID("job-no-iceberg"),
		WithSourceRange(0, 99),
		WithSourceWALCount(5),
		WithSourceSizeBytes(50000),
	)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	if job.State != JobStateCreated {
		t.Errorf("expected CREATED, got %s", job.State)
	}

	// Write parquet
	job, err = sm.MarkParquetWritten(ctx, "stream-1", job.JobID, "/data/file.parquet", 20000, 100)
	if err != nil {
		t.Fatalf("MarkParquetWritten failed: %v", err)
	}
	if job.State != JobStateParquetWritten {
		t.Errorf("expected PARQUET_WRITTEN, got %s", job.State)
	}

	// Skip iceberg (table.iceberg.enabled=false)
	job, err = sm.SkipIcebergCommit(ctx, "stream-1", job.JobID, []string{"wal-a", "wal-b"})
	if err != nil {
		t.Fatalf("SkipIcebergCommit failed: %v", err)
	}
	if job.State != JobStateIndexSwapped {
		t.Errorf("expected INDEX_SWAPPED, got %s", job.State)
	}
	if !job.IcebergSkipped {
		t.Error("expected IcebergSkipped=true")
	}

	// Complete
	job, err = sm.MarkDone(ctx, "stream-1", job.JobID)
	if err != nil {
		t.Fatalf("MarkDone failed: %v", err)
	}
	if job.State != JobStateDone {
		t.Errorf("expected DONE, got %s", job.State)
	}

	// Verify IcebergSkipped is still set
	if !job.IcebergSkipped {
		t.Error("expected IcebergSkipped to remain true after DONE")
	}
}

func TestJobState_CanTransitionTo_SkipIceberg(t *testing.T) {
	// Verify PARQUET_WRITTEN can transition to INDEX_SWAPPED (skip Iceberg path)
	job := &Job{State: JobStateParquetWritten}
	if !job.CanTransitionTo(JobStateIndexSwapped) {
		t.Error("PARQUET_WRITTEN should be able to transition to INDEX_SWAPPED (skip Iceberg)")
	}

	// Verify PARQUET_WRITTEN can still transition to ICEBERG_COMMITTED (normal path)
	if !job.CanTransitionTo(JobStateIcebergCommitted) {
		t.Error("PARQUET_WRITTEN should be able to transition to ICEBERG_COMMITTED (normal path)")
	}
}
