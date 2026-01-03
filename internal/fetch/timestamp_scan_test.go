package fetch

import (
	"context"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/dray-io/dray/internal/index"
)

func TestParquetReader_FindOffsetByTimestamp_Basic(t *testing.T) {
	records := []ParquetRecordWithHeaders{
		{Partition: 0, Offset: 0, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1")},
		{Partition: 0, Offset: 1, Timestamp: 2000, Key: []byte("k2"), Value: []byte("v2")},
		{Partition: 0, Offset: 2, Timestamp: 3000, Key: []byte("k3"), Value: []byte("v3")},
	}

	store := newMockStore()
	parquetData := createParquetFile(t, records)
	store.objects["test.parquet"] = parquetData

	entry := &index.IndexEntry{
		FileType:    index.FileTypeParquet,
		ParquetPath: "test.parquet",
		StartOffset: 0,
		EndOffset:   3,
	}

	reader := NewParquetReader(store)

	tests := []struct {
		name         string
		timestamp    int64
		wantOffset   int64
		wantTimestamp int64
		wantFound    bool
	}{
		{
			name:         "find first record",
			timestamp:    1000,
			wantOffset:   0,
			wantTimestamp: 1000,
			wantFound:    true,
		},
		{
			name:         "find middle record",
			timestamp:    2000,
			wantOffset:   1,
			wantTimestamp: 2000,
			wantFound:    true,
		},
		{
			name:         "find last record",
			timestamp:    3000,
			wantOffset:   2,
			wantTimestamp: 3000,
			wantFound:    true,
		},
		{
			name:         "timestamp before first record",
			timestamp:    500,
			wantOffset:   0,
			wantTimestamp: 1000,
			wantFound:    true,
		},
		{
			name:         "timestamp between records",
			timestamp:    1500,
			wantOffset:   1,
			wantTimestamp: 2000,
			wantFound:    true,
		},
		{
			name:         "timestamp after last record",
			timestamp:    5000,
			wantOffset:   -1,
			wantTimestamp: -1,
			wantFound:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOffset, gotTimestamp, gotFound, err := reader.FindOffsetByTimestamp(context.Background(), entry, tt.timestamp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotFound != tt.wantFound {
				t.Errorf("found = %v, want %v", gotFound, tt.wantFound)
			}
			if gotFound {
				if gotOffset != tt.wantOffset {
					t.Errorf("offset = %d, want %d", gotOffset, tt.wantOffset)
				}
				if gotTimestamp != tt.wantTimestamp {
					t.Errorf("timestamp = %d, want %d", gotTimestamp, tt.wantTimestamp)
				}
			}
		})
	}
}

func TestParquetReader_FindOffsetByTimestamp_UsesRowGroupStats(t *testing.T) {
	// Create a large parquet file with multiple row groups
	// Each row group has 100 records with increasing timestamps
	const (
		rowGroupSize = 100
		numRowGroups = 10
	)

	records := make([]ParquetRecordWithHeaders, 0, rowGroupSize*numRowGroups)
	for i := 0; i < rowGroupSize*numRowGroups; i++ {
		records = append(records, ParquetRecordWithHeaders{
			Partition: 0,
			Offset:    int64(i),
			Timestamp: int64(i * 100), // 0, 100, 200, ...
			Key:       []byte("k"),
			Value:     []byte("v"),
		})
	}

	store := newMockStore()
	parquetData := createParquetFileWithRowGroupSize(t, records, rowGroupSize)
	store.objects["test.parquet"] = parquetData

	entry := &index.IndexEntry{
		FileType:    index.FileTypeParquet,
		ParquetPath: "test.parquet",
		StartOffset: 0,
		EndOffset:   int64(len(records)),
	}

	reader := NewParquetReader(store)

	// Search for a timestamp in the last row group
	// This should skip reading all earlier row groups due to timestamp stats pruning
	searchTimestamp := int64((numRowGroups - 1) * rowGroupSize * 100)

	offset, timestamp, found, err := reader.FindOffsetByTimestamp(context.Background(), entry, searchTimestamp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected to find a matching record")
	}

	expectedOffset := int64((numRowGroups - 1) * rowGroupSize)
	if offset != expectedOffset {
		t.Errorf("offset = %d, want %d", offset, expectedOffset)
	}
	if timestamp != searchTimestamp {
		t.Errorf("timestamp = %d, want %d", timestamp, searchTimestamp)
	}

	// Verify that we used range reads, not full file reads
	store.mu.Lock()
	getCalls := store.getCalls
	getRangeBytes := store.getRangeBytes
	fileSize := int64(len(parquetData))
	store.mu.Unlock()

	if getCalls != 0 {
		t.Errorf("expected 0 full Get calls (using range reads), got %d", getCalls)
	}

	// The key assertion: we should read significantly less than the full file
	// because we're pruning row groups based on timestamp stats
	if getRangeBytes >= fileSize {
		t.Errorf("expected bounded reads (less than file size), but read %d bytes >= file size %d",
			getRangeBytes, fileSize)
	}

	// Log the savings for debugging
	t.Logf("File size: %d bytes, bytes read: %d (%.1f%% of file)",
		fileSize, getRangeBytes, float64(getRangeBytes)/float64(fileSize)*100)
}

func TestParquetReader_FindOffsetByTimestamp_BoundedReadsForLargeFile(t *testing.T) {
	// This is the key test per task requirements:
	// "Add a test with large Parquet files to assert bounded reads for timestamp lookup"
	const (
		rowGroupSize = 200
		numRowGroups = 20
	)

	records := make([]ParquetRecordWithHeaders, 0, rowGroupSize*numRowGroups)
	for i := 0; i < rowGroupSize*numRowGroups; i++ {
		records = append(records, ParquetRecordWithHeaders{
			Partition: 0,
			Offset:    int64(i),
			Timestamp: int64(i * 10),
			Key:       []byte("key-data-padding-to-make-file-larger"),
			Value:     []byte("value-data-padding-to-make-file-larger-for-better-test"),
		})
	}

	store := newMockStore()
	parquetData := createParquetFileWithRowGroupSize(t, records, rowGroupSize)
	store.objects["test.parquet"] = parquetData
	fileSize := int64(len(parquetData))

	entry := &index.IndexEntry{
		FileType:    index.FileTypeParquet,
		ParquetPath: "test.parquet",
		StartOffset: 0,
		EndOffset:   int64(len(records)),
	}

	reader := NewParquetReader(store)

	// Test 1: Search in last row group - should skip most row groups
	t.Run("search_last_row_group", func(t *testing.T) {
		store.mu.Lock()
		store.getCalls = 0
		store.getRangeCalls = 0
		store.getRangeBytes = 0
		store.mu.Unlock()

		lastRowGroupStart := int64((numRowGroups - 1) * rowGroupSize * 10)
		offset, timestamp, found, err := reader.FindOffsetByTimestamp(context.Background(), entry, lastRowGroupStart)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Fatal("expected to find a matching record")
		}

		expectedOffset := int64((numRowGroups - 1) * rowGroupSize)
		if offset != expectedOffset {
			t.Errorf("offset = %d, want %d", offset, expectedOffset)
		}
		if timestamp != lastRowGroupStart {
			t.Errorf("timestamp = %d, want %d", timestamp, lastRowGroupStart)
		}

		store.mu.Lock()
		bytesRead := store.getRangeBytes
		store.mu.Unlock()

		// Should read less than 50% of file when skipping to last row group
		maxExpected := fileSize / 2
		if bytesRead > maxExpected {
			t.Errorf("read %d bytes, expected less than %d (50%% of file %d)",
				bytesRead, maxExpected, fileSize)
		}
		t.Logf("Last row group search: read %d / %d bytes (%.1f%%)",
			bytesRead, fileSize, float64(bytesRead)/float64(fileSize)*100)
	})

	// Test 2: Search in first row group - should read minimal data
	t.Run("search_first_row_group", func(t *testing.T) {
		store.mu.Lock()
		store.getCalls = 0
		store.getRangeCalls = 0
		store.getRangeBytes = 0
		store.mu.Unlock()

		offset, timestamp, found, err := reader.FindOffsetByTimestamp(context.Background(), entry, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Fatal("expected to find a matching record")
		}
		if offset != 0 {
			t.Errorf("offset = %d, want 0", offset)
		}
		if timestamp != 0 {
			t.Errorf("timestamp = %d, want 0", timestamp)
		}

		store.mu.Lock()
		bytesRead := store.getRangeBytes
		store.mu.Unlock()

		// For first row group, we should still use bounded reads (not full file)
		if bytesRead >= fileSize {
			t.Errorf("read %d bytes >= file size %d, expected bounded reads",
				bytesRead, fileSize)
		}
		t.Logf("First row group search: read %d / %d bytes (%.1f%%)",
			bytesRead, fileSize, float64(bytesRead)/float64(fileSize)*100)
	})

	// Test 3: Search in middle row group
	t.Run("search_middle_row_group", func(t *testing.T) {
		store.mu.Lock()
		store.getCalls = 0
		store.getRangeCalls = 0
		store.getRangeBytes = 0
		store.mu.Unlock()

		middleRowGroup := numRowGroups / 2
		middleTimestamp := int64(middleRowGroup * rowGroupSize * 10)

		offset, timestamp, found, err := reader.FindOffsetByTimestamp(context.Background(), entry, middleTimestamp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Fatal("expected to find a matching record")
		}

		expectedOffset := int64(middleRowGroup * rowGroupSize)
		if offset != expectedOffset {
			t.Errorf("offset = %d, want %d", offset, expectedOffset)
		}
		if timestamp != middleTimestamp {
			t.Errorf("timestamp = %d, want %d", timestamp, middleTimestamp)
		}

		store.mu.Lock()
		bytesRead := store.getRangeBytes
		store.mu.Unlock()

		// Should read less than full file
		if bytesRead >= fileSize {
			t.Errorf("read %d bytes >= file size %d, expected bounded reads",
				bytesRead, fileSize)
		}
		t.Logf("Middle row group search: read %d / %d bytes (%.1f%%)",
			bytesRead, fileSize, float64(bytesRead)/float64(fileSize)*100)
	})

	// Test 4: Search for timestamp not found
	t.Run("search_not_found", func(t *testing.T) {
		store.mu.Lock()
		store.getCalls = 0
		store.getRangeCalls = 0
		store.getRangeBytes = 0
		store.mu.Unlock()

		// Search for timestamp beyond all records
		beyondTimestamp := int64(numRowGroups * rowGroupSize * 10 * 2)
		_, _, found, err := reader.FindOffsetByTimestamp(context.Background(), entry, beyondTimestamp)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if found {
			t.Error("expected not to find a matching record")
		}

		store.mu.Lock()
		bytesRead := store.getRangeBytes
		store.mu.Unlock()

		// Even for not found, should still use bounded reads
		// (reading just the metadata/footer and discovering no row groups match)
		if bytesRead >= fileSize {
			t.Errorf("read %d bytes >= file size %d, expected bounded reads even for not found",
				bytesRead, fileSize)
		}
		t.Logf("Not found search: read %d / %d bytes (%.1f%%)",
			bytesRead, fileSize, float64(bytesRead)/float64(fileSize)*100)
	})
}

func TestParquetReader_FindOffsetByTimestamp_FileNotFound(t *testing.T) {
	store := newMockStore()
	entry := &index.IndexEntry{
		FileType:    index.FileTypeParquet,
		ParquetPath: "nonexistent.parquet",
		StartOffset: 0,
		EndOffset:   10,
	}

	reader := NewParquetReader(store)
	_, _, _, err := reader.FindOffsetByTimestamp(context.Background(), entry, 1000)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestParquetReader_FindOffsetByTimestamp_WrongFileType(t *testing.T) {
	store := newMockStore()
	entry := &index.IndexEntry{
		FileType: index.FileTypeWAL,
		WalPath:  "test.wal",
	}

	reader := NewParquetReader(store)
	_, _, _, err := reader.FindOffsetByTimestamp(context.Background(), entry, 1000)
	if err == nil {
		t.Error("expected error for wrong file type")
	}
}

func TestParquetReader_FindOffsetByTimestamp_RespectsOffsetBounds(t *testing.T) {
	// Create records with offsets 0-9, but entry only covers 3-7
	records := make([]ParquetRecordWithHeaders, 10)
	for i := 0; i < 10; i++ {
		records[i] = ParquetRecordWithHeaders{
			Partition: 0,
			Offset:    int64(i),
			Timestamp: int64(i * 100),
			Key:       []byte("k"),
			Value:     []byte("v"),
		}
	}

	store := newMockStore()
	parquetData := createParquetFile(t, records)
	store.objects["test.parquet"] = parquetData

	entry := &index.IndexEntry{
		FileType:    index.FileTypeParquet,
		ParquetPath: "test.parquet",
		StartOffset: 3,
		EndOffset:   7,
	}

	reader := NewParquetReader(store)

	// Search for timestamp that matches offset 0, but entry starts at 3
	// Should find offset 3 instead
	offset, timestamp, found, err := reader.FindOffsetByTimestamp(context.Background(), entry, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected to find a matching record")
	}
	if offset != 3 {
		t.Errorf("offset = %d, want 3 (first in entry range)", offset)
	}
	if timestamp != 300 {
		t.Errorf("timestamp = %d, want 300", timestamp)
	}

	// Search for timestamp that would match offset 8, but entry ends at 7
	// Should not find anything
	_, _, found, err = reader.FindOffsetByTimestamp(context.Background(), entry, 800)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected not to find a record outside entry bounds")
	}
}

func TestSelectRowGroupsByTimestamp(t *testing.T) {
	// Create file with 5 row groups, each with 10 records
	const rowGroupSize = 10
	records := make([]ParquetRecordWithHeaders, 50)
	for i := 0; i < 50; i++ {
		records[i] = ParquetRecordWithHeaders{
			Partition: 0,
			Offset:    int64(i),
			Timestamp: int64(i * 100), // 0-900, 1000-1900, 2000-2900, 3000-3900, 4000-4900
			Key:       []byte("k"),
			Value:     []byte("v"),
		}
	}

	parquetData := createParquetFileWithRowGroupSize(t, records, rowGroupSize)
	bytesFile := newParquetBytesFile(parquetData)

	parquetFile, err := parquet.OpenFile(bytesFile, bytesFile.Size())
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	tests := []struct {
		name            string
		timestamp       int64
		wantRowGroups   int
		skipDescription string
	}{
		{
			name:          "timestamp 0 includes all row groups",
			timestamp:     0,
			wantRowGroups: 5,
		},
		{
			name:            "timestamp 1000 skips first row group",
			timestamp:       1000,
			wantRowGroups:   4,
			skipDescription: "first row group max=900 < 1000",
		},
		{
			name:            "timestamp 2000 skips first two row groups",
			timestamp:       2000,
			wantRowGroups:   3,
			skipDescription: "row groups 0,1 have max < 2000",
		},
		{
			name:            "timestamp 4000 skips first four row groups",
			timestamp:       4000,
			wantRowGroups:   1,
			skipDescription: "row groups 0-3 have max < 4000",
		},
		{
			name:          "timestamp 5000 includes no row groups (beyond all)",
			timestamp:     5000,
			wantRowGroups: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selected := selectRowGroupsByTimestamp(parquetFile, tt.timestamp)
			if len(selected) != tt.wantRowGroups {
				t.Errorf("got %d row groups, want %d", len(selected), tt.wantRowGroups)
				if tt.skipDescription != "" {
					t.Logf("Expected to skip: %s", tt.skipDescription)
				}
			}
		})
	}
}
