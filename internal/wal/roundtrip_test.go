package wal

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

// TestRoundtripMultipleStreams tests encoding and decoding WALs with multiple streams,
// verifying all data is preserved correctly.
func TestRoundtripMultipleStreams(t *testing.T) {
	walID := uuid.MustParse("fedcba98-7654-3210-fedc-ba9876543210")
	original := NewWAL(walID, 99, 1700000000000)

	// Add streams in non-sorted order to test sorting
	streams := []struct {
		streamID   uint64
		data       []byte
		records    uint32
		minTs      int64
		maxTs      int64
		batchCount int
	}{
		{streamID: 500, data: []byte("stream five hundred"), records: 50, minTs: 5000, maxTs: 5999, batchCount: 1},
		{streamID: 100, data: []byte("first stream"), records: 10, minTs: 1000, maxTs: 1999, batchCount: 1},
		{streamID: 300, data: []byte("middle stream data"), records: 30, minTs: 3000, maxTs: 3999, batchCount: 1},
		{streamID: 200, data: []byte("second one"), records: 20, minTs: 2000, maxTs: 2999, batchCount: 1},
		{streamID: 400, data: []byte("stream four hundred here"), records: 40, minTs: 4000, maxTs: 4999, batchCount: 1},
	}

	for _, s := range streams {
		original.AddChunk(Chunk{
			StreamID:       s.streamID,
			Batches:        []BatchEntry{{Data: s.data}},
			RecordCount:    s.records,
			MinTimestampMs: s.minTs,
			MaxTimestampMs: s.maxTs,
		})
	}

	// Encode
	encoded, err := EncodeToBytes(original)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Decode
	decoded, err := DecodeFromBytes(encoded)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	// Verify WAL-level fields
	if decoded.WalID != original.WalID {
		t.Errorf("WalID mismatch: got %v, want %v", decoded.WalID, original.WalID)
	}
	if decoded.MetaDomain != original.MetaDomain {
		t.Errorf("MetaDomain mismatch: got %d, want %d", decoded.MetaDomain, original.MetaDomain)
	}
	if decoded.CreatedAtUnixMs != original.CreatedAtUnixMs {
		t.Errorf("CreatedAtUnixMs mismatch: got %d, want %d", decoded.CreatedAtUnixMs, original.CreatedAtUnixMs)
	}

	// Verify chunk count
	if len(decoded.Chunks) != len(original.Chunks) {
		t.Fatalf("chunk count mismatch: got %d, want %d", len(decoded.Chunks), len(original.Chunks))
	}

	// Build expected sorted order
	sortedExpected := []struct {
		streamID uint64
		data     []byte
		records  uint32
		minTs    int64
		maxTs    int64
	}{
		{100, []byte("first stream"), 10, 1000, 1999},
		{200, []byte("second one"), 20, 2000, 2999},
		{300, []byte("middle stream data"), 30, 3000, 3999},
		{400, []byte("stream four hundred here"), 40, 4000, 4999},
		{500, []byte("stream five hundred"), 50, 5000, 5999},
	}

	// Verify each chunk
	for i, exp := range sortedExpected {
		chunk := decoded.Chunks[i]
		if chunk.StreamID != exp.streamID {
			t.Errorf("chunk[%d].StreamID = %d, want %d", i, chunk.StreamID, exp.streamID)
		}
		if chunk.RecordCount != exp.records {
			t.Errorf("chunk[%d].RecordCount = %d, want %d", i, chunk.RecordCount, exp.records)
		}
		if chunk.MinTimestampMs != exp.minTs {
			t.Errorf("chunk[%d].MinTimestampMs = %d, want %d", i, chunk.MinTimestampMs, exp.minTs)
		}
		if chunk.MaxTimestampMs != exp.maxTs {
			t.Errorf("chunk[%d].MaxTimestampMs = %d, want %d", i, chunk.MaxTimestampMs, exp.maxTs)
		}
		if len(chunk.Batches) != 1 {
			t.Errorf("chunk[%d] batch count = %d, want 1", i, len(chunk.Batches))
			continue
		}
		if !bytes.Equal(chunk.Batches[0].Data, exp.data) {
			t.Errorf("chunk[%d].Batches[0].Data = %q, want %q", i, chunk.Batches[0].Data, exp.data)
		}
	}
}

// TestRoundtripVariousBatchSizes tests encoding/decoding with different batch sizes.
func TestRoundtripVariousBatchSizes(t *testing.T) {
	testCases := []struct {
		name      string
		batchSize int
	}{
		{"empty_batch", 0},
		{"tiny_batch_1_byte", 1},
		{"small_batch_10_bytes", 10},
		{"medium_batch_1KB", 1024},
		{"large_batch_64KB", 64 * 1024},
		{"very_large_batch_1MB", 1024 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			walID := uuid.New()
			original := NewWAL(walID, 42, 1700000000000)

			// Create batch data
			batchData := make([]byte, tc.batchSize)
			for i := range batchData {
				batchData[i] = byte(i % 256)
			}

			original.AddChunk(Chunk{
				StreamID:       1,
				Batches:        []BatchEntry{{Data: batchData}},
				RecordCount:    uint32(tc.batchSize / 10), // arbitrary
				MinTimestampMs: 1000,
				MaxTimestampMs: 2000,
			})

			// Encode
			encoded, err := EncodeToBytes(original)
			if err != nil {
				t.Fatalf("EncodeToBytes failed: %v", err)
			}

			// Decode
			decoded, err := DecodeFromBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeFromBytes failed: %v", err)
			}

			// Verify
			if len(decoded.Chunks) != 1 {
				t.Fatalf("chunk count = %d, want 1", len(decoded.Chunks))
			}
			if len(decoded.Chunks[0].Batches) != 1 {
				t.Fatalf("batch count = %d, want 1", len(decoded.Chunks[0].Batches))
			}
			if !bytes.Equal(decoded.Chunks[0].Batches[0].Data, batchData) {
				t.Errorf("batch data mismatch for size %d", tc.batchSize)
			}
		})
	}
}

// TestRoundtripVariousStreamCounts tests encoding/decoding with different stream counts.
func TestRoundtripVariousStreamCounts(t *testing.T) {
	testCases := []struct {
		name        string
		streamCount int
	}{
		{"single_stream", 1},
		{"few_streams", 5},
		{"ten_streams", 10},
		{"many_streams", 50},
		{"hundred_streams", 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			walID := uuid.New()
			original := NewWAL(walID, 123, 1700000000000)

			// Add streams in reverse order to test sorting
			for i := tc.streamCount; i >= 1; i-- {
				data := make([]byte, 10+i)
				for j := range data {
					data[j] = byte((i + j) % 256)
				}

				original.AddChunk(Chunk{
					StreamID:       uint64(i * 100),
					Batches:        []BatchEntry{{Data: data}},
					RecordCount:    uint32(i),
					MinTimestampMs: int64(i * 1000),
					MaxTimestampMs: int64(i*1000 + 999),
				})
			}

			// Encode
			encoded, err := EncodeToBytes(original)
			if err != nil {
				t.Fatalf("EncodeToBytes failed: %v", err)
			}

			// Decode
			decoded, err := DecodeFromBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeFromBytes failed: %v", err)
			}

			// Verify chunk count
			if len(decoded.Chunks) != tc.streamCount {
				t.Fatalf("chunk count = %d, want %d", len(decoded.Chunks), tc.streamCount)
			}

			// Verify chunks are sorted and all fields preserved
			var prevStreamID uint64
			for i, chunk := range decoded.Chunks {
				// Verify sorted order
				if i > 0 && chunk.StreamID <= prevStreamID {
					t.Errorf("chunks not sorted: streamID %d after %d at index %d",
						chunk.StreamID, prevStreamID, i)
				}
				prevStreamID = chunk.StreamID

				// Verify record count matches expected pattern
				expectedStreamNum := int(chunk.StreamID / 100)
				if chunk.RecordCount != uint32(expectedStreamNum) {
					t.Errorf("chunk[%d].RecordCount = %d, want %d",
						i, chunk.RecordCount, expectedStreamNum)
				}

				// Verify timestamps
				expectedMinTs := int64(expectedStreamNum * 1000)
				expectedMaxTs := int64(expectedStreamNum*1000 + 999)
				if chunk.MinTimestampMs != expectedMinTs {
					t.Errorf("chunk[%d].MinTimestampMs = %d, want %d",
						i, chunk.MinTimestampMs, expectedMinTs)
				}
				if chunk.MaxTimestampMs != expectedMaxTs {
					t.Errorf("chunk[%d].MaxTimestampMs = %d, want %d",
						i, chunk.MaxTimestampMs, expectedMaxTs)
				}

				// Verify batch data
				if len(chunk.Batches) != 1 {
					t.Errorf("chunk[%d] batch count = %d, want 1", i, len(chunk.Batches))
					continue
				}

				expectedLen := 10 + expectedStreamNum
				if len(chunk.Batches[0].Data) != expectedLen {
					t.Errorf("chunk[%d].Batches[0].Data length = %d, want %d",
						i, len(chunk.Batches[0].Data), expectedLen)
				}
			}
		})
	}
}

// TestRoundtripMultipleBatchesPerStream tests streams with multiple batches.
func TestRoundtripMultipleBatchesPerStream(t *testing.T) {
	testCases := []struct {
		name       string
		batchCount int
	}{
		{"single_batch", 1},
		{"two_batches", 2},
		{"five_batches", 5},
		{"ten_batches", 10},
		{"many_batches", 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			walID := uuid.New()
			original := NewWAL(walID, 77, 1700000000000)

			// Create multiple batches for a single stream
			batches := make([]BatchEntry, tc.batchCount)
			for i := 0; i < tc.batchCount; i++ {
				data := make([]byte, 20+i*5)
				for j := range data {
					data[j] = byte((i*7 + j) % 256)
				}
				batches[i] = BatchEntry{Data: data}
			}

			original.AddChunk(Chunk{
				StreamID:       12345,
				Batches:        batches,
				RecordCount:    uint32(tc.batchCount * 10),
				MinTimestampMs: 1000,
				MaxTimestampMs: int64(1000 + tc.batchCount*100),
			})

			// Encode
			encoded, err := EncodeToBytes(original)
			if err != nil {
				t.Fatalf("EncodeToBytes failed: %v", err)
			}

			// Decode
			decoded, err := DecodeFromBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeFromBytes failed: %v", err)
			}

			// Verify
			if len(decoded.Chunks) != 1 {
				t.Fatalf("chunk count = %d, want 1", len(decoded.Chunks))
			}

			chunk := decoded.Chunks[0]
			if len(chunk.Batches) != tc.batchCount {
				t.Fatalf("batch count = %d, want %d", len(chunk.Batches), tc.batchCount)
			}

			// Verify each batch
			for i := 0; i < tc.batchCount; i++ {
				expectedLen := 20 + i*5
				if len(chunk.Batches[i].Data) != expectedLen {
					t.Errorf("batch[%d] length = %d, want %d",
						i, len(chunk.Batches[i].Data), expectedLen)
				}
				// Verify content pattern
				for j, b := range chunk.Batches[i].Data {
					expected := byte((i*7 + j) % 256)
					if b != expected {
						t.Errorf("batch[%d][%d] = %d, want %d", i, j, b, expected)
						break
					}
				}
			}
		})
	}
}

// TestRoundtripComplexWAL tests a complex WAL with multiple streams and multiple batches per stream.
func TestRoundtripComplexWAL(t *testing.T) {
	walID := uuid.MustParse("abcdef12-3456-7890-abcd-ef1234567890")
	original := NewWAL(walID, 999, 1234567890123)

	// Add 10 streams, each with a different number of batches
	for streamNum := 1; streamNum <= 10; streamNum++ {
		streamID := uint64(streamNum * 1000)
		numBatches := streamNum

		batches := make([]BatchEntry, numBatches)
		for b := 0; b < numBatches; b++ {
			data := make([]byte, 50+b*20)
			for i := range data {
				data[i] = byte((streamNum*17 + b*13 + i) % 256)
			}
			batches[b] = BatchEntry{Data: data}
		}

		original.AddChunk(Chunk{
			StreamID:       streamID,
			Batches:        batches,
			RecordCount:    uint32(streamNum * numBatches * 5),
			MinTimestampMs: int64(streamNum * 100000),
			MaxTimestampMs: int64(streamNum*100000 + streamNum*1000),
		})
	}

	// Encode
	encoded, err := EncodeToBytes(original)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Decode
	decoded, err := DecodeFromBytes(encoded)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	// Verify WAL-level fields
	if decoded.WalID != original.WalID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, original.WalID)
	}
	if decoded.MetaDomain != original.MetaDomain {
		t.Errorf("MetaDomain = %d, want %d", decoded.MetaDomain, original.MetaDomain)
	}
	if decoded.CreatedAtUnixMs != original.CreatedAtUnixMs {
		t.Errorf("CreatedAtUnixMs = %d, want %d", decoded.CreatedAtUnixMs, original.CreatedAtUnixMs)
	}

	// Verify chunk count
	if len(decoded.Chunks) != 10 {
		t.Fatalf("chunk count = %d, want 10", len(decoded.Chunks))
	}

	// Verify each chunk
	for i, chunk := range decoded.Chunks {
		streamNum := i + 1
		expectedStreamID := uint64(streamNum * 1000)

		if chunk.StreamID != expectedStreamID {
			t.Errorf("chunk[%d].StreamID = %d, want %d", i, chunk.StreamID, expectedStreamID)
		}

		expectedBatches := streamNum
		if len(chunk.Batches) != expectedBatches {
			t.Errorf("chunk[%d] batch count = %d, want %d", i, len(chunk.Batches), expectedBatches)
			continue
		}

		expectedRecords := uint32(streamNum * expectedBatches * 5)
		if chunk.RecordCount != expectedRecords {
			t.Errorf("chunk[%d].RecordCount = %d, want %d", i, chunk.RecordCount, expectedRecords)
		}

		expectedMinTs := int64(streamNum * 100000)
		expectedMaxTs := int64(streamNum*100000 + streamNum*1000)
		if chunk.MinTimestampMs != expectedMinTs {
			t.Errorf("chunk[%d].MinTimestampMs = %d, want %d", i, chunk.MinTimestampMs, expectedMinTs)
		}
		if chunk.MaxTimestampMs != expectedMaxTs {
			t.Errorf("chunk[%d].MaxTimestampMs = %d, want %d", i, chunk.MaxTimestampMs, expectedMaxTs)
		}

		// Verify batch data content
		for b := 0; b < expectedBatches; b++ {
			expectedLen := 50 + b*20
			if len(chunk.Batches[b].Data) != expectedLen {
				t.Errorf("chunk[%d].batch[%d] length = %d, want %d",
					i, b, len(chunk.Batches[b].Data), expectedLen)
				continue
			}

			// Verify content pattern
			for j, byteVal := range chunk.Batches[b].Data {
				expected := byte((streamNum*17 + b*13 + j) % 256)
				if byteVal != expected {
					t.Errorf("chunk[%d].batch[%d][%d] = %d, want %d", i, b, j, byteVal, expected)
					break
				}
			}
		}
	}
}

// TestRoundtripEdgeCases tests edge cases in encoding/decoding.
func TestRoundtripEdgeCases(t *testing.T) {
	t.Run("zero_metadomain", func(t *testing.T) {
		walID := uuid.New()
		original := NewWAL(walID, 0, 1700000000000)
		original.AddChunk(Chunk{
			StreamID:       1,
			Batches:        []BatchEntry{{Data: []byte("test")}},
			RecordCount:    1,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1000,
		})

		encoded, err := EncodeToBytes(original)
		if err != nil {
			t.Fatalf("EncodeToBytes failed: %v", err)
		}

		decoded, err := DecodeFromBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeFromBytes failed: %v", err)
		}

		if decoded.MetaDomain != 0 {
			t.Errorf("MetaDomain = %d, want 0", decoded.MetaDomain)
		}
	})

	t.Run("max_metadomain", func(t *testing.T) {
		walID := uuid.New()
		original := NewWAL(walID, 0xFFFFFFFF, 1700000000000)
		original.AddChunk(Chunk{
			StreamID:       1,
			Batches:        []BatchEntry{{Data: []byte("test")}},
			RecordCount:    1,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1000,
		})

		encoded, err := EncodeToBytes(original)
		if err != nil {
			t.Fatalf("EncodeToBytes failed: %v", err)
		}

		decoded, err := DecodeFromBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeFromBytes failed: %v", err)
		}

		if decoded.MetaDomain != 0xFFFFFFFF {
			t.Errorf("MetaDomain = %d, want %d", decoded.MetaDomain, uint32(0xFFFFFFFF))
		}
	})

	t.Run("zero_timestamp", func(t *testing.T) {
		walID := uuid.New()
		original := NewWAL(walID, 1, 0)
		original.AddChunk(Chunk{
			StreamID:       1,
			Batches:        []BatchEntry{{Data: []byte("test")}},
			RecordCount:    1,
			MinTimestampMs: 0,
			MaxTimestampMs: 0,
		})

		encoded, err := EncodeToBytes(original)
		if err != nil {
			t.Fatalf("EncodeToBytes failed: %v", err)
		}

		decoded, err := DecodeFromBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeFromBytes failed: %v", err)
		}

		if decoded.CreatedAtUnixMs != 0 {
			t.Errorf("CreatedAtUnixMs = %d, want 0", decoded.CreatedAtUnixMs)
		}
		if decoded.Chunks[0].MinTimestampMs != 0 {
			t.Errorf("MinTimestampMs = %d, want 0", decoded.Chunks[0].MinTimestampMs)
		}
		if decoded.Chunks[0].MaxTimestampMs != 0 {
			t.Errorf("MaxTimestampMs = %d, want 0", decoded.Chunks[0].MaxTimestampMs)
		}
	})

	t.Run("large_timestamp", func(t *testing.T) {
		walID := uuid.New()
		largeTs := int64(1 << 62)
		original := NewWAL(walID, 1, largeTs)
		original.AddChunk(Chunk{
			StreamID:       1,
			Batches:        []BatchEntry{{Data: []byte("test")}},
			RecordCount:    1,
			MinTimestampMs: largeTs,
			MaxTimestampMs: largeTs + 1000,
		})

		encoded, err := EncodeToBytes(original)
		if err != nil {
			t.Fatalf("EncodeToBytes failed: %v", err)
		}

		decoded, err := DecodeFromBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeFromBytes failed: %v", err)
		}

		if decoded.CreatedAtUnixMs != largeTs {
			t.Errorf("CreatedAtUnixMs = %d, want %d", decoded.CreatedAtUnixMs, largeTs)
		}
		if decoded.Chunks[0].MinTimestampMs != largeTs {
			t.Errorf("MinTimestampMs = %d, want %d", decoded.Chunks[0].MinTimestampMs, largeTs)
		}
	})

	t.Run("max_stream_id", func(t *testing.T) {
		walID := uuid.New()
		maxStreamID := uint64(0xFFFFFFFFFFFFFFFF)
		original := NewWAL(walID, 1, 1700000000000)
		original.AddChunk(Chunk{
			StreamID:       maxStreamID,
			Batches:        []BatchEntry{{Data: []byte("test")}},
			RecordCount:    1,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1000,
		})

		encoded, err := EncodeToBytes(original)
		if err != nil {
			t.Fatalf("EncodeToBytes failed: %v", err)
		}

		decoded, err := DecodeFromBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeFromBytes failed: %v", err)
		}

		if decoded.Chunks[0].StreamID != maxStreamID {
			t.Errorf("StreamID = %d, want %d", decoded.Chunks[0].StreamID, maxStreamID)
		}
	})

	t.Run("max_record_count", func(t *testing.T) {
		walID := uuid.New()
		maxRecordCount := uint32(0xFFFFFFFF)
		original := NewWAL(walID, 1, 1700000000000)
		original.AddChunk(Chunk{
			StreamID:       1,
			Batches:        []BatchEntry{{Data: []byte("test")}},
			RecordCount:    maxRecordCount,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1000,
		})

		encoded, err := EncodeToBytes(original)
		if err != nil {
			t.Fatalf("EncodeToBytes failed: %v", err)
		}

		decoded, err := DecodeFromBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeFromBytes failed: %v", err)
		}

		if decoded.Chunks[0].RecordCount != maxRecordCount {
			t.Errorf("RecordCount = %d, want %d", decoded.Chunks[0].RecordCount, maxRecordCount)
		}
	})

	t.Run("nil_uuid", func(t *testing.T) {
		var nilUUID uuid.UUID
		original := NewWAL(nilUUID, 1, 1700000000000)
		original.AddChunk(Chunk{
			StreamID:       1,
			Batches:        []BatchEntry{{Data: []byte("test")}},
			RecordCount:    1,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1000,
		})

		encoded, err := EncodeToBytes(original)
		if err != nil {
			t.Fatalf("EncodeToBytes failed: %v", err)
		}

		decoded, err := DecodeFromBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeFromBytes failed: %v", err)
		}

		if decoded.WalID != nilUUID {
			t.Errorf("WalID = %v, want %v", decoded.WalID, nilUUID)
		}
	})
}

// TestRoundtripBinaryDataPatterns tests various binary data patterns.
func TestRoundtripBinaryDataPatterns(t *testing.T) {
	patterns := []struct {
		name string
		gen  func(size int) []byte
	}{
		{
			name: "all_zeros",
			gen:  func(size int) []byte { return make([]byte, size) },
		},
		{
			name: "all_ones",
			gen: func(size int) []byte {
				data := make([]byte, size)
				for i := range data {
					data[i] = 0xFF
				}
				return data
			},
		},
		{
			name: "alternating",
			gen: func(size int) []byte {
				data := make([]byte, size)
				for i := range data {
					if i%2 == 0 {
						data[i] = 0xAA
					} else {
						data[i] = 0x55
					}
				}
				return data
			},
		},
		{
			name: "ascending",
			gen: func(size int) []byte {
				data := make([]byte, size)
				for i := range data {
					data[i] = byte(i % 256)
				}
				return data
			},
		},
		{
			name: "random",
			gen: func(size int) []byte {
				data := make([]byte, size)
				rng := rand.New(rand.NewSource(42))
				rng.Read(data)
				return data
			},
		},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			walID := uuid.New()
			original := NewWAL(walID, 1, 1700000000000)

			batchData := p.gen(1000)
			original.AddChunk(Chunk{
				StreamID:       1,
				Batches:        []BatchEntry{{Data: batchData}},
				RecordCount:    100,
				MinTimestampMs: 1000,
				MaxTimestampMs: 2000,
			})

			encoded, err := EncodeToBytes(original)
			if err != nil {
				t.Fatalf("EncodeToBytes failed: %v", err)
			}

			decoded, err := DecodeFromBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeFromBytes failed: %v", err)
			}

			if !bytes.Equal(decoded.Chunks[0].Batches[0].Data, batchData) {
				t.Error("batch data mismatch")
			}
		})
	}
}

// TestRoundtripEncoderDecoder tests using the Encoder/Decoder interfaces.
func TestRoundtripEncoderDecoder(t *testing.T) {
	walID := uuid.New()
	original := NewWAL(walID, 88, 1700000000000)

	// Add multiple streams with multiple batches
	for s := 1; s <= 3; s++ {
		batches := make([]BatchEntry, s)
		for b := 0; b < s; b++ {
			data := make([]byte, 50+b*10)
			for i := range data {
				data[i] = byte((s*11 + b*7 + i) % 256)
			}
			batches[b] = BatchEntry{Data: data}
		}
		original.AddChunk(Chunk{
			StreamID:       uint64(s * 100),
			Batches:        batches,
			RecordCount:    uint32(s * 10),
			MinTimestampMs: int64(s * 1000),
			MaxTimestampMs: int64(s*1000 + 999),
		})
	}

	// Encode using Encoder
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)
	n, err := encoder.Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if int(n) != buf.Len() {
		t.Errorf("returned size = %d, buffer size = %d", n, buf.Len())
	}

	// Decode using Decoder
	decoder := NewDecoder(&buf)
	decoded, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify
	if decoded.WalID != original.WalID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, original.WalID)
	}
	if decoded.MetaDomain != original.MetaDomain {
		t.Errorf("MetaDomain = %d, want %d", decoded.MetaDomain, original.MetaDomain)
	}
	if len(decoded.Chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(decoded.Chunks))
	}

	// Verify chunks are sorted
	for i := 0; i < len(decoded.Chunks); i++ {
		expectedStreamID := uint64((i + 1) * 100)
		if decoded.Chunks[i].StreamID != expectedStreamID {
			t.Errorf("chunk[%d].StreamID = %d, want %d",
				i, decoded.Chunks[i].StreamID, expectedStreamID)
		}
	}
}

// TestRoundtripConsistency ensures multiple encode/decode cycles produce identical results.
func TestRoundtripConsistency(t *testing.T) {
	walID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	original := NewWAL(walID, 42, 1700000000000)

	original.AddChunk(Chunk{
		StreamID: 100,
		Batches: []BatchEntry{
			{Data: []byte("batch one")},
			{Data: []byte("batch two")},
		},
		RecordCount:    20,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
	})
	original.AddChunk(Chunk{
		StreamID:       200,
		Batches:        []BatchEntry{{Data: []byte("another stream")}},
		RecordCount:    10,
		MinTimestampMs: 1500,
		MaxTimestampMs: 1500,
	})

	// First encode
	encoded1, err := EncodeToBytes(original)
	if err != nil {
		t.Fatalf("First encode failed: %v", err)
	}

	// Decode
	decoded, err := DecodeFromBytes(encoded1)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Re-encode from decoded
	encoded2, err := EncodeToBytes(decoded)
	if err != nil {
		t.Fatalf("Second encode failed: %v", err)
	}

	// Encoded bytes should be identical
	if !bytes.Equal(encoded1, encoded2) {
		t.Error("re-encoded bytes differ from original")
	}

	// Decode again and verify
	decoded2, err := DecodeFromBytes(encoded2)
	if err != nil {
		t.Fatalf("Second decode failed: %v", err)
	}

	if decoded2.WalID != original.WalID {
		t.Errorf("WalID after 2 cycles = %v, want %v", decoded2.WalID, original.WalID)
	}
	if decoded2.MetaDomain != original.MetaDomain {
		t.Errorf("MetaDomain after 2 cycles = %d, want %d", decoded2.MetaDomain, original.MetaDomain)
	}
	if len(decoded2.Chunks) != 2 {
		t.Errorf("chunk count after 2 cycles = %d, want 2", len(decoded2.Chunks))
	}
}

// TestRoundtripCalculatedSize ensures CalculateEncodedSize matches actual encoded size.
func TestRoundtripCalculatedSize(t *testing.T) {
	testCases := []struct {
		name        string
		streamCount int
		batchCount  int
		batchSize   int
	}{
		{"empty", 0, 0, 0},
		{"small", 1, 1, 10},
		{"medium", 5, 3, 100},
		{"large", 10, 10, 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			walID := uuid.New()
			wal := NewWAL(walID, 1, 1700000000000)

			for s := 0; s < tc.streamCount; s++ {
				batches := make([]BatchEntry, tc.batchCount)
				for b := 0; b < tc.batchCount; b++ {
					batches[b] = BatchEntry{Data: make([]byte, tc.batchSize)}
				}
				if tc.batchCount > 0 {
					wal.AddChunk(Chunk{
						StreamID:       uint64(s + 1),
						Batches:        batches,
						RecordCount:    uint32(tc.batchCount),
						MinTimestampMs: 1000,
						MaxTimestampMs: 2000,
					})
				}
			}

			calculatedSize, err := CalculateEncodedSize(wal)
			if err != nil {
				t.Fatalf("CalculateEncodedSize failed: %v", err)
			}
			encoded, err := EncodeToBytes(wal)
			if err != nil {
				t.Fatalf("EncodeToBytes failed: %v", err)
			}

			if calculatedSize != uint64(len(encoded)) {
				t.Errorf("calculated size = %d, actual = %d", calculatedSize, len(encoded))
			}
		})
	}
}
