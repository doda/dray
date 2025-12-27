package wal

import (
	"bytes"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// FuzzDecodeFromBytes tests the WAL decoder with arbitrary input.
// The fuzzer should not find any panics - all malformed input must result
// in an error being returned.
func FuzzDecodeFromBytes(f *testing.F) {
	// Add seed corpus with valid WAL data
	addValidWALSeeds(f)

	// Add seed corpus with known edge cases
	addEdgeCaseSeeds(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		// The decoder must not panic on any input
		_, _ = DecodeFromBytes(data)
	})
}

// FuzzDecodeHeader tests the header decoder with arbitrary input.
func FuzzDecodeHeader(f *testing.F) {
	// Add seed corpus with valid headers
	addValidWALSeeds(f)

	// Add seed with just the header size
	f.Add(make([]byte, HeaderSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		// The header decoder must not panic on any input
		_, _ = DecodeHeader(data)
	})
}

// FuzzValidateCRC tests CRC validation with arbitrary input.
func FuzzValidateCRC(f *testing.F) {
	addValidWALSeeds(f)

	// Add minimal seed
	f.Add(make([]byte, FooterSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		// CRC validation must not panic on any input
		_ = ValidateCRC(data)
	})
}

// FuzzDecoder tests the Decoder type with arbitrary input.
func FuzzDecoder(f *testing.F) {
	addValidWALSeeds(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		// The Decoder must not panic on any input
		decoder := NewDecoder(bytes.NewReader(data))
		_, _ = decoder.Decode()
	})
}

// FuzzDecodeHeaderFromReader tests DecodeHeaderFromReader with arbitrary input.
func FuzzDecodeHeaderFromReader(f *testing.F) {
	addValidWALSeeds(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeHeaderFromReader(bytes.NewReader(data))
	})
}

// FuzzGetWALID tests GetWALID with arbitrary input.
func FuzzGetWALID(f *testing.F) {
	addValidWALSeeds(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = GetWALID(data)
	})
}

// addValidWALSeeds adds seed corpus entries using valid encoded WALs.
func addValidWALSeeds(f *testing.F) {
	// Empty WAL (no chunks)
	emptyWAL := NewWAL(uuid.MustParse("01234567-89ab-cdef-0123-456789abcdef"), 0, 1000)
	if data, err := EncodeToBytes(emptyWAL); err == nil {
		f.Add(data)
	}

	// Single chunk with single batch
	singleChunk := NewWAL(uuid.MustParse("fedcba98-7654-3210-fedc-ba9876543210"), 1, 2000)
	singleChunk.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("test batch data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})
	if data, err := EncodeToBytes(singleChunk); err == nil {
		f.Add(data)
	}

	// Multiple chunks with multiple batches
	multiChunk := NewWAL(uuid.MustParse("aaaabbbb-cccc-dddd-eeee-ffffffffffff"), 2, 3000)
	multiChunk.AddChunk(Chunk{
		StreamID: 1,
		Batches: []BatchEntry{
			{Data: []byte("batch1")},
			{Data: []byte("batch2 with more data")},
		},
		RecordCount:    5,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
	})
	multiChunk.AddChunk(Chunk{
		StreamID:       2,
		Batches:        []BatchEntry{{Data: []byte("stream 2 data")}},
		RecordCount:    1,
		MinTimestampMs: 1500,
		MaxTimestampMs: 1500,
	})
	if data, err := EncodeToBytes(multiChunk); err == nil {
		f.Add(data)
	}

	// WAL with empty batch data
	emptyBatch := NewWAL(uuid.MustParse("11111111-2222-3333-4444-555555555555"), 0, 4000)
	emptyBatch.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte{}}},
		RecordCount:    0,
		MinTimestampMs: 0,
		MaxTimestampMs: 0,
	})
	if data, err := EncodeToBytes(emptyBatch); err == nil {
		f.Add(data)
	}

	// WAL with large batch data
	largeBatch := NewWAL(uuid.MustParse("66666666-7777-8888-9999-aaaaaaaaaaaa"), 0xFFFFFFFF, 5000)
	largeBatch.AddChunk(Chunk{
		StreamID:       0xFFFFFFFFFFFFFFFF,
		Batches:        []BatchEntry{{Data: make([]byte, 1024)}},
		RecordCount:    100,
		MinTimestampMs: -1,
		MaxTimestampMs: 0x7FFFFFFFFFFFFFFF,
	})
	if data, err := EncodeToBytes(largeBatch); err == nil {
		f.Add(data)
	}
}

// addEdgeCaseSeeds adds seed corpus entries with known edge cases.
func addEdgeCaseSeeds(f *testing.F) {
	// Empty data
	f.Add([]byte{})

	// Just magic bytes
	f.Add([]byte(MagicBytes))

	// Magic with partial header
	partial := make([]byte, HeaderSize-1)
	copy(partial, MagicBytes)
	f.Add(partial)

	// Full header, no footer
	fullHeader := make([]byte, HeaderSize)
	copy(fullHeader, MagicBytes)
	fullHeader[7] = 0 // version high byte
	fullHeader[8] = 1 // version low byte
	f.Add(fullHeader)

	// Header + footer, no chunks
	minWAL := make([]byte, HeaderSize+FooterSize)
	copy(minWAL, MagicBytes)
	minWAL[7] = 0
	minWAL[8] = 1
	f.Add(minWAL)

	// Random garbage
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	// Almost valid magic
	almostMagic := []byte("DRAYWO2")
	f.Add(almostMagic)

	// Valid magic, wrong version
	wrongVersion := make([]byte, HeaderSize+FooterSize)
	copy(wrongVersion, MagicBytes)
	wrongVersion[7] = 0
	wrongVersion[8] = 2 // wrong version
	f.Add(wrongVersion)
}

// TestFuzzDecodeFromBytesNoPanic explicitly tests that the decoder never panics.
func TestFuzzDecodeFromBytesNoPanic(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"nil", nil},
		{"one_byte", []byte{0}},
		{"magic_only", []byte(MagicBytes)},
		{"header_size_zeros", make([]byte, HeaderSize)},
		{"header_plus_footer_zeros", make([]byte, HeaderSize+FooterSize)},
		{"random_small", []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{"all_0xFF", bytes.Repeat([]byte{0xFF}, 100)},
		{"all_0x00", make([]byte, 100)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Should not panic, error is expected
			result, err := DecodeFromBytes(tc.data)
			if err == nil && result == nil {
				t.Error("expected either result or error, got neither")
			}
		})
	}
}

// TestDecoderReturnsErrorsForInvalidData verifies that the decoder returns
// appropriate errors for various types of invalid data.
func TestDecoderReturnsErrorsForInvalidData(t *testing.T) {
	testCases := []struct {
		name          string
		data          []byte
		expectedError error
	}{
		{
			name:          "empty_data",
			data:          []byte{},
			expectedError: ErrTruncatedHeader,
		},
		{
			name:          "too_short",
			data:          make([]byte, HeaderSize-1),
			expectedError: ErrTruncatedHeader,
		},
		{
			name:          "wrong_magic",
			data:          append([]byte("WRONGXX"), make([]byte, HeaderSize+FooterSize-7)...),
			expectedError: ErrInvalidMagic,
		},
		{
			name: "wrong_version",
			data: func() []byte {
				d := make([]byte, HeaderSize+FooterSize)
				copy(d, MagicBytes)
				d[7] = 0xFF
				d[8] = 0xFF
				return d
			}(),
			expectedError: ErrUnsupportedVersion,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeFromBytes(tc.data)
			if err == nil {
				t.Error("expected error, got nil")
				return
			}
			if tc.expectedError != nil && !errors.Is(err, tc.expectedError) {
				t.Fatalf("expected error %v, got %v", tc.expectedError, err)
			}
		})
	}
}
