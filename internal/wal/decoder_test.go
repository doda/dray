package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/google/uuid"
)

func TestDecodeEmptyWAL(t *testing.T) {
	walID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")
	wal := NewWAL(walID, 42, 1703686800000)

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	if decoded.WalID != walID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, walID)
	}
	if decoded.MetaDomain != 42 {
		t.Errorf("MetaDomain = %d, want 42", decoded.MetaDomain)
	}
	if decoded.CreatedAtUnixMs != 1703686800000 {
		t.Errorf("CreatedAtUnixMs = %d, want 1703686800000", decoded.CreatedAtUnixMs)
	}
	if len(decoded.Chunks) != 0 {
		t.Errorf("len(Chunks) = %d, want 0", len(decoded.Chunks))
	}
}

func TestDecodeSingleChunk(t *testing.T) {
	walID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	wal := NewWAL(walID, 1, 1700000000000)

	batchData := []byte("test kafka batch data")
	wal.AddChunk(Chunk{
		StreamID:       100,
		Batches:        []BatchEntry{{Data: batchData}},
		RecordCount:    5,
		MinTimestampMs: 1700000000000,
		MaxTimestampMs: 1700000001000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	if decoded.WalID != walID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, walID)
	}
	if len(decoded.Chunks) != 1 {
		t.Fatalf("len(Chunks) = %d, want 1", len(decoded.Chunks))
	}

	chunk := decoded.Chunks[0]
	if chunk.StreamID != 100 {
		t.Errorf("StreamID = %d, want 100", chunk.StreamID)
	}
	if chunk.RecordCount != 5 {
		t.Errorf("RecordCount = %d, want 5", chunk.RecordCount)
	}
	if chunk.MinTimestampMs != 1700000000000 {
		t.Errorf("MinTimestampMs = %d, want 1700000000000", chunk.MinTimestampMs)
	}
	if chunk.MaxTimestampMs != 1700000001000 {
		t.Errorf("MaxTimestampMs = %d, want 1700000001000", chunk.MaxTimestampMs)
	}
	if len(chunk.Batches) != 1 {
		t.Fatalf("len(Batches) = %d, want 1", len(chunk.Batches))
	}
	if !bytes.Equal(chunk.Batches[0].Data, batchData) {
		t.Errorf("batch data = %q, want %q", chunk.Batches[0].Data, batchData)
	}
}

func TestDecodeMultipleChunks(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 5, 1700000000000)

	wal.AddChunk(Chunk{
		StreamID:       300,
		Batches:        []BatchEntry{{Data: []byte("third")}},
		RecordCount:    3,
		MinTimestampMs: 3000,
		MaxTimestampMs: 3000,
	})
	wal.AddChunk(Chunk{
		StreamID:       100,
		Batches:        []BatchEntry{{Data: []byte("first")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})
	wal.AddChunk(Chunk{
		StreamID:       200,
		Batches:        []BatchEntry{{Data: []byte("second")}},
		RecordCount:    2,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	if len(decoded.Chunks) != 3 {
		t.Fatalf("len(Chunks) = %d, want 3", len(decoded.Chunks))
	}

	// Chunks should be sorted by StreamID
	expectedOrder := []struct {
		streamID    uint64
		data        string
		recordCount uint32
	}{
		{100, "first", 1},
		{200, "second", 2},
		{300, "third", 3},
	}

	for i, exp := range expectedOrder {
		chunk := decoded.Chunks[i]
		if chunk.StreamID != exp.streamID {
			t.Errorf("chunk[%d].StreamID = %d, want %d", i, chunk.StreamID, exp.streamID)
		}
		if chunk.RecordCount != exp.recordCount {
			t.Errorf("chunk[%d].RecordCount = %d, want %d", i, chunk.RecordCount, exp.recordCount)
		}
		if string(chunk.Batches[0].Data) != exp.data {
			t.Errorf("chunk[%d].Batches[0].Data = %q, want %q", i, string(chunk.Batches[0].Data), exp.data)
		}
	}
}

func TestDecodeMultipleBatches(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	batch1 := []byte("batch one data")
	batch2 := []byte("batch two with more bytes")
	batch3 := []byte("b3")

	wal.AddChunk(Chunk{
		StreamID: 1,
		Batches: []BatchEntry{
			{Data: batch1},
			{Data: batch2},
			{Data: batch3},
		},
		RecordCount:    10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 3000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	if len(decoded.Chunks) != 1 {
		t.Fatalf("len(Chunks) = %d, want 1", len(decoded.Chunks))
	}

	chunk := decoded.Chunks[0]
	if len(chunk.Batches) != 3 {
		t.Fatalf("len(Batches) = %d, want 3", len(chunk.Batches))
	}

	if !bytes.Equal(chunk.Batches[0].Data, batch1) {
		t.Errorf("batch[0] = %q, want %q", chunk.Batches[0].Data, batch1)
	}
	if !bytes.Equal(chunk.Batches[1].Data, batch2) {
		t.Errorf("batch[1] = %q, want %q", chunk.Batches[1].Data, batch2)
	}
	if !bytes.Equal(chunk.Batches[2].Data, batch3) {
		t.Errorf("batch[2] = %q, want %q", chunk.Batches[2].Data, batch3)
	}
}

func TestDecodeWithReader(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       42,
		Batches:        []BatchEntry{{Data: []byte("test")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoder := NewDecoder(bytes.NewReader(data))
	decoded, err := decoder.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.WalID != walID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, walID)
	}
	if len(decoded.Chunks) != 1 {
		t.Errorf("len(Chunks) = %d, want 1", len(decoded.Chunks))
	}
}

func TestDecodeInvalidMagic(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Corrupt magic bytes
	data[0] = 'X'

	_, err = DecodeFromBytes(data)
	if !errors.Is(err, ErrInvalidMagic) {
		t.Errorf("expected ErrInvalidMagic, got %v", err)
	}
}

func TestDecodeInvalidVersion(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Corrupt version
	binary.BigEndian.PutUint16(data[7:9], 99)

	_, err = DecodeFromBytes(data)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("expected ErrUnsupportedVersion, got %v", err)
	}
}

func TestDecodeInvalidCRC(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Corrupt the data
	data[HeaderSize+5] ^= 0xff

	_, err = DecodeFromBytes(data)
	if !errors.Is(err, ErrInvalidCRC) {
		t.Errorf("expected ErrInvalidCRC, got %v", err)
	}
}

func TestDecodeTruncatedHeader(t *testing.T) {
	_, err := DecodeFromBytes([]byte("short"))
	if !errors.Is(err, ErrTruncatedHeader) {
		t.Errorf("expected ErrTruncatedHeader, got %v", err)
	}

	// Just under header size
	_, err = DecodeFromBytes(make([]byte, HeaderSize-1))
	if !errors.Is(err, ErrTruncatedHeader) {
		t.Errorf("expected ErrTruncatedHeader, got %v", err)
	}
}

func TestDecodeTruncatedIndex(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Truncate before chunk index completes (remove last 10 bytes of index)
	truncated := data[:len(data)-FooterSize-10]
	// Fix CRC for truncated data
	crc := crc32.Checksum(truncated, crc32cTable)
	truncated = append(truncated, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(truncated[len(truncated)-4:], crc)

	_, err = DecodeFromBytes(truncated)
	if !errors.Is(err, ErrTruncatedIndex) {
		t.Errorf("expected ErrTruncatedIndex, got %v", err)
	}
}

func TestDecodeLargePayload(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	// Create a 1MB payload
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: largeData}},
		RecordCount:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	if len(decoded.Chunks) != 1 {
		t.Fatalf("len(Chunks) = %d, want 1", len(decoded.Chunks))
	}
	if !bytes.Equal(decoded.Chunks[0].Batches[0].Data, largeData) {
		t.Error("large payload data mismatch")
	}
}

func TestDecodeManyChunks(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	numChunks := 100
	for i := 0; i < numChunks; i++ {
		wal.AddChunk(Chunk{
			StreamID:       uint64(numChunks - i), // reverse order
			Batches:        []BatchEntry{{Data: []byte{byte(i)}}},
			RecordCount:    1,
			MinTimestampMs: int64(i * 1000),
			MaxTimestampMs: int64(i * 1000),
		})
	}

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	if len(decoded.Chunks) != numChunks {
		t.Fatalf("len(Chunks) = %d, want %d", len(decoded.Chunks), numChunks)
	}

	// Verify chunks are sorted by StreamID
	var prevStreamID uint64
	for i, chunk := range decoded.Chunks {
		if i > 0 && chunk.StreamID <= prevStreamID {
			t.Errorf("chunks not sorted: streamID %d after %d at index %d", chunk.StreamID, prevStreamID, i)
		}
		prevStreamID = chunk.StreamID
	}
}

func TestDecodeHeader(t *testing.T) {
	walID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")
	wal := NewWAL(walID, 42, 1703686800000)
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	header, err := DecodeHeader(data)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if string(header.Magic[:]) != MagicBytes {
		t.Errorf("Magic = %q, want %q", string(header.Magic[:]), MagicBytes)
	}
	if header.Version != Version {
		t.Errorf("Version = %d, want %d", header.Version, Version)
	}
	if header.WalID != walID {
		t.Errorf("WalID = %v, want %v", header.WalID, walID)
	}
	if header.MetaDomain != 42 {
		t.Errorf("MetaDomain = %d, want 42", header.MetaDomain)
	}
	if header.CreatedAtUnixMs != 1703686800000 {
		t.Errorf("CreatedAtUnixMs = %d, want 1703686800000", header.CreatedAtUnixMs)
	}
	if header.ChunkCount != 1 {
		t.Errorf("ChunkCount = %d, want 1", header.ChunkCount)
	}
}

func TestDecodeHeaderFromReader(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 99, 1700000000000)

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	header, err := DecodeHeaderFromReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeHeaderFromReader failed: %v", err)
	}

	if header.WalID != walID {
		t.Errorf("WalID = %v, want %v", header.WalID, walID)
	}
	if header.MetaDomain != 99 {
		t.Errorf("MetaDomain = %d, want 99", header.MetaDomain)
	}
}

func TestValidateCRC(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Valid CRC
	if err := ValidateCRC(data); err != nil {
		t.Errorf("ValidateCRC failed on valid data: %v", err)
	}

	// Invalid CRC
	corrupt := make([]byte, len(data))
	copy(corrupt, data)
	corrupt[HeaderSize+2] ^= 0xff

	if err := ValidateCRC(corrupt); !errors.Is(err, ErrInvalidCRC) {
		t.Errorf("expected ErrInvalidCRC, got %v", err)
	}
}

func TestGetWALID(t *testing.T) {
	walID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	wal := NewWAL(walID, 1, 1700000000000)

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	extractedID, err := GetWALID(data)
	if err != nil {
		t.Fatalf("GetWALID failed: %v", err)
	}

	if extractedID != walID {
		t.Errorf("WalID = %v, want %v", extractedID, walID)
	}
}

func TestRoundTripEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		wal  *WAL
	}{
		{
			name: "empty",
			wal:  NewWAL(uuid.New(), 0, 0),
		},
		{
			name: "single chunk single batch",
			wal: func() *WAL {
				w := NewWAL(uuid.New(), 1, 1000)
				w.AddChunk(Chunk{
					StreamID:       1,
					Batches:        []BatchEntry{{Data: []byte("hello")}},
					RecordCount:    1,
					MinTimestampMs: 1000,
					MaxTimestampMs: 1000,
				})
				return w
			}(),
		},
		{
			name: "single chunk multiple batches",
			wal: func() *WAL {
				w := NewWAL(uuid.New(), 2, 2000)
				w.AddChunk(Chunk{
					StreamID: 1,
					Batches: []BatchEntry{
						{Data: []byte("batch1")},
						{Data: []byte("batch2")},
						{Data: []byte("batch3")},
					},
					RecordCount:    10,
					MinTimestampMs: 1000,
					MaxTimestampMs: 3000,
				})
				return w
			}(),
		},
		{
			name: "multiple chunks",
			wal: func() *WAL {
				w := NewWAL(uuid.New(), 3, 3000)
				for i := uint64(1); i <= 5; i++ {
					w.AddChunk(Chunk{
						StreamID:       i * 100,
						Batches:        []BatchEntry{{Data: []byte{byte(i)}}},
						RecordCount:    uint32(i),
						MinTimestampMs: int64(i * 1000),
						MaxTimestampMs: int64(i * 2000),
					})
				}
				return w
			}(),
		},
		{
			name: "max meta domain",
			wal:  NewWAL(uuid.New(), 0xFFFFFFFF, 0),
		},
		{
			name: "large timestamps",
			wal: func() *WAL {
				w := NewWAL(uuid.New(), 1, 1<<62)
				w.AddChunk(Chunk{
					StreamID:       1,
					Batches:        []BatchEntry{{Data: []byte("data")}},
					RecordCount:    1,
					MinTimestampMs: 1 << 62,
					MaxTimestampMs: (1 << 62) + 1000,
				})
				return w
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := EncodeToBytes(tt.wal)
			if err != nil {
				t.Fatalf("EncodeToBytes failed: %v", err)
			}

			decoded, err := DecodeFromBytes(data)
			if err != nil {
				t.Fatalf("DecodeFromBytes failed: %v", err)
			}

			if decoded.WalID != tt.wal.WalID {
				t.Errorf("WalID = %v, want %v", decoded.WalID, tt.wal.WalID)
			}
			if decoded.MetaDomain != tt.wal.MetaDomain {
				t.Errorf("MetaDomain = %d, want %d", decoded.MetaDomain, tt.wal.MetaDomain)
			}
			if decoded.CreatedAtUnixMs != tt.wal.CreatedAtUnixMs {
				t.Errorf("CreatedAtUnixMs = %d, want %d", decoded.CreatedAtUnixMs, tt.wal.CreatedAtUnixMs)
			}
			if len(decoded.Chunks) != len(tt.wal.Chunks) {
				t.Fatalf("len(Chunks) = %d, want %d", len(decoded.Chunks), len(tt.wal.Chunks))
			}
		})
	}
}

func TestRoundTripPreservesAllFields(t *testing.T) {
	walID := uuid.MustParse("01234567-89ab-cdef-0123-456789abcdef")
	wal := NewWAL(walID, 12345, 9876543210000)

	batch1 := []byte("first batch with some data")
	batch2 := []byte("second batch with different data")

	wal.AddChunk(Chunk{
		StreamID: 555,
		Batches: []BatchEntry{
			{Data: batch1},
			{Data: batch2},
		},
		RecordCount:    42,
		MinTimestampMs: 1000000000000,
		MaxTimestampMs: 2000000000000,
	})

	wal.AddChunk(Chunk{
		StreamID:       111, // will be sorted before 555
		Batches:        []BatchEntry{{Data: []byte("single batch")}},
		RecordCount:    7,
		MinTimestampMs: 500000000000,
		MaxTimestampMs: 500000001000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	// Verify WAL-level fields
	if decoded.WalID != walID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, walID)
	}
	if decoded.MetaDomain != 12345 {
		t.Errorf("MetaDomain = %d, want 12345", decoded.MetaDomain)
	}
	if decoded.CreatedAtUnixMs != 9876543210000 {
		t.Errorf("CreatedAtUnixMs = %d, want 9876543210000", decoded.CreatedAtUnixMs)
	}

	if len(decoded.Chunks) != 2 {
		t.Fatalf("len(Chunks) = %d, want 2", len(decoded.Chunks))
	}

	// Chunks should be sorted by StreamID
	chunk0 := decoded.Chunks[0]
	if chunk0.StreamID != 111 {
		t.Errorf("chunk[0].StreamID = %d, want 111", chunk0.StreamID)
	}
	if chunk0.RecordCount != 7 {
		t.Errorf("chunk[0].RecordCount = %d, want 7", chunk0.RecordCount)
	}
	if chunk0.MinTimestampMs != 500000000000 {
		t.Errorf("chunk[0].MinTimestampMs = %d, want 500000000000", chunk0.MinTimestampMs)
	}
	if chunk0.MaxTimestampMs != 500000001000 {
		t.Errorf("chunk[0].MaxTimestampMs = %d, want 500000001000", chunk0.MaxTimestampMs)
	}
	if len(chunk0.Batches) != 1 {
		t.Fatalf("chunk[0] len(Batches) = %d, want 1", len(chunk0.Batches))
	}
	if !bytes.Equal(chunk0.Batches[0].Data, []byte("single batch")) {
		t.Errorf("chunk[0].Batches[0].Data = %q, want 'single batch'", chunk0.Batches[0].Data)
	}

	chunk1 := decoded.Chunks[1]
	if chunk1.StreamID != 555 {
		t.Errorf("chunk[1].StreamID = %d, want 555", chunk1.StreamID)
	}
	if chunk1.RecordCount != 42 {
		t.Errorf("chunk[1].RecordCount = %d, want 42", chunk1.RecordCount)
	}
	if chunk1.MinTimestampMs != 1000000000000 {
		t.Errorf("chunk[1].MinTimestampMs = %d, want 1000000000000", chunk1.MinTimestampMs)
	}
	if chunk1.MaxTimestampMs != 2000000000000 {
		t.Errorf("chunk[1].MaxTimestampMs = %d, want 2000000000000", chunk1.MaxTimestampMs)
	}
	if len(chunk1.Batches) != 2 {
		t.Fatalf("chunk[1] len(Batches) = %d, want 2", len(chunk1.Batches))
	}
	if !bytes.Equal(chunk1.Batches[0].Data, batch1) {
		t.Errorf("chunk[1].Batches[0].Data mismatch")
	}
	if !bytes.Equal(chunk1.Batches[1].Data, batch2) {
		t.Errorf("chunk[1].Batches[1].Data mismatch")
	}
}

func TestDecodeEmptyBatchData(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte{}}},
		RecordCount:    0,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	if len(decoded.Chunks) != 1 {
		t.Fatalf("len(Chunks) = %d, want 1", len(decoded.Chunks))
	}
	if len(decoded.Chunks[0].Batches) != 1 {
		t.Fatalf("len(Batches) = %d, want 1", len(decoded.Chunks[0].Batches))
	}
	if len(decoded.Chunks[0].Batches[0].Data) != 0 {
		t.Errorf("batch data length = %d, want 0", len(decoded.Chunks[0].Batches[0].Data))
	}
}

func TestDecodeWithoutFooter(t *testing.T) {
	walID := uuid.MustParse("abcdef01-2345-6789-abcd-ef0123456789")
	wal := NewWAL(walID, 99, 1700000000000)

	batch1 := []byte("first batch data here")
	batch2 := []byte("second batch")
	wal.AddChunk(Chunk{
		StreamID: 100,
		Batches: []BatchEntry{
			{Data: batch1},
			{Data: batch2},
		},
		RecordCount:    5,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
	})
	wal.AddChunk(Chunk{
		StreamID:       200,
		Batches:        []BatchEntry{{Data: []byte("stream 200 data")}},
		RecordCount:    1,
		MinTimestampMs: 3000,
		MaxTimestampMs: 3000,
	})

	// Encode normally (with footer)
	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Verify it works with footer
	_, err = DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes with footer failed: %v", err)
	}

	// Strip the CRC32C footer (last 4 bytes)
	dataWithoutFooter := data[:len(data)-FooterSize]

	// Decode without footer should succeed
	decoded, err := DecodeFromBytes(dataWithoutFooter)
	if err != nil {
		t.Fatalf("DecodeFromBytes without footer failed: %v", err)
	}

	// Verify the decoded data matches
	if decoded.WalID != walID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, walID)
	}
	if decoded.MetaDomain != 99 {
		t.Errorf("MetaDomain = %d, want 99", decoded.MetaDomain)
	}
	if decoded.CreatedAtUnixMs != 1700000000000 {
		t.Errorf("CreatedAtUnixMs = %d, want 1700000000000", decoded.CreatedAtUnixMs)
	}
	if len(decoded.Chunks) != 2 {
		t.Fatalf("len(Chunks) = %d, want 2", len(decoded.Chunks))
	}

	// Chunks are sorted by StreamID
	chunk0 := decoded.Chunks[0]
	if chunk0.StreamID != 100 {
		t.Errorf("chunk[0].StreamID = %d, want 100", chunk0.StreamID)
	}
	if chunk0.RecordCount != 5 {
		t.Errorf("chunk[0].RecordCount = %d, want 5", chunk0.RecordCount)
	}
	if len(chunk0.Batches) != 2 {
		t.Fatalf("chunk[0] len(Batches) = %d, want 2", len(chunk0.Batches))
	}
	if !bytes.Equal(chunk0.Batches[0].Data, batch1) {
		t.Errorf("chunk[0].Batches[0].Data = %q, want %q", chunk0.Batches[0].Data, batch1)
	}
	if !bytes.Equal(chunk0.Batches[1].Data, batch2) {
		t.Errorf("chunk[0].Batches[1].Data = %q, want %q", chunk0.Batches[1].Data, batch2)
	}

	chunk1 := decoded.Chunks[1]
	if chunk1.StreamID != 200 {
		t.Errorf("chunk[1].StreamID = %d, want 200", chunk1.StreamID)
	}
	if chunk1.RecordCount != 1 {
		t.Errorf("chunk[1].RecordCount = %d, want 1", chunk1.RecordCount)
	}
}

func TestDecodeEmptyWALWithoutFooter(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 42, 1703686800000)

	// Encode with footer
	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Strip footer
	dataWithoutFooter := data[:len(data)-FooterSize]

	// Decode without footer
	decoded, err := DecodeFromBytes(dataWithoutFooter)
	if err != nil {
		t.Fatalf("DecodeFromBytes without footer failed: %v", err)
	}

	if decoded.WalID != walID {
		t.Errorf("WalID = %v, want %v", decoded.WalID, walID)
	}
	if decoded.MetaDomain != 42 {
		t.Errorf("MetaDomain = %d, want 42", decoded.MetaDomain)
	}
	if len(decoded.Chunks) != 0 {
		t.Errorf("len(Chunks) = %d, want 0", len(decoded.Chunks))
	}
}

// --- Streaming Decoder Tests ---

func TestStreamingDecoderEmpty(t *testing.T) {
	walID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")
	wal := NewWAL(walID, 42, 1703686800000)

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	reader := NewBytesRangeReader(data)
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	header := sd.Header()
	if header.WalID != walID {
		t.Errorf("WalID = %v, want %v", header.WalID, walID)
	}
	if header.MetaDomain != 42 {
		t.Errorf("MetaDomain = %d, want 42", header.MetaDomain)
	}
	if sd.ChunkCount() != 0 {
		t.Errorf("ChunkCount = %d, want 0", sd.ChunkCount())
	}
}

func TestStreamingDecoderSingleChunk(t *testing.T) {
	walID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	wal := NewWAL(walID, 1, 1700000000000)

	batchData := []byte("test kafka batch data")
	wal.AddChunk(Chunk{
		StreamID:       100,
		Batches:        []BatchEntry{{Data: batchData}},
		RecordCount:    5,
		MinTimestampMs: 1700000000000,
		MaxTimestampMs: 1700000001000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	reader := NewBytesRangeReader(data)
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	if sd.ChunkCount() != 1 {
		t.Fatalf("ChunkCount = %d, want 1", sd.ChunkCount())
	}

	// Verify chunk index entry
	entry := sd.ChunkIndex(0)
	if entry == nil {
		t.Fatal("ChunkIndex(0) returned nil")
	}
	if entry.StreamID != 100 {
		t.Errorf("StreamID = %d, want 100", entry.StreamID)
	}
	if entry.RecordCount != 5 {
		t.Errorf("RecordCount = %d, want 5", entry.RecordCount)
	}

	// Read chunk
	chunk, err := sd.ReadChunk(0)
	if err != nil {
		t.Fatalf("ReadChunk(0) failed: %v", err)
	}
	if chunk.StreamID != 100 {
		t.Errorf("chunk.StreamID = %d, want 100", chunk.StreamID)
	}
	if len(chunk.Batches) != 1 {
		t.Fatalf("len(Batches) = %d, want 1", len(chunk.Batches))
	}
	if !bytes.Equal(chunk.Batches[0].Data, batchData) {
		t.Errorf("batch data = %q, want %q", chunk.Batches[0].Data, batchData)
	}
}

func TestStreamingDecoderMultipleChunks(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 5, 1700000000000)

	wal.AddChunk(Chunk{
		StreamID:       300,
		Batches:        []BatchEntry{{Data: []byte("third")}},
		RecordCount:    3,
		MinTimestampMs: 3000,
		MaxTimestampMs: 3000,
	})
	wal.AddChunk(Chunk{
		StreamID:       100,
		Batches:        []BatchEntry{{Data: []byte("first")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})
	wal.AddChunk(Chunk{
		StreamID:       200,
		Batches:        []BatchEntry{{Data: []byte("second")}},
		RecordCount:    2,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	reader := NewBytesRangeReader(data)
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	if sd.ChunkCount() != 3 {
		t.Fatalf("ChunkCount = %d, want 3", sd.ChunkCount())
	}

	// Chunks should be sorted by StreamID in index
	expectedOrder := []struct {
		streamID    uint64
		data        string
		recordCount uint32
	}{
		{100, "first", 1},
		{200, "second", 2},
		{300, "third", 3},
	}

	for i, exp := range expectedOrder {
		entry := sd.ChunkIndex(i)
		if entry.StreamID != exp.streamID {
			t.Errorf("entry[%d].StreamID = %d, want %d", i, entry.StreamID, exp.streamID)
		}
		if entry.RecordCount != exp.recordCount {
			t.Errorf("entry[%d].RecordCount = %d, want %d", i, entry.RecordCount, exp.recordCount)
		}

		chunk, err := sd.ReadChunk(i)
		if err != nil {
			t.Fatalf("ReadChunk(%d) failed: %v", i, err)
		}
		if string(chunk.Batches[0].Data) != exp.data {
			t.Errorf("chunk[%d].Batches[0].Data = %q, want %q", i, string(chunk.Batches[0].Data), exp.data)
		}
	}
}

func TestStreamingDecoderReadChunkRaw(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	batch1 := []byte("batch one")
	batch2 := []byte("batch two")
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: batch1}, {Data: batch2}},
		RecordCount:    10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	reader := NewBytesRangeReader(data)
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	rawChunk, err := sd.ReadChunkRaw(0)
	if err != nil {
		t.Fatalf("ReadChunkRaw(0) failed: %v", err)
	}

	// Verify raw data length matches expected: 4 + len(batch1) + 4 + len(batch2)
	expectedLen := 4 + len(batch1) + 4 + len(batch2)
	if len(rawChunk) != expectedLen {
		t.Errorf("raw chunk length = %d, want %d", len(rawChunk), expectedLen)
	}

	// Verify first batch length prefix
	batch1Len := binary.BigEndian.Uint32(rawChunk[0:4])
	if batch1Len != uint32(len(batch1)) {
		t.Errorf("batch1 length prefix = %d, want %d", batch1Len, len(batch1))
	}
}

func TestStreamingDecoderDecodeAll(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 7, 1700000000000)

	for i := 0; i < 5; i++ {
		wal.AddChunk(Chunk{
			StreamID:       uint64(i + 1),
			Batches:        []BatchEntry{{Data: []byte{byte(i)}}},
			RecordCount:    uint32(i + 1),
			MinTimestampMs: int64(i * 1000),
			MaxTimestampMs: int64(i * 1000),
		})
	}

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Compare DecodeFromBytes and StreamingDecoder.DecodeAll
	fullDecoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("DecodeFromBytes failed: %v", err)
	}

	reader := NewBytesRangeReader(data)
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	streamDecoded, err := sd.DecodeAll()
	if err != nil {
		t.Fatalf("DecodeAll failed: %v", err)
	}

	if fullDecoded.WalID != streamDecoded.WalID {
		t.Errorf("WalID mismatch")
	}
	if fullDecoded.MetaDomain != streamDecoded.MetaDomain {
		t.Errorf("MetaDomain mismatch")
	}
	if len(fullDecoded.Chunks) != len(streamDecoded.Chunks) {
		t.Fatalf("Chunk count mismatch: %d vs %d", len(fullDecoded.Chunks), len(streamDecoded.Chunks))
	}

	for i, fc := range fullDecoded.Chunks {
		sc := streamDecoded.Chunks[i]
		if fc.StreamID != sc.StreamID {
			t.Errorf("chunk[%d].StreamID mismatch", i)
		}
		if fc.RecordCount != sc.RecordCount {
			t.Errorf("chunk[%d].RecordCount mismatch", i)
		}
		if len(fc.Batches) != len(sc.Batches) {
			t.Errorf("chunk[%d].Batches length mismatch", i)
		}
		for j := range fc.Batches {
			if !bytes.Equal(fc.Batches[j].Data, sc.Batches[j].Data) {
				t.Errorf("chunk[%d].Batches[%d].Data mismatch", i, j)
			}
		}
	}
}

func TestStreamingDecoderChunkIndexOutOfBounds(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	reader := NewBytesRangeReader(data)
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	// Out of bounds access should return nil for ChunkIndex
	if sd.ChunkIndex(-1) != nil {
		t.Error("ChunkIndex(-1) should return nil")
	}
	if sd.ChunkIndex(1) != nil {
		t.Error("ChunkIndex(1) should return nil for 1-chunk WAL")
	}

	// Out of bounds ReadChunk should error
	_, err = sd.ReadChunk(-1)
	if err == nil {
		t.Error("ReadChunk(-1) should error")
	}
	_, err = sd.ReadChunk(1)
	if err == nil {
		t.Error("ReadChunk(1) should error for 1-chunk WAL")
	}
}

func TestStreamingDecoderChunkEntries(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	for i := 0; i < 3; i++ {
		wal.AddChunk(Chunk{
			StreamID:       uint64((i + 1) * 100),
			Batches:        []BatchEntry{{Data: []byte{byte(i)}}},
			RecordCount:    uint32(i + 1),
			MinTimestampMs: int64(i * 1000),
			MaxTimestampMs: int64(i * 2000),
		})
	}

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	reader := NewBytesRangeReader(data)
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	entries := sd.ChunkEntries()
	if len(entries) != 3 {
		t.Fatalf("len(ChunkEntries) = %d, want 3", len(entries))
	}

	// Verify entries are copies, not references
	entries[0].StreamID = 999
	originalEntry := sd.ChunkIndex(0)
	if originalEntry.StreamID == 999 {
		t.Error("ChunkEntries should return a copy, not a reference")
	}
}

// trackingRangeReader tracks bytes read for verifying bounded memory usage.
type trackingRangeReader struct {
	data        []byte
	bytesRead   int64
	readCalls   int
	maxSingleRead int64
}

func (r *trackingRangeReader) ReadRange(start, end int64) ([]byte, error) {
	if start < 0 || start >= int64(len(r.data)) {
		return nil, errors.New("invalid start")
	}
	if end >= int64(len(r.data)) {
		end = int64(len(r.data)) - 1
	}
	if end < start {
		return nil, errors.New("invalid range")
	}
	size := end - start + 1
	r.bytesRead += size
	r.readCalls++
	if size > r.maxSingleRead {
		r.maxSingleRead = size
	}
	result := make([]byte, size)
	copy(result, r.data[start:end+1])
	return result, nil
}

func TestStreamingDecoderBoundedMemoryUsage(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	// Create a WAL with 100 chunks, each with 10KB payload
	numChunks := 100
	payloadSize := 10 * 1024 // 10KB per chunk
	totalPayloadSize := numChunks * payloadSize

	for i := 0; i < numChunks; i++ {
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte(i + j)
		}
		wal.AddChunk(Chunk{
			StreamID:       uint64(i + 1),
			Batches:        []BatchEntry{{Data: payload}},
			RecordCount:    uint32(i + 1),
			MinTimestampMs: int64(i * 1000),
			MaxTimestampMs: int64(i * 1000),
		})
	}

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Total WAL size should be > 1MB (100 chunks * 10KB = 1MB payload + overhead)
	if len(data) < 100*1024 {
		t.Fatalf("WAL too small for test: %d bytes (expected >100KB)", len(data))
	}
	t.Logf("WAL size: %d bytes (%d chunks, %d bytes payload each)", len(data), numChunks, payloadSize)

	reader := &trackingRangeReader{data: data}

	// Create streaming decoder - should only read header + index
	sd, err := NewStreamingDecoder(reader)
	if err != nil {
		t.Fatalf("NewStreamingDecoder failed: %v", err)
	}

	// After initialization, should have read:
	// - Header: 49 bytes
	// - Chunk index: numChunks * 44 bytes = 4400 bytes
	// Total: ~4.5KB, NOT the full 1MB+ of chunk data
	expectedInitBytes := int64(HeaderSize) + int64(numChunks*ChunkIndexEntrySize)
	if reader.bytesRead != expectedInitBytes {
		t.Errorf("Init bytes read = %d, want %d", reader.bytesRead, expectedInitBytes)
	}
	t.Logf("After init: read %d bytes (%.2f%% of WAL)", reader.bytesRead, float64(reader.bytesRead)*100/float64(len(data)))

	// Verify we can access chunk metadata without reading chunk data
	for i := 0; i < numChunks; i++ {
		entry := sd.ChunkIndex(i)
		if entry == nil {
			t.Fatalf("ChunkIndex(%d) returned nil", i)
		}
		if entry.StreamID != uint64(i+1) {
			t.Errorf("ChunkIndex(%d).StreamID = %d, want %d", i, entry.StreamID, i+1)
		}
	}

	// Bytes read should still be the same - no chunk data read yet
	if reader.bytesRead != expectedInitBytes {
		t.Errorf("After metadata access: bytes read = %d, want %d", reader.bytesRead, expectedInitBytes)
	}

	// Now read only 3 specific chunks (0, 50, 99)
	chunksToRead := []int{0, 50, 99}
	expectedChunkBytes := int64(0)
	for _, i := range chunksToRead {
		entry := sd.ChunkIndex(i)
		expectedChunkBytes += int64(entry.ChunkLength)

		chunk, err := sd.ReadChunk(i)
		if err != nil {
			t.Fatalf("ReadChunk(%d) failed: %v", i, err)
		}
		if chunk.StreamID != uint64(i+1) {
			t.Errorf("chunk[%d].StreamID = %d, want %d", i, chunk.StreamID, i+1)
		}
	}

	// Verify we only read the chunks we asked for
	totalExpectedBytes := expectedInitBytes + expectedChunkBytes
	if reader.bytesRead != totalExpectedBytes {
		t.Errorf("After reading 3 chunks: bytes read = %d, want %d", reader.bytesRead, totalExpectedBytes)
	}

	// Critical check: we should NOT have read anywhere close to the full payload
	if reader.bytesRead > int64(totalPayloadSize/2) {
		t.Errorf("Read too many bytes (%d) - streaming not working properly", reader.bytesRead)
	}

	t.Logf("Final: read %d bytes (%.2f%% of WAL) for header+index+3chunks",
		reader.bytesRead, float64(reader.bytesRead)*100/float64(len(data)))
	t.Logf("Maximum single read: %d bytes", reader.maxSingleRead)
}

func TestBytesRangeReaderEdgeCases(t *testing.T) {
	data := []byte("0123456789")
	reader := NewBytesRangeReader(data)

	// Valid reads
	result, err := reader.ReadRange(0, 4)
	if err != nil {
		t.Fatalf("ReadRange(0,4) failed: %v", err)
	}
	if string(result) != "01234" {
		t.Errorf("ReadRange(0,4) = %q, want '01234'", result)
	}

	// Read to end
	result, err = reader.ReadRange(5, 100) // end beyond length
	if err != nil {
		t.Fatalf("ReadRange(5,100) failed: %v", err)
	}
	if string(result) != "56789" {
		t.Errorf("ReadRange(5,100) = %q, want '56789'", result)
	}

	// Single byte
	result, err = reader.ReadRange(3, 3)
	if err != nil {
		t.Fatalf("ReadRange(3,3) failed: %v", err)
	}
	if string(result) != "3" {
		t.Errorf("ReadRange(3,3) = %q, want '3'", result)
	}

	// Invalid start
	_, err = reader.ReadRange(-1, 5)
	if err == nil {
		t.Error("ReadRange(-1,5) should error")
	}

	// Start beyond data
	_, err = reader.ReadRange(100, 105)
	if err == nil {
		t.Error("ReadRange(100,105) should error")
	}
}
