package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"testing"
	"unsafe"

	"github.com/google/uuid"
)

func TestHeaderSize(t *testing.T) {
	// Verify constant matches actual header size
	// 7 (magic) + 2 (version) + 16 (uuid) + 4 (metaDomain) + 8 (createdAt) + 4 (chunkCount) + 8 (chunkIndexOffset) = 49
	expected := 7 + 2 + 16 + 4 + 8 + 4 + 8
	if HeaderSize != expected {
		t.Errorf("HeaderSize = %d, want %d", HeaderSize, expected)
	}
}

func TestChunkIndexEntrySize(t *testing.T) {
	// 8 (streamId) + 8 (chunkOffset) + 4 (chunkLength) + 4 (recordCount) + 4 (batchCount) + 8 (minTs) + 8 (maxTs) = 44
	expected := 8 + 8 + 4 + 4 + 4 + 8 + 8
	if ChunkIndexEntrySize != expected {
		t.Errorf("ChunkIndexEntrySize = %d, want %d", ChunkIndexEntrySize, expected)
	}
}

func TestEncodeEmptyWAL(t *testing.T) {
	walID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")
	wal := NewWAL(walID, 42, 1703686800000)

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Empty WAL: 49 (header) + 0 (no chunk bodies) + 0 (no chunk index entries) + 4 (footer) = 53
	expectedSize := HeaderSize + FooterSize
	if len(data) != expectedSize {
		t.Errorf("encoded size = %d, want %d", len(data), expectedSize)
	}

	// Verify magic bytes
	if string(data[0:7]) != MagicBytes {
		t.Errorf("magic = %q, want %q", string(data[0:7]), MagicBytes)
	}

	// Verify version
	version := binary.BigEndian.Uint16(data[7:9])
	if version != Version {
		t.Errorf("version = %d, want %d", version, Version)
	}

	// Verify UUID
	var parsedID uuid.UUID
	copy(parsedID[:], data[9:25])
	if parsedID != walID {
		t.Errorf("walID = %v, want %v", parsedID, walID)
	}

	// Verify metaDomain
	metaDomain := binary.BigEndian.Uint32(data[25:29])
	if metaDomain != 42 {
		t.Errorf("metaDomain = %d, want %d", metaDomain, 42)
	}

	// Verify createdAtUnixMs
	createdAt := int64(binary.BigEndian.Uint64(data[29:37]))
	if createdAt != 1703686800000 {
		t.Errorf("createdAt = %d, want %d", createdAt, 1703686800000)
	}

	// Verify chunkCount
	chunkCount := binary.BigEndian.Uint32(data[37:41])
	if chunkCount != 0 {
		t.Errorf("chunkCount = %d, want 0", chunkCount)
	}

	// Verify chunkIndexOffset points to where index would be (right after header for empty)
	chunkIndexOffset := binary.BigEndian.Uint64(data[41:49])
	if chunkIndexOffset != uint64(HeaderSize) {
		t.Errorf("chunkIndexOffset = %d, want %d", chunkIndexOffset, HeaderSize)
	}

	// Verify CRC32C footer
	expectedCRC := crc32.Checksum(data[:len(data)-FooterSize], crc32cTable)
	actualCRC := binary.BigEndian.Uint32(data[len(data)-FooterSize:])
	if actualCRC != expectedCRC {
		t.Errorf("CRC = %08x, want %08x", actualCRC, expectedCRC)
	}
}

func TestEncodeSingleChunk(t *testing.T) {
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

	// Calculate expected size
	chunkBodySize := 4 + len(batchData) // batchLength + batchData
	expectedSize := HeaderSize + chunkBodySize + ChunkIndexEntrySize + FooterSize
	if len(data) != expectedSize {
		t.Errorf("encoded size = %d, want %d", len(data), expectedSize)
	}

	// Verify header
	if string(data[0:7]) != MagicBytes {
		t.Errorf("magic = %q, want %q", string(data[0:7]), MagicBytes)
	}

	chunkCount := binary.BigEndian.Uint32(data[37:41])
	if chunkCount != 1 {
		t.Errorf("chunkCount = %d, want 1", chunkCount)
	}

	// Verify chunk index offset
	expectedChunkIndexOffset := uint64(HeaderSize + chunkBodySize)
	chunkIndexOffset := binary.BigEndian.Uint64(data[41:49])
	if chunkIndexOffset != expectedChunkIndexOffset {
		t.Errorf("chunkIndexOffset = %d, want %d", chunkIndexOffset, expectedChunkIndexOffset)
	}

	// Verify chunk body (right after header)
	chunkBodyStart := HeaderSize
	batchLength := binary.BigEndian.Uint32(data[chunkBodyStart : chunkBodyStart+4])
	if batchLength != uint32(len(batchData)) {
		t.Errorf("batchLength = %d, want %d", batchLength, len(batchData))
	}
	actualBatchData := data[chunkBodyStart+4 : chunkBodyStart+4+int(batchLength)]
	if !bytes.Equal(actualBatchData, batchData) {
		t.Errorf("batchData = %q, want %q", actualBatchData, batchData)
	}

	// Verify chunk index entry
	indexStart := int(chunkIndexOffset)
	streamID := binary.BigEndian.Uint64(data[indexStart : indexStart+8])
	if streamID != 100 {
		t.Errorf("streamID = %d, want 100", streamID)
	}

	chunkOffset := binary.BigEndian.Uint64(data[indexStart+8 : indexStart+16])
	if chunkOffset != uint64(HeaderSize) {
		t.Errorf("chunkOffset = %d, want %d", chunkOffset, HeaderSize)
	}

	chunkLength := binary.BigEndian.Uint32(data[indexStart+16 : indexStart+20])
	if chunkLength != uint32(chunkBodySize) {
		t.Errorf("chunkLength = %d, want %d", chunkLength, chunkBodySize)
	}

	recordCount := binary.BigEndian.Uint32(data[indexStart+20 : indexStart+24])
	if recordCount != 5 {
		t.Errorf("recordCount = %d, want 5", recordCount)
	}

	batchCount := binary.BigEndian.Uint32(data[indexStart+24 : indexStart+28])
	if batchCount != 1 {
		t.Errorf("batchCount = %d, want 1", batchCount)
	}

	minTs := int64(binary.BigEndian.Uint64(data[indexStart+28 : indexStart+36]))
	if minTs != 1700000000000 {
		t.Errorf("minTimestampMs = %d, want 1700000000000", minTs)
	}

	maxTs := int64(binary.BigEndian.Uint64(data[indexStart+36 : indexStart+44]))
	if maxTs != 1700000001000 {
		t.Errorf("maxTimestampMs = %d, want 1700000001000", maxTs)
	}

	// Verify CRC32C
	expectedCRC := crc32.Checksum(data[:len(data)-FooterSize], crc32cTable)
	actualCRC := binary.BigEndian.Uint32(data[len(data)-FooterSize:])
	if actualCRC != expectedCRC {
		t.Errorf("CRC = %08x, want %08x", actualCRC, expectedCRC)
	}
}

func TestEncodeMultipleChunks(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 5, 1700000000000)

	// Add chunks in non-sorted order to verify sorting
	wal.AddChunk(Chunk{
		StreamID:       300,
		Batches:        []BatchEntry{{Data: []byte("third")}},
		RecordCount:    1,
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
		RecordCount:    1,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Verify chunkCount
	chunkCount := binary.BigEndian.Uint32(data[37:41])
	if chunkCount != 3 {
		t.Errorf("chunkCount = %d, want 3", chunkCount)
	}

	// Get chunk index offset
	chunkIndexOffset := binary.BigEndian.Uint64(data[41:49])

	// Verify chunks are sorted by StreamID in index
	indexStart := int(chunkIndexOffset)
	streamIDs := []uint64{
		binary.BigEndian.Uint64(data[indexStart : indexStart+8]),
		binary.BigEndian.Uint64(data[indexStart+ChunkIndexEntrySize : indexStart+ChunkIndexEntrySize+8]),
		binary.BigEndian.Uint64(data[indexStart+2*ChunkIndexEntrySize : indexStart+2*ChunkIndexEntrySize+8]),
	}

	if streamIDs[0] != 100 || streamIDs[1] != 200 || streamIDs[2] != 300 {
		t.Errorf("streamIDs not sorted: got %v, want [100, 200, 300]", streamIDs)
	}

	// Verify chunk bodies are also in sorted order
	// Chunk 0 (streamID 100) should have "first"
	chunk0Offset := binary.BigEndian.Uint64(data[indexStart+8 : indexStart+16])
	chunk0Length := binary.BigEndian.Uint32(data[indexStart+16 : indexStart+20])
	batchLen0 := binary.BigEndian.Uint32(data[chunk0Offset : chunk0Offset+4])
	batchData0 := data[chunk0Offset+4 : chunk0Offset+4+uint64(batchLen0)]
	if string(batchData0) != "first" {
		t.Errorf("chunk0 data = %q, want 'first'", string(batchData0))
	}
	_ = chunk0Length // silence unused warning
}

func TestEncodeMultipleBatches(t *testing.T) {
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

	// Verify batch count in index
	chunkIndexOffset := binary.BigEndian.Uint64(data[41:49])
	batchCount := binary.BigEndian.Uint32(data[chunkIndexOffset+24 : chunkIndexOffset+28])
	if batchCount != 3 {
		t.Errorf("batchCount = %d, want 3", batchCount)
	}

	// Verify all batch data is present in chunk body
	chunkBodyStart := HeaderSize
	offset := chunkBodyStart

	// Batch 1
	len1 := binary.BigEndian.Uint32(data[offset : offset+4])
	if len1 != uint32(len(batch1)) {
		t.Errorf("batch1 length = %d, want %d", len1, len(batch1))
	}
	if !bytes.Equal(data[offset+4:offset+4+int(len1)], batch1) {
		t.Errorf("batch1 data mismatch")
	}
	offset += 4 + int(len1)

	// Batch 2
	len2 := binary.BigEndian.Uint32(data[offset : offset+4])
	if len2 != uint32(len(batch2)) {
		t.Errorf("batch2 length = %d, want %d", len2, len(batch2))
	}
	if !bytes.Equal(data[offset+4:offset+4+int(len2)], batch2) {
		t.Errorf("batch2 data mismatch")
	}
	offset += 4 + int(len2)

	// Batch 3
	len3 := binary.BigEndian.Uint32(data[offset : offset+4])
	if len3 != uint32(len(batch3)) {
		t.Errorf("batch3 length = %d, want %d", len3, len(batch3))
	}
	if !bytes.Equal(data[offset+4:offset+4+int(len3)], batch3) {
		t.Errorf("batch3 data mismatch")
	}
}

func TestEncoderWriter(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       42,
		Batches:        []BatchEntry{{Data: []byte("test")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	var buf bytes.Buffer
	encoder := NewEncoder(&buf)
	n, err := encoder.Encode(wal)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if n != int64(buf.Len()) {
		t.Errorf("returned size = %d, actual buffer size = %d", n, buf.Len())
	}

	// Compare with EncodeToBytes
	expected, _ := EncodeToBytes(wal)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Encoder output differs from EncodeToBytes")
	}
}

func TestEncoderShortWriteFails(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       42,
		Batches:        []BatchEntry{{Data: []byte("test")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	writer := &shortWriter{maxOnce: 8}
	encoder := NewEncoder(writer)
	n, err := encoder.Encode(wal)
	if err == nil {
		t.Fatal("expected short write error")
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", err)
	}
	if n != int64(writer.maxOnce) {
		t.Errorf("returned size = %d, want %d", n, writer.maxOnce)
	}
}

func TestEncodeLayoutMismatchReturnsError(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("first-batch")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})
	wal.AddChunk(Chunk{
		StreamID:       2,
		Batches:        []BatchEntry{{Data: []byte("second-batch")}},
		RecordCount:    1,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2000,
	})

	encoderLayoutHook = func() {
		wal.Chunks[0].Batches[0].Data = wal.Chunks[0].Batches[0].Data[:1]
	}
	t.Cleanup(func() {
		encoderLayoutHook = nil
	})

	var buf bytes.Buffer
	encoder := NewEncoder(&buf)
	_, err := encoder.Encode(wal)
	if err == nil {
		t.Fatal("expected layout mismatch error")
	}
	if !errors.Is(err, ErrLayoutMismatch) {
		t.Fatalf("expected ErrLayoutMismatch, got %v", err)
	}
}

type shortWriter struct {
	maxOnce int
	calls   int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		if len(p) <= w.maxOnce {
			return len(p), nil
		}
		return w.maxOnce, nil
	}
	return 0, nil
}

func TestCalculateEncodedSize(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	// Empty WAL
	size, err := CalculateEncodedSize(wal)
	if err != nil {
		t.Fatalf("CalculateEncodedSize failed: %v", err)
	}
	data, _ := EncodeToBytes(wal)
	if size != uint64(len(data)) {
		t.Errorf("calculated size = %d, actual = %d", size, len(data))
	}

	// With one chunk
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("test data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	size, err = CalculateEncodedSize(wal)
	if err != nil {
		t.Fatalf("CalculateEncodedSize failed: %v", err)
	}
	data, _ = EncodeToBytes(wal)
	if size != uint64(len(data)) {
		t.Errorf("calculated size = %d, actual = %d", size, len(data))
	}

	// With multiple chunks and batches
	wal.AddChunk(Chunk{
		StreamID: 2,
		Batches: []BatchEntry{
			{Data: []byte("batch1")},
			{Data: []byte("batch2longer")},
		},
		RecordCount:    5,
		MinTimestampMs: 2000,
		MaxTimestampMs: 3000,
	})

	size, err = CalculateEncodedSize(wal)
	if err != nil {
		t.Fatalf("CalculateEncodedSize failed: %v", err)
	}
	data, _ = EncodeToBytes(wal)
	if size != uint64(len(data)) {
		t.Errorf("calculated size = %d, actual = %d", size, len(data))
	}
}

func TestChunkBodyLengthOverflow(t *testing.T) {
	// Skip when running with race detector since unsafe.Slice with invalid
	// bounds triggers checkptr panics in race mode
	if raceEnabled {
		t.Skip("skipping test with race detector: unsafe.Slice bounds check")
	}

	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	dummy := new(byte)
	oversized := unsafe.Slice(dummy, int(math.MaxUint32))
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: oversized}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	_, err := EncodeToBytes(wal)
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if !errors.Is(err, ErrChunkBodyTooLarge) {
		t.Fatalf("expected ErrChunkBodyTooLarge, got %v", err)
	}
}

func TestCRC32CIntegrity(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 99, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("important data")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	data, err := EncodeToBytes(wal)
	if err != nil {
		t.Fatalf("EncodeToBytes failed: %v", err)
	}

	// Verify CRC covers the right range
	crcOffset := len(data) - FooterSize
	expectedCRC := crc32.Checksum(data[:crcOffset], crc32cTable)
	actualCRC := binary.BigEndian.Uint32(data[crcOffset:])

	if actualCRC != expectedCRC {
		t.Errorf("CRC mismatch: got %08x, want %08x", actualCRC, expectedCRC)
	}

	// Verify that modifying data changes CRC
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	dataCopy[HeaderSize+4] ^= 0xff // flip a byte in batch data

	modifiedCRC := crc32.Checksum(dataCopy[:crcOffset], crc32cTable)
	if modifiedCRC == expectedCRC {
		t.Error("CRC did not change when data was modified")
	}
}

func TestLargePayload(t *testing.T) {
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

	// Verify the large payload is present
	chunkIndexOffset := binary.BigEndian.Uint64(data[41:49])
	chunkOffset := binary.BigEndian.Uint64(data[chunkIndexOffset+8 : chunkIndexOffset+16])
	batchLen := binary.BigEndian.Uint32(data[chunkOffset : chunkOffset+4])

	if batchLen != uint32(len(largeData)) {
		t.Errorf("batch length = %d, want %d", batchLen, len(largeData))
	}

	extractedData := data[chunkOffset+4 : chunkOffset+4+uint64(batchLen)]
	if !bytes.Equal(extractedData, largeData) {
		t.Error("large payload data mismatch")
	}
}

func TestManyChunks(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	// Add 100 chunks
	numChunks := 100
	for i := 0; i < numChunks; i++ {
		wal.AddChunk(Chunk{
			StreamID:       uint64(numChunks - i), // reverse order to test sorting
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

	// Verify chunk count
	chunkCount := binary.BigEndian.Uint32(data[37:41])
	if chunkCount != uint32(numChunks) {
		t.Errorf("chunkCount = %d, want %d", chunkCount, numChunks)
	}

	// Verify chunks are sorted
	chunkIndexOffset := binary.BigEndian.Uint64(data[41:49])
	var prevStreamID uint64
	for i := 0; i < numChunks; i++ {
		indexPos := int(chunkIndexOffset) + i*ChunkIndexEntrySize
		streamID := binary.BigEndian.Uint64(data[indexPos : indexPos+8])
		if i > 0 && streamID <= prevStreamID {
			t.Errorf("chunks not sorted: streamID %d after %d at index %d", streamID, prevStreamID, i)
		}
		prevStreamID = streamID
	}

	// Verify CRC
	expectedCRC := crc32.Checksum(data[:len(data)-FooterSize], crc32cTable)
	actualCRC := binary.BigEndian.Uint32(data[len(data)-FooterSize:])
	if actualCRC != expectedCRC {
		t.Errorf("CRC mismatch")
	}
}

func TestStreamingEncoderEmpty(t *testing.T) {
	walID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")
	wal := NewWAL(walID, 42, 1703686800000)

	var buf bytes.Buffer
	encoder := NewStreamingEncoder(&buf)
	n, err := encoder.Encode(wal)
	if err != nil {
		t.Fatalf("StreamingEncoder.Encode failed: %v", err)
	}

	if n != int64(buf.Len()) {
		t.Errorf("returned size = %d, actual buffer size = %d", n, buf.Len())
	}

	// Compare with EncodeToBytes
	expected, _ := EncodeToBytes(wal)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("StreamingEncoder output differs from EncodeToBytes")
	}
}

func TestStreamingEncoderSingleChunk(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)
	wal.AddChunk(Chunk{
		StreamID:       42,
		Batches:        []BatchEntry{{Data: []byte("test")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})

	var buf bytes.Buffer
	encoder := NewStreamingEncoder(&buf)
	n, err := encoder.Encode(wal)
	if err != nil {
		t.Fatalf("StreamingEncoder.Encode failed: %v", err)
	}

	if n != int64(buf.Len()) {
		t.Errorf("returned size = %d, actual buffer size = %d", n, buf.Len())
	}

	// Compare with EncodeToBytes
	expected, _ := EncodeToBytes(wal)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("StreamingEncoder output differs from EncodeToBytes")
	}

	if encoder.BytesWritten() != n {
		t.Errorf("BytesWritten() = %d, want %d", encoder.BytesWritten(), n)
	}
}

func TestStreamingEncoderMultipleChunks(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 5, 1700000000000)

	// Add chunks in non-sorted order
	wal.AddChunk(Chunk{
		StreamID:       300,
		Batches:        []BatchEntry{{Data: []byte("third")}},
		RecordCount:    1,
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
		RecordCount:    1,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2000,
	})

	var buf bytes.Buffer
	encoder := NewStreamingEncoder(&buf)
	_, err := encoder.Encode(wal)
	if err != nil {
		t.Fatalf("StreamingEncoder.Encode failed: %v", err)
	}

	// Compare with EncodeToBytes
	expected, _ := EncodeToBytes(wal)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("StreamingEncoder output differs from EncodeToBytes")
	}
}

func TestStreamingEncoderMultipleBatches(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	wal.AddChunk(Chunk{
		StreamID: 1,
		Batches: []BatchEntry{
			{Data: []byte("batch one data")},
			{Data: []byte("batch two with more bytes")},
			{Data: []byte("b3")},
		},
		RecordCount:    10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 3000,
	})

	var buf bytes.Buffer
	encoder := NewStreamingEncoder(&buf)
	_, err := encoder.Encode(wal)
	if err != nil {
		t.Fatalf("StreamingEncoder.Encode failed: %v", err)
	}

	// Compare with EncodeToBytes
	expected, _ := EncodeToBytes(wal)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("StreamingEncoder output differs from EncodeToBytes")
	}
}

func TestStreamingEncoderLargePayload(t *testing.T) {
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

	var buf bytes.Buffer
	encoder := NewStreamingEncoder(&buf)
	_, err := encoder.Encode(wal)
	if err != nil {
		t.Fatalf("StreamingEncoder.Encode failed: %v", err)
	}

	// Compare with EncodeToBytes
	expected, _ := EncodeToBytes(wal)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("StreamingEncoder output differs from EncodeToBytes")
	}
}

func TestStreamingEncoderDuplicateStreamID(t *testing.T) {
	walID := uuid.New()
	wal := NewWAL(walID, 1, 1700000000000)

	wal.AddChunk(Chunk{
		StreamID:       1,
		Batches:        []BatchEntry{{Data: []byte("first")}},
		RecordCount:    1,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1000,
	})
	wal.AddChunk(Chunk{
		StreamID:       1, // Duplicate
		Batches:        []BatchEntry{{Data: []byte("second")}},
		RecordCount:    1,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2000,
	})

	var buf bytes.Buffer
	encoder := NewStreamingEncoder(&buf)
	_, err := encoder.Encode(wal)
	if !errors.Is(err, ErrDuplicateStreamID) {
		t.Errorf("expected ErrDuplicateStreamID, got %v", err)
	}
}
