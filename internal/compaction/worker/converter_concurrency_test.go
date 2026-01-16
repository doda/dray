package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

func TestConverterCollectRecordsParallel(t *testing.T) {
	origProcs := runtime.GOMAXPROCS(0)
	runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(origProcs)

	store := newTrackingStore()
	streamID := "stream-1"

	entries := make([]*index.IndexEntry, 4)
	for i := range entries {
		batch := buildBatchWithTimestampsLite([]int64{int64(1000 + i)})
		chunk := buildChunkDataLite(batch)
		key := fmt.Sprintf("wal/%d", i)
		store.putObject(key, chunk)

		entries[i] = &index.IndexEntry{
			StreamID:    streamID,
			FileType:    index.FileTypeWAL,
			StartOffset: int64(i),
			EndOffset:   int64(i + 1),
			ChunkOffset: 0,
			ChunkLength: uint32(len(chunk)),
			WalPath:     key,
		}
	}

	converter := NewConverter(store, nil)
	result, err := converter.Convert(context.Background(), entries, 0, "topic", nil)
	if err != nil {
		t.Fatalf("converter.Convert returned error: %v", err)
	}

	if result.RecordCount != int64(len(entries)) {
		t.Fatalf("expected %d records, got %d", len(entries), result.RecordCount)
	}

	expectedMax := int32(len(entries))
	if expectedMax > int32(runtime.GOMAXPROCS(0)) {
		expectedMax = int32(runtime.GOMAXPROCS(0))
	}

	maxSeen := store.maxConcurrent.Load()
	if maxSeen < 2 {
		t.Fatalf("expected concurrent GetRange calls, max seen %d", maxSeen)
	}
	if maxSeen > expectedMax {
		t.Fatalf("max concurrent GetRange %d exceeds expected limit %d", maxSeen, expectedMax)
	}
}

type trackingStore struct {
	mu            sync.Mutex
	objects       map[string][]byte
	current       atomic.Int32
	maxConcurrent atomic.Int32
}

func newTrackingStore() *trackingStore {
	return &trackingStore{
		objects: make(map[string][]byte),
	}
}

func (s *trackingStore) putObject(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = data
}

func (s *trackingStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return s.PutWithOptions(ctx, key, reader, size, contentType, objectstore.PutOptions{})
}

func (s *trackingStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.putObject(key, data)
	return nil
}

func (s *trackingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.GetRange(ctx, key, 0, -1)
}

func (s *trackingStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}

	if end == -1 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	if start < 0 || start > end || end >= int64(len(data)) {
		return nil, objectstore.ErrInvalidRange
	}

	inFlight := s.current.Add(1)
	defer s.current.Add(-1)

	for {
		prev := s.maxConcurrent.Load()
		if inFlight > prev && s.maxConcurrent.CompareAndSwap(prev, inFlight) {
			break
		}
		if inFlight <= prev {
			break
		}
	}

	// Small sleep to allow overlap between goroutines
	time.Sleep(20 * time.Millisecond)

	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (s *trackingStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (s *trackingStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}

func (s *trackingStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []objectstore.ObjectMeta
	for key, data := range s.objects {
		if len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix) {
			result = append(result, objectstore.ObjectMeta{
				Key:  key,
				Size: int64(len(data)),
			})
		}
	}
	return result, nil
}

func (s *trackingStore) Close() error {
	return nil
}

// Helper functions to build minimal Kafka batches for tests.
func buildChunkDataLite(batches ...[]byte) []byte {
	var buf bytes.Buffer
	var lenBuf [4]byte
	for _, batch := range batches {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(batch)))
		buf.Write(lenBuf[:])
		buf.Write(batch)
	}
	return buf.Bytes()
}

func buildBatchWithTimestampsLite(timestamps []int64) []byte {
	firstTimestamp := timestamps[0]
	maxTimestamp := timestamps[0]
	var records bytes.Buffer

	for i, ts := range timestamps {
		if ts > maxTimestamp {
			maxTimestamp = ts
		}
		writeTestRecordLite(&records, ts-firstTimestamp, int64(i))
	}

	recordsBytes := records.Bytes()
	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + len(recordsBytes)
	totalSize := 8 + 4 + batchLength

	batch := make([]byte, totalSize)
	offset := 0

	binary.BigEndian.PutUint64(batch[offset:], 0)
	offset += 8

	binary.BigEndian.PutUint32(batch[offset:], uint32(batchLength))
	offset += 4

	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	batch[offset] = 2
	offset++

	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	binary.BigEndian.PutUint16(batch[offset:], 0)
	offset += 2

	binary.BigEndian.PutUint32(batch[offset:], uint32(len(timestamps)-1))
	offset += 4

	binary.BigEndian.PutUint64(batch[offset:], uint64(firstTimestamp))
	offset += 8

	binary.BigEndian.PutUint64(batch[offset:], uint64(maxTimestamp))
	offset += 8

	binary.BigEndian.PutUint64(batch[offset:], 0xFFFFFFFFFFFFFFFF)
	offset += 8

	binary.BigEndian.PutUint16(batch[offset:], 0xFFFF)
	offset += 2

	binary.BigEndian.PutUint32(batch[offset:], 0xFFFFFFFF)
	offset += 4

	binary.BigEndian.PutUint32(batch[offset:], uint32(len(timestamps)))
	offset += 4

	copy(batch[offset:], recordsBytes)

	crc := crc32.Checksum(batch[21:], crc32cTable)
	binary.BigEndian.PutUint32(batch[17:], crc)

	return batch
}

func writeTestRecordLite(buf *bytes.Buffer, timestampDelta int64, offsetDelta int64) {
	var record bytes.Buffer
	record.WriteByte(0)
	record.Write(encodeVarintLite(timestampDelta))
	record.Write(encodeVarintLite(offsetDelta))
	record.Write(encodeVarintLite(-1))
	record.Write(encodeVarintLite(-1))
	record.Write(encodeVarintLite(0))

	recordBytes := record.Bytes()
	buf.Write(encodeVarintLite(int64(len(recordBytes))))
	buf.Write(recordBytes)
}

func encodeVarintLite(v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63))
	var out []byte
	for {
		b := byte(uv & 0x7F)
		uv >>= 7
		if uv == 0 {
			out = append(out, b)
			break
		}
		out = append(out, b|0x80)
	}
	return out
}
