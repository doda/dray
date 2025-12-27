package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dray-io/dray/internal/objectstore"
)

var (
	testMinioProc    *os.Process
	testMinioPort    = "19000"
	testMinioDir     string
	minioAvailable   bool
	minioSkipMessage string
)

func TestMain(m *testing.M) {
	if err := startMinio(); err != nil {
		minioSkipMessage = fmt.Sprintf("MinIO not available: %v", err)
		minioAvailable = false
	} else {
		minioAvailable = true
	}
	code := m.Run()
	stopMinio()
	os.Exit(code)
}

func skipIfMinioUnavailable(t *testing.T) {
	t.Helper()
	if !minioAvailable {
		t.Skip(minioSkipMessage)
	}
}

func startMinio() error {
	minioPath := "/tmp/minio"
	if _, err := os.Stat(minioPath); os.IsNotExist(err) {
		return fmt.Errorf("minio binary not found at %s", minioPath)
	}

	dataDir, err := os.MkdirTemp("", "minio-data-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	testMinioDir = dataDir

	os.Setenv("MINIO_ROOT_USER", "minioadmin")
	os.Setenv("MINIO_ROOT_PASSWORD", "minioadmin")

	cmd := exec.Command(minioPath, "server", dataDir, "--address", ":"+testMinioPort, "--quiet")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		os.RemoveAll(dataDir)
		return fmt.Errorf("failed to start minio: %w", err)
	}

	testMinioProc = cmd.Process

	// Wait for MinIO to be ready
	endpoint := "http://localhost:" + testMinioPort
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		store, err := New(ctx, Config{
			Bucket:          "test-bucket",
			Endpoint:        endpoint,
			Region:          "us-east-1",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
			UsePathStyle:    true,
		})
		cancel()
		if err == nil {
			store.Close()
			break
		}
	}

	return nil
}

func stopMinio() {
	if testMinioProc != nil {
		testMinioProc.Kill()
		testMinioProc.Wait()
	}
	if testMinioDir != "" {
		os.RemoveAll(testMinioDir)
	}
}

func testStore(t *testing.T, bucket string) *Store {
	t.Helper()
	skipIfMinioUnavailable(t)
	endpoint := "http://localhost:" + testMinioPort
	ctx := context.Background()

	store, err := New(ctx, Config{
		Bucket:          bucket,
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UsePathStyle:    true,
	})
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Create bucket using S3 API
	createBucket(t, store, bucket)

	t.Cleanup(func() {
		deleteBucket(t, store, bucket)
		store.Close()
	})

	return store
}

func createBucket(t *testing.T, store *Store, bucket string) {
	t.Helper()
	ctx := context.Background()

	_, err := store.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("Failed to create bucket: %v", err)
	}
}

func deleteBucket(t *testing.T, store *Store, bucket string) {
	t.Helper()
	ctx := context.Background()

	// List and delete all objects first
	objects, _ := store.List(ctx, "")
	for _, obj := range objects {
		store.Delete(ctx, obj.Key)
	}

	// Abort any in-progress multipart uploads
	uploads, err := store.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		for _, upload := range uploads.Uploads {
			store.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      upload.Key,
				UploadId: upload.UploadId,
			})
		}
	}

	// Delete the bucket
	store.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
}

func TestNew(t *testing.T) {
	t.Run("missing bucket", func(t *testing.T) {
		_, err := New(context.Background(), Config{})
		if err == nil {
			t.Fatal("expected error for missing bucket")
		}
		if !strings.Contains(err.Error(), "bucket name is required") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("valid config", func(t *testing.T) {
		skipIfMinioUnavailable(t)
		store, err := New(context.Background(), Config{
			Bucket:          "test-bucket",
			Endpoint:        "http://localhost:" + testMinioPort,
			Region:          "us-east-1",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
			UsePathStyle:    true,
		})
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}
		store.Close()
	})
}

func TestPutAndGet(t *testing.T) {
	store := testStore(t, "test-put-get")
	ctx := context.Background()

	key := "test/object.txt"
	data := []byte("hello, world!")

	// Put
	err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %q, want %q", got, data)
	}
}

func TestPutWithMetadata(t *testing.T) {
	store := testStore(t, "test-put-metadata")
	ctx := context.Background()

	key := "test/with-metadata.txt"
	data := []byte("test data")
	metadata := map[string]string{
		"x-custom-header": "custom-value",
		"another-header":  "another-value",
	}

	err := store.PutWithOptions(ctx, key, bytes.NewReader(data), int64(len(data)), "text/plain", objectstore.PutOptions{
		Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("PutWithOptions failed: %v", err)
	}

	meta, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}

	if meta.Size != int64(len(data)) {
		t.Errorf("size mismatch: got %d, want %d", meta.Size, len(data))
	}

	// Note: S3 lowercases metadata keys
	if v, ok := meta.Metadata["x-custom-header"]; !ok || v != "custom-value" {
		t.Errorf("metadata mismatch for x-custom-header: got %v", meta.Metadata)
	}
}

func TestGetRange(t *testing.T) {
	store := testStore(t, "test-get-range")
	ctx := context.Background()

	key := "test/range.txt"
	data := []byte("0123456789")

	err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	tests := []struct {
		name  string
		start int64
		end   int64
		want  string
	}{
		{"first 5 bytes", 0, 4, "01234"},
		{"middle bytes", 3, 6, "3456"},
		{"last 3 bytes", 7, 9, "789"},
		{"from start to end", 5, -1, "56789"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc, err := store.GetRange(ctx, key, tt.start, tt.end)
			if err != nil {
				t.Fatalf("GetRange(%d, %d) failed: %v", tt.start, tt.end, err)
			}
			defer rc.Close()

			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("ReadAll failed: %v", err)
			}

			if string(got) != tt.want {
				t.Errorf("GetRange(%d, %d) = %q, want %q", tt.start, tt.end, got, tt.want)
			}
		})
	}
}

func TestHead(t *testing.T) {
	store := testStore(t, "test-head")
	ctx := context.Background()

	key := "test/head.txt"
	data := []byte("test content for head")

	err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	meta, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}

	if meta.Key != key {
		t.Errorf("key mismatch: got %q, want %q", meta.Key, key)
	}

	if meta.Size != int64(len(data)) {
		t.Errorf("size mismatch: got %d, want %d", meta.Size, len(data))
	}

	if meta.ContentType != "text/plain" {
		t.Errorf("content type mismatch: got %q, want %q", meta.ContentType, "text/plain")
	}

	if meta.ETag == "" {
		t.Error("ETag should not be empty")
	}

	if meta.LastModified == 0 {
		t.Error("LastModified should not be zero")
	}
}

func TestHeadNotFound(t *testing.T) {
	store := testStore(t, "test-head-404")
	ctx := context.Background()

	_, err := store.Head(ctx, "nonexistent/key.txt")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}

	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDelete(t *testing.T) {
	store := testStore(t, "test-delete")
	ctx := context.Background()

	key := "test/to-delete.txt"
	data := []byte("delete me")

	err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify it exists
	_, err = store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed before delete: %v", err)
	}

	// Delete
	err = store.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, err = store.Head(ctx, key)
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got: %v", err)
	}

	// Delete again (idempotent)
	err = store.Delete(ctx, key)
	if err != nil {
		t.Errorf("delete of nonexistent key should succeed, got: %v", err)
	}
}

func TestList(t *testing.T) {
	store := testStore(t, "test-list")
	ctx := context.Background()

	// Create some objects
	objects := []string{
		"dir1/file1.txt",
		"dir1/file2.txt",
		"dir2/file1.txt",
		"dir2/subdir/file1.txt",
	}

	for _, key := range objects {
		data := []byte("content of " + key)
		err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "text/plain")
		if err != nil {
			t.Fatalf("Put %q failed: %v", key, err)
		}
	}

	t.Run("list all", func(t *testing.T) {
		results, err := store.List(ctx, "")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != len(objects) {
			t.Errorf("expected %d objects, got %d", len(objects), len(results))
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		results, err := store.List(ctx, "dir1/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 objects with prefix dir1/, got %d", len(results))
		}

		for _, obj := range results {
			if !strings.HasPrefix(obj.Key, "dir1/") {
				t.Errorf("unexpected key %q", obj.Key)
			}
		}
	})

	t.Run("list with nested prefix", func(t *testing.T) {
		results, err := store.List(ctx, "dir2/subdir/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("expected 1 object with prefix dir2/subdir/, got %d", len(results))
		}
	})
}

func TestMultipartUpload(t *testing.T) {
	store := testStore(t, "test-multipart")
	ctx := context.Background()

	key := "test/multipart-object.bin"

	upload, err := store.CreateMultipartUpload(ctx, key, "application/octet-stream")
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	if upload.UploadID() == "" {
		t.Error("UploadID should not be empty")
	}

	// MinIO minimum part size is 5MB, but we'll use a smaller size for testing
	// since MinIO allows smaller parts in dev mode
	partSize := 5 * 1024 * 1024 // 5MB

	var etags []string
	var totalData bytes.Buffer

	for i := 1; i <= 3; i++ {
		data := make([]byte, partSize)
		for j := range data {
			data[j] = byte(i)
		}
		totalData.Write(data)

		etag, err := upload.UploadPart(ctx, i, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", i, err)
		}
		if etag == "" {
			t.Errorf("ETag for part %d should not be empty", i)
		}
		etags = append(etags, etag)
	}

	err = upload.Complete(ctx, etags)
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	// Verify the object exists and has correct size
	meta, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed after multipart complete: %v", err)
	}

	expectedSize := int64(partSize * 3)
	if meta.Size != expectedSize {
		t.Errorf("size mismatch: got %d, want %d", meta.Size, expectedSize)
	}

	// Verify content
	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, totalData.Bytes()) {
		t.Error("multipart content mismatch")
	}
}

func TestMultipartUploadAbort(t *testing.T) {
	store := testStore(t, "test-multipart-abort")
	ctx := context.Background()

	key := "test/multipart-abort.bin"

	upload, err := store.CreateMultipartUpload(ctx, key, "application/octet-stream")
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Upload one part
	data := make([]byte, 5*1024*1024)
	_, err = upload.UploadPart(ctx, 1, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Abort the upload
	err = upload.Abort(ctx)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Verify the object doesn't exist
	_, err = store.Head(ctx, key)
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("expected ErrNotFound after abort, got: %v", err)
	}

	// Abort again (idempotent)
	err = upload.Abort(ctx)
	if err != nil {
		t.Errorf("second abort should succeed, got: %v", err)
	}
}

func TestMultipartUploadWithOptions(t *testing.T) {
	store := testStore(t, "test-multipart-opts")
	ctx := context.Background()

	key := "test/multipart-with-metadata.bin"
	metadata := map[string]string{
		"x-custom-key": "custom-value",
	}

	upload, err := store.CreateMultipartUploadWithOptions(ctx, key, "application/octet-stream", objectstore.PutOptions{
		Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUploadWithOptions failed: %v", err)
	}

	data := make([]byte, 5*1024*1024)
	etag, err := upload.UploadPart(ctx, 1, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	err = upload.Complete(ctx, []string{etag})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	meta, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}

	if v, ok := meta.Metadata["x-custom-key"]; !ok || v != "custom-value" {
		t.Errorf("metadata mismatch: got %v", meta.Metadata)
	}
}

func TestGetNotFound(t *testing.T) {
	store := testStore(t, "test-get-404")
	ctx := context.Background()

	_, err := store.Get(ctx, "nonexistent/key.txt")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}

	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestClosedStore(t *testing.T) {
	skipIfMinioUnavailable(t)
	endpoint := "http://localhost:" + testMinioPort
	ctx := context.Background()
	bucket := "test-closed"

	store, err := New(ctx, Config{
		Bucket:          bucket,
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UsePathStyle:    true,
	})
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	store.Close()

	_, err = store.Get(ctx, "any-key")
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Errorf("expected closed error, got: %v", err)
	}

	err = store.Put(ctx, "any-key", bytes.NewReader([]byte("test")), 4, "text/plain")
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Errorf("expected closed error, got: %v", err)
	}

	_, err = store.Head(ctx, "any-key")
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Errorf("expected closed error, got: %v", err)
	}
}

func TestInterfaceCompliance(t *testing.T) {
	var _ objectstore.Store = (*Store)(nil)
	var _ objectstore.MultipartStore = (*Store)(nil)
}

func TestWALObjectPath(t *testing.T) {
	store := testStore(t, "test-wal")
	ctx := context.Background()

	// Test with WAL-like path
	key := "wal/domain=0/abc123-def456.wo"
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// List with prefix
	results, err := store.List(ctx, "wal/domain=0/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	if results[0].Key != key {
		t.Errorf("key mismatch: got %q, want %q", results[0].Key, key)
	}

	// Range read (simulating fetch)
	rc, err := store.GetRange(ctx, key, 100, 199)
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}
	defer rc.Close()

	chunk, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(chunk) != 100 {
		t.Errorf("expected 100 bytes, got %d", len(chunk))
	}

	for i, b := range chunk {
		expected := byte((100 + i) % 256)
		if b != expected {
			t.Errorf("byte %d mismatch: got %d, want %d", i, b, expected)
			break
		}
	}
}

func TestLargeObject(t *testing.T) {
	store := testStore(t, "test-large")
	ctx := context.Background()

	key := "test/large-object.bin"
	size := 10 * 1024 * 1024 // 10MB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	meta, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}

	if meta.Size != int64(size) {
		t.Errorf("size mismatch: got %d, want %d", meta.Size, size)
	}

	// Read the whole thing and verify
	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Error("data mismatch for large object")
	}
}

// Benchmark tests
func BenchmarkPut(b *testing.B) {
	if !minioAvailable {
		b.Skip(minioSkipMessage)
	}
	endpoint := "http://localhost:" + testMinioPort
	ctx := context.Background()
	bucket := "bench-put"

	store, err := New(ctx, Config{
		Bucket:          bucket,
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UsePathStyle:    true,
	})
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create bucket
	store.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := filepath.Join("bench", fmt.Sprintf("object-%d.bin", i))
		err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	if !minioAvailable {
		b.Skip(minioSkipMessage)
	}
	endpoint := "http://localhost:" + testMinioPort
	ctx := context.Background()
	bucket := "bench-get"

	store, err := New(ctx, Config{
		Bucket:          bucket,
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UsePathStyle:    true,
	})
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create bucket and put an object
	store.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})

	key := "bench/object.bin"
	data := make([]byte, 1024)
	store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := store.Get(ctx, key)
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
}
