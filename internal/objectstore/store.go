// Package objectstore defines the ObjectStore interface for S3-compatible storage.
//
// This package provides the core abstraction for object storage operations used
// throughout Dray for storing WAL objects and compacted Parquet files. The interface
// is designed to be compatible with S3, GCS, and Azure Blob Storage.
//
// # Usage
//
// The primary interface is [Store], which provides basic object operations:
//
//	store, err := s3.New(ctx, cfg)
//	if err != nil {
//	    return err
//	}
//	defer store.Close()
//
//	// Store a WAL object
//	err = store.Put(ctx, "wal/domain=0/abc123.wo", reader, size, "application/octet-stream")
//
//	// Retrieve with range read for fetch operations
//	rc, err := store.GetRange(ctx, key, startByte, endByte)
//	if err != nil {
//	    if errors.Is(err, objectstore.ErrNotFound) {
//	        // Handle missing object
//	    }
//	    return err
//	}
//	defer rc.Close()
//
// For large objects, use [MultipartStore] which extends Store with multipart upload:
//
//	mstore := store.(objectstore.MultipartStore)
//	upload, err := mstore.CreateMultipartUpload(ctx, key, contentType)
//	if err != nil {
//	    return err
//	}
//
//	var etags []string
//	for i, part := range parts {
//	    etag, err := upload.UploadPart(ctx, i+1, part.Reader, part.Size)
//	    if err != nil {
//	        upload.Abort(ctx)
//	        return err
//	    }
//	    etags = append(etags, etag)
//	}
//	return upload.Complete(ctx, etags)
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// Common errors returned by Store implementations.
var (
	// ErrNotFound is returned when the requested object does not exist.
	ErrNotFound = errors.New("object not found")

	// ErrPreconditionFailed is returned when a conditional write fails
	// (e.g., if-match ETag check fails).
	ErrPreconditionFailed = errors.New("precondition failed")

	// ErrBucketNotFound is returned when the configured bucket does not exist.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrAccessDenied is returned when the credentials lack permission for the operation.
	ErrAccessDenied = errors.New("access denied")

	// ErrInvalidRange is returned when the requested byte range is invalid.
	ErrInvalidRange = errors.New("invalid range")
)

// ObjectError wraps an error with the object key for context.
type ObjectError struct {
	Op  string // Operation that failed (e.g., "Put", "Get", "Delete")
	Key string // Object key
	Err error  // Underlying error
}

func (e *ObjectError) Error() string {
	return fmt.Sprintf("objectstore: %s %q: %v", e.Op, e.Key, e.Err)
}

func (e *ObjectError) Unwrap() error {
	return e.Err
}

// ObjectMeta contains metadata about an object.
type ObjectMeta struct {
	// Key is the object's key (path) in the bucket.
	Key string

	// Size is the object's size in bytes.
	Size int64

	// ContentType is the MIME type of the object.
	ContentType string

	// ETag is the entity tag, typically an MD5 hash of the object content.
	// For multipart uploads, the format is "hash-partCount".
	ETag string

	// LastModified is the Unix timestamp (milliseconds) when the object was last modified.
	LastModified int64

	// Metadata contains user-defined key-value metadata.
	Metadata map[string]string
}

// PutOptions configures a Put operation.
type PutOptions struct {
	// Metadata is optional user-defined key-value pairs stored with the object.
	// Keys are case-insensitive and may be prefixed by the storage provider.
	Metadata map[string]string

	// IfNoneMatch when set to "*" causes the Put to fail with ErrPreconditionFailed
	// if an object already exists at the key. This enables atomic create operations.
	IfNoneMatch string
}

// Store is the interface for object storage operations.
//
// All methods accept a context for cancellation and deadline propagation.
// Implementations should return wrapped errors using [ObjectError] where appropriate.
//
// Thread Safety: Implementations must be safe for concurrent use.
type Store interface {
	// Put stores an object at the given key.
	//
	// The reader is consumed until EOF or error. The size parameter must match
	// the total bytes that will be read; some storage providers require this upfront.
	//
	// contentType should be a valid MIME type (e.g., "application/octet-stream").
	//
	// Returns an error if the write fails. Common errors:
	//   - ErrBucketNotFound: bucket doesn't exist
	//   - ErrAccessDenied: insufficient permissions
	Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error

	// PutWithOptions stores an object with additional options.
	//
	// This method supports conditional writes via opts.IfNoneMatch and
	// user-defined metadata via opts.Metadata.
	PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts PutOptions) error

	// Get retrieves an entire object.
	//
	// The caller must close the returned ReadCloser when done.
	//
	// Returns an error if the object doesn't exist or can't be retrieved:
	//   - ErrNotFound: object doesn't exist
	//   - ErrAccessDenied: insufficient permissions
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// GetRange retrieves a byte range of an object.
	//
	// The range is inclusive: [start, end]. For example, GetRange(ctx, key, 0, 99)
	// retrieves the first 100 bytes.
	//
	// If end is -1, the range extends to the end of the object.
	// If start is negative, it specifies an offset from the end (e.g., -100 = last 100 bytes).
	//
	// Returns an error if the range is invalid or the object doesn't exist:
	//   - ErrNotFound: object doesn't exist
	//   - ErrInvalidRange: range extends beyond object or is malformed
	GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)

	// Head retrieves object metadata without the body.
	//
	// This is useful for checking if an object exists and its size before
	// downloading, or for conditional operations based on ETag.
	//
	// Returns an error if the object doesn't exist:
	//   - ErrNotFound: object doesn't exist
	Head(ctx context.Context, key string) (ObjectMeta, error)

	// Delete removes an object.
	//
	// Delete is idempotent: deleting a non-existent object succeeds silently.
	// This matches S3 behavior and enables safe retries.
	//
	// Returns an error only for actual failures:
	//   - ErrAccessDenied: insufficient permissions
	Delete(ctx context.Context, key string) error

	// List returns objects matching the given prefix.
	//
	// The prefix should include a trailing "/" to list a "directory", e.g.,
	// "wal/domain=0/" lists all objects in that logical directory.
	//
	// Results are returned in lexicographic order by key. For large result sets,
	// implementations may paginate internally but return all results.
	List(ctx context.Context, prefix string) ([]ObjectMeta, error)

	// Close releases resources associated with the store.
	//
	// After Close returns, all other methods will return errors.
	Close() error
}

// MultipartUpload represents an in-progress multipart upload.
//
// Multipart uploads are used for large objects (typically >5MB) to enable
// parallel uploads and resumability. Parts can be uploaded in any order.
//
// Usage pattern:
//  1. Create upload with CreateMultipartUpload
//  2. Upload parts with UploadPart (collect returned ETags)
//  3. Call Complete with all ETags to finalize, or Abort to cancel
//
// If neither Complete nor Abort is called, the upload remains pending until
// it's explicitly aborted or garbage collected by the storage provider.
type MultipartUpload interface {
	// UploadID returns the unique identifier for this upload.
	// This can be used to resume an interrupted upload.
	UploadID() string

	// UploadPart uploads a single part.
	//
	// partNum must be between 1 and 10,000 (S3 limits). Parts can be uploaded
	// in any order and can be uploaded concurrently.
	//
	// Returns the ETag of the uploaded part, which must be provided to Complete.
	//
	// Minimum part size is typically 5MB (except for the last part).
	// Maximum part size is typically 5GB.
	UploadPart(ctx context.Context, partNum int, reader io.Reader, size int64) (etag string, err error)

	// Complete finalizes the multipart upload.
	//
	// etags must be provided in part number order (1, 2, 3, ...).
	// The length of etags must match the number of parts uploaded.
	//
	// After Complete returns successfully, the object is visible and durable.
	Complete(ctx context.Context, etags []string) error

	// Abort cancels the multipart upload.
	//
	// Any uploaded parts are deleted. This should be called if the upload
	// cannot be completed (e.g., due to an error uploading a part).
	//
	// Abort is idempotent: aborting an already-aborted or completed upload succeeds.
	Abort(ctx context.Context) error
}

// MultipartStore extends Store with multipart upload support.
//
// All S3-compatible stores support multipart uploads, so implementations
// of Store should also implement MultipartStore.
type MultipartStore interface {
	Store

	// CreateMultipartUpload initiates a new multipart upload.
	//
	// Returns a MultipartUpload handle for uploading parts and completing the upload.
	//
	// contentType should be a valid MIME type for the final object.
	CreateMultipartUpload(ctx context.Context, key string, contentType string) (MultipartUpload, error)

	// CreateMultipartUploadWithOptions initiates a multipart upload with options.
	//
	// This allows setting metadata on the final object.
	CreateMultipartUploadWithOptions(ctx context.Context, key string, contentType string, opts PutOptions) (MultipartUpload, error)
}
