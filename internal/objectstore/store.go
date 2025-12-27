// Package objectstore defines the ObjectStore interface for S3-compatible storage.
package objectstore

import (
	"context"
	"io"
)

// ObjectMeta contains metadata about an object.
type ObjectMeta struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified int64
}

// Store is the interface for object storage operations.
type Store interface {
	// Put stores an object.
	Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error

	// Get retrieves an entire object.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// GetRange retrieves a byte range of an object.
	GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)

	// Head retrieves object metadata without the body.
	Head(ctx context.Context, key string) (ObjectMeta, error)

	// Delete removes an object.
	Delete(ctx context.Context, key string) error

	// List lists objects with the given prefix.
	List(ctx context.Context, prefix string) ([]ObjectMeta, error)

	// Close releases resources.
	Close() error
}

// MultipartUpload represents an in-progress multipart upload.
type MultipartUpload interface {
	// UploadPart uploads a part.
	UploadPart(ctx context.Context, partNum int, reader io.Reader, size int64) (string, error)

	// Complete completes the multipart upload.
	Complete(ctx context.Context, etags []string) error

	// Abort cancels the multipart upload.
	Abort(ctx context.Context) error
}

// MultipartStore extends Store with multipart upload support.
type MultipartStore interface {
	Store

	// CreateMultipartUpload initiates a multipart upload.
	CreateMultipartUpload(ctx context.Context, key string, contentType string) (MultipartUpload, error)
}
