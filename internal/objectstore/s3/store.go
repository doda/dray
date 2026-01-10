// Package s3 implements the ObjectStore interface using AWS SDK for S3-compatible storage.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dray-io/dray/internal/objectstore"
)

// Config configures an S3 store.
type Config struct {
	// Bucket is the name of the S3 bucket.
	Bucket string

	// Region is the AWS region (e.g., "us-east-1").
	// Required for AWS S3, optional for S3-compatible endpoints.
	Region string

	// Endpoint is the S3 endpoint URL (e.g., "http://localhost:9000" for MinIO).
	// If empty, uses the default AWS endpoint for the region.
	Endpoint string

	// AccessKeyID is the AWS access key ID.
	// If empty, uses the default credential chain.
	AccessKeyID string

	// SecretAccessKey is the AWS secret access key.
	// If empty, uses the default credential chain.
	SecretAccessKey string

	// UsePathStyle enables path-style addressing (required for MinIO and some S3-compatible stores).
	// When true: http://endpoint/bucket/key
	// When false (default): http://bucket.endpoint/key
	UsePathStyle bool

	// DisableSSL disables HTTPS for the endpoint (for local testing only).
	DisableSSL bool
}

// Store implements objectstore.Store and objectstore.MultipartStore using AWS S3.
type Store struct {
	client *s3.Client
	bucket string
	closed bool
	mu     sync.RWMutex
}

// New creates a new S3 store with the given configuration.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket name is required")
	}

	opts := []func(*config.LoadOptions) error{}

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	} else {
		opts = append(opts, config.WithRegion("us-east-1"))
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3: failed to load AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			// Suppress "Response has no supported checksum" warnings.
			// S3 doesn't always provide checksums for all response types,
			// which is normal and doesn't indicate a problem.
			o.DisableLogOutputChecksumValidationSkipped = true
		},
	}

	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	if cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &Store{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

func (s *Store) checkClosed() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errors.New("s3: store is closed")
	}
	return nil
}

// Put stores an object at the given key.
func (s *Store) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return s.PutWithOptions(ctx, key, reader, size, contentType, objectstore.PutOptions{})
}

// PutWithOptions stores an object with additional options.
func (s *Store) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	if err := s.checkClosed(); err != nil {
		return err
	}

	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          reader,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
	}

	if len(opts.Metadata) > 0 {
		input.Metadata = opts.Metadata
	}

	if opts.IfNoneMatch == "*" {
		input.IfNoneMatch = aws.String("*")
	}

	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		return s.wrapError("Put", key, err)
	}

	return nil
}

// Get retrieves an entire object.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, s.wrapError("Get", key, err)
	}

	return output.Body, nil
}

// GetRange retrieves a byte range of an object.
func (s *Store) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	var rangeHeader string
	switch {
	case start < 0:
		rangeHeader = fmt.Sprintf("bytes=%d", start)
	case end < 0:
		rangeHeader = fmt.Sprintf("bytes=%d-", start)
	default:
		rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
	}

	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, s.wrapError("GetRange", key, err)
	}

	return output.Body, nil
}

// Head retrieves object metadata without the body.
func (s *Store) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	if err := s.checkClosed(); err != nil {
		return objectstore.ObjectMeta{}, err
	}

	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return objectstore.ObjectMeta{}, s.wrapError("Head", key, err)
	}

	meta := objectstore.ObjectMeta{
		Key:         key,
		Size:        aws.ToInt64(output.ContentLength),
		ContentType: aws.ToString(output.ContentType),
		ETag:        aws.ToString(output.ETag),
		Metadata:    output.Metadata,
	}

	if output.LastModified != nil {
		meta.LastModified = output.LastModified.UnixMilli()
	}

	return meta, nil
}

// Delete removes an object.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		wrapped := s.wrapError("Delete", key, err)
		if errors.Is(wrapped, objectstore.ErrNotFound) {
			return nil
		}
		return wrapped
	}

	return nil
}

// List returns objects matching the given prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	var results []objectstore.ObjectMeta
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, s.wrapError("List", prefix, err)
		}

		for _, obj := range page.Contents {
			meta := objectstore.ObjectMeta{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
				ETag: aws.ToString(obj.ETag),
			}
			if obj.LastModified != nil {
				meta.LastModified = obj.LastModified.UnixMilli()
			}
			results = append(results, meta)
		}
	}

	return results, nil
}

// Close releases resources associated with the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// CreateMultipartUpload initiates a new multipart upload.
func (s *Store) CreateMultipartUpload(ctx context.Context, key string, contentType string) (objectstore.MultipartUpload, error) {
	return s.CreateMultipartUploadWithOptions(ctx, key, contentType, objectstore.PutOptions{})
}

// CreateMultipartUploadWithOptions initiates a multipart upload with options.
func (s *Store) CreateMultipartUploadWithOptions(ctx context.Context, key string, contentType string, opts objectstore.PutOptions) (objectstore.MultipartUpload, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}

	if len(opts.Metadata) > 0 {
		input.Metadata = opts.Metadata
	}

	output, err := s.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, s.wrapError("CreateMultipartUpload", key, err)
	}

	return &multipartUpload{
		store:    s,
		bucket:   s.bucket,
		key:      key,
		uploadID: aws.ToString(output.UploadId),
	}, nil
}

func (s *Store) wrapError(op, key string, err error) error {
	if err == nil {
		return nil
	}

	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.HTTPStatusCode() {
		case http.StatusNotFound:
			return &objectstore.ObjectError{Op: op, Key: key, Err: objectstore.ErrNotFound}
		case http.StatusForbidden:
			return &objectstore.ObjectError{Op: op, Key: key, Err: objectstore.ErrAccessDenied}
		case http.StatusPreconditionFailed:
			return &objectstore.ObjectError{Op: op, Key: key, Err: objectstore.ErrPreconditionFailed}
		case http.StatusRequestedRangeNotSatisfiable:
			return &objectstore.ObjectError{Op: op, Key: key, Err: objectstore.ErrInvalidRange}
		}
	}

	var noSuchBucket *types.NoSuchBucket
	if errors.As(err, &noSuchBucket) {
		return &objectstore.ObjectError{Op: op, Key: key, Err: objectstore.ErrBucketNotFound}
	}

	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return &objectstore.ObjectError{Op: op, Key: key, Err: objectstore.ErrNotFound}
	}

	return &objectstore.ObjectError{Op: op, Key: key, Err: err}
}

// multipartUpload implements objectstore.MultipartUpload.
type multipartUpload struct {
	store    *Store
	bucket   string
	key      string
	uploadID string
}

func (u *multipartUpload) UploadID() string {
	return u.uploadID
}

func (u *multipartUpload) UploadPart(ctx context.Context, partNum int, reader io.Reader, size int64) (string, error) {
	if err := u.store.checkClosed(); err != nil {
		return "", err
	}

	output, err := u.store.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(u.key),
		UploadId:      aws.String(u.uploadID),
		PartNumber:    aws.Int32(int32(partNum)),
		Body:          reader,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return "", u.store.wrapError("UploadPart", u.key, err)
	}

	return aws.ToString(output.ETag), nil
}

func (u *multipartUpload) Complete(ctx context.Context, etags []string) error {
	if err := u.store.checkClosed(); err != nil {
		return err
	}

	parts := make([]types.CompletedPart, len(etags))
	for i, etag := range etags {
		parts[i] = types.CompletedPart{
			PartNumber: aws.Int32(int32(i + 1)),
			ETag:       aws.String(etag),
		}
	}

	_, err := u.store.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return u.store.wrapError("CompleteMultipartUpload", u.key, err)
	}

	return nil
}

func (u *multipartUpload) Abort(ctx context.Context) error {
	if err := u.store.checkClosed(); err != nil {
		return err
	}

	_, err := u.store.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
	})
	if err != nil {
		var noSuchUpload *types.NoSuchUpload
		if errors.As(err, &noSuchUpload) {
			return nil
		}
		return u.store.wrapError("AbortMultipartUpload", u.key, err)
	}

	return nil
}

// Verify interface compliance at compile time.
var (
	_ objectstore.Store           = (*Store)(nil)
	_ objectstore.MultipartStore  = (*Store)(nil)
	_ objectstore.MultipartUpload = (*multipartUpload)(nil)
)
