package objectstore

import (
	"errors"
	"testing"
)

func TestObjectErrorFormat(t *testing.T) {
	tests := []struct {
		name     string
		err      *ObjectError
		expected string
	}{
		{
			name: "get not found",
			err: &ObjectError{
				Op:  "Get",
				Key: "wal/v1/zone=zone-a/domain=0/date=2025/01/02/abc123.wo",
				Err: ErrNotFound,
			},
			expected: `objectstore: Get "wal/v1/zone=zone-a/domain=0/date=2025/01/02/abc123.wo": object not found`,
		},
		{
			name: "put access denied",
			err: &ObjectError{
				Op:  "Put",
				Key: "wal/v1/zone=zone-b/domain=1/date=2025/01/02/xyz789.wo",
				Err: ErrAccessDenied,
			},
			expected: `objectstore: Put "wal/v1/zone=zone-b/domain=1/date=2025/01/02/xyz789.wo": access denied`,
		},
		{
			name: "get range invalid",
			err: &ObjectError{
				Op:  "GetRange",
				Key: "parquet/topic/part-0.parquet",
				Err: ErrInvalidRange,
			},
			expected: `objectstore: GetRange "parquet/topic/part-0.parquet": invalid range`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("ObjectError.Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestObjectErrorUnwrap(t *testing.T) {
	err := &ObjectError{
		Op:  "Get",
		Key: "test/key",
		Err: ErrNotFound,
	}

	if !errors.Is(err, ErrNotFound) {
		t.Error("ObjectError should unwrap to ErrNotFound")
	}

	if errors.Is(err, ErrAccessDenied) {
		t.Error("ObjectError should not unwrap to ErrAccessDenied")
	}
}

func TestErrorSentinels(t *testing.T) {
	// Verify all sentinel errors are distinct
	errs := []error{
		ErrNotFound,
		ErrPreconditionFailed,
		ErrBucketNotFound,
		ErrAccessDenied,
		ErrInvalidRange,
	}

	for i, e1 := range errs {
		for j, e2 := range errs {
			if i != j && errors.Is(e1, e2) {
				t.Errorf("error %v should not match %v", e1, e2)
			}
		}
	}
}

func TestPutOptionsDefaults(t *testing.T) {
	var opts PutOptions

	if opts.Metadata != nil {
		t.Error("default Metadata should be nil")
	}

	if opts.IfNoneMatch != "" {
		t.Error("default IfNoneMatch should be empty")
	}
}

func TestObjectMetaZeroValue(t *testing.T) {
	var meta ObjectMeta

	if meta.Key != "" {
		t.Error("default Key should be empty")
	}
	if meta.Size != 0 {
		t.Error("default Size should be 0")
	}
	if meta.ContentType != "" {
		t.Error("default ContentType should be empty")
	}
	if meta.ETag != "" {
		t.Error("default ETag should be empty")
	}
	if meta.LastModified != 0 {
		t.Error("default LastModified should be 0")
	}
	if meta.Metadata != nil {
		t.Error("default Metadata should be nil")
	}
}
