// Package server implements the TCP server for the Kafka wire protocol.
package server

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/objectstore"
)

// MetadataStoreChecker implements ReadinessChecker for the metadata store.
// It verifies the Oxia connection is healthy by performing a simple Get operation.
type MetadataStoreChecker struct {
	store metadata.MetadataStore
}

// NewMetadataStoreChecker creates a new MetadataStoreChecker.
func NewMetadataStoreChecker(store metadata.MetadataStore) *MetadataStoreChecker {
	return &MetadataStoreChecker{store: store}
}

// Name returns the name of this component for health status display.
func (c *MetadataStoreChecker) Name() string {
	return "metadata_store"
}

// CheckReady verifies the metadata store is accessible.
// It performs a simple Get operation to verify connectivity.
func (c *MetadataStoreChecker) CheckReady(ctx context.Context) error {
	if c.store == nil {
		return errors.New("metadata store not configured")
	}

	// Perform a simple Get operation to verify connectivity.
	// We use a known non-existent key - we just want to verify the store responds.
	_, err := c.store.Get(ctx, "/dray/v1/health-check")
	if err != nil && !errors.Is(err, metadata.ErrKeyNotFound) {
		// ErrKeyNotFound is expected (the key doesn't exist), any other error is a problem
		return err
	}
	return nil
}

// ObjectStoreChecker implements ReadinessChecker for the object store.
// It verifies the S3-compatible store is reachable by performing a Head operation.
type ObjectStoreChecker struct {
	store objectstore.Store
}

// NewObjectStoreChecker creates a new ObjectStoreChecker.
func NewObjectStoreChecker(store objectstore.Store) *ObjectStoreChecker {
	return &ObjectStoreChecker{store: store}
}

// Name returns the name of this component for health status display.
func (c *ObjectStoreChecker) Name() string {
	return "object_store"
}

// CheckReady verifies the object store is accessible.
// It performs a List operation with a known prefix to verify connectivity.
func (c *ObjectStoreChecker) CheckReady(ctx context.Context) error {
	if c.store == nil {
		return errors.New("object store not configured")
	}

	// Perform a List operation to verify connectivity.
	// We use an empty prefix to just check we can reach the bucket.
	// The result doesn't matter - we just want to verify the store responds.
	_, err := c.store.List(ctx, "dray-health-check-nonexistent/")
	if err != nil {
		// Check if it's a "not found" type error which is actually OK
		// (the prefix doesn't exist, but the store is reachable)
		if errors.Is(err, objectstore.ErrNotFound) ||
			errors.Is(err, objectstore.ErrBucketNotFound) {
			// These indicate connectivity issues, not just missing data
			return err
		}
		// For other errors, check if it's an ObjectError
		var objErr *objectstore.ObjectError
		if errors.As(err, &objErr) {
			// If the underlying error is about object not found, that's OK
			if errors.Is(objErr.Err, objectstore.ErrNotFound) {
				return nil
			}
		}
		return err
	}
	return nil
}

// CompactorChecker implements ReadinessChecker for the compactor component.
// This is only relevant when the broker is running in compactor mode.
type CompactorChecker struct {
	isRunning func() bool
}

// NewCompactorChecker creates a new CompactorChecker.
// The isRunning function should return true if the compactor is healthy.
func NewCompactorChecker(isRunning func() bool) *CompactorChecker {
	return &CompactorChecker{isRunning: isRunning}
}

// Name returns the name of this component for health status display.
func (c *CompactorChecker) Name() string {
	return "compactor"
}

// CheckReady verifies the compactor is healthy.
func (c *CompactorChecker) CheckReady(ctx context.Context) error {
	if c.isRunning == nil {
		return nil // No compactor configured, that's OK
	}
	if !c.isRunning() {
		return errors.New("compactor is not running")
	}
	return nil
}

// FuncChecker is a simple ReadinessChecker that wraps a function.
// Useful for ad-hoc checks or testing.
type FuncChecker struct {
	name  string
	check func(context.Context) error
}

// NewFuncChecker creates a new FuncChecker with the given name and check function.
func NewFuncChecker(name string, check func(context.Context) error) *FuncChecker {
	return &FuncChecker{name: name, check: check}
}

// Name returns the name of this component.
func (c *FuncChecker) Name() string {
	return c.name
}

// CheckReady calls the wrapped function.
func (c *FuncChecker) CheckReady(ctx context.Context) error {
	if c.check == nil {
		return nil
	}
	return c.check(ctx)
}

// ObjectStoreHeadChecker implements ReadinessChecker for the object store using Head.
// This variant uses a Head operation on a well-known test key to minimize overhead.
type ObjectStoreHeadChecker struct {
	store objectstore.Store
}

// NewObjectStoreHeadChecker creates a new ObjectStoreHeadChecker.
func NewObjectStoreHeadChecker(store objectstore.Store) *ObjectStoreHeadChecker {
	return &ObjectStoreHeadChecker{store: store}
}

// Name returns the name of this component for health status display.
func (c *ObjectStoreHeadChecker) Name() string {
	return "object_store"
}

// CheckReady verifies the object store is accessible by attempting to read a tiny object.
// We try to Get a non-existent key - a NotFound error means the store is reachable.
func (c *ObjectStoreHeadChecker) CheckReady(ctx context.Context) error {
	if c.store == nil {
		return errors.New("object store not configured")
	}

	// Try to get a non-existent object. ErrNotFound means the store is reachable.
	rc, err := c.store.Get(ctx, "dray-health-check-nonexistent-key")
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			// This is the expected case - the key doesn't exist but the store is reachable
			return nil
		}
		// For ObjectError, check the wrapped error
		var objErr *objectstore.ObjectError
		if errors.As(err, &objErr) {
			if errors.Is(objErr.Err, objectstore.ErrNotFound) {
				return nil
			}
			// Access denied or bucket not found are real problems
			if errors.Is(objErr.Err, objectstore.ErrAccessDenied) ||
				errors.Is(objErr.Err, objectstore.ErrBucketNotFound) {
				return err
			}
		}
		// Check for common patterns that indicate the store is down
		errStr := err.Error()
		if strings.Contains(errStr, "connection refused") ||
			strings.Contains(errStr, "no such host") ||
			strings.Contains(errStr, "timeout") {
			return err
		}
		// Some implementations might return a different error for not found
		return nil
	}
	// If we got a reader, close it - the object unexpectedly exists
	if rc != nil {
		io.Copy(io.Discard, rc)
		rc.Close()
	}
	return nil
}
