package catalog

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/dray-io/dray/internal/objectstore"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIcebergManifestCompliance(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMockStore()
	catalog := NewFileCatalog(store, "s3://bucket/warehouse")

	identifier := TableIdentifier{
		Namespace: []string{"default"},
		Name:      "compliance_test",
	}

	// 1. Create table
	schema := DefaultSchema()
	spec := DefaultPartitionSpec()
	opts := CreateTableOptions{
		Schema:        schema,
		PartitionSpec: &spec,
	}

	table, err := catalog.CreateTableIfMissing(ctx, identifier, opts)
	require.NoError(t, err)

	// 2. Append files
	files := []DataFile{
		{
			Path:           "s3://bucket/warehouse/default/compliance_test/data/f1.parquet",
			Format:         FormatParquet,
			PartitionValue: 0,
			RecordCount:    100,
			FileSizeBytes:  1024,
		},
	}

	snapshot, err := table.AppendFiles(ctx, files, nil)
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	// 3. Verify Manifest List (Avro)
	manifestListPath := snapshot.ManifestListPath
	// Strip s3://bucket/ for mock store
	manifestListKey := manifestListPath
	if strings.HasPrefix(manifestListKey, "s3://bucket/") {
		manifestListKey = manifestListKey[len("s3://bucket/"):]
	}

	rc, err := store.Get(ctx, manifestListKey)
	require.NoError(t, err, "manifest list should exist in store")
	listData, err := io.ReadAll(rc)
	require.NoError(t, err)

	// Verify it's a valid Avro OCF file with the correct schema
	_, err = avro.Parse(manifestListSchema)
	require.NoError(t, err)
	
	dec, err := ocf.NewDecoder(bytes.NewReader(listData))
	require.NoError(t, err, "should be a valid Avro OCF file")
	
	var manifestListEntries []AvroManifestListEntry
	for dec.HasNext() {
		var entry AvroManifestListEntry
		err := dec.Decode(&entry)
		require.NoError(t, err, "should decode manifest list entry")
		manifestListEntries = append(manifestListEntries, entry)
	}
	assert.Len(t, manifestListEntries, 1)
	assert.Equal(t, int64(100), manifestListEntries[0].AddedRowsCount)
	assert.Equal(t, int32(1), manifestListEntries[0].AddedDataFilesCount)

	// 4. Verify Manifest File (Avro)
	manifestPath := manifestListEntries[0].ManifestPath
	manifestKey := manifestPath
	if strings.HasPrefix(manifestKey, "s3://bucket/") {
		manifestKey = manifestKey[len("s3://bucket/"):]
	}

	rc, err = store.Get(ctx, manifestKey)
	require.NoError(t, err, "manifest file should exist in store")
	manifestData, err := io.ReadAll(rc)
	require.NoError(t, err)

	// Verify manifest file content
	_, err = avro.Parse(manifestFileSchema)
	require.NoError(t, err)

	dec, err = ocf.NewDecoder(bytes.NewReader(manifestData))
	require.NoError(t, err, "manifest file should be valid Avro OCF")
	
	var manifestEntries []AvroManifestEntry
	for dec.HasNext() {
		var entry AvroManifestEntry
		err := dec.Decode(&entry)
		require.NoError(t, err, "should decode manifest entry")
		manifestEntries = append(manifestEntries, entry)
	}
	assert.Len(t, manifestEntries, 1)
	assert.Equal(t, int32(1), manifestEntries[0].Status) // ADDED
	assert.Equal(t, files[0].Path, manifestEntries[0].DataFile.FilePath)
	assert.Equal(t, files[0].RecordCount, manifestEntries[0].DataFile.RecordCount)
}
