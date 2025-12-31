package catalog

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/dray-io/dray/internal/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileCatalog(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMockStore()
	catalog := NewFileCatalog(store, "iceberg")

	identifier := TableIdentifier{
		Namespace: []string{"default"},
		Name:      "test_table",
	}

	t.Run("CreateTableIfMissing", func(t *testing.T) {
		schema := DefaultSchema()
		spec := DefaultPartitionSpec()
		opts := CreateTableOptions{
			Schema:        schema,
			PartitionSpec: &spec,
			Properties:    TableProperties{"custom": "value"},
		}

		table, err := catalog.CreateTableIfMissing(ctx, identifier, opts)
		require.NoError(t, err)
		assert.Equal(t, identifier, table.Identifier())
		assert.Equal(t, "value", table.Properties()["custom"])

		// Verify files in mock store
		metaPath := "iceberg/default/test_table/metadata/v0.metadata.json"
		hintPath := "iceberg/default/test_table/metadata/version-hint.text"

		_, err = store.Head(ctx, metaPath)
		assert.NoError(t, err)

		rc, err := store.Get(ctx, hintPath)
		require.NoError(t, err)
		data, _ := io.ReadAll(rc)
		assert.Equal(t, "0", strings.TrimSpace(string(data)))
	})

	t.Run("LoadTable", func(t *testing.T) {
		table, err := catalog.LoadTable(ctx, identifier)
		require.NoError(t, err)
		assert.Equal(t, identifier, table.Identifier())
	})

	t.Run("AppendFiles", func(t *testing.T) {
		table, err := catalog.LoadTable(ctx, identifier)
		require.NoError(t, err)

		files := []DataFile{
			{
				Path:           "s3://bucket/data1.parquet",
				Format:         FormatParquet,
				PartitionValue: 0,
				RecordCount:    100,
				FileSizeBytes:  1024,
			},
		}

		snapshot, err := table.AppendFiles(ctx, files, nil)
		require.NoError(t, err)
		assert.NotNil(t, snapshot)
		assert.Equal(t, "100", snapshot.Summary["added-records"])

		// Verify new metadata version
		hintPath := "iceberg/default/test_table/metadata/version-hint.text"
		rc, err := store.Get(ctx, hintPath)
		require.NoError(t, err)
		data, _ := io.ReadAll(rc)
		assert.Equal(t, "1", strings.TrimSpace(string(data)))

		metaPath := "iceberg/default/test_table/metadata/v1.metadata.json"
		rc, err = store.Get(ctx, metaPath)
		require.NoError(t, err)
		var metadata TableMetadata
		err = json.NewDecoder(rc).Decode(&metadata)
		require.NoError(t, err)
		assert.Equal(t, 1, len(metadata.Snapshots))
	})

	t.Run("TableExists", func(t *testing.T) {
		exists, err := catalog.TableExists(ctx, identifier)
		require.NoError(t, err)
		assert.True(t, exists)

		missing := TableIdentifier{Name: "missing"}
		exists, err = catalog.TableExists(ctx, missing)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("DropTable", func(t *testing.T) {
		err := catalog.DropTable(ctx, identifier)
		require.NoError(t, err)

		exists, err := catalog.TableExists(ctx, identifier)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("S3WarehouseURI", func(t *testing.T) {
		s3Catalog := NewFileCatalog(store, "s3://getajobdemo/iceberg")
		id := TableIdentifier{Name: "uri_test"}

		table, err := s3Catalog.CreateTableIfMissing(ctx, id, CreateTableOptions{
			Schema: DefaultSchema(),
		})
		require.NoError(t, err)

		// Key should NOT include s3://
		metaPath := "iceberg/uri_test/metadata/v0.metadata.json"
		_, err = store.Head(ctx, metaPath)
		assert.NoError(t, err)

		// Location SHOULD include s3://
		assert.Equal(t, "s3://getajobdemo/iceberg/uri_test", table.Location())
	})
}
