package catalog

import (
	"context"
	"testing"
)

func TestNewTableCreator(t *testing.T) {
	catalog := newMockCatalog()

	t.Run("default namespace", func(t *testing.T) {
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		if len(creator.Namespace()) != 1 || creator.Namespace()[0] != "dray" {
			t.Errorf("expected default namespace ['dray'], got %v", creator.Namespace())
		}
	})

	t.Run("custom namespace", func(t *testing.T) {
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			Namespace: []string{"prod", "dray"},
			ClusterID: "test-cluster",
		})

		if len(creator.Namespace()) != 2 || creator.Namespace()[0] != "prod" {
			t.Errorf("expected namespace ['prod', 'dray'], got %v", creator.Namespace())
		}
	})

	t.Run("cluster ID", func(t *testing.T) {
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "my-cluster-123",
		})

		if creator.ClusterID() != "my-cluster-123" {
			t.Errorf("expected cluster ID 'my-cluster-123', got %q", creator.ClusterID())
		}
	})
}

func TestTableCreator_CreateTableForTopic(t *testing.T) {
	ctx := context.Background()

	t.Run("creates table with correct schema", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		table, err := creator.CreateTableForTopic(ctx, "my-topic")
		if err != nil {
			t.Fatalf("CreateTableForTopic failed: %v", err)
		}

		schema := table.Schema()
		if len(schema.Fields) != 10 {
			t.Errorf("expected 10 schema fields, got %d", len(schema.Fields))
		}

		// Verify required fields exist
		fieldNames := make(map[string]bool)
		for _, f := range schema.Fields {
			fieldNames[f.Name] = true
		}

		required := []string{"partition", "offset", "timestamp_ms", "key", "value", "headers", "attributes"}
		for _, name := range required {
			if !fieldNames[name] {
				t.Errorf("missing required field %q", name)
			}
		}
	})

	t.Run("creates table with partition spec", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		_, err := creator.CreateTableForTopic(ctx, "partitioned-topic")
		if err != nil {
			t.Fatalf("CreateTableForTopic failed: %v", err)
		}

		// The mock catalog stores the partition spec via CreateTableOptions
		// Verify by checking the table was created with expected properties
		table, _ := catalog.LoadTable(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "partitioned-topic",
		})
		if table == nil {
			t.Fatal("table should exist")
		}
	})

	t.Run("creates table with dray.topic property", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		table, err := creator.CreateTableForTopic(ctx, "property-topic")
		if err != nil {
			t.Fatalf("CreateTableForTopic failed: %v", err)
		}

		props := table.Properties()
		if props[PropertyDrayTopic] != "property-topic" {
			t.Errorf("expected dray.topic = 'property-topic', got %q", props[PropertyDrayTopic])
		}
	})

	t.Run("creates table with dray.cluster_id property", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "my-cluster-id",
		})

		table, err := creator.CreateTableForTopic(ctx, "cluster-topic")
		if err != nil {
			t.Fatalf("CreateTableForTopic failed: %v", err)
		}

		props := table.Properties()
		if props[PropertyDrayClusterID] != "my-cluster-id" {
			t.Errorf("expected dray.cluster_id = 'my-cluster-id', got %q", props[PropertyDrayClusterID])
		}
	})

	t.Run("creates table with schema version property", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		table, err := creator.CreateTableForTopic(ctx, "version-topic")
		if err != nil {
			t.Fatalf("CreateTableForTopic failed: %v", err)
		}

		props := table.Properties()
		if props[PropertyDraySchemaVersion] != "1" {
			t.Errorf("expected dray.schema_version = '1', got %q", props[PropertyDraySchemaVersion])
		}
	})

	t.Run("handles existing table gracefully", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		// Create the table first time
		table1, err := creator.CreateTableForTopic(ctx, "existing-topic")
		if err != nil {
			t.Fatalf("first CreateTableForTopic failed: %v", err)
		}

		// Create again - should succeed without error
		table2, err := creator.CreateTableForTopic(ctx, "existing-topic")
		if err != nil {
			t.Fatalf("second CreateTableForTopic failed: %v", err)
		}

		// Should return the same table (by identifier)
		if table1.Identifier().String() != table2.Identifier().String() {
			t.Error("expected same table identifier for both calls")
		}
	})

	t.Run("returns error when catalog is nil", func(t *testing.T) {
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   nil,
			ClusterID: "test-cluster",
		})

		_, err := creator.CreateTableForTopic(ctx, "nil-catalog-topic")
		if err != ErrCatalogUnavailable {
			t.Errorf("expected ErrCatalogUnavailable, got %v", err)
		}
	})

	t.Run("uses correct namespace", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			Namespace: []string{"production", "streaming"},
			ClusterID: "test-cluster",
		})

		table, err := creator.CreateTableForTopic(ctx, "ns-topic")
		if err != nil {
			t.Fatalf("CreateTableForTopic failed: %v", err)
		}

		id := table.Identifier()
		if len(id.Namespace) != 2 || id.Namespace[0] != "production" || id.Namespace[1] != "streaming" {
			t.Errorf("expected namespace ['production', 'streaming'], got %v", id.Namespace)
		}
	})
}

func TestTableCreator_DropTableForTopic(t *testing.T) {
	ctx := context.Background()

	t.Run("drops existing table", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		// Create then drop
		_, _ = creator.CreateTableForTopic(ctx, "drop-topic")
		err := creator.DropTableForTopic(ctx, "drop-topic")
		if err != nil {
			t.Fatalf("DropTableForTopic failed: %v", err)
		}

		// Verify table is gone
		exists, _ := creator.TableExistsForTopic(ctx, "drop-topic")
		if exists {
			t.Error("table should not exist after drop")
		}
	})

	t.Run("handles non-existent table gracefully", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		// Drop non-existent table should not error (idempotent)
		err := creator.DropTableForTopic(ctx, "never-existed")
		if err != nil {
			t.Errorf("expected nil error for non-existent table, got %v", err)
		}
	})

	t.Run("returns error when catalog is nil", func(t *testing.T) {
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   nil,
			ClusterID: "test-cluster",
		})

		err := creator.DropTableForTopic(ctx, "nil-catalog-topic")
		if err != ErrCatalogUnavailable {
			t.Errorf("expected ErrCatalogUnavailable, got %v", err)
		}
	})
}

func TestTableCreator_TableExistsForTopic(t *testing.T) {
	ctx := context.Background()

	t.Run("returns false for non-existent table", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		exists, err := creator.TableExistsForTopic(ctx, "missing-topic")
		if err != nil {
			t.Fatalf("TableExistsForTopic failed: %v", err)
		}
		if exists {
			t.Error("expected false for non-existent table")
		}
	})

	t.Run("returns true for existing table", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		_, _ = creator.CreateTableForTopic(ctx, "exists-topic")
		exists, err := creator.TableExistsForTopic(ctx, "exists-topic")
		if err != nil {
			t.Fatalf("TableExistsForTopic failed: %v", err)
		}
		if !exists {
			t.Error("expected true for existing table")
		}
	})

	t.Run("returns error when catalog is nil", func(t *testing.T) {
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   nil,
			ClusterID: "test-cluster",
		})

		_, err := creator.TableExistsForTopic(ctx, "any-topic")
		if err != ErrCatalogUnavailable {
			t.Errorf("expected ErrCatalogUnavailable, got %v", err)
		}
	})
}

func TestTableCreator_LoadTableForTopic(t *testing.T) {
	ctx := context.Background()

	t.Run("loads existing table", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		_, _ = creator.CreateTableForTopic(ctx, "load-topic")
		table, err := creator.LoadTableForTopic(ctx, "load-topic")
		if err != nil {
			t.Fatalf("LoadTableForTopic failed: %v", err)
		}
		if table.Identifier().Name != "load-topic" {
			t.Errorf("expected table name 'load-topic', got %q", table.Identifier().Name)
		}
	})

	t.Run("returns error for non-existent table", func(t *testing.T) {
		catalog := newMockCatalog()
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   catalog,
			ClusterID: "test-cluster",
		})

		_, err := creator.LoadTableForTopic(ctx, "missing-topic")
		if err != ErrTableNotFound {
			t.Errorf("expected ErrTableNotFound, got %v", err)
		}
	})

	t.Run("returns error when catalog is nil", func(t *testing.T) {
		creator := NewTableCreator(TableCreatorConfig{
			Catalog:   nil,
			ClusterID: "test-cluster",
		})

		_, err := creator.LoadTableForTopic(ctx, "any-topic")
		if err != ErrCatalogUnavailable {
			t.Errorf("expected ErrCatalogUnavailable, got %v", err)
		}
	})
}

func TestTableCreator_SchemaCompliance(t *testing.T) {
	// Verify the schema matches SPEC.md section 5.3
	ctx := context.Background()
	catalog := newMockCatalog()
	creator := NewTableCreator(TableCreatorConfig{
		Catalog:   catalog,
		ClusterID: "test-cluster",
	})

	table, err := creator.CreateTableForTopic(ctx, "schema-test")
	if err != nil {
		t.Fatalf("CreateTableForTopic failed: %v", err)
	}

	schema := table.Schema()

	// Expected fields per SPEC.md section 5.3
	expectedFields := []struct {
		name     string
		typ      string
		required bool
	}{
		{"partition", "int", true},
		{"offset", "long", true},
		{"timestamp_ms", "long", true},
		{"key", "binary", false},
		{"value", "binary", false},
		{"headers", "string", false},
		{"producer_id", "long", false},
		{"producer_epoch", "int", false},
		{"base_sequence", "int", false},
		{"attributes", "int", true},
	}

	if len(schema.Fields) != len(expectedFields) {
		t.Errorf("expected %d fields, got %d", len(expectedFields), len(schema.Fields))
	}

	fieldMap := make(map[string]Field)
	for _, f := range schema.Fields {
		fieldMap[f.Name] = f
	}

	for _, exp := range expectedFields {
		field, ok := fieldMap[exp.name]
		if !ok {
			t.Errorf("missing field %q", exp.name)
			continue
		}
		if field.Type != exp.typ {
			t.Errorf("field %q: expected type %q, got %q", exp.name, exp.typ, field.Type)
		}
		if field.Required != exp.required {
			t.Errorf("field %q: expected required=%v, got %v", exp.name, exp.required, field.Required)
		}
	}
}

func TestTableCreator_PartitionSpecCompliance(t *testing.T) {
	// Verify partition spec matches SPEC.md section 5.3:
	// "Partition by partition (identity)"
	spec := DefaultPartitionSpec()

	if len(spec.Fields) != 1 {
		t.Fatalf("expected 1 partition field, got %d", len(spec.Fields))
	}

	pf := spec.Fields[0]
	if pf.Name != "partition" {
		t.Errorf("expected partition field name 'partition', got %q", pf.Name)
	}
	if pf.Transform != "identity" {
		t.Errorf("expected transform 'identity', got %q", pf.Transform)
	}
	if pf.SourceID != FieldIDPartition {
		t.Errorf("expected source ID %d, got %d", FieldIDPartition, pf.SourceID)
	}
}

func TestTableCreator_PropertiesCompliance(t *testing.T) {
	// Verify all required properties are set per SPEC.md section 4.3
	props := DefaultTableProperties("test-topic", "test-cluster")

	if props[PropertyDrayTopic] != "test-topic" {
		t.Errorf("expected dray.topic = 'test-topic', got %q", props[PropertyDrayTopic])
	}

	if props[PropertyDrayClusterID] != "test-cluster" {
		t.Errorf("expected dray.cluster_id = 'test-cluster', got %q", props[PropertyDrayClusterID])
	}

	if props[PropertyDraySchemaVersion] != "1" {
		t.Errorf("expected dray.schema_version = '1', got %q", props[PropertyDraySchemaVersion])
	}
}
