package catalog

import (
	"context"

	"github.com/dray-io/dray/internal/projection"
)

// TableCreator handles Iceberg table creation for Dray topics.
// This implements the stream-table duality by creating an Iceberg table
// whenever a Kafka topic is created, per SPEC.md section 4.3.
type TableCreator struct {
	catalog      Catalog
	namespace    []string
	clusterID    string
	projByTopic  map[string]projection.TopicProjection
	partitioning []string // partition expressions from config
}

// TableCreatorConfig configures the TableCreator.
type TableCreatorConfig struct {
	// Catalog is the Iceberg catalog to use for table operations.
	Catalog Catalog

	// Namespace is the Iceberg namespace for tables.
	// Default: ["dray"]
	Namespace []string

	// ClusterID is the Dray cluster identifier stored in table properties.
	ClusterID string

	// ValueProjections defines optional projected columns per topic.
	ValueProjections []projection.TopicProjection

	// Partitioning defines partition expressions for tables.
	// Examples: ["partition", "day(created_at)"]
	// Default: ["partition"]
	Partitioning []string
}

// NewTableCreator creates a new TableCreator.
func NewTableCreator(cfg TableCreatorConfig) *TableCreator {
	namespace := cfg.Namespace
	if len(namespace) == 0 {
		namespace = []string{"dray"}
	}

	partitioning := cfg.Partitioning
	if len(partitioning) == 0 {
		partitioning = []string{"partition"} // default
	}

	return &TableCreator{
		catalog:      cfg.Catalog,
		namespace:    namespace,
		clusterID:    cfg.ClusterID,
		projByTopic:  projection.ByTopic(projection.Normalize(cfg.ValueProjections)),
		partitioning: partitioning,
	}
}

// CreateTableForTopic creates an Iceberg table for a Kafka topic.
// This implements the stream-table duality per SPEC.md section 4.3.
//
// The table is created with:
//   - Schema per spec section 5.3 (partition, offset, timestamp, key, value, headers, etc.)
//   - Partition spec from config (default: identity on "partition" column)
//   - Properties: dray.topic, dray.cluster_id, dray.schema_version
//
// If the table already exists, it returns the existing table without error.
// This allows idempotent topic creation per architectural invariant 4.
func (c *TableCreator) CreateTableForTopic(ctx context.Context, topicName string) (Table, error) {
	if c.catalog == nil {
		return nil, ErrCatalogUnavailable
	}

	identifier := NewTableIdentifier(c.namespace, topicName)

	var projections []projection.FieldSpec
	if proj, ok := c.projByTopic[topicName]; ok {
		projections = proj.Fields
	}
	schema := SchemaWithProjections(projections)

	// Build partition spec from config
	spec, err := PartitionSpecFromConfig(c.partitioning, schema)
	if err != nil {
		return nil, err
	}

	opts := CreateTableOptions{
		Schema:        schema,
		PartitionSpec: &spec,
		Properties:    TablePropertiesForSchema(topicName, c.clusterID, schema),
	}

	return c.catalog.CreateTableIfMissing(ctx, identifier, opts)
}

// DropTableForTopic drops the Iceberg table for a Kafka topic.
// Returns nil if the table does not exist (idempotent).
func (c *TableCreator) DropTableForTopic(ctx context.Context, topicName string) error {
	if c.catalog == nil {
		return ErrCatalogUnavailable
	}

	identifier := NewTableIdentifier(c.namespace, topicName)

	err := c.catalog.DropTable(ctx, identifier)
	if err == ErrTableNotFound {
		return nil
	}
	return err
}

// TableExistsForTopic checks if an Iceberg table exists for a topic.
func (c *TableCreator) TableExistsForTopic(ctx context.Context, topicName string) (bool, error) {
	if c.catalog == nil {
		return false, ErrCatalogUnavailable
	}

	identifier := NewTableIdentifier(c.namespace, topicName)

	return c.catalog.TableExists(ctx, identifier)
}

// LoadTableForTopic loads the Iceberg table for a topic.
func (c *TableCreator) LoadTableForTopic(ctx context.Context, topicName string) (Table, error) {
	if c.catalog == nil {
		return nil, ErrCatalogUnavailable
	}

	identifier := NewTableIdentifier(c.namespace, topicName)

	return c.catalog.LoadTable(ctx, identifier)
}

// Namespace returns the configured namespace.
func (c *TableCreator) Namespace() []string {
	return c.namespace
}

// ClusterID returns the configured cluster ID.
func (c *TableCreator) ClusterID() string {
	return c.clusterID
}
