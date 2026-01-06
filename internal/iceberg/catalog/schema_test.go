package catalog

import (
	"testing"

	"github.com/apache/iceberg-go"
)

// collectFields converts the iterator to a slice for easier testing
func collectFields(spec iceberg.PartitionSpec) []iceberg.PartitionField {
	var fields []iceberg.PartitionField
	for field := range spec.Fields() {
		fields = append(fields, field)
	}
	return fields
}

func TestPartitionSpecFromConfig_Default(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig(nil, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	field := fields[0]
	if field.Name != "partition" {
		t.Errorf("expected field name 'partition', got %q", field.Name)
	}
	if _, ok := field.Transform.(iceberg.IdentityTransform); !ok {
		t.Errorf("expected IdentityTransform, got %T", field.Transform)
	}
}

func TestPartitionSpecFromConfig_Identity(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig([]string{"partition"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	field := fields[0]
	if field.Name != "partition" {
		t.Errorf("expected field name 'partition', got %q", field.Name)
	}
	if _, ok := field.Transform.(iceberg.IdentityTransform); !ok {
		t.Errorf("expected IdentityTransform, got %T", field.Transform)
	}
}

func TestPartitionSpecFromConfig_DayTransform(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig([]string{"day(timestamp)"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	field := fields[0]
	if field.Name != "timestamp_day" {
		t.Errorf("expected field name 'timestamp_day', got %q", field.Name)
	}
	if _, ok := field.Transform.(iceberg.DayTransform); !ok {
		t.Errorf("expected DayTransform, got %T", field.Transform)
	}
	if field.SourceID != FieldIDTimestamp {
		t.Errorf("expected source ID %d, got %d", FieldIDTimestamp, field.SourceID)
	}
}

func TestPartitionSpecFromConfig_Multiple(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig([]string{"partition", "day(timestamp)"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}

	// First field: partition identity
	field0 := fields[0]
	if field0.Name != "partition" {
		t.Errorf("expected first field name 'partition', got %q", field0.Name)
	}
	if _, ok := field0.Transform.(iceberg.IdentityTransform); !ok {
		t.Errorf("expected IdentityTransform for first field, got %T", field0.Transform)
	}

	// Second field: day transform on timestamp
	field1 := fields[1]
	if field1.Name != "timestamp_day" {
		t.Errorf("expected second field name 'timestamp_day', got %q", field1.Name)
	}
	if _, ok := field1.Transform.(iceberg.DayTransform); !ok {
		t.Errorf("expected DayTransform for second field, got %T", field1.Transform)
	}
}

func TestPartitionSpecFromConfig_HourTransform(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig([]string{"hour(timestamp)"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	field := fields[0]
	if field.Name != "timestamp_hour" {
		t.Errorf("expected field name 'timestamp_hour', got %q", field.Name)
	}
	if _, ok := field.Transform.(iceberg.HourTransform); !ok {
		t.Errorf("expected HourTransform, got %T", field.Transform)
	}
}

func TestPartitionSpecFromConfig_MonthTransform(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig([]string{"month(timestamp)"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	field := fields[0]
	if field.Name != "timestamp_month" {
		t.Errorf("expected field name 'timestamp_month', got %q", field.Name)
	}
	if _, ok := field.Transform.(iceberg.MonthTransform); !ok {
		t.Errorf("expected MonthTransform, got %T", field.Transform)
	}
}

func TestPartitionSpecFromConfig_YearTransform(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig([]string{"year(timestamp)"}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	field := fields[0]
	if field.Name != "timestamp_year" {
		t.Errorf("expected field name 'timestamp_year', got %q", field.Name)
	}
	if _, ok := field.Transform.(iceberg.YearTransform); !ok {
		t.Errorf("expected YearTransform, got %T", field.Transform)
	}
}

func TestPartitionSpecFromConfig_InvalidColumn(t *testing.T) {
	schema := DefaultSchema()
	_, err := PartitionSpecFromConfig([]string{"day(nonexistent)"}, schema)
	if err == nil {
		t.Fatal("expected error for nonexistent column")
	}
}

func TestPartitionSpecFromConfig_InvalidTransform(t *testing.T) {
	schema := DefaultSchema()
	_, err := PartitionSpecFromConfig([]string{"bucket(partition)"}, schema)
	if err == nil {
		t.Fatal("expected error for unsupported transform")
	}
}

func TestPartitionSpecFromConfig_EmptyExpressions(t *testing.T) {
	schema := DefaultSchema()
	spec, err := PartitionSpecFromConfig([]string{""}, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields := collectFields(spec)
	// Should fall back to default
	if len(fields) != 1 {
		t.Fatalf("expected 1 field (default), got %d", len(fields))
	}
}
