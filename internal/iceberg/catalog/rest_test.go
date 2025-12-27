package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewRestCatalog(t *testing.T) {
	tests := []struct {
		name    string
		config  RestCatalogConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: RestCatalogConfig{
				URI:       "http://localhost:8181",
				Warehouse: "s3://bucket/warehouse",
			},
			wantErr: false,
		},
		{
			name:    "missing URI",
			config:  RestCatalogConfig{},
			wantErr: true,
		},
		{
			name: "URI without scheme",
			config: RestCatalogConfig{
				URI: "localhost:8181",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, err := NewRestCatalog(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRestCatalog() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && catalog == nil {
				t.Error("NewRestCatalog() returned nil catalog")
			}
		})
	}
}

func TestRestCatalog_LoadTable(t *testing.T) {
	tableResp := loadTableResponse{
		MetadataLocation: "s3://bucket/warehouse/test_table/metadata/v1.metadata.json",
		Metadata: tableMetadata{
			FormatVersion:   2,
			TableUUID:       "test-uuid-1234",
			Location:        "s3://bucket/warehouse/test_table",
			LastUpdatedMs:   1234567890000,
			LastColumnID:    10,
			CurrentSchemaID: 0,
			Schemas: []apiSchema{
				{
					Type:     "struct",
					SchemaID: 0,
					Fields: []apiField{
						{ID: 1, Name: "id", Required: true, Type: "long"},
						{ID: 2, Name: "data", Required: false, Type: "string"},
					},
				},
			},
			PartitionSpecs: []apiPartitionSpec{
				{SpecID: 0, Fields: []apiPartitionField{}},
			},
			Properties: map[string]string{
				"dray.topic":      "test-topic",
				"dray.cluster_id": "cluster-1",
			},
			CurrentSnapshotID: ptr(int64(1000)),
			Snapshots: []apiSnapshot{
				{
					SnapshotID:     1000,
					SequenceNumber: 1,
					TimestampMs:    1234567890000,
					ManifestList:   "s3://bucket/warehouse/test_table/metadata/snap-1000.avro",
					Summary: map[string]string{
						"operation":     "append",
						"added-records": "100",
					},
				},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}
		expectedPath := "/v1/namespaces/test_ns/tables/test_table"
		if r.URL.Path != expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tableResp)
	}))
	defer server.Close()

	catalog, err := NewRestCatalog(RestCatalogConfig{URI: server.URL})
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	table, err := catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"test_ns"},
		Name:      "test_table",
	})
	if err != nil {
		t.Fatalf("LoadTable() error = %v", err)
	}

	// Verify table properties
	if table.Location() != "s3://bucket/warehouse/test_table" {
		t.Errorf("Expected location s3://bucket/warehouse/test_table, got %s", table.Location())
	}

	schema := table.Schema()
	if len(schema.Fields) != 2 {
		t.Errorf("Expected 2 schema fields, got %d", len(schema.Fields))
	}

	props := table.Properties()
	if props["dray.topic"] != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", props["dray.topic"])
	}

	snapshot, err := table.CurrentSnapshot(context.Background())
	if err != nil {
		t.Errorf("CurrentSnapshot() error = %v", err)
	}
	if snapshot.SnapshotID != 1000 {
		t.Errorf("Expected snapshot ID 1000, got %d", snapshot.SnapshotID)
	}
}

func TestRestCatalog_LoadTable_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"message": "Table does not exist",
				"type":    "NoSuchTableException",
			},
		})
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	_, err := catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "missing",
	})
	if err == nil {
		t.Error("Expected error for missing table")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Expected 'not found' error, got: %v", err)
	}
}

func TestRestCatalog_CreateTable(t *testing.T) {
	createCalled := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			// First LoadTable call returns 404
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"message": "Table does not exist",
				},
			})
			return
		}

		if r.Method == "POST" {
			createCalled = true

			var req createTableRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("Failed to decode request: %v", err)
			}

			if req.Name != "new_table" {
				t.Errorf("Expected table name 'new_table', got %s", req.Name)
			}

			resp := loadTableResponse{
				MetadataLocation: "s3://bucket/new_table/metadata/v1.metadata.json",
				Metadata: tableMetadata{
					FormatVersion:   2,
					TableUUID:       "new-uuid",
					Location:        "s3://bucket/new_table",
					CurrentSchemaID: 0,
					Schemas: []apiSchema{
						{SchemaID: 0, Type: "struct", Fields: []apiField{}},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	table, err := catalog.CreateTableIfMissing(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "new_table",
	}, CreateTableOptions{
		Schema: DefaultSchema(),
		Properties: TableProperties{
			"dray.topic": "my-topic",
		},
	})

	if err != nil {
		t.Fatalf("CreateTableIfMissing() error = %v", err)
	}
	if !createCalled {
		t.Error("Expected create API to be called")
	}
	if table.Identifier().Name != "new_table" {
		t.Errorf("Expected table name 'new_table', got %s", table.Identifier().Name)
	}
}

func TestRestCatalog_CreateTableIfMissing_ExistingTable(t *testing.T) {
	getCalled := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			getCalled++
			resp := loadTableResponse{
				Metadata: tableMetadata{
					FormatVersion: 2,
					TableUUID:     "existing-uuid",
					Location:      "s3://bucket/existing",
					Schemas:       []apiSchema{{SchemaID: 0, Type: "struct", Fields: []apiField{}}},
				},
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
		t.Error("Unexpected non-GET request for existing table")
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	table, err := catalog.CreateTableIfMissing(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "existing",
	}, CreateTableOptions{
		Schema: DefaultSchema(),
	})

	if err != nil {
		t.Fatalf("CreateTableIfMissing() error = %v", err)
	}
	if getCalled != 1 {
		t.Errorf("Expected GET called once, called %d times", getCalled)
	}
	if table.Location() != "s3://bucket/existing" {
		t.Errorf("Expected location s3://bucket/existing, got %s", table.Location())
	}
}

func TestRestCatalog_DropTable(t *testing.T) {
	deleted := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			deleted = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		t.Errorf("Unexpected method: %s", r.Method)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	err := catalog.DropTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "to_delete",
	})

	if err != nil {
		t.Errorf("DropTable() error = %v", err)
	}
	if !deleted {
		t.Error("Expected DELETE request")
	}
}

func TestRestCatalog_ListTables(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}
		resp := map[string]interface{}{
			"identifiers": []map[string]interface{}{
				{"namespace": []string{"ns"}, "name": "table1"},
				{"namespace": []string{"ns"}, "name": "table2"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	tables, err := catalog.ListTables(context.Background(), []string{"ns"})
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}

	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
	if tables[0].Name != "table1" || tables[1].Name != "table2" {
		t.Errorf("Unexpected table names: %v", tables)
	}
}

func TestRestCatalog_TableExists(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		exists     bool
	}{
		{"exists", http.StatusOK, true},
		{"not found", http.StatusNotFound, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.statusCode == http.StatusOK {
					resp := loadTableResponse{
						Metadata: tableMetadata{
							FormatVersion: 2,
							Schemas:       []apiSchema{{SchemaID: 0, Type: "struct"}},
						},
					}
					json.NewEncoder(w).Encode(resp)
				} else {
					w.WriteHeader(tt.statusCode)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"error": map[string]interface{}{"message": "Table not found"},
					})
				}
			}))
			defer server.Close()

			catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

			exists, err := catalog.TableExists(context.Background(), TableIdentifier{
				Namespace: []string{"ns"},
				Name:      "test",
			})
			if err != nil {
				t.Fatalf("TableExists() error = %v", err)
			}
			if exists != tt.exists {
				t.Errorf("TableExists() = %v, want %v", exists, tt.exists)
			}
		})
	}
}

func TestRestCatalog_BasicAuth(t *testing.T) {
	authHeader := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		resp := loadTableResponse{
			Metadata: tableMetadata{
				FormatVersion: 2,
				Schemas:       []apiSchema{{SchemaID: 0, Type: "struct"}},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{
		URI:            server.URL,
		Credential:     "user:pass",
		CredentialType: "basic",
	})

	_, _ = catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	if !strings.HasPrefix(authHeader, "Basic ") {
		t.Errorf("Expected Basic auth header, got: %s", authHeader)
	}
}

func TestRestCatalog_BearerAuth(t *testing.T) {
	authHeader := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		resp := loadTableResponse{
			Metadata: tableMetadata{
				FormatVersion: 2,
				Schemas:       []apiSchema{{SchemaID: 0, Type: "struct"}},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{
		URI:            server.URL,
		Token:          "my-token-123",
		CredentialType: "bearer",
	})

	_, _ = catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	if authHeader != "Bearer my-token-123" {
		t.Errorf("Expected 'Bearer my-token-123', got: %s", authHeader)
	}
}

func TestRestCatalog_OAuth(t *testing.T) {
	tokenCalled := false
	authHeader := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/oauth/tokens" {
			tokenCalled = true

			if r.Method != "POST" {
				t.Errorf("Expected POST for token, got %s", r.Method)
			}

			resp := map[string]interface{}{
				"access_token": "oauth-token-xyz",
				"token_type":   "bearer",
				"expires_in":   3600,
			}
			json.NewEncoder(w).Encode(resp)
			return
		}

		authHeader = r.Header.Get("Authorization")
		resp := loadTableResponse{
			Metadata: tableMetadata{
				FormatVersion: 2,
				Schemas:       []apiSchema{{SchemaID: 0, Type: "struct"}},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{
		URI:            server.URL,
		Credential:     "client_id:client_secret",
		CredentialType: "oauth",
	})

	_, _ = catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	if !tokenCalled {
		t.Error("Expected OAuth token endpoint to be called")
	}
	if authHeader != "Bearer oauth-token-xyz" {
		t.Errorf("Expected 'Bearer oauth-token-xyz', got: %s", authHeader)
	}
}

func TestRestCatalog_CommitConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			resp := loadTableResponse{
				Metadata: tableMetadata{
					FormatVersion:     2,
					CurrentSnapshotID: ptr(int64(1)),
					Schemas:           []apiSchema{{SchemaID: 0, Type: "struct"}},
					Snapshots:         []apiSnapshot{{SnapshotID: 1}},
				},
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
		if r.Method == "POST" {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"message": "Commit conflict: snapshot was updated",
				},
			})
		}
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	table, _ := catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	_, err := table.AppendFiles(context.Background(), []DataFile{
		{Path: "s3://bucket/data.parquet", RecordCount: 100},
	}, nil)

	if err == nil {
		t.Error("Expected commit conflict error")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Errorf("Expected conflict error, got: %v", err)
	}
}

func TestRestCatalog_AppendFiles(t *testing.T) {
	commitCalled := false
	var receivedDataFiles []apiDataFile

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			resp := loadTableResponse{
				Metadata: tableMetadata{
					FormatVersion:     2,
					CurrentSchemaID:   0,
					DefaultSpecID:     0,
					CurrentSnapshotID: ptr(int64(1000)),
					Schemas:           []apiSchema{{SchemaID: 0, Type: "struct"}},
					Snapshots: []apiSnapshot{
						{SnapshotID: 1000, SequenceNumber: 1, TimestampMs: 1000},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
		if r.Method == "POST" {
			commitCalled = true

			var req commitTableRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("Failed to decode commit request: %v", err)
			}

			// Verify updates - check for append-files action with data files
			hasAppendFiles := false
			for _, u := range req.Updates {
				if u.Action == "append-files" {
					hasAppendFiles = true
					receivedDataFiles = u.DataFiles
					if len(u.DataFiles) != 2 {
						t.Errorf("Expected 2 data files, got %d", len(u.DataFiles))
					}
				}
			}
			if !hasAppendFiles {
				t.Error("Missing append-files update with data files")
			}

			resp := loadTableResponse{
				Metadata: tableMetadata{
					FormatVersion:     2,
					CurrentSnapshotID: ptr(int64(2000)),
					Schemas:           []apiSchema{{SchemaID: 0, Type: "struct"}},
					Snapshots: []apiSnapshot{
						{SnapshotID: 1000, SequenceNumber: 1, TimestampMs: 1000},
						{SnapshotID: 2000, SequenceNumber: 2, TimestampMs: 2000, Summary: map[string]string{"operation": "append"}},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	table, _ := catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	snapshot, err := table.AppendFiles(context.Background(), []DataFile{
		{Path: "s3://bucket/file1.parquet", Format: FormatParquet, RecordCount: 100, FileSizeBytes: 1024, PartitionValue: 0},
		{Path: "s3://bucket/file2.parquet", Format: FormatParquet, RecordCount: 50, FileSizeBytes: 512, PartitionValue: 1},
	}, &AppendFilesOptions{
		SnapshotProperties: map[string]string{"dray.commit-id": "abc123"},
	})

	if err != nil {
		t.Fatalf("AppendFiles() error = %v", err)
	}
	if !commitCalled {
		t.Error("Expected commit API to be called")
	}
	if snapshot.SnapshotID != 2000 {
		t.Errorf("Expected snapshot ID 2000, got %d", snapshot.SnapshotID)
	}

	// Verify data files were properly included in the request
	if len(receivedDataFiles) != 2 {
		t.Fatalf("Expected 2 data files in request, got %d", len(receivedDataFiles))
	}

	// Verify first data file
	if receivedDataFiles[0].FilePath != "s3://bucket/file1.parquet" {
		t.Errorf("Expected file path s3://bucket/file1.parquet, got %s", receivedDataFiles[0].FilePath)
	}
	if receivedDataFiles[0].RecordCount != 100 {
		t.Errorf("Expected record count 100, got %d", receivedDataFiles[0].RecordCount)
	}
	if receivedDataFiles[0].FileSizeInBytes != 1024 {
		t.Errorf("Expected file size 1024, got %d", receivedDataFiles[0].FileSizeInBytes)
	}
	if receivedDataFiles[0].FileFormat != "PARQUET" {
		t.Errorf("Expected file format PARQUET, got %s", receivedDataFiles[0].FileFormat)
	}

	// Verify second data file
	if receivedDataFiles[1].FilePath != "s3://bucket/file2.parquet" {
		t.Errorf("Expected file path s3://bucket/file2.parquet, got %s", receivedDataFiles[1].FilePath)
	}
	if receivedDataFiles[1].RecordCount != 50 {
		t.Errorf("Expected record count 50, got %d", receivedDataFiles[1].RecordCount)
	}
	if receivedDataFiles[1].FileSizeInBytes != 512 {
		t.Errorf("Expected file size 512, got %d", receivedDataFiles[1].FileSizeInBytes)
	}
}

func TestRestTable_CurrentSnapshot_NoSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := loadTableResponse{
			Metadata: tableMetadata{
				FormatVersion:     2,
				CurrentSnapshotID: nil, // No snapshot
				Schemas:           []apiSchema{{SchemaID: 0, Type: "struct"}},
				Snapshots:         []apiSnapshot{},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	table, _ := catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "empty",
	})

	_, err := table.CurrentSnapshot(context.Background())
	if err == nil {
		t.Error("Expected error for empty table")
	}
	if err != ErrSnapshotNotFound {
		t.Errorf("Expected ErrSnapshotNotFound, got: %v", err)
	}
}

func TestRestTable_Refresh(t *testing.T) {
	callCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		resp := loadTableResponse{
			Metadata: tableMetadata{
				FormatVersion:     2,
				CurrentSnapshotID: ptr(int64(callCount * 1000)),
				Schemas:           []apiSchema{{SchemaID: 0, Type: "struct"}},
				Snapshots: []apiSnapshot{
					{SnapshotID: int64(callCount * 1000)},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

	table, _ := catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	snap1, _ := table.CurrentSnapshot(context.Background())
	if snap1.SnapshotID != 1000 {
		t.Errorf("Expected snapshot ID 1000, got %d", snap1.SnapshotID)
	}

	if err := table.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}

	snap2, _ := table.CurrentSnapshot(context.Background())
	if snap2.SnapshotID != 2000 {
		t.Errorf("Expected snapshot ID 2000 after refresh, got %d", snap2.SnapshotID)
	}
}

func TestRestCatalog_Prefix(t *testing.T) {
	requestedPath := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		resp := loadTableResponse{
			Metadata: tableMetadata{
				FormatVersion: 2,
				Schemas:       []apiSchema{{SchemaID: 0, Type: "struct"}},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{
		URI:    server.URL,
		Prefix: "my-warehouse",
	})

	_, _ = catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	expected := "/v1/my-warehouse/namespaces/ns/tables/test"
	if requestedPath != expected {
		t.Errorf("Expected path %s, got %s", expected, requestedPath)
	}
}

func TestRestCatalog_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		json.NewEncoder(w).Encode(loadTableResponse{})
	}))
	defer server.Close()

	catalog, _ := NewRestCatalog(RestCatalogConfig{
		URI:            server.URL,
		RequestTimeout: 100 * time.Millisecond,
	})

	_, err := catalog.LoadTable(context.Background(), TableIdentifier{
		Namespace: []string{"ns"},
		Name:      "test",
	})

	if err == nil {
		t.Error("Expected timeout error")
	}
}

func TestSchemaConversion(t *testing.T) {
	schema := DefaultSchema()
	apiSchema := schemaToAPI(schema)

	if apiSchema.Type != "struct" {
		t.Errorf("Expected type 'struct', got %s", apiSchema.Type)
	}
	if len(apiSchema.Fields) != len(schema.Fields) {
		t.Errorf("Expected %d fields, got %d", len(schema.Fields), len(apiSchema.Fields))
	}

	converted := schemaFromAPI(apiSchema)
	if len(converted.Fields) != len(schema.Fields) {
		t.Errorf("Round-trip failed: expected %d fields, got %d", len(schema.Fields), len(converted.Fields))
	}

	for i, f := range converted.Fields {
		if f.Name != schema.Fields[i].Name {
			t.Errorf("Field %d: expected name %s, got %s", i, schema.Fields[i].Name, f.Name)
		}
		if f.Type != schema.Fields[i].Type {
			t.Errorf("Field %d: expected type %s, got %s", i, schema.Fields[i].Type, f.Type)
		}
	}
}

func TestPartitionSpecConversion(t *testing.T) {
	spec := DefaultPartitionSpec()
	apiSpec := partitionSpecToAPI(&spec)

	if apiSpec.SpecID != spec.SpecID {
		t.Errorf("Expected spec ID %d, got %d", spec.SpecID, apiSpec.SpecID)
	}
	if len(apiSpec.Fields) != len(spec.Fields) {
		t.Errorf("Expected %d fields, got %d", len(spec.Fields), len(apiSpec.Fields))
	}
	if apiSpec.Fields[0].Transform != "identity" {
		t.Errorf("Expected transform 'identity', got %s", apiSpec.Fields[0].Transform)
	}
}

func TestSnapshotConversion(t *testing.T) {
	apiSnap := apiSnapshot{
		SnapshotID:       12345,
		ParentSnapshotID: ptr(int64(12344)),
		SequenceNumber:   5,
		TimestampMs:      1234567890000,
		ManifestList:     "s3://bucket/manifests/snap-12345.avro",
		Summary: map[string]string{
			"operation":        "append",
			"added-data-files": "3",
			"added-records":    "1000",
		},
	}

	snap := snapshotFromAPI(apiSnap)

	if snap.SnapshotID != 12345 {
		t.Errorf("Expected snapshot ID 12345, got %d", snap.SnapshotID)
	}
	if snap.ParentSnapshotID == nil || *snap.ParentSnapshotID != 12344 {
		t.Errorf("Expected parent snapshot ID 12344")
	}
	if snap.Operation != OpAppend {
		t.Errorf("Expected operation 'append', got %s", snap.Operation)
	}
	if snap.Summary["added-records"] != "1000" {
		t.Errorf("Expected summary added-records '1000', got %s", snap.Summary["added-records"])
	}
}

func TestDataFileConversion(t *testing.T) {
	sortOrderID := int32(1)
	dataFile := DataFile{
		Path:           "s3://bucket/data/file.parquet",
		Format:         FormatParquet,
		PartitionValue: 5,
		RecordCount:    1000,
		FileSizeBytes:  10240,
		ColumnSizes:    map[int32]int64{1: 100, 2: 200},
		ValueCounts:    map[int32]int64{1: 1000, 2: 1000},
		NullValueCounts: map[int32]int64{1: 0, 2: 50},
		LowerBounds:    map[int32][]byte{1: {0, 0, 0, 1}},
		UpperBounds:    map[int32][]byte{1: {0, 0, 3, 232}},
		SplitOffsets:   []int64{0, 5000},
		SortOrderID:    &sortOrderID,
	}

	api := dataFileToAPI(dataFile, 0, 10)

	// Verify basic fields
	if api.FilePath != dataFile.Path {
		t.Errorf("Expected path %s, got %s", dataFile.Path, api.FilePath)
	}
	if api.FileFormat != "PARQUET" {
		t.Errorf("Expected format PARQUET, got %s", api.FileFormat)
	}
	if api.RecordCount != dataFile.RecordCount {
		t.Errorf("Expected record count %d, got %d", dataFile.RecordCount, api.RecordCount)
	}
	if api.FileSizeInBytes != dataFile.FileSizeBytes {
		t.Errorf("Expected file size %d, got %d", dataFile.FileSizeBytes, api.FileSizeInBytes)
	}
	if api.Content != 0 {
		t.Errorf("Expected content type 0 (DATA), got %d", api.Content)
	}

	// Verify partition value
	if api.Partition == nil || api.Partition["partition"] != int32(5) {
		t.Errorf("Expected partition value 5, got %v", api.Partition)
	}

	// Verify column sizes are converted
	if len(api.ColumnSizes) != 2 {
		t.Errorf("Expected 2 column sizes, got %d", len(api.ColumnSizes))
	}
	if api.ColumnSizes["1"] != 100 {
		t.Errorf("Expected column size 100 for field 1, got %d", api.ColumnSizes["1"])
	}

	// Verify value counts
	if len(api.ValueCounts) != 2 {
		t.Errorf("Expected 2 value counts, got %d", len(api.ValueCounts))
	}

	// Verify null counts
	if len(api.NullValueCounts) != 2 {
		t.Errorf("Expected 2 null counts, got %d", len(api.NullValueCounts))
	}
	if api.NullValueCounts["2"] != 50 {
		t.Errorf("Expected null count 50 for field 2, got %d", api.NullValueCounts["2"])
	}

	// Verify bounds are base64 encoded
	if len(api.LowerBounds) != 1 || len(api.UpperBounds) != 1 {
		t.Errorf("Expected 1 bound each, got lower=%d upper=%d", len(api.LowerBounds), len(api.UpperBounds))
	}

	// Verify split offsets
	if len(api.SplitOffsets) != 2 {
		t.Errorf("Expected 2 split offsets, got %d", len(api.SplitOffsets))
	}

	// Verify sort order
	if api.SortOrderID == nil || *api.SortOrderID != 1 {
		t.Errorf("Expected sort order ID 1")
	}

	// Verify sequence number
	if api.SequenceNumber == nil || *api.SequenceNumber != 10 {
		t.Errorf("Expected sequence number 10")
	}
}

func TestDataFileConversion_PartitionZero(t *testing.T) {
	// Test that partition 0 (valid Kafka partition) is properly included
	dataFile := DataFile{
		Path:           "s3://bucket/data/partition0.parquet",
		Format:         FormatParquet,
		PartitionValue: 0, // Partition 0 is valid and must be included
		RecordCount:    500,
		FileSizeBytes:  2048,
	}

	api := dataFileToAPI(dataFile, 0, 1)

	// Verify partition value 0 is explicitly included, not omitted
	if api.Partition == nil {
		t.Error("Partition map should not be nil for partition 0")
	}
	if api.Partition["partition"] != int32(0) {
		t.Errorf("Expected partition value 0, got %v", api.Partition["partition"])
	}
}

func TestNamespacePath(t *testing.T) {
	tests := []struct {
		namespace []string
		want      string
	}{
		{[]string{"ns"}, "ns"},
		{[]string{"a", "b", "c"}, "a\x1Fb\x1Fc"},
		{[]string{}, ""},
	}

	for _, tt := range tests {
		got := namespacePath(tt.namespace)
		if got != tt.want {
			t.Errorf("namespacePath(%v) = %q, want %q", tt.namespace, got, tt.want)
		}
	}
}

func TestRestCatalog_ErrorResponses(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantErr    string
	}{
		{
			name:       "service unavailable",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"error":{"message":"Service overloaded"}}`,
			wantErr:    "unavailable",
		},
		{
			name:       "bad gateway",
			statusCode: http.StatusBadGateway,
			body:       `{"error":{"message":"Upstream error"}}`,
			wantErr:    "unavailable",
		},
		{
			name:       "unauthorized",
			statusCode: http.StatusUnauthorized,
			body:       `{"error":{"message":"Invalid credentials"}}`,
			wantErr:    "authentication",
		},
		{
			name:       "internal error",
			statusCode: http.StatusInternalServerError,
			body:       `{"error":{"message":"Something went wrong"}}`,
			wantErr:    "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			catalog, _ := NewRestCatalog(RestCatalogConfig{URI: server.URL})

			_, err := catalog.LoadTable(context.Background(), TableIdentifier{
				Namespace: []string{"ns"},
				Name:      "test",
			})

			if err == nil {
				t.Error("Expected error")
			}
			if !strings.Contains(strings.ToLower(err.Error()), tt.wantErr) {
				t.Errorf("Expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestRestCatalog_Close(t *testing.T) {
	catalog, _ := NewRestCatalog(RestCatalogConfig{URI: "http://localhost:8181"})
	if err := catalog.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// Helper function
func ptr[T any](v T) *T {
	return &v
}
