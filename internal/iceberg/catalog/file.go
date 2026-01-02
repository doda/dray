package catalog

import (
	"bytes"
        "context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/objectstore"
	"github.com/google/uuid"
)

// FileCatalog implements the Catalog interface using a Hadoop-style file structure.
// Metadata is stored directly in object storage at <warehouse>/<topic>/metadata/.
type FileCatalog struct {
	store        objectstore.Store
	warehouse    string // Normalized path for keys
	warehouseURI string // Raw URI for metadata Location
}

// NewFileCatalog creates a new FileCatalog.
func NewFileCatalog(store objectstore.Store, warehouse string) *FileCatalog {
	warehouseURI := strings.TrimSuffix(warehouse, "/")
	normalized := warehouseURI

	// If warehouse is a full S3 URI, strip the s3://bucket/ part for 'normalized'
	if strings.HasPrefix(normalized, "s3://") {
		parts := strings.SplitN(normalized[5:], "/", 2)
		if len(parts) > 1 {
			normalized = parts[1]
		} else {
			normalized = ""
		}
	}

	return &FileCatalog{
		store:        store,
		warehouse:    normalized,
		warehouseURI: warehouseURI,
	}
}

func (c *FileCatalog) tablePath(identifier TableIdentifier) string {
	var parts []string
	if c.warehouse != "" {
		parts = append(parts, c.warehouse)
	}
	if len(identifier.Namespace) > 0 {
		parts = append(parts, strings.Join(identifier.Namespace, "/"))
	}
	parts = append(parts, identifier.Name)
	return strings.Join(parts, "/")
}

func (c *FileCatalog) tableURI(identifier TableIdentifier) string {
	var parts []string
	if c.warehouseURI != "" {
		parts = append(parts, c.warehouseURI)
	}
	if len(identifier.Namespace) > 0 {
		parts = append(parts, strings.Join(identifier.Namespace, "/"))
	}
	parts = append(parts, identifier.Name)
	return strings.Join(parts, "/")
}

func (c *FileCatalog) metadataPath(identifier TableIdentifier) string {
	return c.tablePath(identifier) + "/metadata"
}

func (c *FileCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	version, err := c.getLatestVersion(ctx, identifier)
	if err != nil {
		return nil, err
	}

	metadata, err := c.readMetadata(ctx, identifier, version)
	if err != nil {
		return nil, err
	}

	return &fileTable{
		catalog:    c,
		identifier: identifier,
		metadata:   metadata,
		version:    version,
	}, nil
}

func (c *FileCatalog) CreateTableIfMissing(ctx context.Context, identifier TableIdentifier, opts CreateTableOptions) (Table, error) {
	table, err := c.LoadTable(ctx, identifier)
	if err == nil {
		return table, nil
	}
	if !errors.Is(err, ErrTableNotFound) {
		return nil, err
	}

	// Create new table metadata
	now := time.Now().UnixMilli()
	metadata := TableMetadata{
		FormatVersion:   2,
		TableUUID:       uuid.New().String(),
		Location:        opts.Location,
		LastUpdatedMs:   now,
		LastColumnID:    int32(len(opts.Schema.Fields)),
		Schemas:         []IcebergSchema{schemaToIceberg(opts.Schema)},
		CurrentSchemaID: opts.Schema.SchemaID,
		DefaultSpecID:   0,
		LastPartitionID: 999, // Partition field IDs start at 1000
		Properties:      opts.Properties,
	}

	if metadata.Location == "" {
		metadata.Location = c.tableURI(identifier)
	}

	if opts.PartitionSpec != nil {
		metadata.PartitionSpecs = []IcebergPartitionSpec{*partitionSpecToIceberg(opts.PartitionSpec)}
		metadata.DefaultSpecID = opts.PartitionSpec.SpecID
		for _, f := range opts.PartitionSpec.Fields {
			if f.FieldID > metadata.LastPartitionID {
				metadata.LastPartitionID = f.FieldID
			}
		}
	}

	err = c.writeMetadata(ctx, identifier, 0, metadata)
	if err != nil {
		// Handle race condition: table was created by another process
		if errors.Is(err, ErrCommitConflict) {
			return c.LoadTable(ctx, identifier)
		}
		return nil, err
	}

	return &fileTable{
		catalog:    c,
		identifier: identifier,
		metadata:   metadata,
		version:    0,
	}, nil
}

func (c *FileCatalog) GetCurrentSnapshot(ctx context.Context, identifier TableIdentifier) (*Snapshot, error) {
	table, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return table.CurrentSnapshot(ctx)
}

func (c *FileCatalog) AppendDataFiles(ctx context.Context, identifier TableIdentifier, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	table, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return table.AppendFiles(ctx, files, opts)
}

func (c *FileCatalog) DropTable(ctx context.Context, identifier TableIdentifier) error {
	// In a simple file catalog, dropping a table means removing its metadata.
	// For safety, we only remove the version-hint and metadata files.
	prefix := c.metadataPath(identifier)
	objs, err := c.store.List(ctx, prefix+"/")
	if err != nil {
		return err
	}

	for _, obj := range objs {
		_ = c.store.Delete(ctx, obj.Key)
	}

	return c.store.Delete(ctx, prefix+"/version-hint.text")
}

func (c *FileCatalog) ListTables(ctx context.Context, namespace []string) ([]TableIdentifier, error) {
	prefix := c.warehouse
	ns := strings.Join(namespace, "/")
	if ns != "" {
		prefix = prefix + "/" + ns
	}

	objs, err := c.store.List(ctx, prefix+"/")
	if err != nil {
		return nil, err
	}

	var tables []TableIdentifier
	seen := make(map[string]bool)

	for _, obj := range objs {
		rel := strings.TrimPrefix(obj.Key, prefix+"/")
		parts := strings.Split(rel, "/")
		if len(parts) >= 1 {
			name := parts[0]
			if !seen[name] {
				tables = append(tables, TableIdentifier{Namespace: namespace, Name: name})
				seen[name] = true
			}
		}
	}

	return tables, nil
}

func (c *FileCatalog) TableExists(ctx context.Context, identifier TableIdentifier) (bool, error) {
	hintPath := c.metadataPath(identifier) + "/version-hint.text"
	_, err := c.store.Head(ctx, hintPath)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, objectstore.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (c *FileCatalog) Close() error {
	return nil
}

func (c *FileCatalog) getLatestVersion(ctx context.Context, identifier TableIdentifier) (int, error) {
	hintPath := c.metadataPath(identifier) + "/version-hint.text"
	rc, err := c.store.Get(ctx, hintPath)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return -1, ErrTableNotFound
		}
		return -1, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return -1, err
	}

	version, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return -1, fmt.Errorf("invalid version hint: %w", err)
	}

	return version, nil
}

func (c *FileCatalog) readMetadata(ctx context.Context, identifier TableIdentifier, version int) (TableMetadata, error) {
	metaPath := fmt.Sprintf("%s/v%d.metadata.json", c.metadataPath(identifier), version)
	rc, err := c.store.Get(ctx, metaPath)
	if err != nil {
		return TableMetadata{}, err
	}
	defer rc.Close()

	var metadata TableMetadata
	if err := json.NewDecoder(rc).Decode(&metadata); err != nil {
		return TableMetadata{}, err
	}

	return metadata, nil
}

func (c *FileCatalog) writeMetadata(ctx context.Context, identifier TableIdentifier, version int, metadata TableMetadata) error {
	metaPath := fmt.Sprintf("%s/v%d.metadata.json", c.metadataPath(identifier), version)
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	// Use conditional write to ensure we don't overwrite an existing version
	err = c.store.PutWithOptions(ctx, metaPath, strings.NewReader(string(data)), int64(len(data)), "application/json", objectstore.PutOptions{
		IfNoneMatch: "*",
	})
	if err != nil {
		if errors.Is(err, objectstore.ErrPreconditionFailed) {
			return ErrCommitConflict
		}
		return err
	}

	// Update version hint
	hintPath := c.metadataPath(identifier) + "/version-hint.text"
	versionStr := strconv.Itoa(version)
	return c.store.Put(ctx, hintPath, strings.NewReader(versionStr), int64(len(versionStr)), "text/plain")
}

type fileTable struct {
	catalog    *FileCatalog
	identifier TableIdentifier
	metadata   TableMetadata
	version    int
	mu         sync.RWMutex
}

func (t *fileTable) Identifier() TableIdentifier {
	return t.identifier
}

func (t *fileTable) Schema() Schema {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, s := range t.metadata.Schemas {
		if s.SchemaID == t.metadata.CurrentSchemaID {
			return schemaFromIceberg(s)
		}
	}
	return schemaFromIceberg(t.metadata.Schemas[0])
}

func (t *fileTable) CurrentSnapshot(ctx context.Context) (*Snapshot, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.metadata.CurrentSnapshotID == nil {
		return nil, ErrSnapshotNotFound
	}

	for _, snap := range t.metadata.Snapshots {
		if snap.SnapshotID == *t.metadata.CurrentSnapshotID {
			return snapshotFromIceberg(snap), nil
		}
	}

	return nil, ErrSnapshotNotFound
}

func (t *fileTable) Snapshots(ctx context.Context) ([]Snapshot, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]Snapshot, len(t.metadata.Snapshots))
	for i, snap := range t.metadata.Snapshots {
		result[i] = *snapshotFromIceberg(snap)
	}
	return result, nil
}

func (t *fileTable) AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	if len(files) == 0 {
		return nil, errors.New("no files to append")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if opts != nil && opts.ExpectedSnapshotID != nil {
		if t.metadata.CurrentSnapshotID == nil || *t.metadata.CurrentSnapshotID != *opts.ExpectedSnapshotID {
			return nil, ErrCommitConflict
		}
	}

	newSnapshotID := time.Now().UnixNano()
	now := time.Now().UnixMilli()

	var parentID *int64
	seqNum := int64(1)
	if t.metadata.CurrentSnapshotID != nil {
		parentID = t.metadata.CurrentSnapshotID
		for _, snap := range t.metadata.Snapshots {
			if snap.SnapshotID == *parentID {
				seqNum = snap.SequenceNumber + 1
				break
			}
		}
	}

	// 1. Write Manifest File (Avro)
	manifestData, err := writeManifestFile(files, newSnapshotID, seqNum)
	if err != nil {
		return nil, fmt.Errorf("failed to create manifest file: %w", err)
	}

	manifestName := fmt.Sprintf("manifest-%d.avro", newSnapshotID)
	manifestPath := fmt.Sprintf("%s/metadata/%s", t.metadata.Location, manifestName)
	// Strip s3://bucket/ for store.Put
	manifestKey := manifestPath
	if strings.HasPrefix(manifestKey, "s3://") {
		parts := strings.SplitN(manifestKey[5:], "/", 2)
		if len(parts) > 1 {
			manifestKey = parts[1]
		}
	}

	if err := t.catalog.store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), "application/avro"); err != nil {
		return nil, fmt.Errorf("failed to upload manifest file: %w", err)
	}

	// 2. Write Manifest List (Avro)
	// Include all manifests from the parent snapshot plus the new manifest
	var allManifestEntries []AvroManifestListEntry

	// If there's a parent snapshot, read its manifest list and include those manifests
	if parentID != nil {
		for _, snap := range t.metadata.Snapshots {
			if snap.SnapshotID == *parentID && snap.ManifestList != "" {
				// Read parent's manifest list from S3
				parentManifestListKey := snap.ManifestList
				if strings.HasPrefix(parentManifestListKey, "s3://") {
					parts := strings.SplitN(parentManifestListKey[5:], "/", 2)
					if len(parts) > 1 {
						parentManifestListKey = parts[1]
					}
				}

				rc, err := t.catalog.store.Get(ctx, parentManifestListKey)
				if err == nil {
					parentData, readErr := io.ReadAll(rc)
					rc.Close()
					if readErr == nil {
						parentEntries, parseErr := readManifestList(parentData)
						if parseErr == nil {
							// Convert parent entries: move "added" counts to "existing"
							for _, entry := range parentEntries {
								existingEntry := entry
								existingEntry.ExistingDataFilesCount = entry.AddedDataFilesCount + entry.ExistingDataFilesCount
								existingEntry.ExistingRowsCount = entry.AddedRowsCount + entry.ExistingRowsCount
								existingEntry.AddedDataFilesCount = 0
								existingEntry.AddedRowsCount = 0
								allManifestEntries = append(allManifestEntries, existingEntry)
							}
						}
					}
				}
				break
			}
		}
	}

	// Add the new manifest entry
	manifestListEntry := AvroManifestListEntry{
		ManifestPath:        manifestPath,
		ManifestLength:      int64(len(manifestData)),
		PartitionSpecID:     t.metadata.DefaultSpecID,
		AddedSnapshotID:     newSnapshotID,
		AddedDataFilesCount: int32(len(files)),
		AddedRowsCount:      0,
		Content:             0, // DATA
		SequenceNumber:      seqNum,
	}
	for _, f := range files {
		manifestListEntry.AddedRowsCount += f.RecordCount
	}
	allManifestEntries = append(allManifestEntries, manifestListEntry)

	manifestListData, err := writeManifestList(allManifestEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to create manifest list: %w", err)
	}

	manifestListName := fmt.Sprintf("snap-%d.avro", newSnapshotID)
	manifestListPath := fmt.Sprintf("%s/metadata/%s", t.metadata.Location, manifestListName)
	manifestListKey := manifestListPath
	if strings.HasPrefix(manifestListKey, "s3://") {
		parts := strings.SplitN(manifestListKey[5:], "/", 2)
		if len(parts) > 1 {
			manifestListKey = parts[1]
		}
	}

	if err := t.catalog.store.Put(ctx, manifestListKey, bytes.NewReader(manifestListData), int64(len(manifestListData)), "application/avro"); err != nil {
		return nil, fmt.Errorf("failed to upload manifest list: %w", err)
	}

	summary := map[string]string{
		"operation":        "append",
		"added-data-files": strconv.Itoa(len(files)),
		"added-records":    strconv.FormatInt(manifestListEntry.AddedRowsCount, 10),
		"added-files-size": "0", // Sum from files
		"total-data-files": strconv.Itoa(len(files)),
		"total-records":    "0",
		"total-files-size": "0",
	}

	var totalBytes int64
	for _, f := range files {
		totalBytes += f.FileSizeBytes
	}
	summary["added-files-size"] = strconv.FormatInt(totalBytes, 10)

	if opts != nil && opts.SnapshotProperties != nil {
		for k, v := range opts.SnapshotProperties {
			summary[k] = v
		}
	}

	// Update summary with totals if parent exists
	if parentID != nil {
		for _, snap := range t.metadata.Snapshots {
			if snap.SnapshotID == *parentID {
				tr, _ := strconv.ParseInt(snap.Summary["total-records"], 10, 64)
				tf, _ := strconv.ParseInt(snap.Summary["total-data-files"], 10, 64)
				ts, _ := strconv.ParseInt(snap.Summary["total-files-size"], 10, 64)

				summary["total-records"] = strconv.FormatInt(tr+manifestListEntry.AddedRowsCount, 10)
				summary["total-data-files"] = strconv.Itoa(int(tf) + len(files))
				summary["total-files-size"] = strconv.FormatInt(ts+totalBytes, 10)
				break
			}
		}
	} else {
		summary["total-records"] = summary["added-records"]
		summary["total-data-files"] = summary["added-data-files"]
		summary["total-files-size"] = summary["added-files-size"]
	}

	newSnapshot := IcebergSnapshot{
		SnapshotID:       newSnapshotID,
		ParentSnapshotID: parentID,
		SequenceNumber:   seqNum,
		TimestampMs:      now,
		Summary:          summary,
		SchemaID:         &t.metadata.CurrentSchemaID,
		ManifestList:     manifestListPath,
	}

	// Update metadata
	newMetadata := t.metadata
	newMetadata.LastUpdatedMs = now
	newMetadata.CurrentSnapshotID = &newSnapshotID
	newMetadata.Snapshots = append(newMetadata.Snapshots, newSnapshot)
	newMetadata.SnapshotLog = append(newMetadata.SnapshotLog, SnapshotLogEntry{
		TimestampMs: now,
		SnapshotID:  newSnapshotID,
	})

	err = t.catalog.writeMetadata(ctx, t.identifier, t.version+1, newMetadata)
	if err != nil {
		return nil, err
	}

	t.metadata = newMetadata
	t.version++

	return snapshotFromIceberg(newSnapshot), nil
}

func (t *fileTable) Properties() TableProperties {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.metadata.Properties
}

func (t *fileTable) Location() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.metadata.Location
}

func (t *fileTable) Refresh(ctx context.Context) error {
	newTable, err := t.catalog.LoadTable(ctx, t.identifier)
	if err != nil {
		return err
	}

	ft := newTable.(*fileTable)
	t.mu.Lock()
	t.metadata = ft.metadata
	t.version = ft.version
	t.mu.Unlock()

	return nil
}
