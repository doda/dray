// Package catalog implements Iceberg catalog clients for Dray's stream-table duality.
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RestCatalogConfig holds configuration for the REST catalog client.
type RestCatalogConfig struct {
	// URI is the base URL of the REST catalog service.
	// Example: "http://localhost:8181"
	URI string

	// Warehouse is the warehouse location for the catalog.
	// Example: "s3://bucket/warehouse"
	Warehouse string

	// Prefix is an optional path prefix for catalog operations.
	// Corresponds to the {prefix} path parameter in the REST API.
	Prefix string

	// Credential is used for authentication.
	// For basic auth: "username:password"
	// For OAuth: client_id:client_secret (used with CredentialType=OAuth)
	Credential string

	// CredentialType specifies the authentication type.
	// Supported: "none", "basic", "oauth", "bearer"
	CredentialType string

	// Token is used for bearer token authentication.
	// If set and CredentialType is "bearer", this token is used directly.
	Token string

	// OAuthServerURI is the OAuth token endpoint.
	// If empty, defaults to {URI}/v1/oauth/tokens (deprecated endpoint).
	OAuthServerURI string

	// OAuthScope is the scope requested for OAuth tokens.
	// If empty, defaults to "catalog".
	OAuthScope string

	// HTTPClient is an optional custom HTTP client.
	// If nil, http.DefaultClient is used.
	HTTPClient *http.Client

	// RequestTimeout is the timeout for individual HTTP requests.
	// If zero, defaults to 30 seconds.
	RequestTimeout time.Duration
}

// RestCatalog implements the Catalog interface using the Iceberg REST catalog API.
type RestCatalog struct {
	config     RestCatalogConfig
	httpClient *http.Client
	baseURL    string

	// OAuth token management
	tokenMu     sync.RWMutex
	accessToken string
	tokenExpiry time.Time
}

// NewRestCatalog creates a new REST catalog client.
func NewRestCatalog(config RestCatalogConfig) (*RestCatalog, error) {
	if config.URI == "" {
		return nil, errors.New("catalog URI is required")
	}

	baseURL := strings.TrimSuffix(config.URI, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}

	client := config.HTTPClient
	if client == nil {
		client = &http.Client{}
	}

	if config.RequestTimeout > 0 {
		client.Timeout = config.RequestTimeout
	} else if client.Timeout == 0 {
		client.Timeout = 30 * time.Second
	}

	catalog := &RestCatalog{
		config:     config,
		httpClient: client,
		baseURL:    baseURL,
	}

	return catalog, nil
}

// buildPath constructs the full API path with optional prefix.
func (c *RestCatalog) buildPath(parts ...string) string {
	base := "/v1"
	if c.config.Prefix != "" {
		base = base + "/" + strings.Trim(c.config.Prefix, "/")
	}
	for _, p := range parts {
		base = base + "/" + url.PathEscape(p)
	}
	return base
}

// namespacePath encodes namespace components for URL path.
func namespacePath(namespace []string) string {
	return strings.Join(namespace, "\x1F") // Unit separator per Iceberg spec
}

// do executes an HTTP request with authentication and error handling.
func (c *RestCatalog) do(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	u := c.baseURL + path

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if err := c.setAuthHeader(ctx, req); err != nil {
		return fmt.Errorf("failed to set auth header: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCatalogUnavailable, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return c.handleErrorResponse(resp.StatusCode, respBody)
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}
	}

	return nil
}

// setAuthHeader sets the appropriate authentication header on the request.
func (c *RestCatalog) setAuthHeader(ctx context.Context, req *http.Request) error {
	switch c.config.CredentialType {
	case "basic":
		parts := strings.SplitN(c.config.Credential, ":", 2)
		if len(parts) != 2 {
			return errors.New("basic auth credential must be in format 'username:password'")
		}
		req.SetBasicAuth(parts[0], parts[1])

	case "bearer":
		if c.config.Token == "" {
			return errors.New("bearer token is required")
		}
		req.Header.Set("Authorization", "Bearer "+c.config.Token)

	case "oauth":
		token, err := c.getOAuthToken(ctx)
		if err != nil {
			return fmt.Errorf("failed to get OAuth token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)

	case "none", "":
		// No authentication
	}

	return nil
}

// getOAuthToken returns a valid OAuth token, refreshing if necessary.
func (c *RestCatalog) getOAuthToken(ctx context.Context) (string, error) {
	c.tokenMu.RLock()
	if c.accessToken != "" && time.Now().Before(c.tokenExpiry) {
		token := c.accessToken
		c.tokenMu.RUnlock()
		return token, nil
	}
	c.tokenMu.RUnlock()

	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	// Double-check after acquiring write lock
	if c.accessToken != "" && time.Now().Before(c.tokenExpiry) {
		return c.accessToken, nil
	}

	return c.refreshOAuthToken(ctx)
}

// refreshOAuthToken fetches a new OAuth token.
func (c *RestCatalog) refreshOAuthToken(ctx context.Context) (string, error) {
	tokenURL := c.config.OAuthServerURI
	if tokenURL == "" {
		tokenURL = c.baseURL + "/v1/oauth/tokens"
	}

	parts := strings.SplitN(c.config.Credential, ":", 2)
	if len(parts) != 2 {
		return "", errors.New("OAuth credential must be in format 'client_id:client_secret'")
	}

	scope := c.config.OAuthScope
	if scope == "" {
		scope = "catalog"
	}

	formData := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {parts[0]},
		"client_secret": {parts[1]},
		"scope":         {scope},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token request returned status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}

	c.accessToken = tokenResp.AccessToken
	// Set expiry with a 30-second buffer
	expiresIn := tokenResp.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600 // Default 1 hour
	}
	c.tokenExpiry = time.Now().Add(time.Duration(expiresIn-30) * time.Second)

	return c.accessToken, nil
}

// handleErrorResponse maps HTTP error responses to catalog errors.
func (c *RestCatalog) handleErrorResponse(statusCode int, body []byte) error {
	var errResp struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    int    `json:"code"`
		} `json:"error"`
	}

	_ = json.Unmarshal(body, &errResp) // Best effort parse

	msg := errResp.Error.Message
	if msg == "" {
		msg = string(body)
	}

	switch statusCode {
	case http.StatusNotFound:
		if strings.Contains(strings.ToLower(msg), "table") {
			return fmt.Errorf("%w: %s", ErrTableNotFound, msg)
		}
		if strings.Contains(strings.ToLower(msg), "snapshot") {
			return fmt.Errorf("%w: %s", ErrSnapshotNotFound, msg)
		}
		return fmt.Errorf("resource not found: %s", msg)

	case http.StatusConflict:
		if strings.Contains(strings.ToLower(msg), "already exists") {
			return fmt.Errorf("%w: %s", ErrTableAlreadyExists, msg)
		}
		return fmt.Errorf("%w: %s", ErrCommitConflict, msg)

	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		return fmt.Errorf("%w: %s", ErrCatalogUnavailable, msg)

	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("authentication failed: %s", msg)

	default:
		return fmt.Errorf("catalog error (status %d): %s", statusCode, msg)
	}
}

// LoadTable loads a table by identifier.
func (c *RestCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	path := c.buildPath("namespaces", namespacePath(identifier.Namespace), "tables", identifier.Name)

	var resp loadTableResponse
	if err := c.do(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}

	return c.parseTableResponse(&resp, identifier), nil
}

// CreateTableIfMissing creates a table if it doesn't exist, or returns the existing table.
func (c *RestCatalog) CreateTableIfMissing(ctx context.Context, identifier TableIdentifier, opts CreateTableOptions) (Table, error) {
	// First try to load the existing table
	table, err := c.LoadTable(ctx, identifier)
	if err == nil {
		return table, nil
	}
	if !errors.Is(err, ErrTableNotFound) {
		return nil, err
	}

	// Table doesn't exist, create it
	path := c.buildPath("namespaces", namespacePath(identifier.Namespace), "tables")

	req := createTableRequest{
		Name:          identifier.Name,
		Schema:        schemaToIceberg(opts.Schema),
		Properties:    opts.Properties,
		Location:      opts.Location,
		StageCreate:   false,
		PartitionSpec: partitionSpecToIceberg(opts.PartitionSpec),
	}

	var resp loadTableResponse
	if err := c.do(ctx, "POST", path, req, &resp); err != nil {
		// Handle race condition: table was created by another process
		if errors.Is(err, ErrTableAlreadyExists) {
			return c.LoadTable(ctx, identifier)
		}
		return nil, err
	}

	return c.parseTableResponse(&resp, identifier), nil
}

// GetCurrentSnapshot returns the current snapshot for a table.
func (c *RestCatalog) GetCurrentSnapshot(ctx context.Context, identifier TableIdentifier) (*Snapshot, error) {
	table, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return table.CurrentSnapshot(ctx)
}

// AppendDataFiles appends data files to a table.
func (c *RestCatalog) AppendDataFiles(ctx context.Context, identifier TableIdentifier, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	table, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return table.AppendFiles(ctx, files, opts)
}

// DropTable drops a table.
func (c *RestCatalog) DropTable(ctx context.Context, identifier TableIdentifier) error {
	path := c.buildPath("namespaces", namespacePath(identifier.Namespace), "tables", identifier.Name)
	return c.do(ctx, "DELETE", path, nil, nil)
}

// ListTables lists all tables in a namespace.
func (c *RestCatalog) ListTables(ctx context.Context, namespace []string) ([]TableIdentifier, error) {
	path := c.buildPath("namespaces", namespacePath(namespace), "tables")

	var resp struct {
		Identifiers []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
	}

	if err := c.do(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}

	result := make([]TableIdentifier, len(resp.Identifiers))
	for i, id := range resp.Identifiers {
		result[i] = TableIdentifier{
			Namespace: id.Namespace,
			Name:      id.Name,
		}
	}

	return result, nil
}

// TableExists checks if a table exists.
func (c *RestCatalog) TableExists(ctx context.Context, identifier TableIdentifier) (bool, error) {
	_, err := c.LoadTable(ctx, identifier)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrTableNotFound) {
		return false, nil
	}
	return false, err
}

// Close releases resources held by the catalog.
func (c *RestCatalog) Close() error {
	return nil
}

// parseTableResponse converts API response to a Table implementation.
func (c *RestCatalog) parseTableResponse(resp *loadTableResponse, identifier TableIdentifier) *restTable {
	return &restTable{
		catalog:    c,
		identifier: identifier,
		metadata:   resp.Metadata,
		config:     resp.Config,
	}
}

// restTable implements the Table interface for REST catalog tables.
type restTable struct {
	catalog    *RestCatalog
	identifier TableIdentifier
	metadata   TableMetadata
	config     map[string]string
	mu         sync.RWMutex
}

func (t *restTable) Identifier() TableIdentifier {
	return t.identifier
}

func (t *restTable) Schema() Schema {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.metadata.Schemas) == 0 {
		return Schema{}
	}

	for _, s := range t.metadata.Schemas {
		if s.SchemaID == t.metadata.CurrentSchemaID {
			return schemaFromIceberg(s)
		}
	}

	return schemaFromIceberg(t.metadata.Schemas[0])
}

func (t *restTable) CurrentSnapshot(ctx context.Context) (*Snapshot, error) {
	t.mu.RLock()
	currentID := t.metadata.CurrentSnapshotID
	snapshots := t.metadata.Snapshots
	t.mu.RUnlock()

	if currentID == nil {
		return nil, ErrSnapshotNotFound
	}

	for _, snap := range snapshots {
		if snap.SnapshotID == *currentID {
			return snapshotFromIceberg(snap), nil
		}
	}

	return nil, ErrSnapshotNotFound
}

func (t *restTable) Snapshots(ctx context.Context) ([]Snapshot, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]Snapshot, len(t.metadata.Snapshots))
	for i, snap := range t.metadata.Snapshots {
		result[i] = *snapshotFromIceberg(snap)
	}

	return result, nil
}

func (t *restTable) AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	if len(files) == 0 {
		return nil, errors.New("no files to append")
	}

	path := t.catalog.buildPath("namespaces", namespacePath(t.identifier.Namespace), "tables", t.identifier.Name)

	// Build requirements
	requirements := []tableRequirement{}

	if opts != nil && opts.ExpectedSnapshotID != nil {
		requirements = append(requirements, tableRequirement{
			Type:       "assert-ref-snapshot-id",
			Ref:        "main",
			SnapshotID: opts.ExpectedSnapshotID,
		})
	}

	// Create a new snapshot
	snapshotID := time.Now().UnixNano()
	timestampMs := time.Now().UnixMilli()

	t.mu.RLock()
	var parentSnapshotID *int64
	seqNum := int64(0)
	schemaID := t.metadata.CurrentSchemaID
	specID := t.metadata.DefaultSpecID
	if t.metadata.CurrentSnapshotID != nil {
		parentSnapshotID = t.metadata.CurrentSnapshotID
		for _, snap := range t.metadata.Snapshots {
			if snap.SnapshotID == *t.metadata.CurrentSnapshotID {
				seqNum = snap.SequenceNumber + 1
				break
			}
		}
	}
	t.mu.RUnlock()

	// Build snapshot summary with data file statistics
	var totalRecords int64
	var totalBytes int64
	for _, f := range files {
		totalRecords += f.RecordCount
		totalBytes += f.FileSizeBytes
	}

	summary := map[string]string{
		"operation":              "append",
		"added-data-files":       strconv.Itoa(len(files)),
		"added-records":          strconv.FormatInt(totalRecords, 10),
		"added-files-size":       strconv.FormatInt(totalBytes, 10),
		"total-data-files":       strconv.Itoa(len(files)),
		"total-records":          strconv.FormatInt(totalRecords, 10),
		"total-files-size":       strconv.FormatInt(totalBytes, 10),
		"total-delete-files":     "0",
		"total-position-deletes": "0",
		"total-equality-deletes": "0",
	}

	if opts != nil && opts.SnapshotProperties != nil {
		for k, v := range opts.SnapshotProperties {
			summary[k] = v
		}
	}

	// Convert DataFiles to API format
	apiDataFiles := make([]IcebergDataFile, len(files))
	for i, f := range files {
		apiDataFiles[i] = dataFileToIceberg(f, specID, seqNum)
	}

	// Build updates using append-files action which includes data files directly
	// This is the server-side append approach per Iceberg REST catalog spec
	updates := []tableUpdate{
		{
			Action:    "append-files",
			DataFiles: apiDataFiles,
		},
	}

	// For servers that require explicit snapshot management, we also include
	// the snapshot update. Most REST catalog servers handle this automatically
	// with append-files, but we include both for compatibility.
	snapshot := IcebergSnapshot{
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      timestampMs,
		Summary:          summary,
		SchemaID:         &schemaID,
	}

	// Try append-files first (modern REST catalog servers)
	req := commitTableRequest{
		Requirements: requirements,
		Updates:      updates,
	}

	var resp loadTableResponse
	err := t.catalog.do(ctx, "POST", path, req, &resp)

	// If append-files isn't supported, fall back to add-snapshot with manifest reference
	if err != nil && strings.Contains(err.Error(), "unknown") {
		// Fallback: Use add-snapshot with the data files embedded in a generated manifest
		// This requires the server to support embedded data files in the snapshot
		updates = []tableUpdate{
			{
				Action:   "add-snapshot",
				Snapshot: &snapshot,
			},
			{
				Action:     "set-snapshot-ref",
				RefName:    "main",
				RefType:    "branch",
				SnapshotID: &snapshotID,
			},
		}

		req = commitTableRequest{
			Requirements: requirements,
			Updates:      updates,
		}

		err = t.catalog.do(ctx, "POST", path, req, &resp)
	}

	if err != nil {
		return nil, err
	}

	// Update local metadata
	t.mu.Lock()
	t.metadata = resp.Metadata
	t.mu.Unlock()

	return t.CurrentSnapshot(ctx)
}

func (t *restTable) Properties() TableProperties {
	t.mu.RLock()
	defer t.mu.RUnlock()

	props := make(TableProperties)
	for k, v := range t.metadata.Properties {
		props[k] = v
	}
	return props
}

func (t *restTable) Location() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.metadata.Location
}

func (t *restTable) Refresh(ctx context.Context) error {
	table, err := t.catalog.LoadTable(ctx, t.identifier)
	if err != nil {
		return err
	}

	rt := table.(*restTable)

	t.mu.Lock()
	t.metadata = rt.metadata
	t.config = rt.config
	t.mu.Unlock()

	return nil
}

// API types for JSON marshaling/unmarshaling

type loadTableResponse struct {
	MetadataLocation string            `json:"metadata-location"`
	Metadata         TableMetadata     `json:"metadata"`
	Config           map[string]string `json:"config,omitempty"`
}

type createTableRequest struct {
	Name          string                `json:"name"`
	Location      string                `json:"location,omitempty"`
	Schema        IcebergSchema         `json:"schema"`
	PartitionSpec *IcebergPartitionSpec `json:"partition-spec,omitempty"`
	WriteOrder    *IcebergSortOrder     `json:"write-order,omitempty"`
	StageCreate   bool                  `json:"stage-create,omitempty"`
	Properties    map[string]string     `json:"properties,omitempty"`
}

type commitTableRequest struct {
	Requirements []tableRequirement `json:"requirements"`
	Updates      []tableUpdate      `json:"updates"`
}

type tableRequirement struct {
	Type       string `json:"type"`
	Ref        string `json:"ref,omitempty"`
	SnapshotID *int64 `json:"snapshot-id,omitempty"`
}

type tableUpdate struct {
	Action     string            `json:"action"`
	Snapshot   *IcebergSnapshot  `json:"snapshot,omitempty"`
	RefName    string            `json:"ref-name,omitempty"`
	RefType    string            `json:"type,omitempty"`
	SnapshotID *int64            `json:"snapshot-id,omitempty"`
	DataFiles  []IcebergDataFile `json:"data-files,omitempty"`
}
