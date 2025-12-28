package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// ResourceType represents the type of Kafka resource for ACL.
type ResourceType string

const (
	ResourceTypeUnknown         ResourceType = "UNKNOWN"
	ResourceTypeTopic           ResourceType = "TOPIC"
	ResourceTypeGroup           ResourceType = "GROUP"
	ResourceTypeCluster         ResourceType = "CLUSTER"
	ResourceTypeTransactionalID ResourceType = "TRANSACTIONAL_ID"
	ResourceTypeDelegationToken ResourceType = "DELEGATION_TOKEN"
)

// ParseResourceType parses a resource type string (case-insensitive).
func ParseResourceType(s string) ResourceType {
	switch strings.ToUpper(s) {
	case "TOPIC":
		return ResourceTypeTopic
	case "GROUP":
		return ResourceTypeGroup
	case "CLUSTER":
		return ResourceTypeCluster
	case "TRANSACTIONAL_ID":
		return ResourceTypeTransactionalID
	case "DELEGATION_TOKEN":
		return ResourceTypeDelegationToken
	default:
		return ResourceTypeUnknown
	}
}

// Operation represents a Kafka operation for ACL.
type Operation string

const (
	OperationUnknown         Operation = "UNKNOWN"
	OperationAll             Operation = "ALL"
	OperationRead            Operation = "READ"
	OperationWrite           Operation = "WRITE"
	OperationCreate          Operation = "CREATE"
	OperationDelete          Operation = "DELETE"
	OperationAlter           Operation = "ALTER"
	OperationDescribe        Operation = "DESCRIBE"
	OperationClusterAction   Operation = "CLUSTER_ACTION"
	OperationDescribeConfigs Operation = "DESCRIBE_CONFIGS"
	OperationAlterConfigs    Operation = "ALTER_CONFIGS"
	OperationIdempotentWrite Operation = "IDEMPOTENT_WRITE"
)

// ParseOperation parses an operation string (case-insensitive).
func ParseOperation(s string) Operation {
	switch strings.ToUpper(s) {
	case "ALL":
		return OperationAll
	case "READ":
		return OperationRead
	case "WRITE":
		return OperationWrite
	case "CREATE":
		return OperationCreate
	case "DELETE":
		return OperationDelete
	case "ALTER":
		return OperationAlter
	case "DESCRIBE":
		return OperationDescribe
	case "CLUSTER_ACTION":
		return OperationClusterAction
	case "DESCRIBE_CONFIGS":
		return OperationDescribeConfigs
	case "ALTER_CONFIGS":
		return OperationAlterConfigs
	case "IDEMPOTENT_WRITE":
		return OperationIdempotentWrite
	default:
		return OperationUnknown
	}
}

// Permission represents the permission type (allow/deny).
type Permission string

const (
	PermissionUnknown Permission = "UNKNOWN"
	PermissionAllow   Permission = "ALLOW"
	PermissionDeny    Permission = "DENY"
)

// ParsePermission parses a permission string (case-insensitive).
func ParsePermission(s string) Permission {
	switch strings.ToUpper(s) {
	case "ALLOW":
		return PermissionAllow
	case "DENY":
		return PermissionDeny
	default:
		return PermissionUnknown
	}
}

// PatternType represents the resource pattern type.
type PatternType string

const (
	PatternTypeUnknown  PatternType = "UNKNOWN"
	PatternTypeLiteral  PatternType = "LITERAL"
	PatternTypePrefixed PatternType = "PREFIXED"
)

// ParsePatternType parses a pattern type string (case-insensitive).
func ParsePatternType(s string) PatternType {
	switch strings.ToUpper(s) {
	case "LITERAL":
		return PatternTypeLiteral
	case "PREFIXED":
		return PatternTypePrefixed
	default:
		return PatternTypeUnknown
	}
}

// ACLEntry represents an Access Control List entry stored in Oxia.
type ACLEntry struct {
	// ResourceType is the type of Kafka resource (TOPIC, GROUP, CLUSTER, etc.).
	ResourceType ResourceType `json:"resourceType"`
	// ResourceName is the name of the resource (topic name, group ID, etc.).
	ResourceName string `json:"resourceName"`
	// PatternType indicates if this is LITERAL or PREFIXED match.
	PatternType PatternType `json:"patternType"`
	// Principal is the user principal (e.g., "User:alice", "User:*").
	Principal string `json:"principal"`
	// Host is the client host pattern (e.g., "*" for any host).
	Host string `json:"host"`
	// Operation is the allowed/denied operation.
	Operation Operation `json:"operation"`
	// Permission is ALLOW or DENY.
	Permission Permission `json:"permission"`
	// CreatedAtMs is when this ACL was created.
	CreatedAtMs int64 `json:"createdAtMs"`
}

// Common errors for ACL operations.
var (
	ErrACLNotFound        = errors.New("acl: not found")
	ErrACLAlreadyExists   = errors.New("acl: already exists")
	ErrInvalidResourceType = errors.New("acl: invalid resource type")
	ErrInvalidOperation   = errors.New("acl: invalid operation")
	ErrInvalidPermission  = errors.New("acl: invalid permission")
	ErrInvalidPrincipal   = errors.New("acl: invalid principal")
	ErrInvalidPatternType = errors.New("acl: invalid pattern type")
)

// ACLStore provides ACL storage operations backed by MetadataStore.
type ACLStore struct {
	meta metadata.MetadataStore
}

// NewACLStore creates a new ACL store.
func NewACLStore(meta metadata.MetadataStore) *ACLStore {
	return &ACLStore{meta: meta}
}

// aclKeyPath returns the full key path for an ACL entry.
// Format: /dray/v1/acls/<resourceType>/<resourceName>/<patternType>/<principal>/<operation>/<permission>/<host>
// We include pattern type, operation, permission, and host for uniqueness.
func aclKeyPath(entry *ACLEntry) string {
	return keys.ACLKeyPath(
		string(entry.ResourceType),
		entry.ResourceName,
		string(entry.PatternType),
		sanitizeKeyComponent(entry.Principal),
		string(entry.Operation),
		string(entry.Permission),
		sanitizeKeyComponent(entry.Host),
	)
}

// sanitizeKeyComponent replaces problematic characters in key components.
func sanitizeKeyComponent(s string) string {
	return strings.ReplaceAll(s, "/", "_SLASH_")
}

// unsanitizeKeyComponent reverses the sanitization.
func unsanitizeKeyComponent(s string) string {
	return strings.ReplaceAll(s, "_SLASH_", "/")
}

// Validate checks if the ACL entry is valid.
func (e *ACLEntry) Validate() error {
	if e.ResourceType == ResourceTypeUnknown || e.ResourceType == "" {
		return ErrInvalidResourceType
	}
	if e.ResourceName == "" {
		return fmt.Errorf("%w: resource name cannot be empty", ErrInvalidResourceType)
	}
	if e.PatternType == PatternTypeUnknown || e.PatternType == "" {
		return ErrInvalidPatternType
	}
	if e.Principal == "" {
		return ErrInvalidPrincipal
	}
	if e.Operation == OperationUnknown || e.Operation == "" {
		return ErrInvalidOperation
	}
	if e.Permission == PermissionUnknown || e.Permission == "" {
		return ErrInvalidPermission
	}
	if e.Host == "" {
		e.Host = "*"
	}
	return nil
}

// CreateACL creates a new ACL entry.
func (s *ACLStore) CreateACL(ctx context.Context, entry *ACLEntry) error {
	if err := entry.Validate(); err != nil {
		return err
	}

	key := aclKeyPath(entry)
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("acl: marshal: %w", err)
	}

	// Use Put with version 0 to ensure key doesn't exist
	_, err = s.meta.Put(ctx, key, data, metadata.WithExpectedVersion(0))
	if errors.Is(err, metadata.ErrVersionMismatch) {
		return ErrACLAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("acl: create: %w", err)
	}

	return nil
}

// DeleteACL deletes an ACL entry.
func (s *ACLStore) DeleteACL(ctx context.Context, entry *ACLEntry) error {
	if err := entry.Validate(); err != nil {
		return err
	}

	key := aclKeyPath(entry)
	return s.meta.Delete(ctx, key)
}

// GetACL retrieves an ACL entry by its key fields.
func (s *ACLStore) GetACL(ctx context.Context, entry *ACLEntry) (*ACLEntry, error) {
	if err := entry.Validate(); err != nil {
		return nil, err
	}

	key := aclKeyPath(entry)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("acl: get: %w", err)
	}
	if !result.Exists {
		return nil, ErrACLNotFound
	}

	var stored ACLEntry
	if err := json.Unmarshal(result.Value, &stored); err != nil {
		return nil, fmt.Errorf("acl: unmarshal: %w", err)
	}

	return &stored, nil
}

// ACLFilter specifies criteria for listing ACLs.
type ACLFilter struct {
	ResourceType ResourceType
	ResourceName string
	PatternType  PatternType
	Principal    string
	Operation    Operation
	Permission   Permission
	Host         string
}

// Matches checks if an ACL entry matches this filter.
func (f *ACLFilter) Matches(entry *ACLEntry) bool {
	if f.ResourceType != "" && f.ResourceType != ResourceTypeUnknown && f.ResourceType != entry.ResourceType {
		return false
	}
	if f.ResourceName != "" && f.ResourceName != entry.ResourceName {
		return false
	}
	if f.PatternType != "" && f.PatternType != PatternTypeUnknown && f.PatternType != entry.PatternType {
		return false
	}
	if f.Principal != "" && f.Principal != entry.Principal {
		return false
	}
	if f.Operation != "" && f.Operation != OperationUnknown && f.Operation != entry.Operation {
		return false
	}
	if f.Permission != "" && f.Permission != PermissionUnknown && f.Permission != entry.Permission {
		return false
	}
	if f.Host != "" && f.Host != entry.Host {
		return false
	}
	return true
}

// ListACLs retrieves ACLs matching the filter.
func (s *ACLStore) ListACLs(ctx context.Context, filter *ACLFilter) ([]*ACLEntry, error) {
	// Build the most specific prefix we can
	prefix := keys.ACLsPrefix + "/"
	if filter != nil && filter.ResourceType != "" && filter.ResourceType != ResourceTypeUnknown {
		prefix = keys.ACLTypePrefix(string(filter.ResourceType))
		if filter.ResourceName != "" {
			prefix = keys.ACLResourcePrefix(string(filter.ResourceType), filter.ResourceName)
		}
	}

	kvs, err := s.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("acl: list: %w", err)
	}

	var entries []*ACLEntry
	for _, kv := range kvs {
		var entry ACLEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			continue // Skip malformed entries
		}
		if filter != nil && !filter.Matches(&entry) {
			continue
		}
		entries = append(entries, &entry)
	}

	return entries, nil
}

// DeleteACLs deletes all ACLs matching the filter and returns the count.
func (s *ACLStore) DeleteACLs(ctx context.Context, filter *ACLFilter) (int, error) {
	entries, err := s.ListACLs(ctx, filter)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, entry := range entries {
		key := aclKeyPath(entry)
		if err := s.meta.Delete(ctx, key); err != nil {
			continue // Best effort
		}
		count++
	}

	return count, nil
}
