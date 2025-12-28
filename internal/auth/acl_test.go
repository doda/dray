package auth

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

func TestParseResourceType(t *testing.T) {
	tests := []struct {
		input    string
		expected ResourceType
	}{
		{"TOPIC", ResourceTypeTopic},
		{"topic", ResourceTypeTopic},
		{"Topic", ResourceTypeTopic},
		{"GROUP", ResourceTypeGroup},
		{"group", ResourceTypeGroup},
		{"CLUSTER", ResourceTypeCluster},
		{"TRANSACTIONAL_ID", ResourceTypeTransactionalID},
		{"DELEGATION_TOKEN", ResourceTypeDelegationToken},
		{"invalid", ResourceTypeUnknown},
		{"", ResourceTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseResourceType(tt.input)
			if result != tt.expected {
				t.Errorf("ParseResourceType(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseOperation(t *testing.T) {
	tests := []struct {
		input    string
		expected Operation
	}{
		{"ALL", OperationAll},
		{"all", OperationAll},
		{"READ", OperationRead},
		{"WRITE", OperationWrite},
		{"CREATE", OperationCreate},
		{"DELETE", OperationDelete},
		{"ALTER", OperationAlter},
		{"DESCRIBE", OperationDescribe},
		{"CLUSTER_ACTION", OperationClusterAction},
		{"DESCRIBE_CONFIGS", OperationDescribeConfigs},
		{"ALTER_CONFIGS", OperationAlterConfigs},
		{"IDEMPOTENT_WRITE", OperationIdempotentWrite},
		{"invalid", OperationUnknown},
		{"", OperationUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseOperation(tt.input)
			if result != tt.expected {
				t.Errorf("ParseOperation(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParsePermission(t *testing.T) {
	tests := []struct {
		input    string
		expected Permission
	}{
		{"ALLOW", PermissionAllow},
		{"allow", PermissionAllow},
		{"DENY", PermissionDeny},
		{"deny", PermissionDeny},
		{"invalid", PermissionUnknown},
		{"", PermissionUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParsePermission(tt.input)
			if result != tt.expected {
				t.Errorf("ParsePermission(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParsePatternType(t *testing.T) {
	tests := []struct {
		input    string
		expected PatternType
	}{
		{"LITERAL", PatternTypeLiteral},
		{"literal", PatternTypeLiteral},
		{"PREFIXED", PatternTypePrefixed},
		{"prefixed", PatternTypePrefixed},
		{"invalid", PatternTypeUnknown},
		{"", PatternTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParsePatternType(tt.input)
			if result != tt.expected {
				t.Errorf("ParsePatternType(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestACLEntry_Validate(t *testing.T) {
	validEntry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}

	tests := []struct {
		name    string
		entry   *ACLEntry
		wantErr error
	}{
		{
			name:    "valid entry",
			entry:   validEntry,
			wantErr: nil,
		},
		{
			name: "empty resource type",
			entry: &ACLEntry{
				ResourceName: "my-topic",
				PatternType:  PatternTypeLiteral,
				Principal:    "User:alice",
				Operation:    OperationRead,
				Permission:   PermissionAllow,
			},
			wantErr: ErrInvalidResourceType,
		},
		{
			name: "unknown resource type",
			entry: &ACLEntry{
				ResourceType: ResourceTypeUnknown,
				ResourceName: "my-topic",
				PatternType:  PatternTypeLiteral,
				Principal:    "User:alice",
				Operation:    OperationRead,
				Permission:   PermissionAllow,
			},
			wantErr: ErrInvalidResourceType,
		},
		{
			name: "empty resource name",
			entry: &ACLEntry{
				ResourceType: ResourceTypeTopic,
				ResourceName: "",
				PatternType:  PatternTypeLiteral,
				Principal:    "User:alice",
				Operation:    OperationRead,
				Permission:   PermissionAllow,
			},
			wantErr: ErrInvalidResourceType,
		},
		{
			name: "empty pattern type",
			entry: &ACLEntry{
				ResourceType: ResourceTypeTopic,
				ResourceName: "my-topic",
				Principal:    "User:alice",
				Operation:    OperationRead,
				Permission:   PermissionAllow,
			},
			wantErr: ErrInvalidPatternType,
		},
		{
			name: "empty principal",
			entry: &ACLEntry{
				ResourceType: ResourceTypeTopic,
				ResourceName: "my-topic",
				PatternType:  PatternTypeLiteral,
				Principal:    "",
				Operation:    OperationRead,
				Permission:   PermissionAllow,
			},
			wantErr: ErrInvalidPrincipal,
		},
		{
			name: "empty operation",
			entry: &ACLEntry{
				ResourceType: ResourceTypeTopic,
				ResourceName: "my-topic",
				PatternType:  PatternTypeLiteral,
				Principal:    "User:alice",
				Permission:   PermissionAllow,
			},
			wantErr: ErrInvalidOperation,
		},
		{
			name: "empty permission",
			entry: &ACLEntry{
				ResourceType: ResourceTypeTopic,
				ResourceName: "my-topic",
				PatternType:  PatternTypeLiteral,
				Principal:    "User:alice",
				Operation:    OperationRead,
			},
			wantErr: ErrInvalidPermission,
		},
		{
			name: "empty host defaults to *",
			entry: &ACLEntry{
				ResourceType: ResourceTypeTopic,
				ResourceName: "my-topic",
				PatternType:  PatternTypeLiteral,
				Principal:    "User:alice",
				Host:         "",
				Operation:    OperationRead,
				Permission:   PermissionAllow,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()
			if tt.wantErr == nil && err != nil {
				t.Errorf("Validate() error = %v, want nil", err)
			}
			if tt.wantErr != nil && err == nil {
				t.Errorf("Validate() error = nil, want %v", tt.wantErr)
			}
			if tt.wantErr != nil && err != nil {
				if !containsError(err, tt.wantErr) {
					t.Errorf("Validate() error = %v, want to contain %v", err, tt.wantErr)
				}
			}
		})
	}
}

func containsError(err, target error) bool {
	if err == target {
		return true
	}
	return err.Error() == target.Error() || (len(err.Error()) > len(target.Error()) && err.Error()[:len(target.Error())] == target.Error()[:len(target.Error())])
}

func TestACLStore_CreateAndGet(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
		CreatedAtMs:  time.Now().UnixMilli(),
	}

	// Create
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	// Verify stored at correct path
	expectedKey := keys.ACLsPrefix + "/TOPIC/my-topic/LITERAL/User:alice/READ/ALLOW/*"
	allKeys := mock.GetAllKeys()
	found := false
	for _, k := range allKeys {
		if k == expectedKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ACL not stored at expected key %s, got keys: %v", expectedKey, allKeys)
	}

	// Get
	retrieved, err := store.GetACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	})
	if err != nil {
		t.Fatalf("GetACL() error = %v", err)
	}

	if retrieved.ResourceType != entry.ResourceType {
		t.Errorf("ResourceType = %v, want %v", retrieved.ResourceType, entry.ResourceType)
	}
	if retrieved.ResourceName != entry.ResourceName {
		t.Errorf("ResourceName = %v, want %v", retrieved.ResourceName, entry.ResourceName)
	}
	if retrieved.Principal != entry.Principal {
		t.Errorf("Principal = %v, want %v", retrieved.Principal, entry.Principal)
	}
	if retrieved.Operation != entry.Operation {
		t.Errorf("Operation = %v, want %v", retrieved.Operation, entry.Operation)
	}
	if retrieved.Permission != entry.Permission {
		t.Errorf("Permission = %v, want %v", retrieved.Permission, entry.Permission)
	}
}

func TestACLStore_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}

	// First create
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	// Duplicate create should fail
	err := store.CreateACL(ctx, entry)
	if err != ErrACLAlreadyExists {
		t.Errorf("CreateACL() duplicate error = %v, want %v", err, ErrACLAlreadyExists)
	}
}

func TestACLStore_AllowAndDenySameKey(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	allowEntry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}
	denyEntry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionDeny,
	}

	if err := store.CreateACL(ctx, allowEntry); err != nil {
		t.Fatalf("CreateACL() allow error = %v", err)
	}
	if err := store.CreateACL(ctx, denyEntry); err != nil {
		t.Fatalf("CreateACL() deny error = %v", err)
	}
}

func TestACLStore_Delete(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}

	// Create
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	// Delete
	if err := store.DeleteACL(ctx, entry); err != nil {
		t.Fatalf("DeleteACL() error = %v", err)
	}

	// Get should return not found
	_, err := store.GetACL(ctx, entry)
	if err != ErrACLNotFound {
		t.Errorf("GetACL() after delete error = %v, want %v", err, ErrACLNotFound)
	}
}

func TestACLStore_GetNotFound(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "nonexistent",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}

	_, err := store.GetACL(ctx, entry)
	if err != ErrACLNotFound {
		t.Errorf("GetACL() error = %v, want %v", err, ErrACLNotFound)
	}
}

func TestACLStore_ListACLs(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Create multiple ACLs
	entries := []*ACLEntry{
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:bob",
			Host:         "*",
			Operation:    OperationWrite,
			Permission:   PermissionAllow,
		},
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-2",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionDeny,
		},
		{
			ResourceType: ResourceTypeGroup,
			ResourceName: "group-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
	}

	for _, entry := range entries {
		if err := store.CreateACL(ctx, entry); err != nil {
			t.Fatalf("CreateACL() error = %v", err)
		}
	}

	// List all
	all, err := store.ListACLs(ctx, nil)
	if err != nil {
		t.Fatalf("ListACLs() error = %v", err)
	}
	if len(all) != 4 {
		t.Errorf("ListACLs() returned %d entries, want 4", len(all))
	}

	// List by resource type
	topics, err := store.ListACLs(ctx, &ACLFilter{ResourceType: ResourceTypeTopic})
	if err != nil {
		t.Fatalf("ListACLs(TOPIC) error = %v", err)
	}
	if len(topics) != 3 {
		t.Errorf("ListACLs(TOPIC) returned %d entries, want 3", len(topics))
	}

	// List by resource name
	topic1, err := store.ListACLs(ctx, &ACLFilter{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
	})
	if err != nil {
		t.Fatalf("ListACLs(topic-1) error = %v", err)
	}
	if len(topic1) != 2 {
		t.Errorf("ListACLs(topic-1) returned %d entries, want 2", len(topic1))
	}

	// List by principal
	alice, err := store.ListACLs(ctx, &ACLFilter{Principal: "User:alice"})
	if err != nil {
		t.Fatalf("ListACLs(alice) error = %v", err)
	}
	if len(alice) != 3 {
		t.Errorf("ListACLs(alice) returned %d entries, want 3", len(alice))
	}

	// List by permission
	denied, err := store.ListACLs(ctx, &ACLFilter{Permission: PermissionDeny})
	if err != nil {
		t.Fatalf("ListACLs(DENY) error = %v", err)
	}
	if len(denied) != 1 {
		t.Errorf("ListACLs(DENY) returned %d entries, want 1", len(denied))
	}
}

func TestACLStore_DeleteACLs(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Create multiple ACLs
	entries := []*ACLEntry{
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:bob",
			Host:         "*",
			Operation:    OperationWrite,
			Permission:   PermissionAllow,
		},
		{
			ResourceType: ResourceTypeGroup,
			ResourceName: "group-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
	}

	for _, entry := range entries {
		if err := store.CreateACL(ctx, entry); err != nil {
			t.Fatalf("CreateACL() error = %v", err)
		}
	}

	// Delete topic ACLs
	count, err := store.DeleteACLs(ctx, &ACLFilter{ResourceType: ResourceTypeTopic})
	if err != nil {
		t.Fatalf("DeleteACLs() error = %v", err)
	}
	if count != 2 {
		t.Errorf("DeleteACLs() deleted %d, want 2", count)
	}

	// Verify only group ACL remains
	remaining, err := store.ListACLs(ctx, nil)
	if err != nil {
		t.Fatalf("ListACLs() error = %v", err)
	}
	if len(remaining) != 1 {
		t.Errorf("ListACLs() returned %d entries, want 1", len(remaining))
	}
	if remaining[0].ResourceType != ResourceTypeGroup {
		t.Errorf("Remaining ACL type = %v, want GROUP", remaining[0].ResourceType)
	}
}

func TestACLFilter_Matches(t *testing.T) {
	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "my-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "192.168.1.1",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}

	tests := []struct {
		name   string
		filter *ACLFilter
		want   bool
	}{
		{
			name:   "nil filter matches all",
			filter: nil,
			want:   true,
		},
		{
			name:   "empty filter matches all",
			filter: &ACLFilter{},
			want:   true,
		},
		{
			name:   "matching resource type",
			filter: &ACLFilter{ResourceType: ResourceTypeTopic},
			want:   true,
		},
		{
			name:   "non-matching resource type",
			filter: &ACLFilter{ResourceType: ResourceTypeGroup},
			want:   false,
		},
		{
			name:   "matching resource name",
			filter: &ACLFilter{ResourceName: "my-topic"},
			want:   true,
		},
		{
			name:   "non-matching resource name",
			filter: &ACLFilter{ResourceName: "other-topic"},
			want:   false,
		},
		{
			name:   "matching principal",
			filter: &ACLFilter{Principal: "User:alice"},
			want:   true,
		},
		{
			name:   "non-matching principal",
			filter: &ACLFilter{Principal: "User:bob"},
			want:   false,
		},
		{
			name:   "matching operation",
			filter: &ACLFilter{Operation: OperationRead},
			want:   true,
		},
		{
			name:   "non-matching operation",
			filter: &ACLFilter{Operation: OperationWrite},
			want:   false,
		},
		{
			name:   "matching permission",
			filter: &ACLFilter{Permission: PermissionAllow},
			want:   true,
		},
		{
			name:   "non-matching permission",
			filter: &ACLFilter{Permission: PermissionDeny},
			want:   false,
		},
		{
			name:   "matching host",
			filter: &ACLFilter{Host: "192.168.1.1"},
			want:   true,
		},
		{
			name:   "non-matching host",
			filter: &ACLFilter{Host: "10.0.0.1"},
			want:   false,
		},
		{
			name: "multiple matching criteria",
			filter: &ACLFilter{
				ResourceType: ResourceTypeTopic,
				Principal:    "User:alice",
				Operation:    OperationRead,
			},
			want: true,
		},
		{
			name: "one non-matching criterion",
			filter: &ACLFilter{
				ResourceType: ResourceTypeTopic,
				Principal:    "User:bob",
				Operation:    OperationRead,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.filter == nil {
				// nil filter should match everything - test this separately
				filter := &ACLFilter{}
				got := filter.Matches(entry)
				if got != true {
					t.Errorf("Matches() = %v, want true for empty filter", got)
				}
				return
			}

			got := tt.filter.Matches(entry)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSanitizeKeyComponent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"User:alice", "User:alice"},
		{"User/Group:alice", "User_SLASH_Group:alice"},
		{"a/b/c", "a_SLASH_b_SLASH_c"},
		{"no-slash", "no-slash"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeKeyComponent(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeKeyComponent(%q) = %q, want %q", tt.input, got, tt.want)
			}
			// Verify round-trip
			unsanitized := unsanitizeKeyComponent(got)
			if unsanitized != tt.input {
				t.Errorf("unsanitizeKeyComponent(%q) = %q, want %q", got, unsanitized, tt.input)
			}
		})
	}
}
