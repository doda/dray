package auth

import (
	"context"
	"testing"

	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
)

func TestEnforcer_IsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		expected bool
	}{
		{
			name:     "enabled",
			enabled:  true,
			expected: true,
		},
		{
			name:     "disabled",
			enabled:  false,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enforcer := NewEnforcer(nil, EnforcerConfig{Enabled: tt.enabled}, nil)
			if got := enforcer.IsEnabled(); got != tt.expected {
				t.Errorf("IsEnabled() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestEnforcer_Authorize_Disabled(t *testing.T) {
	// When disabled, all authorization should pass
	enforcer := NewEnforcer(nil, EnforcerConfig{Enabled: false}, nil)

	allowed := enforcer.Authorize(context.Background(),
		ResourceTypeTopic, "test-topic", "User:alice", "*", OperationRead)
	if !allowed {
		t.Error("expected authorization to pass when disabled")
	}
}

func TestEnforcer_Authorize_NilCache(t *testing.T) {
	// When cache is nil, authorization should pass
	enforcer := NewEnforcer(nil, EnforcerConfig{Enabled: true}, nil)

	allowed := enforcer.Authorize(context.Background(),
		ResourceTypeTopic, "test-topic", "User:alice", "*", OperationRead)
	if !allowed {
		t.Error("expected authorization to pass with nil cache")
	}
}

func TestEnforcer_Authorize_WithCache(t *testing.T) {
	// Create mock store with ACLs
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	// Start cache
	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Create an allow rule for alice to read topic-1
	err := store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to create ACL: %v", err)
	}

	// Invalidate cache to pick up new ACL
	if err := cache.Invalidate(ctx); err != nil {
		t.Fatalf("failed to invalidate cache: %v", err)
	}

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError) // Suppress logs in tests
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	tests := []struct {
		name         string
		resourceType ResourceType
		resourceName string
		principal    string
		host         string
		operation    Operation
		expected     bool
	}{
		{
			name:         "alice can read topic-1",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:alice",
			host:         "*",
			operation:    OperationRead,
			expected:     true,
		},
		{
			name:         "alice cannot write topic-1 (no rule)",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:alice",
			host:         "*",
			operation:    OperationWrite,
			expected:     false,
		},
		{
			name:         "bob cannot read topic-1 (no rule)",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:bob",
			host:         "*",
			operation:    OperationRead,
			expected:     false,
		},
		{
			name:         "alice cannot read topic-2 (wrong topic)",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-2",
			principal:    "User:alice",
			host:         "*",
			operation:    OperationRead,
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed := enforcer.Authorize(ctx, tt.resourceType, tt.resourceName, tt.principal, tt.host, tt.operation)
			if allowed != tt.expected {
				t.Errorf("Authorize() = %v, want %v", allowed, tt.expected)
			}
		})
	}
}

func TestEnforcer_AuthorizeTopic(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Allow alice to read topic-1
	err := store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to create ACL: %v", err)
	}
	cache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	// Allowed case
	errCode := enforcer.AuthorizeTopic(ctx, "topic-1", "User:alice", "*", OperationRead)
	if errCode != nil {
		t.Errorf("expected nil error code, got %d", *errCode)
	}

	// Denied case
	errCode = enforcer.AuthorizeTopic(ctx, "topic-1", "User:bob", "*", OperationRead)
	if errCode == nil {
		t.Error("expected error code for denied access")
	} else if *errCode != ErrCodeTopicAuthorizationFailed {
		t.Errorf("expected error code %d, got %d", ErrCodeTopicAuthorizationFailed, *errCode)
	}
}

func TestEnforcer_AuthorizeGroup(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Allow alice to read group-1
	err := store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeGroup,
		ResourceName: "group-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to create ACL: %v", err)
	}
	cache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	// Allowed case
	errCode := enforcer.AuthorizeGroup(ctx, "group-1", "User:alice", "*", OperationRead)
	if errCode != nil {
		t.Errorf("expected nil error code, got %d", *errCode)
	}

	// Denied case
	errCode = enforcer.AuthorizeGroup(ctx, "group-1", "User:bob", "*", OperationRead)
	if errCode == nil {
		t.Error("expected error code for denied access")
	} else if *errCode != ErrCodeGroupAuthorizationFailed {
		t.Errorf("expected error code %d, got %d", ErrCodeGroupAuthorizationFailed, *errCode)
	}
}

func TestEnforcer_AuthorizeCluster(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Allow alice to describe cluster
	err := store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeCluster,
		ResourceName: "kafka-cluster",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationDescribe,
		Permission:   PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to create ACL: %v", err)
	}
	cache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	// Allowed case
	errCode := enforcer.AuthorizeCluster(ctx, "User:alice", "*", OperationDescribe)
	if errCode != nil {
		t.Errorf("expected nil error code, got %d", *errCode)
	}

	// Denied case
	errCode = enforcer.AuthorizeCluster(ctx, "User:bob", "*", OperationDescribe)
	if errCode == nil {
		t.Error("expected error code for denied access")
	} else if *errCode != ErrCodeClusterAuthorizationFailed {
		t.Errorf("expected error code %d, got %d", ErrCodeClusterAuthorizationFailed, *errCode)
	}
}

func TestEnforcer_AuthorizeWithLog(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	// This should log a warning (but we can't easily verify that in a unit test)
	allowed := enforcer.AuthorizeWithLog(ctx, ResourceTypeTopic, "topic-1", "User:alice", "*", OperationRead)
	if allowed {
		t.Error("expected authorization to fail (no ACL rules)")
	}
}

func TestNoopAuthorizer(t *testing.T) {
	auth := &NoopAuthorizer{}

	if auth.IsEnabled() {
		t.Error("expected NoopAuthorizer to not be enabled")
	}

	ctx := context.Background()
	if !auth.Authorize(ctx, ResourceTypeTopic, "any-topic", "User:anyone", "*", OperationWrite) {
		t.Error("NoopAuthorizer should always allow")
	}

	if !auth.AuthorizeWithLog(ctx, ResourceTypeGroup, "any-group", "User:anyone", "*", OperationRead) {
		t.Error("NoopAuthorizer should always allow with log")
	}
}

func TestMakePrincipal(t *testing.T) {
	tests := []struct {
		name     string
		username string
		expected string
	}{
		{
			name:     "normal user",
			username: "alice",
			expected: "User:alice",
		},
		{
			name:     "empty username",
			username: "",
			expected: DefaultPrincipal,
		},
		{
			name:     "admin user",
			username: "admin",
			expected: "User:admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakePrincipal(tt.username)
			if got != tt.expected {
				t.Errorf("MakePrincipal(%q) = %q, want %q", tt.username, got, tt.expected)
			}
		})
	}
}

func TestEnforcer_DenyTakesPrecedence(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Allow alice to read topic-1
	store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	})

	// Also deny alice to read topic-1 (deny should take precedence)
	store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionDeny,
	})

	cache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	// Deny should take precedence
	allowed := enforcer.Authorize(ctx, ResourceTypeTopic, "topic-1", "User:alice", "*", OperationRead)
	if allowed {
		t.Error("expected deny to take precedence over allow")
	}
}

func TestEnforcer_PrefixedPattern(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Allow alice to read topics starting with "app-"
	store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "app-",
		PatternType:  PatternTypePrefixed,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	})

	cache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	tests := []struct {
		name     string
		topic    string
		expected bool
	}{
		{"matches prefix", "app-logs", true},
		{"matches prefix exactly", "app-", true},
		{"does not match prefix", "other-topic", false},
		{"similar but different start", "application-logs", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed := enforcer.Authorize(ctx, ResourceTypeTopic, tt.topic, "User:alice", "*", OperationRead)
			if allowed != tt.expected {
				t.Errorf("Authorize(%q) = %v, want %v", tt.topic, allowed, tt.expected)
			}
		})
	}
}

func TestEnforcer_WildcardPrincipal(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Allow all users to read public-topic
	store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "public-topic",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:*",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	})

	cache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	// Any user should be able to read
	users := []string{"User:alice", "User:bob", "User:admin", "User:ANONYMOUS"}
	for _, user := range users {
		allowed := enforcer.Authorize(ctx, ResourceTypeTopic, "public-topic", user, "*", OperationRead)
		if !allowed {
			t.Errorf("expected %s to be allowed to read public-topic", user)
		}
	}
}

func TestEnforcer_OperationAll(t *testing.T) {
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	ctx := context.Background()
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}
	defer cache.Stop()

	// Allow alice ALL operations on topic-1
	store.CreateACL(ctx, &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationAll,
		Permission:   PermissionAllow,
	})

	cache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := NewEnforcer(cache, EnforcerConfig{Enabled: true}, logger)

	// Alice should be able to perform any operation
	operations := []Operation{OperationRead, OperationWrite, OperationDelete, OperationDescribe}
	for _, op := range operations {
		allowed := enforcer.Authorize(ctx, ResourceTypeTopic, "topic-1", "User:alice", "*", op)
		if !allowed {
			t.Errorf("expected alice to be allowed %s on topic-1 with ALL permission", op)
		}
	}
}
