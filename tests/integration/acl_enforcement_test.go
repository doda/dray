package integration

import (
	"context"
	"testing"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
)

// TestACLEnforcement_TopicAuthorization verifies that ACL enforcement
// returns TOPIC_AUTHORIZATION_FAILED for denied topic access.
func TestACLEnforcement_TopicAuthorization(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()

	// Set up ACL store and cache
	aclStore := auth.NewACLStore(mock)
	aclCache := auth.NewACLCache(aclStore, mock)
	if err := aclCache.Start(ctx); err != nil {
		t.Fatalf("failed to start ACL cache: %v", err)
	}
	defer aclCache.Stop()

	// Create ACL rule: alice can read and write to topic-1
	err := aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to create ACL: %v", err)
	}
	err = aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationWrite,
		Permission:   auth.PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to create ACL: %v", err)
	}
	aclCache.Invalidate(ctx)

	// Create enforcer with ACL enforcement enabled
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := auth.NewEnforcer(aclCache, auth.EnforcerConfig{Enabled: true}, logger)

	// Test cases
	tests := []struct {
		name         string
		principal    string
		topicName    string
		operation    auth.Operation
		expectDenied bool
	}{
		{
			name:         "alice can read topic-1",
			principal:    "User:alice",
			topicName:    "topic-1",
			operation:    auth.OperationRead,
			expectDenied: false,
		},
		{
			name:         "alice can write topic-1",
			principal:    "User:alice",
			topicName:    "topic-1",
			operation:    auth.OperationWrite,
			expectDenied: false,
		},
		{
			name:         "bob cannot read topic-1",
			principal:    "User:bob",
			topicName:    "topic-1",
			operation:    auth.OperationRead,
			expectDenied: true,
		},
		{
			name:         "alice cannot delete topic-1 (no ACL)",
			principal:    "User:alice",
			topicName:    "topic-1",
			operation:    auth.OperationDelete,
			expectDenied: true,
		},
		{
			name:         "anonymous cannot read topic-1",
			principal:    auth.DefaultPrincipal,
			topicName:    "topic-1",
			operation:    auth.OperationRead,
			expectDenied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errCode := enforcer.AuthorizeTopic(ctx, tt.topicName, tt.principal, "*", tt.operation)
			if tt.expectDenied {
				if errCode == nil {
					t.Error("expected authorization to be denied")
				} else if *errCode != auth.ErrCodeTopicAuthorizationFailed {
					t.Errorf("expected error code %d, got %d", auth.ErrCodeTopicAuthorizationFailed, *errCode)
				}
			} else {
				if errCode != nil {
					t.Errorf("expected authorization to pass, got error code %d", *errCode)
				}
			}
		})
	}
}

// TestACLEnforcement_GroupAuthorization verifies that ACL enforcement
// returns GROUP_AUTHORIZATION_FAILED for denied group access.
func TestACLEnforcement_GroupAuthorization(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()

	// Set up ACL store and cache
	aclStore := auth.NewACLStore(mock)
	aclCache := auth.NewACLCache(aclStore, mock)
	if err := aclCache.Start(ctx); err != nil {
		t.Fatalf("failed to start ACL cache: %v", err)
	}
	defer aclCache.Stop()

	// Create ACL rule: consumer can read consumer-group-1
	err := aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeGroup,
		ResourceName: "consumer-group-1",
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:consumer",
		Host:         "*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	})
	if err != nil {
		t.Fatalf("failed to create ACL: %v", err)
	}
	aclCache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := auth.NewEnforcer(aclCache, auth.EnforcerConfig{Enabled: true}, logger)

	// Test cases
	tests := []struct {
		name         string
		principal    string
		groupID      string
		operation    auth.Operation
		expectDenied bool
	}{
		{
			name:         "consumer can read consumer-group-1",
			principal:    "User:consumer",
			groupID:      "consumer-group-1",
			operation:    auth.OperationRead,
			expectDenied: false,
		},
		{
			name:         "other-user cannot read consumer-group-1",
			principal:    "User:other-user",
			groupID:      "consumer-group-1",
			operation:    auth.OperationRead,
			expectDenied: true,
		},
		{
			name:         "consumer cannot read different-group",
			principal:    "User:consumer",
			groupID:      "different-group",
			operation:    auth.OperationRead,
			expectDenied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errCode := enforcer.AuthorizeGroup(ctx, tt.groupID, tt.principal, "*", tt.operation)
			if tt.expectDenied {
				if errCode == nil {
					t.Error("expected authorization to be denied")
				} else if *errCode != auth.ErrCodeGroupAuthorizationFailed {
					t.Errorf("expected error code %d, got %d", auth.ErrCodeGroupAuthorizationFailed, *errCode)
				}
			} else {
				if errCode != nil {
					t.Errorf("expected authorization to pass, got error code %d", *errCode)
				}
			}
		})
	}
}

// TestACLEnforcement_DenyTakesPrecedence verifies that deny rules
// take precedence over allow rules.
func TestACLEnforcement_DenyTakesPrecedence(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()

	aclStore := auth.NewACLStore(mock)
	aclCache := auth.NewACLCache(aclStore, mock)
	if err := aclCache.Start(ctx); err != nil {
		t.Fatalf("failed to start ACL cache: %v", err)
	}
	defer aclCache.Stop()

	// Create allow rule for alice
	aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "sensitive-topic",
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	})

	// Create deny rule for alice (should take precedence)
	aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "sensitive-topic",
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionDeny,
	})

	aclCache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := auth.NewEnforcer(aclCache, auth.EnforcerConfig{Enabled: true}, logger)

	// Deny should take precedence
	errCode := enforcer.AuthorizeTopic(ctx, "sensitive-topic", "User:alice", "*", auth.OperationRead)
	if errCode == nil {
		t.Error("expected deny to take precedence, but access was allowed")
	}
}

// TestACLEnforcement_DisabledAllowsAll verifies that when ACL enforcement
// is disabled, all requests are allowed.
func TestACLEnforcement_DisabledAllowsAll(t *testing.T) {
	ctx := context.Background()

	// Create enforcer with ACL enforcement disabled
	enforcer := auth.NewEnforcer(nil, auth.EnforcerConfig{Enabled: false}, nil)

	// Even without any ACL rules, access should be allowed
	errCode := enforcer.AuthorizeTopic(ctx, "any-topic", "User:anyone", "*", auth.OperationWrite)
	if errCode != nil {
		t.Errorf("expected access to be allowed when enforcement is disabled, got error code %d", *errCode)
	}

	errCode = enforcer.AuthorizeGroup(ctx, "any-group", "User:anyone", "*", auth.OperationRead)
	if errCode != nil {
		t.Errorf("expected access to be allowed when enforcement is disabled, got error code %d", *errCode)
	}
}

// TestACLEnforcement_PrefixedPattern verifies prefixed pattern matching.
func TestACLEnforcement_PrefixedPattern(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()

	aclStore := auth.NewACLStore(mock)
	aclCache := auth.NewACLCache(aclStore, mock)
	if err := aclCache.Start(ctx); err != nil {
		t.Fatalf("failed to start ACL cache: %v", err)
	}
	defer aclCache.Stop()

	// Allow alice to read topics with prefix "logs-"
	aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "logs-",
		PatternType:  auth.PatternTypePrefixed,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	})
	aclCache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := auth.NewEnforcer(aclCache, auth.EnforcerConfig{Enabled: true}, logger)

	tests := []struct {
		topicName    string
		expectDenied bool
	}{
		{"logs-app", false},
		{"logs-system", false},
		{"logs-", false}, // prefix itself matches
		{"metrics-app", true},
		{"app-logs", true}, // doesn't start with prefix
	}

	for _, tt := range tests {
		t.Run(tt.topicName, func(t *testing.T) {
			errCode := enforcer.AuthorizeTopic(ctx, tt.topicName, "User:alice", "*", auth.OperationRead)
			if tt.expectDenied && errCode == nil {
				t.Errorf("expected access to %s to be denied", tt.topicName)
			} else if !tt.expectDenied && errCode != nil {
				t.Errorf("expected access to %s to be allowed", tt.topicName)
			}
		})
	}
}

// TestACLEnforcement_ContextHelpers verifies context helper functions.
func TestACLEnforcement_ContextHelpers(t *testing.T) {
	ctx := context.Background()

	// Test default values
	if got := auth.PrincipalFromContext(ctx); got != auth.DefaultPrincipal {
		t.Errorf("expected default principal %q, got %q", auth.DefaultPrincipal, got)
	}
	if got := auth.HostFromContext(ctx); got != auth.DefaultHost {
		t.Errorf("expected default host %q, got %q", auth.DefaultHost, got)
	}

	// Test setting values
	ctx = auth.WithPrincipal(ctx, "User:alice")
	ctx = auth.WithHost(ctx, "192.168.1.1")

	if got := auth.PrincipalFromContext(ctx); got != "User:alice" {
		t.Errorf("expected principal User:alice, got %q", got)
	}
	if got := auth.HostFromContext(ctx); got != "192.168.1.1" {
		t.Errorf("expected host 192.168.1.1, got %q", got)
	}
}

// TestACLEnforcement_AuthorizeFromCtx verifies the context-based authorization methods.
func TestACLEnforcement_AuthorizeFromCtx(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()

	aclStore := auth.NewACLStore(mock)
	aclCache := auth.NewACLCache(aclStore, mock)
	if err := aclCache.Start(ctx); err != nil {
		t.Fatalf("failed to start ACL cache: %v", err)
	}
	defer aclCache.Stop()

	// Create ACL for alice
	aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	})
	aclCache.Invalidate(ctx)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := auth.NewEnforcer(aclCache, auth.EnforcerConfig{Enabled: true}, logger)

	// Set alice as principal in context
	ctxAlice := auth.WithPrincipal(ctx, "User:alice")
	ctxAlice = auth.WithHost(ctxAlice, "192.168.1.1")

	// Alice should be allowed
	if errCode := enforcer.AuthorizeTopicFromCtx(ctxAlice, "topic-1", auth.OperationRead); errCode != nil {
		t.Errorf("expected alice to be allowed, got error code %d", *errCode)
	}

	// Bob should be denied
	ctxBob := auth.WithPrincipal(ctx, "User:bob")
	if errCode := enforcer.AuthorizeTopicFromCtx(ctxBob, "topic-1", auth.OperationRead); errCode == nil {
		t.Error("expected bob to be denied")
	}

	// Anonymous should be denied
	if errCode := enforcer.AuthorizeTopicFromCtx(ctx, "topic-1", auth.OperationRead); errCode == nil {
		t.Error("expected anonymous to be denied")
	}
}

// TestACLEnforcement_LogsDeniedAccess verifies that denied access is logged.
// This is a basic test that just ensures the logging doesn't panic.
func TestACLEnforcement_LogsDeniedAccess(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()

	aclStore := auth.NewACLStore(mock)
	aclCache := auth.NewACLCache(aclStore, mock)
	if err := aclCache.Start(ctx); err != nil {
		t.Fatalf("failed to start ACL cache: %v", err)
	}
	defer aclCache.Stop()

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError) // Suppress log output in tests
	enforcer := auth.NewEnforcer(aclCache, auth.EnforcerConfig{Enabled: true}, logger)

	// This should log a denied access warning
	_ = enforcer.AuthorizeWithLog(ctx, auth.ResourceTypeTopic, "topic-1", "User:alice", "*", auth.OperationRead)
}

