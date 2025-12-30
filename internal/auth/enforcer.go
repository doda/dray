package auth

import (
	"context"
	"net"

	"github.com/dray-io/dray/internal/logging"
)

// Context keys for storing principal and host in request context.
type principalContextKey struct{}
type hostContextKey struct{}

// WithPrincipal returns a new context with the given principal.
func WithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// PrincipalFromContext retrieves the principal from context.
// Returns DefaultPrincipal if not set.
func PrincipalFromContext(ctx context.Context) string {
	if v := ctx.Value(principalContextKey{}); v != nil {
		return v.(string)
	}
	return DefaultPrincipal
}

// WithHost returns a new context with the given client host.
func WithHost(ctx context.Context, host string) context.Context {
	return context.WithValue(ctx, hostContextKey{}, host)
}

// HostFromContext retrieves the client host from context.
// Returns DefaultHost if not set.
func HostFromContext(ctx context.Context) string {
	if v := ctx.Value(hostContextKey{}); v != nil {
		return v.(string)
	}
	return DefaultHost
}

// ExtractHostFromAddr extracts the host portion from a net.Addr.
func ExtractHostFromAddr(addr net.Addr) string {
	if addr == nil {
		return DefaultHost
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// Kafka error codes for authorization failures.
const (
	ErrCodeClusterAuthorizationFailed int16 = 31
	ErrCodeTopicAuthorizationFailed   int16 = 29
	ErrCodeGroupAuthorizationFailed   int16 = 30
)

// Authorizer provides ACL enforcement functionality.
type Authorizer interface {
	// Authorize checks if the given action is allowed.
	// Returns true if allowed, false if denied.
	// If not enabled, always returns true.
	Authorize(ctx context.Context, resourceType ResourceType, resourceName, principal, host string, operation Operation) bool

	// AuthorizeWithLog checks authorization and logs denied attempts.
	// Returns true if allowed, false if denied.
	AuthorizeWithLog(ctx context.Context, resourceType ResourceType, resourceName, principal, host string, operation Operation) bool

	// IsEnabled returns true if ACL enforcement is enabled.
	IsEnabled() bool
}

// Enforcer provides ACL enforcement backed by an ACL cache.
type Enforcer struct {
	cache   *ACLCache
	enabled bool
	logger  *logging.Logger
}

// EnforcerConfig configures the ACL enforcer.
type EnforcerConfig struct {
	// Enabled controls whether ACL enforcement is active.
	// When false, all authorization checks pass.
	Enabled bool
}

// NewEnforcer creates a new ACL enforcer.
func NewEnforcer(cache *ACLCache, cfg EnforcerConfig, logger *logging.Logger) *Enforcer {
	if logger == nil {
		logger = logging.DefaultLogger()
	}
	return &Enforcer{
		cache:   cache,
		enabled: cfg.Enabled,
		logger:  logger,
	}
}

// IsEnabled returns true if ACL enforcement is enabled.
func (e *Enforcer) IsEnabled() bool {
	return e.enabled
}

// Authorize checks if the given action is allowed.
// Returns true if allowed, false if denied.
// If enforcement is not enabled, always returns true.
func (e *Enforcer) Authorize(ctx context.Context, resourceType ResourceType, resourceName, principal, host string, operation Operation) bool {
	if !e.enabled {
		return true
	}
	if e.cache == nil {
		return false
	}
	return e.cache.Authorize(resourceType, resourceName, principal, host, operation)
}

// AuthorizeWithLog checks authorization and logs denied attempts.
// Returns true if allowed, false if denied.
func (e *Enforcer) AuthorizeWithLog(ctx context.Context, resourceType ResourceType, resourceName, principal, host string, operation Operation) bool {
	allowed := e.Authorize(ctx, resourceType, resourceName, principal, host, operation)
	if !allowed {
		e.logDeniedAccess(ctx, resourceType, resourceName, principal, host, operation)
	}
	return allowed
}

// logDeniedAccess logs information about a denied access attempt.
func (e *Enforcer) logDeniedAccess(ctx context.Context, resourceType ResourceType, resourceName, principal, host string, operation Operation) {
	logger := logging.FromCtx(ctx)
	if logger == nil {
		logger = e.logger
	}

	logger.Warnf("ACL denied access", map[string]any{
		"resourceType": string(resourceType),
		"resourceName": resourceName,
		"principal":    principal,
		"host":         host,
		"operation":    string(operation),
	})
}

// AuthorizeTopic checks if the given topic operation is allowed.
// Returns nil if allowed, or the appropriate error code if denied.
func (e *Enforcer) AuthorizeTopic(ctx context.Context, topicName, principal, host string, operation Operation) *int16 {
	if e.AuthorizeWithLog(ctx, ResourceTypeTopic, topicName, principal, host, operation) {
		return nil
	}
	code := ErrCodeTopicAuthorizationFailed
	return &code
}

// AuthorizeGroup checks if the given group operation is allowed.
// Returns nil if allowed, or the appropriate error code if denied.
func (e *Enforcer) AuthorizeGroup(ctx context.Context, groupID, principal, host string, operation Operation) *int16 {
	if e.AuthorizeWithLog(ctx, ResourceTypeGroup, groupID, principal, host, operation) {
		return nil
	}
	code := ErrCodeGroupAuthorizationFailed
	return &code
}

// AuthorizeCluster checks if the given cluster operation is allowed.
// Returns nil if allowed, or the appropriate error code if denied.
func (e *Enforcer) AuthorizeCluster(ctx context.Context, principal, host string, operation Operation) *int16 {
	if e.AuthorizeWithLog(ctx, ResourceTypeCluster, "kafka-cluster", principal, host, operation) {
		return nil
	}
	code := ErrCodeClusterAuthorizationFailed
	return &code
}

// NoopAuthorizer is an authorizer that always allows all operations.
type NoopAuthorizer struct{}

// Authorize always returns true.
func (n *NoopAuthorizer) Authorize(ctx context.Context, resourceType ResourceType, resourceName, principal, host string, operation Operation) bool {
	return true
}

// AuthorizeWithLog always returns true.
func (n *NoopAuthorizer) AuthorizeWithLog(ctx context.Context, resourceType ResourceType, resourceName, principal, host string, operation Operation) bool {
	return true
}

// IsEnabled returns false.
func (n *NoopAuthorizer) IsEnabled() bool {
	return false
}

// DefaultHost is the default host when the client's host is not available.
const DefaultHost = "*"

// DefaultPrincipal is used when no principal is set (anonymous access).
const DefaultPrincipal = "User:ANONYMOUS"

// MakePrincipal creates a principal string from a username.
// Per Kafka convention, principals are prefixed with "User:".
func MakePrincipal(username string) string {
	if username == "" {
		return DefaultPrincipal
	}
	return "User:" + username
}

// AuthorizeTopicFromCtx checks if the given topic operation is allowed.
// It reads principal and host from the context.
// Returns nil if allowed, or the appropriate error code if denied.
func (e *Enforcer) AuthorizeTopicFromCtx(ctx context.Context, topicName string, operation Operation) *int16 {
	return e.AuthorizeTopic(ctx, topicName, PrincipalFromContext(ctx), HostFromContext(ctx), operation)
}

// AuthorizeGroupFromCtx checks if the given group operation is allowed.
// It reads principal and host from the context.
// Returns nil if allowed, or the appropriate error code if denied.
func (e *Enforcer) AuthorizeGroupFromCtx(ctx context.Context, groupID string, operation Operation) *int16 {
	return e.AuthorizeGroup(ctx, groupID, PrincipalFromContext(ctx), HostFromContext(ctx), operation)
}

// AuthorizeClusterFromCtx checks if the given cluster operation is allowed.
// It reads principal and host from the context.
// Returns nil if allowed, or the appropriate error code if denied.
func (e *Enforcer) AuthorizeClusterFromCtx(ctx context.Context, operation Operation) *int16 {
	return e.AuthorizeCluster(ctx, PrincipalFromContext(ctx), HostFromContext(ctx), operation)
}
