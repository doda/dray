package protocol

import (
	"context"
	"testing"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/metadata"
)

func newTestEnforcer(t *testing.T, entries ...*auth.ACLEntry) *auth.Enforcer {
	t.Helper()

	meta := metadata.NewMockStore()
	store := auth.NewACLStore(meta)
	cache := auth.NewACLCache(store, meta)
	ctx := context.Background()

	for _, entry := range entries {
		if err := store.CreateACL(ctx, entry); err != nil {
			t.Fatalf("CreateACL: %v", err)
		}
	}

	if err := cache.Invalidate(ctx); err != nil {
		t.Fatalf("Invalidate ACL cache: %v", err)
	}

	return auth.NewEnforcer(cache, auth.EnforcerConfig{Enabled: true}, nil)
}
