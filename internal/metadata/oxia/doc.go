// Package oxia implements the MetadataStore interface using Oxia.
//
// Oxia is a distributed metadata store designed for high-performance streaming systems.
// This package wraps the Oxia Go SDK to provide the MetadataStore interface used by Dray.
//
// Usage:
//
//	store, err := oxia.New(ctx, oxia.Config{
//	    ServiceAddress: "localhost:6648",
//	    Namespace:      "dray/my-cluster",
//	})
//	if err != nil {
//	    return err
//	}
//	defer store.Close()
//
//	// Store a value
//	version, err := store.Put(ctx, "/dray/v1/topics/my-topic", data)
//
//	// Retrieve a value
//	result, err := store.Get(ctx, "/dray/v1/topics/my-topic")
//
// Namespace:
//
// Per the Dray specification, each cluster uses a dedicated namespace in Oxia:
// "dray/<cluster_id>". This ensures isolation between clusters sharing an Oxia instance.
//
// Ephemeral Keys:
//
// PutEphemeral creates keys that are automatically deleted when the client session ends.
// This is used for broker registration and other service discovery patterns.
//
// Transactions:
//
// Transactions use Oxia's shard-scoped write batch API to provide atomic multi-key
// updates within a single shard (PartitionKey scope).
//
// Notifications:
//
// The Notifications method returns a stream of change events for cache invalidation
// and other reactive patterns. Once subscribed, all subsequent changes are delivered.
package oxia
