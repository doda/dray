// Package routing implements zone-aware routing, partition affinity, and broker registration.
//
// Broker Registration
//
// Brokers register themselves in Oxia using ephemeral keys that are automatically
// deleted when the broker's session expires. This enables:
//
//   - Service discovery: clients and other brokers can list all live brokers
//   - Failure detection: crashed brokers are automatically removed from the registry
//   - Zone awareness: brokers are tagged with their zone for locality-aware routing
//
// The registration key format is:
//
//	/dray/v1/cluster/<clusterId>/brokers/<brokerId>
//
// The value is a JSON object containing:
//
//	{
//	  "brokerId": "broker-1",
//	  "nodeId": 1,
//	  "zoneId": "us-east-1a",
//	  "advertisedListeners": ["broker-1.example.com:9092"],
//	  "startedAt": 1703721600000,
//	  "buildInfo": {
//	    "version": "0.1.0",
//	    "gitCommit": "abc123",
//	    "buildTime": "2024-01-01T00:00:00Z"
//	  }
//	}
//
// Zone Awareness
//
// Clients can specify their zone in the Kafka client.id field using a comma-separated
// key=value format:
//
//	client.id = "zone_id=us-east-1a,app=myservice"
//
// When a client includes zone_id, the broker filters metadata responses to prefer
// brokers in the same zone, reducing cross-zone traffic and latency.
//
// Uses rendezvous hashing for deterministic broker selection within a zone.
//
// See SPEC.md section 7 for full zone routing specification.
package routing
