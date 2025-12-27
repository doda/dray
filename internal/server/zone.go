package server

import "strings"

// ParseZoneID extracts the zone_id from a Kafka client.id string.
// Per Ursa's spec, the client.id format is:
//
//	zone_id=<zone-id>,key1=value1,key2=value2
//
// Returns the zone_id value, or empty string if not found or invalid.
func ParseZoneID(clientID string) string {
	if clientID == "" {
		return ""
	}

	for _, part := range strings.Split(clientID, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		key, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		if key == "zone_id" {
			return strings.TrimSpace(value)
		}
	}

	return ""
}

// ParseClientIDPairs parses the client.id as comma-separated k=v pairs.
// Returns a map of all key-value pairs found.
func ParseClientIDPairs(clientID string) map[string]string {
	result := make(map[string]string)
	if clientID == "" {
		return result
	}

	for _, part := range strings.Split(clientID, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		key, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" {
			result[key] = value
		}
	}

	return result
}
