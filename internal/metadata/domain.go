package metadata

import (
	"hash/fnv"
)

// MetaDomain represents a metadata shard domain.
// Streams are partitioned across domains for transaction atomicity.
// Each WAL object contains streams from exactly one MetaDomain.
// See SPEC.md section 9.2.
type MetaDomain uint32

// CalculateMetaDomain computes the MetaDomain for a given streamId.
// The formula is: Hash(streamId) % numDomains
//
// The same streamId always maps to the same domain, ensuring consistent
// routing for WAL batching and atomic multi-stream commits.
//
// numDomains must be positive.
func CalculateMetaDomain(streamId string, numDomains int) MetaDomain {
	if numDomains <= 0 {
		panic("metadata: numDomains must be positive")
	}

	h := fnv.New64a()
	h.Write([]byte(streamId))
	hash := h.Sum64()

	return MetaDomain(hash % uint64(numDomains))
}

// DomainCalculator computes MetaDomains using a configured number of domains.
// This wraps CalculateMetaDomain with a fixed numDomains from configuration.
type DomainCalculator struct {
	numDomains int
}

// NewDomainCalculator creates a calculator with the given number of domains.
// numDomains must be positive.
func NewDomainCalculator(numDomains int) *DomainCalculator {
	if numDomains <= 0 {
		panic("metadata: numDomains must be positive")
	}
	return &DomainCalculator{numDomains: numDomains}
}

// Calculate returns the MetaDomain for the given streamId.
func (c *DomainCalculator) Calculate(streamId string) MetaDomain {
	return CalculateMetaDomain(streamId, c.numDomains)
}

// NumDomains returns the configured number of domains.
func (c *DomainCalculator) NumDomains() int {
	return c.numDomains
}
