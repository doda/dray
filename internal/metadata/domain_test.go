package metadata

import (
	"testing"

	"github.com/google/uuid"
)

func TestCalculateMetaDomain(t *testing.T) {
	tests := []struct {
		name       string
		streamId   string
		numDomains int
	}{
		{
			name:       "simple stream id",
			streamId:   "stream-1",
			numDomains: 16,
		},
		{
			name:       "uuid stream id",
			streamId:   "550e8400-e29b-41d4-a716-446655440000",
			numDomains: 16,
		},
		{
			name:       "single domain",
			streamId:   "any-stream",
			numDomains: 1,
		},
		{
			name:       "large domain count",
			streamId:   "test",
			numDomains: 256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			domain := CalculateMetaDomain(tt.streamId, tt.numDomains)

			if int(domain) >= tt.numDomains {
				t.Errorf("domain %d >= numDomains %d", domain, tt.numDomains)
			}
		})
	}
}

func TestCalculateMetaDomainConsistency(t *testing.T) {
	numDomains := 16

	streamIds := []string{
		"stream-1",
		"stream-2",
		"550e8400-e29b-41d4-a716-446655440000",
		"topic/partition/0",
		"",
	}

	for _, streamId := range streamIds {
		first := CalculateMetaDomain(streamId, numDomains)

		// Verify same streamId always maps to same domain
		for i := 0; i < 100; i++ {
			domain := CalculateMetaDomain(streamId, numDomains)
			if domain != first {
				t.Errorf("streamId %q mapped to different domains: %d and %d",
					streamId, first, domain)
			}
		}
	}
}

func TestCalculateMetaDomainDistribution(t *testing.T) {
	numDomains := 16
	numStreams := 10000

	counts := make(map[MetaDomain]int)
	for i := 0; i < numStreams; i++ {
		streamId := uuid.New().String()
		domain := CalculateMetaDomain(streamId, numDomains)
		counts[domain]++
	}

	// Check that all domains are used
	if len(counts) != numDomains {
		t.Errorf("expected %d unique domains, got %d", numDomains, len(counts))
	}

	// Check distribution is roughly uniform (within 50% of expected)
	expected := numStreams / numDomains
	minExpected := expected / 2
	maxExpected := expected * 2

	for domain, count := range counts {
		if count < minExpected || count > maxExpected {
			t.Errorf("domain %d has %d streams, expected between %d and %d",
				domain, count, minExpected, maxExpected)
		}
	}
}

func TestCalculateMetaDomainPanicOnInvalidDomains(t *testing.T) {
	tests := []struct {
		name       string
		numDomains int
	}{
		{"zero domains", 0},
		{"negative domains", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for numDomains=%d", tt.numDomains)
				}
			}()
			CalculateMetaDomain("test", tt.numDomains)
		})
	}
}

func TestDomainCalculator(t *testing.T) {
	numDomains := 32
	calc := NewDomainCalculator(numDomains)

	if calc.NumDomains() != numDomains {
		t.Errorf("expected NumDomains() = %d, got %d", numDomains, calc.NumDomains())
	}

	streamId := "test-stream"
	expected := CalculateMetaDomain(streamId, numDomains)
	actual := calc.Calculate(streamId)

	if actual != expected {
		t.Errorf("calculator returned %d, expected %d", actual, expected)
	}
}

func TestDomainCalculatorConsistency(t *testing.T) {
	calc := NewDomainCalculator(16)

	streamId := "550e8400-e29b-41d4-a716-446655440000"
	first := calc.Calculate(streamId)

	for i := 0; i < 100; i++ {
		if calc.Calculate(streamId) != first {
			t.Errorf("calculator returned inconsistent results")
		}
	}
}

func TestDomainCalculatorPanicOnInvalidDomains(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for zero domains")
		}
	}()
	NewDomainCalculator(0)
}

func TestMetaDomainRange(t *testing.T) {
	// Verify MetaDomain values are always in valid range for various inputs
	numDomains := 16

	// Test with many random UUIDs
	for i := 0; i < 1000; i++ {
		streamId := uuid.New().String()
		domain := CalculateMetaDomain(streamId, numDomains)

		if domain < 0 || int(domain) >= numDomains {
			t.Errorf("domain %d out of range [0, %d)", domain, numDomains)
		}
	}
}

func TestDifferentNumDomainsYieldsDifferentResults(t *testing.T) {
	streamId := "test-stream"

	domain16 := CalculateMetaDomain(streamId, 16)
	domain32 := CalculateMetaDomain(streamId, 32)

	// With different numDomains, the same streamId may map to different domains
	// (though not necessarily). The key invariant is that results are deterministic.
	t.Logf("domain with 16: %d, domain with 32: %d", domain16, domain32)

	// Both should still be in valid range
	if domain16 >= 16 {
		t.Errorf("domain16 %d >= 16", domain16)
	}
	if domain32 >= 32 {
		t.Errorf("domain32 %d >= 32", domain32)
	}
}

func BenchmarkCalculateMetaDomain(b *testing.B) {
	streamId := "550e8400-e29b-41d4-a716-446655440000"
	numDomains := 16

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculateMetaDomain(streamId, numDomains)
	}
}

func BenchmarkDomainCalculator(b *testing.B) {
	calc := NewDomainCalculator(16)
	streamId := "550e8400-e29b-41d4-a716-446655440000"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		calc.Calculate(streamId)
	}
}
