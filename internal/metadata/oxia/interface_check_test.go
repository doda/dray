package oxia

import (
	"github.com/dray-io/dray/internal/metadata"
)

// Compile-time check that Store implements MetadataStore
var _ metadata.MetadataStore = (*Store)(nil)
