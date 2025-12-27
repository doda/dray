package config

import "testing"

func TestDefaultConfig(t *testing.T) {
	cfg := Default()

	if cfg.Broker.ListenAddr != ":9092" {
		t.Errorf("expected default listen addr :9092, got %s", cfg.Broker.ListenAddr)
	}

	if cfg.Metadata.OxiaEndpoint != "localhost:6648" {
		t.Errorf("expected default oxia endpoint localhost:6648, got %s", cfg.Metadata.OxiaEndpoint)
	}

	if cfg.WAL.FlushSizeBytes != 16*1024*1024 {
		t.Errorf("expected default flush size 16MB, got %d", cfg.WAL.FlushSizeBytes)
	}

	if !cfg.Compaction.Enabled {
		t.Error("expected compaction to be enabled by default")
	}

	if !cfg.Iceberg.Enabled {
		t.Error("expected iceberg to be enabled by default")
	}
}
