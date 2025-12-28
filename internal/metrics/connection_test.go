package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewConnectionMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConnectionMetricsWithRegistry(reg)

	if m.ActiveConnections == nil {
		t.Fatal("ActiveConnections is nil")
	}
	if m.RequestsTotal == nil {
		t.Fatal("RequestsTotal is nil")
	}
	if m.ErrorsTotal == nil {
		t.Fatal("ErrorsTotal is nil")
	}
}

func TestConnectionMetrics_ActiveConnections(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConnectionMetricsWithRegistry(reg)

	// Initially should be 0
	metric := &dto.Metric{}
	if err := m.ActiveConnections.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Gauge.GetValue(); got != 0 {
		t.Errorf("initial active connections = %f, want 0", got)
	}

	// Open 3 connections
	m.ConnectionOpened()
	m.ConnectionOpened()
	m.ConnectionOpened()

	if err := m.ActiveConnections.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Gauge.GetValue(); got != 3 {
		t.Errorf("active connections after 3 opens = %f, want 3", got)
	}

	// Close 2 connections
	m.ConnectionClosed()
	m.ConnectionClosed()

	if err := m.ActiveConnections.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Gauge.GetValue(); got != 1 {
		t.Errorf("active connections after 2 closes = %f, want 1", got)
	}

	// Close final connection
	m.ConnectionClosed()

	if err := m.ActiveConnections.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Gauge.GetValue(); got != 0 {
		t.Errorf("active connections after all closed = %f, want 0", got)
	}
}

func TestConnectionMetrics_RequestsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConnectionMetricsWithRegistry(reg)

	// Record some requests
	m.RecordRequest("Produce", true)
	m.RecordRequest("Produce", true)
	m.RecordRequest("Produce", false)
	m.RecordRequest("Fetch", true)
	m.RecordRequest("Metadata", true)
	m.RecordRequest("Metadata", false)

	// Check Produce success
	produceSuccessCounter := m.RequestsTotal.WithLabelValues("Produce", StatusSuccess)
	metric := &dto.Metric{}
	if err := produceSuccessCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 2 {
		t.Errorf("Produce success count = %f, want 2", got)
	}

	// Check Produce failure
	produceFailureCounter := m.RequestsTotal.WithLabelValues("Produce", StatusFailure)
	if err := produceFailureCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("Produce failure count = %f, want 1", got)
	}

	// Check Fetch success
	fetchSuccessCounter := m.RequestsTotal.WithLabelValues("Fetch", StatusSuccess)
	if err := fetchSuccessCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("Fetch success count = %f, want 1", got)
	}

	// Check Metadata counts
	metadataSuccessCounter := m.RequestsTotal.WithLabelValues("Metadata", StatusSuccess)
	if err := metadataSuccessCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("Metadata success count = %f, want 1", got)
	}

	metadataFailureCounter := m.RequestsTotal.WithLabelValues("Metadata", StatusFailure)
	if err := metadataFailureCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("Metadata failure count = %f, want 1", got)
	}
}

func TestConnectionMetrics_RecordSuccessAndFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConnectionMetricsWithRegistry(reg)

	m.RecordSuccess("Produce")
	m.RecordSuccess("Produce")
	m.RecordFailure("Produce")
	m.RecordSuccess("Fetch")
	m.RecordSuccess("Fetch")
	m.RecordSuccess("Fetch")

	// Check Produce success
	produceSuccessCounter := m.RequestsTotal.WithLabelValues("Produce", StatusSuccess)
	metric := &dto.Metric{}
	if err := produceSuccessCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 2 {
		t.Errorf("Produce success count = %f, want 2", got)
	}

	// Check Produce failure
	produceFailureCounter := m.RequestsTotal.WithLabelValues("Produce", StatusFailure)
	if err := produceFailureCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("Produce failure count = %f, want 1", got)
	}

	// Check Fetch success
	fetchSuccessCounter := m.RequestsTotal.WithLabelValues("Fetch", StatusSuccess)
	if err := fetchSuccessCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 3 {
		t.Errorf("Fetch success count = %f, want 3", got)
	}
}

func TestConnectionMetrics_ErrorsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConnectionMetricsWithRegistry(reg)

	// Record various errors
	m.RecordError("Produce", "UNKNOWN_TOPIC_OR_PARTITION")
	m.RecordError("Produce", "UNKNOWN_TOPIC_OR_PARTITION")
	m.RecordError("Produce", "REQUEST_TIMED_OUT")
	m.RecordError("Fetch", "OFFSET_OUT_OF_RANGE")
	m.RecordError("JoinGroup", "UNKNOWN_MEMBER_ID")
	m.RecordError("JoinGroup", "UNKNOWN_MEMBER_ID")
	m.RecordError("JoinGroup", "REBALANCE_IN_PROGRESS")

	// Check Produce UNKNOWN_TOPIC_OR_PARTITION errors
	produceUnknownTopicCounter := m.ErrorsTotal.WithLabelValues("Produce", "UNKNOWN_TOPIC_OR_PARTITION")
	metric := &dto.Metric{}
	if err := produceUnknownTopicCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 2 {
		t.Errorf("Produce UNKNOWN_TOPIC_OR_PARTITION error count = %f, want 2", got)
	}

	// Check Produce REQUEST_TIMED_OUT errors
	produceTimeoutCounter := m.ErrorsTotal.WithLabelValues("Produce", "REQUEST_TIMED_OUT")
	if err := produceTimeoutCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("Produce REQUEST_TIMED_OUT error count = %f, want 1", got)
	}

	// Check Fetch OFFSET_OUT_OF_RANGE errors
	fetchOffsetCounter := m.ErrorsTotal.WithLabelValues("Fetch", "OFFSET_OUT_OF_RANGE")
	if err := fetchOffsetCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("Fetch OFFSET_OUT_OF_RANGE error count = %f, want 1", got)
	}

	// Check JoinGroup UNKNOWN_MEMBER_ID errors
	joinGroupUnknownMemberCounter := m.ErrorsTotal.WithLabelValues("JoinGroup", "UNKNOWN_MEMBER_ID")
	if err := joinGroupUnknownMemberCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 2 {
		t.Errorf("JoinGroup UNKNOWN_MEMBER_ID error count = %f, want 2", got)
	}

	// Check JoinGroup REBALANCE_IN_PROGRESS errors
	joinGroupRebalanceCounter := m.ErrorsTotal.WithLabelValues("JoinGroup", "REBALANCE_IN_PROGRESS")
	if err := joinGroupRebalanceCounter.Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	if got := metric.Counter.GetValue(); got != 1 {
		t.Errorf("JoinGroup REBALANCE_IN_PROGRESS error count = %f, want 1", got)
	}
}

func TestConnectionMetrics_MultipleAPITypes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConnectionMetricsWithRegistry(reg)

	// Simulate realistic traffic pattern
	apiCounts := map[string]struct{ success, failure int }{
		"Produce":      {10, 1},
		"Fetch":        {20, 2},
		"Metadata":     {5, 0},
		"ApiVersions":  {3, 0},
		"ListOffsets":  {2, 1},
		"OffsetCommit": {4, 0},
		"OffsetFetch":  {4, 0},
	}

	for api, counts := range apiCounts {
		for i := 0; i < counts.success; i++ {
			m.RecordSuccess(api)
		}
		for i := 0; i < counts.failure; i++ {
			m.RecordFailure(api)
		}
	}

	// Verify all counts
	metric := &dto.Metric{}
	for api, expected := range apiCounts {
		successCounter := m.RequestsTotal.WithLabelValues(api, StatusSuccess)
		if err := successCounter.Write(metric); err != nil {
			t.Fatalf("failed to write success metric for %s: %v", api, err)
		}
		if got := metric.Counter.GetValue(); int(got) != expected.success {
			t.Errorf("%s success count = %f, want %d", api, got, expected.success)
		}

		failureCounter := m.RequestsTotal.WithLabelValues(api, StatusFailure)
		if err := failureCounter.Write(metric); err != nil {
			t.Fatalf("failed to write failure metric for %s: %v", api, err)
		}
		if got := metric.Counter.GetValue(); int(got) != expected.failure {
			t.Errorf("%s failure count = %f, want %d", api, got, expected.failure)
		}
	}
}

func TestConnectionMetrics_DefaultRegistry(t *testing.T) {
	// This test just verifies that NewConnectionMetrics doesn't panic
	// We can't easily test the default registry without side effects
	m := NewConnectionMetrics()

	if m.ActiveConnections == nil {
		t.Fatal("ActiveConnections is nil")
	}
	if m.RequestsTotal == nil {
		t.Fatal("RequestsTotal is nil")
	}
	if m.ErrorsTotal == nil {
		t.Fatal("ErrorsTotal is nil")
	}
}
