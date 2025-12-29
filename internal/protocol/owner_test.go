package protocol

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/server"
)

type staticLeaderSelector struct {
	leader int32
}

func (s staticLeaderSelector) GetPartitionLeader(_ context.Context, _ string, _ string, _ int32) (int32, error) {
	return s.leader, nil
}

func newLogContext() (context.Context, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	logger := logging.New(logging.Config{
		Level:  logging.LevelDebug,
		Format: logging.FormatText,
		Output: buf,
	})
	ctx := logging.WithLoggerCtx(context.Background(), logger)
	ctx = server.WithZoneID(ctx, "zone-a")
	return ctx, buf
}

func assertLogContains(t *testing.T, buf *bytes.Buffer, msg string) {
	t.Helper()
	if !strings.Contains(buf.String(), msg) {
		t.Fatalf("expected log to contain %q, got %q", msg, buf.String())
	}
}
