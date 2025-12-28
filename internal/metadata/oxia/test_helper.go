package oxia

import (
	"io"
	"os"
	"sync"
	"testing"

	"github.com/oxia-db/oxia/oxiad/dataserver"
)

// TestServer represents an embedded Oxia standalone server for testing.
type TestServer struct {
	standalone *dataserver.Standalone
	addr       string
	dir        string
}

// Addr returns the service address of the test server.
func (s *TestServer) Addr() string {
	return s.addr
}

// Close shuts down the test server and cleans up resources.
func (s *TestServer) Close() error {
	var err error
	if s.standalone != nil {
		err = s.standalone.Close()
	}
	if s.dir != "" {
		os.RemoveAll(s.dir)
	}
	return err
}

var (
	externalAddr     string
	externalAddrOnce sync.Once
)

// getExternalServiceAddress returns the service address from environment
// variable OXIA_SERVICE_ADDRESS if set, empty string otherwise.
func getExternalServiceAddress() string {
	externalAddrOnce.Do(func() {
		externalAddr = os.Getenv("OXIA_SERVICE_ADDRESS")
	})
	return externalAddr
}

// StartTestServer starts an Oxia standalone server for testing.
// If OXIA_SERVICE_ADDRESS is set, it returns a wrapper around the external server.
// Otherwise, it starts an embedded standalone server.
// The server is automatically closed via t.Cleanup.
func StartTestServer(t *testing.T) *TestServer {
	t.Helper()

	// Check if external server address is set
	if addr := getExternalServiceAddress(); addr != "" {
		t.Logf("Using external Oxia server at %s", addr)
		return &TestServer{addr: addr}
	}

	// Create temporary directory for the standalone server
	dir, err := os.MkdirTemp("", "oxia-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	config := dataserver.NewTestConfig(dir)
	standalone, err := dataserver.NewStandalone(config)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to start Oxia standalone server: %v", err)
	}

	server := &TestServer{
		standalone: standalone,
		addr:       standalone.ServiceAddr(),
		dir:        dir,
	}

	t.Cleanup(func() {
		server.Close()
	})

	t.Logf("Started embedded Oxia server at %s", server.addr)
	return server
}

var _ io.Closer = (*TestServer)(nil)
