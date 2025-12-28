package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/logging"
)

func generateTestCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPath = filepath.Join(dir, "cert.pem")
	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("failed to create cert file: %v", err)
	}
	pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certFile.Close()

	keyPath = filepath.Join(dir, "key.pem")
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("failed to create key file: %v", err)
	}
	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}
	pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
	keyFile.Close()

	return certPath, keyPath
}

func TestCertReloader_Load(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	reloader, err := NewCertReloader(certPath, keyPath, logger)
	if err != nil {
		t.Fatalf("NewCertReloader failed: %v", err)
	}

	cert, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}
	if cert == nil {
		t.Fatal("certificate should not be nil")
	}
}

func TestCertReloader_Reload(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	reloader, err := NewCertReloader(certPath, keyPath, logger)
	if err != nil {
		t.Fatalf("NewCertReloader failed: %v", err)
	}

	cert1, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}

	// Generate a new certificate (same paths)
	generateTestCert(t, dir)

	if err := reloader.Reload(); err != nil {
		t.Fatalf("Reload failed: %v", err)
	}

	cert2, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate after reload failed: %v", err)
	}

	// Certificates should be different (new serial number)
	if cert1 == cert2 {
		t.Error("certificates should be different after reload")
	}
}

func TestCertReloader_InvalidCert(t *testing.T) {
	dir := t.TempDir()

	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	// Write invalid content
	os.WriteFile(certPath, []byte("invalid cert"), 0644)
	os.WriteFile(keyPath, []byte("invalid key"), 0644)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	_, err := NewCertReloader(certPath, keyPath, logger)
	if err == nil {
		t.Error("expected error for invalid certificate")
	}
}

func TestCertReloader_MissingFiles(t *testing.T) {
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	_, err := NewCertReloader("/nonexistent/cert.pem", "/nonexistent/key.pem", logger)
	if err == nil {
		t.Error("expected error for missing files")
	}
}

func TestNewTLSListener(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	tlsCfg := TLSConfig{
		Enabled:  true,
		CertFile: certPath,
		KeyFile:  keyPath,
	}

	ln, reloader, err := NewTLSListener("127.0.0.1:0", tlsCfg, logger)
	if err != nil {
		t.Fatalf("NewTLSListener failed: %v", err)
	}
	defer ln.Close()

	if reloader == nil {
		t.Error("reloader should not be nil")
	}

	if ln.Addr() == nil {
		t.Error("listener should have an address")
	}
}

func TestNewTLSListener_Disabled(t *testing.T) {
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	tlsCfg := TLSConfig{
		Enabled: false,
	}

	_, _, err := NewTLSListener("127.0.0.1:0", tlsCfg, logger)
	if err == nil {
		t.Error("expected error when TLS is disabled")
	}
}

func TestNewTLSListener_MissingCert(t *testing.T) {
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	tlsCfg := TLSConfig{
		Enabled:  true,
		CertFile: "",
		KeyFile:  "",
	}

	_, _, err := NewTLSListener("127.0.0.1:0", tlsCfg, logger)
	if err == nil {
		t.Error("expected error for missing cert files")
	}
}

func TestServerWithTLS(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TLS = TLSConfig{
		Enabled:  true,
		CertFile: certPath,
		KeyFile:  keyPath,
	}

	srv := New(cfg, handler, logger)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	addr := srv.Addr()
	if addr == nil {
		t.Fatal("server should have an address")
	}

	// Connect with TLS
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
	if err != nil {
		t.Fatalf("failed to connect with TLS: %v", err)
	}
	defer conn.Close()

	// Send a request
	request := buildKafkaRequest(18, 0, 12345, "test-client", []byte("hello"))
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	// Read response
	response, err := readKafkaResponse(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if len(response) < 4 {
		t.Fatalf("response too short: %d", len(response))
	}
	correlationID := int32(binary.BigEndian.Uint32(response[:4]))
	if correlationID != 12345 {
		t.Errorf("expected correlation ID 12345, got %d", correlationID)
	}

	// Close server
	if err := srv.Close(); err != nil {
		t.Errorf("failed to close server: %v", err)
	}

	select {
	case err := <-errCh:
		if err != ErrServerClosed {
			t.Errorf("expected ErrServerClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Error("server didn't stop in time")
	}
}

func TestServerTLSCertReload(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TLS = TLSConfig{
		Enabled:  true,
		CertFile: certPath,
		KeyFile:  keyPath,
	}

	srv := New(cfg, handler, logger)

	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	// Connect and verify initial cert works
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	conn1, err := tls.Dial("tcp", srv.Addr().String(), tlsConfig)
	if err != nil {
		t.Fatalf("failed to connect with TLS: %v", err)
	}
	conn1.Close()

	// Regenerate certificate
	generateTestCert(t, dir)

	// Reload certificate
	if err := srv.ReloadCertificate(); err != nil {
		t.Fatalf("failed to reload certificate: %v", err)
	}

	// Connect again - should work with new cert
	conn2, err := tls.Dial("tcp", srv.Addr().String(), tlsConfig)
	if err != nil {
		t.Fatalf("failed to connect after cert reload: %v", err)
	}
	defer conn2.Close()

	// Send a request to verify connection works
	request := buildKafkaRequest(18, 0, 67890, "test-client", nil)
	if _, err := conn2.Write(request); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	response, err := readKafkaResponse(conn2)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	correlationID := int32(binary.BigEndian.Uint32(response[:4]))
	if correlationID != 67890 {
		t.Errorf("expected correlation ID 67890, got %d", correlationID)
	}
}

func TestServerReloadCertificateNoTLS(t *testing.T) {
	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	// TLS not enabled

	srv := New(cfg, handler, logger)

	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	err := srv.ReloadCertificate()
	if err == nil {
		t.Error("expected error when reloading without TLS")
	}
}

func TestServerTLSMultipleConnections(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TLS = TLSConfig{
		Enabled:  true,
		CertFile: certPath,
		KeyFile:  keyPath,
	}

	srv := New(cfg, handler, logger)

	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			conn, err := tls.Dial("tcp", srv.Addr().String(), tlsConfig)
			if err != nil {
				t.Errorf("connection %d failed: %v", id, err)
				done <- false
				return
			}
			defer conn.Close()

			request := buildKafkaRequest(18, 0, int32(id*1000), "test", nil)
			if _, err := conn.Write(request); err != nil {
				t.Errorf("connection %d write failed: %v", id, err)
				done <- false
				return
			}

			response, err := readKafkaResponse(conn)
			if err != nil {
				t.Errorf("connection %d read failed: %v", id, err)
				done <- false
				return
			}

			correlationID := int32(binary.BigEndian.Uint32(response[:4]))
			if correlationID != int32(id*1000) {
				t.Errorf("connection %d: expected %d, got %d", id, id*1000, correlationID)
				done <- false
				return
			}

			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for connections")
		}
	}
}

func TestCertReloader_WatcherStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	reloader, err := NewCertReloader(certPath, keyPath, logger)
	if err != nil {
		t.Fatalf("NewCertReloader failed: %v", err)
	}

	reloader.StartWatcher(100 * time.Millisecond)

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Stop should complete without blocking
	done := make(chan struct{})
	go func() {
		reloader.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good, stopped cleanly
	case <-time.After(time.Second):
		t.Fatal("Stop() blocked for too long")
	}
}
