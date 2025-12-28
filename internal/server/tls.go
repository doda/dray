package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dray-io/dray/internal/logging"
)

// TLSConfig holds TLS configuration for the server.
type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
}

// CertReloader manages TLS certificate reloading without server restart.
// It watches for file changes and atomically swaps the certificate.
type CertReloader struct {
	certFile string
	keyFile  string
	cert     atomic.Pointer[tls.Certificate]
	logger   *logging.Logger
	mu       sync.RWMutex
	lastMod  time.Time
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewCertReloader creates a new CertReloader that manages certificate loading and reloading.
func NewCertReloader(certFile, keyFile string, logger *logging.Logger) (*CertReloader, error) {
	if logger == nil {
		logger = logging.DefaultLogger()
	}

	r := &CertReloader{
		certFile: certFile,
		keyFile:  keyFile,
		logger:   logger,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	if err := r.loadCertificate(); err != nil {
		return nil, fmt.Errorf("failed to load initial certificate: %w", err)
	}

	return r, nil
}

// loadCertificate loads the certificate from disk and stores it atomically.
func (r *CertReloader) loadCertificate() error {
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("failed to load certificate pair: %w", err)
	}

	r.cert.Store(&cert)
	r.logger.Infof("TLS certificate loaded", map[string]any{
		"certFile": r.certFile,
		"keyFile":  r.keyFile,
	})

	return nil
}

// GetCertificate returns the current certificate for use in TLS handshakes.
// This implements the tls.Config GetCertificate callback.
func (r *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	cert := r.cert.Load()
	if cert == nil {
		return nil, errors.New("no certificate loaded")
	}
	return cert, nil
}

// Reload attempts to reload the certificate from disk.
// Returns nil if successful, error otherwise.
func (r *CertReloader) Reload() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldCert := r.cert.Load()
	if err := r.loadCertificate(); err != nil {
		r.logger.Errorf("failed to reload certificate", map[string]any{"error": err.Error()})
		return err
	}

	if oldCert != nil {
		r.logger.Infof("TLS certificate reloaded successfully", nil)
	}
	return nil
}

// StartWatcher starts a background goroutine that watches for certificate file changes.
// It checks file modification times periodically and reloads if changed.
func (r *CertReloader) StartWatcher(checkInterval time.Duration) {
	if checkInterval <= 0 {
		checkInterval = 30 * time.Second
	}

	go func() {
		defer close(r.doneCh)
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-r.stopCh:
				return
			case <-ticker.C:
				if r.shouldReload() {
					if err := r.Reload(); err != nil {
						r.logger.Warnf("certificate reload failed", map[string]any{"error": err.Error()})
					}
				}
			}
		}
	}()

	r.logger.Infof("certificate watcher started", map[string]any{"interval": checkInterval.String()})
}

// shouldReload checks if either certificate file has been modified.
func (r *CertReloader) shouldReload() bool {
	r.mu.RLock()
	lastMod := r.lastMod
	r.mu.RUnlock()

	certInfo, err := os.Stat(r.certFile)
	if err != nil {
		return false
	}

	keyInfo, err := os.Stat(r.keyFile)
	if err != nil {
		return false
	}

	latestMod := certInfo.ModTime()
	if keyInfo.ModTime().After(latestMod) {
		latestMod = keyInfo.ModTime()
	}

	if latestMod.After(lastMod) {
		r.mu.Lock()
		r.lastMod = latestMod
		r.mu.Unlock()
		return true
	}

	return false
}

// Stop stops the certificate watcher goroutine.
func (r *CertReloader) Stop() {
	close(r.stopCh)
	<-r.doneCh
}

// TLSListenerConfig configures a TLS listener.
type TLSListenerConfig struct {
	CertFile string
	KeyFile  string
	MinTLS   uint16
}

// NewTLSListener creates a TLS listener with the given configuration.
// It returns the listener and a CertReloader for hot-reloading certificates.
func NewTLSListener(addr string, tlsCfg TLSConfig, logger *logging.Logger) (net.Listener, *CertReloader, error) {
	if !tlsCfg.Enabled {
		return nil, nil, errors.New("TLS is not enabled")
	}

	if tlsCfg.CertFile == "" || tlsCfg.KeyFile == "" {
		return nil, nil, errors.New("certificate and key files are required")
	}

	reloader, err := NewCertReloader(tlsCfg.CertFile, tlsCfg.KeyFile, logger)
	if err != nil {
		return nil, nil, err
	}

	config := &tls.Config{
		GetCertificate: reloader.GetCertificate,
		MinVersion:     tls.VersionTLS12,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	tlsLn := tls.NewListener(ln, config)
	return tlsLn, reloader, nil
}
