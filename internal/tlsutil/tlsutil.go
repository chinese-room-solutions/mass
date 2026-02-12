package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/chinese-room-solutions/mass/internal/config"
)

// ServerTLSConfig returns a *tls.Config for the MASS server.
// The PEM file must contain both the certificate and private key.
func ServerTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	if cfg.CertFile == "" {
		return nil, fmt.Errorf("TLS enabled but cert_file not set")
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.CertFile)
	if err != nil {
		return nil, fmt.Errorf("loading TLS certificate: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ClientTLSConfig returns a *tls.Config for agents connecting to MASS.
// If caFile is provided, the CA is added to the root pool (for self-signed certs).
// If caFile is empty, the system root pool is used.
func ClientTLSConfig(caFile string) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("CA file contains no valid certificates")
		}
		tlsCfg.RootCAs = pool
	}

	return tlsCfg, nil
}
