package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/stretchr/testify/require"
)

// generateTestPEM creates a self-signed PEM file with cert+key for testing.
func generateTestPEM(t *testing.T, dir string) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	path := filepath.Join(dir, "test.pem")
	require.NoError(t, os.WriteFile(path, append(certPEM, keyPEM...), 0600))
	return path
}

func TestServerTLSConfig_Valid(t *testing.T) {
	pemFile := generateTestPEM(t, t.TempDir())

	tlsCfg, err := ServerTLSConfig(config.TLSConfig{Enabled: true, CertFile: pemFile})
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	require.Len(t, tlsCfg.Certificates, 1)
	require.Equal(t, uint16(tls.VersionTLS12), tlsCfg.MinVersion)
}

func TestServerTLSConfig_NoCert(t *testing.T) {
	_, err := ServerTLSConfig(config.TLSConfig{Enabled: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cert_file not set")
}

func TestServerTLSConfig_InvalidCert(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "bad.pem")
	require.NoError(t, os.WriteFile(bad, []byte("not a cert"), 0644))

	_, err := ServerTLSConfig(config.TLSConfig{Enabled: true, CertFile: bad})
	require.Error(t, err)
	require.Contains(t, err.Error(), "loading TLS certificate")
}

func TestClientTLSConfig_NoCA(t *testing.T) {
	tlsCfg, err := ClientTLSConfig("")
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	require.Nil(t, tlsCfg.RootCAs)
	require.Equal(t, uint16(tls.VersionTLS12), tlsCfg.MinVersion)
}

func TestClientTLSConfig_WithCA(t *testing.T) {
	pemFile := generateTestPEM(t, t.TempDir())

	tlsCfg, err := ClientTLSConfig(pemFile)
	require.NoError(t, err)
	require.NotNil(t, tlsCfg.RootCAs)
}

func TestClientTLSConfig_InvalidCA(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "bad.pem")
	require.NoError(t, os.WriteFile(bad, []byte("not a cert"), 0644))

	_, err := ClientTLSConfig(bad)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no valid certificates")
}

func TestClientTLSConfig_MissingCA(t *testing.T) {
	_, err := ClientTLSConfig("/nonexistent/ca.pem")
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading CA file")
}
