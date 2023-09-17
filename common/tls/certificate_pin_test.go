package tls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
)

type certificatePinTestEngine func(options *option.OutboundTLSOptions)

func formatPinSHA256(hash []byte) string {
	encoded := strings.ToUpper(hex.EncodeToString(hash))
	parts := make([]string, 0, len(encoded)/2)
	for i := 0; i < len(encoded); i += 2 {
		parts = append(parts, encoded[i:i+2])
	}
	return strings.Join(parts, ":")
}

func newCertificatePinTestChain(t *testing.T, serverName string) (stdtls.Certificate, *x509.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pin test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	require.NoError(t, err)
	caCertificate, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: serverName},
		DNSNames:     []string{serverName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCertificate, leafKey.Public(), caKey)
	require.NoError(t, err)
	return stdtls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: leafKey}, caCertificate
}

func dialCertificatePinTestServer(t *testing.T, serverConfig *stdtls.Config, clientOptions option.OutboundTLSOptions, overrideServerName string) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(pinnedCertificateTestTimeout))
		stdtls.Server(conn, serverConfig).Handshake()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), pinnedCertificateTestTimeout)
	defer cancel()
	clientConfig, err := NewClientWithOptions(ClientOptions{
		Context: ctx,
		Logger:  logger.NOP(),
		Options: clientOptions,
	})
	require.NoError(t, err)
	if overrideServerName != "" {
		clientConfig = clientConfig.Clone()
		clientConfig.SetServerName(overrideServerName)
	}
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), pinnedCertificateTestTimeout)
	require.NoError(t, err)
	defer conn.Close()
	tlsConn, err := ClientHandshake(ctx, conn, clientConfig)
	if err == nil {
		tlsConn.Close()
	}
	<-serverDone
	return err
}

func testCertificatePin(t *testing.T, applyEngine certificatePinTestEngine) {
	t.Run("insecure keeps pinning", func(t *testing.T) {
		t.Parallel()
		testInsecureKeepsPinnedCertificate(t, applyEngine)
	})
	t.Run("disable_sni keeps pinning", func(t *testing.T) {
		t.Parallel()
		testDisableSNIKeepsCertificatePin(t, applyEngine)
	})
	t.Run("pinned CA validates chain", func(t *testing.T) {
		t.Parallel()
		testCertificatePinSHA256Chain(t, applyEngine)
	})
}

// insecure only skips CA and hostname validation, every kind of pinning is still enforced.
func testInsecureKeepsPinnedCertificate(t *testing.T, applyEngine certificatePinTestEngine) {
	serverCertificate := newPinnedTestCertificate(t, "localhost")
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{serverCertificate.Certificate}}
	certificateHash := sha256.Sum256(serverCertificate.Leaf.Raw)
	wrongHash := sha256.Sum256([]byte("not the certificate"))
	publicKeyDER, err := x509.MarshalPKIXPublicKey(serverCertificate.Leaf.PublicKey)
	require.NoError(t, err)
	publicKeyHash := sha256.Sum256(publicKeyDER)

	dial := func(options option.OutboundTLSOptions) error {
		options.Enabled = true
		options.ServerName = "localhost"
		options.Insecure = true
		applyEngine(&options)
		return dialCertificatePinTestServer(t, serverConfig, options, "")
	}

	require.NoError(t, dial(option.OutboundTLSOptions{}))
	require.NoError(t, dial(option.OutboundTLSOptions{
		CertificateSHA256: badoption.Listable[[]byte]{certificateHash[:]},
	}))
	require.Error(t, dial(option.OutboundTLSOptions{
		CertificateSHA256: badoption.Listable[[]byte]{wrongHash[:]},
	}))
	require.NoError(t, dial(option.OutboundTLSOptions{
		CertificatePublicKeySHA256: badoption.Listable[[]byte]{publicKeyHash[:]},
	}))
	require.Error(t, dial(option.OutboundTLSOptions{
		CertificatePublicKeySHA256: badoption.Listable[[]byte]{wrongHash[:]},
	}))
	require.NoError(t, dial(option.OutboundTLSOptions{
		CertificatePinSHA256: formatPinSHA256(certificateHash[:]),
	}))
	require.Error(t, dial(option.OutboundTLSOptions{
		CertificatePinSHA256: formatPinSHA256(wrongHash[:]),
	}))
}

// disable_sni must neither drop certificate_pin_sha256 nor add CA validation on top of it.
func testDisableSNIKeepsCertificatePin(t *testing.T, applyEngine certificatePinTestEngine) {
	serverCertificate := newPinnedTestCertificate(t, "localhost")
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{serverCertificate.Certificate}}
	certificateHash := sha256.Sum256(serverCertificate.Leaf.Raw)
	wrongHash := sha256.Sum256([]byte("not the certificate"))

	for _, insecure := range []bool{false, true} {
		for _, overrideServerName := range []string{"", "localhost"} {
			dial := func(pin []byte) error {
				options := option.OutboundTLSOptions{
					Enabled:              true,
					ServerName:           "localhost",
					DisableSNI:           true,
					Insecure:             insecure,
					CertificatePinSHA256: formatPinSHA256(pin),
				}
				applyEngine(&options)
				return dialCertificatePinTestServer(t, serverConfig, options, overrideServerName)
			}
			require.NoError(t, dial(certificateHash[:]), "insecure=%v clone=%v", insecure, overrideServerName != "")
			require.Error(t, dial(wrongHash[:]), "insecure=%v clone=%v", insecure, overrideServerName != "")
		}
	}
}

// A pinned CA must anchor a valid chain for the current server name, including after SetServerName.
func testCertificatePinSHA256Chain(t *testing.T, applyEngine certificatePinTestEngine) {
	serverCertificate, caCertificate := newCertificatePinTestChain(t, "localhost")
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{serverCertificate}}
	caHash := sha256.Sum256(caCertificate.Raw)

	dial := func(serverName string, overrideServerName string) error {
		options := option.OutboundTLSOptions{
			Enabled:              true,
			ServerName:           serverName,
			CertificatePinSHA256: formatPinSHA256(caHash[:]),
		}
		applyEngine(&options)
		return dialCertificatePinTestServer(t, serverConfig, options, overrideServerName)
	}

	require.NoError(t, dial("localhost", ""))
	require.Error(t, dial("other.example", ""))
	require.NoError(t, dial("other.example", "localhost"))
	require.Error(t, dial("localhost", "other.example"))
}

func TestSTDClientCertificatePin(t *testing.T) {
	t.Parallel()
	testCertificatePin(t, func(options *option.OutboundTLSOptions) {})
}
