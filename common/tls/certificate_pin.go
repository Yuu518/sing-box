package tls

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"strings"
	"time"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func parseCertificatePinSHA256(options option.OutboundTLSOptions) ([]byte, error) {
	if options.CertificatePinSHA256 == "" {
		return nil, nil
	}
	if len(options.CertificateSHA256) > 0 || len(options.CertificatePublicKeySHA256) > 0 || len(options.Certificate) > 0 || options.CertificatePath != "" {
		return nil, E.New("certificate_pin_sha256 is conflict with certificate_sha256 or certificate_public_key_sha256 or certificate or certificate_path")
	}
	fingerprint := strings.TrimSpace(strings.ReplaceAll(options.CertificatePinSHA256, ":", ""))
	pin, err := hex.DecodeString(fingerprint)
	if err != nil {
		return nil, E.Cause(err, "decode fingerprint string")
	}
	if len(pin) != sha256.Size {
		return nil, E.New("fingerprint string length error, need sha256 fingerprint")
	}
	return pin, nil
}

// certificatePinSHA256Verifier is installed as VerifyPeerCertificate, which disable_sni never replaces,
// and is rebuilt in SetServerName so chain validation follows the current server name.
func certificatePinSHA256Verifier(pin []byte, serverName string, timeFunc func() time.Time) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		return verifyCertificatePinSHA256(pin, rawCerts, serverName, timeFunc)
	}
}

// verifyCertificatePinSHA256 accepts the peer when any certificate in its chain matches the pin.
// A pinned intermediate or root must also anchor a valid chain for the leaf.
func verifyCertificatePinSHA256(pin []byte, rawCerts [][]byte, serverName string, timeFunc func() time.Time) error {
	for i, rawCert := range rawCerts {
		hash := sha256.Sum256(rawCert)
		if !bytes.Equal(pin, hash[:]) {
			continue
		}
		if i == 0 {
			return nil
		}
		certs := make([]*x509.Certificate, i+1)
		for j := range certs {
			cert, err := x509.ParseCertificate(rawCerts[j])
			if err != nil {
				return E.Cause(err, "parse peer certificate")
			}
			certs[j] = cert
		}
		opts := x509.VerifyOptions{
			Roots:         x509.NewCertPool(),
			Intermediates: x509.NewCertPool(),
			DNSName:       serverName,
		}
		if timeFunc != nil {
			opts.CurrentTime = timeFunc()
		}
		opts.Roots.AddCert(certs[i])
		for _, cert := range certs[1 : i+1] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := certs[0].Verify(opts)
		return err
	}
	return E.New("certificate fingerprint mismatch")
}
