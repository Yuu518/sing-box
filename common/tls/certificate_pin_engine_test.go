package tls

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

func TestCertificatePinRejectedInUnsupportedEngines(t *testing.T) {
	_, err := ValidateSystemTLSOptions(context.Background(), option.OutboundTLSOptions{
		Enabled:              true,
		CertificatePinSHA256: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	}, "test engine")
	require.ErrorContains(t, err, "certificate_pin_sha256 is unsupported")

	_, err = NewRealityClient(context.Background(), log.NewNOPFactory().NewLogger("tls"), "example.com", option.OutboundTLSOptions{
		Enabled:              true,
		CertificatePinSHA256: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		UTLS:                 &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "chrome"},
		Reality:              &option.OutboundRealityOptions{Enabled: true},
	})
	require.ErrorContains(t, err, "certificate_pin_sha256 is unsupported in reality")
}
