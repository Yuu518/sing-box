//go:build with_utls

package tls

import (
	"testing"

	"github.com/sagernet/sing-box/option"
)

func TestUTLSClientCertificatePin(t *testing.T) {
	t.Parallel()
	testCertificatePin(t, func(options *option.OutboundTLSOptions) {
		options.UTLS = &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "chrome"}
	})
}
