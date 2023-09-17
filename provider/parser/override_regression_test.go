package parser

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"

	"github.com/stretchr/testify/require"
)

func TestMalformedBoxSubscription(t *testing.T) {
	for _, content := range []string{`{"outbounds":null}`, `{"outbounds":{}}`, `{"outbounds":[null]}`, `{"outbounds":[5]}`, `{"outbounds":[{"type":123}]}`, `{"outbounds":[{"type":""}]}`} {
		t.Run(content, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, _, err := ParseBoxSubscription(context.Background(), content)
				require.Error(t, err)
			})
		})
	}
}

func TestMalformedSubscriptionFallbacks(t *testing.T) {
	for _, content := range []string{"null", "~", `{"outbounds":null}`, `{"outbounds":[null]}`} {
		t.Run(content, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, _, err := ParseSubscription(context.Background(), content, nil, nil, nil)
				require.Error(t, err)
			})
		})
	}
}

func TestDialerOverridePreservesResolver(t *testing.T) {
	resolver := &option.DomainResolveOptions{Server: "subscription-dns"}
	options := option.DialerOptions{AbstractDialerOptions: option.AbstractDialerOptions{DomainResolver: resolver}}
	updated := overrideDialerOption(options, &option.OverrideDialerOptions{TCPFastOpen: common.Ptr(true)})
	require.Same(t, resolver, updated.DomainResolver)
	require.True(t, updated.TCPFastOpen)
	replacement := &option.DomainResolveOptions{Server: "override-dns"}
	updated = overrideDialerOption(options, &option.OverrideDialerOptions{DomainResolver: replacement})
	require.Same(t, replacement, updated.DomainResolver)
}

func TestTLSOverrideEnablesPlainOutbound(t *testing.T) {
	for _, original := range []*option.OutboundTLSOptions{nil, {Enabled: false}} {
		options := &option.HTTPOutboundOptions{OutboundTLSOptionsContainer: option.OutboundTLSOptionsContainer{TLS: original}}
		overrideOutbounds([]option.Outbound{{Options: options}}, nil, &option.OverrideTLSOptions{Enabled: common.Ptr(true), ServerName: common.Ptr("override.example")}, nil)
		require.NotNil(t, options.TLS)
		require.True(t, options.TLS.Enabled)
		require.Equal(t, "override.example", options.TLS.ServerName)
	}
}

func TestTLSOverridePinsAndNestedFields(t *testing.T) {
	original := &option.OutboundTLSOptions{
		Enabled: true, Insecure: true, ServerName: "original.example",
		UTLS:    &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "chrome"},
		Reality: &option.OutboundRealityOptions{Enabled: true, PublicKey: "key", ShortID: "old"},
	}
	pins := badoption.Listable[[]byte]{make([]byte, 32)}
	updated := overrideTLSOption(original, &option.OverrideTLSOptions{
		Insecure: common.Ptr(false), CertificatePublicKeySHA256: &pins,
		ALPN:                  common.Ptr(badoption.Listable[string]{"h2"}),
		ClientCertificatePath: common.Ptr("client.pem"), ClientKeyPath: common.Ptr("client.key"),
		UTLS:    &option.OverrideUTLSOptions{Enabled: common.Ptr(false)},
		Reality: &option.OverrideRealityOptions{ShortID: common.Ptr("")},
		ECH:     &option.OverrideECHOptions{Enabled: common.Ptr(true), ConfigPath: common.Ptr("ech.pem")},
	})
	require.False(t, updated.Insecure)
	require.Equal(t, pins, updated.CertificatePublicKeySHA256)
	require.Equal(t, badoption.Listable[string]{"h2"}, updated.ALPN)
	require.Equal(t, "client.pem", updated.ClientCertificatePath)
	require.Equal(t, "client.key", updated.ClientKeyPath)
	require.Equal(t, "original.example", updated.ServerName)
	require.False(t, updated.UTLS.Enabled)
	require.Equal(t, "chrome", updated.UTLS.Fingerprint)
	require.Equal(t, "key", updated.Reality.PublicKey)
	require.Empty(t, updated.Reality.ShortID)
	require.True(t, updated.ECH.Enabled)
	require.Equal(t, "ech.pem", updated.ECH.ConfigPath)
	require.True(t, original.UTLS.Enabled)
	require.Equal(t, "old", original.Reality.ShortID)
	require.False(t, overrideTLSOption(updated, &option.OverrideTLSOptions{Enabled: common.Ptr(false)}).Enabled)
}
