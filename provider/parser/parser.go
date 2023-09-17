package parser

import (
	"context"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

var subscriptionParsers = []func(ctx context.Context, content string) ([]option.Outbound, []option.Endpoint, error){
	ParseBoxSubscription,
	ParseClashSubscription,
	ParseSIP008Subscription,
	ParseRawSubscription,
}

func ParseSubscription(ctx context.Context, content string, overrideDialerOptions *option.OverrideDialerOptions, overrideTLSOptions *option.OverrideTLSOptions, overrideAnyTLSOptions *option.OverrideAnyTLSOptions) ([]option.Outbound, []option.Endpoint, error) {
	var pErr error
	for _, parser := range subscriptionParsers {
		outbounds, endpoints, err := parser(ctx, content)
		if len(outbounds) > 0 || len(endpoints) > 0 {
			return overrideOutbounds(outbounds, overrideDialerOptions, overrideTLSOptions, overrideAnyTLSOptions),
				overrideEndpoints(endpoints, overrideDialerOptions),
				nil
		}
		pErr = E.Errors(pErr, err)
	}
	return nil, nil, E.Cause(pErr, "no servers found")
}

func overrideOutbounds(outbounds []option.Outbound, dialer *option.OverrideDialerOptions, tls *option.OverrideTLSOptions, anyTLS *option.OverrideAnyTLSOptions) []option.Outbound {
	for _, outbound := range outbounds {
		if wrapper, ok := outbound.Options.(option.DialerOptionsWrapper); ok {
			wrapper.ReplaceDialerOptions(overrideDialerOption(wrapper.TakeDialerOptions(), dialer))
		}
		if wrapper, ok := outbound.Options.(option.OutboundTLSOptionsWrapper); ok {
			wrapper.ReplaceOutboundTLSOptions(overrideTLSOption(wrapper.TakeOutboundTLSOptions(), tls))
		}
		if options, ok := outbound.Options.(*option.AnyTLSOutboundOptions); ok && anyTLS != nil {
			applyOverride(&options.ClientMetadata, anyTLS.ClientMetadata)
		}
	}
	return outbounds
}

func overrideEndpoints(endpoints []option.Endpoint, dialer *option.OverrideDialerOptions) []option.Endpoint {
	for _, endpoint := range endpoints {
		if wrapper, ok := endpoint.Options.(option.DialerOptionsWrapper); ok {
			wrapper.ReplaceDialerOptions(overrideDialerOption(wrapper.TakeDialerOptions(), dialer))
		}
	}
	return endpoints
}

func overrideDialerOption(options option.DialerOptions, overrideDialerOptions *option.OverrideDialerOptions) option.DialerOptions {
	if overrideDialerOptions == nil {
		return options
	}
	if overrideDialerOptions.Detour != nil {
		options.Detour = *overrideDialerOptions.Detour
	}
	if overrideDialerOptions.BindInterface != nil {
		options.BindInterface = *overrideDialerOptions.BindInterface
	}
	if overrideDialerOptions.Inet4BindAddress != nil {
		options.Inet4BindAddress = overrideDialerOptions.Inet4BindAddress
	}
	if overrideDialerOptions.Inet6BindAddress != nil {
		options.Inet6BindAddress = overrideDialerOptions.Inet6BindAddress
	}
	if overrideDialerOptions.ProtectPath != nil {
		options.ProtectPath = *overrideDialerOptions.ProtectPath
	}
	if overrideDialerOptions.RoutingMark != nil {
		options.RoutingMark = *overrideDialerOptions.RoutingMark
	}
	if overrideDialerOptions.ReuseAddr != nil {
		options.ReuseAddr = *overrideDialerOptions.ReuseAddr
	}
	if overrideDialerOptions.ConnectTimeout != nil {
		options.ConnectTimeout = *overrideDialerOptions.ConnectTimeout
	}
	if overrideDialerOptions.TCPFastOpen != nil {
		options.TCPFastOpen = *overrideDialerOptions.TCPFastOpen
	}
	if overrideDialerOptions.TCPMultiPath != nil {
		options.TCPMultiPath = *overrideDialerOptions.TCPMultiPath
	}
	if overrideDialerOptions.UDPFragment != nil {
		options.UDPFragment = overrideDialerOptions.UDPFragment
	}
	if overrideDialerOptions.DomainResolver != nil {
		options.DomainResolver = overrideDialerOptions.DomainResolver
	}
	if overrideDialerOptions.NetworkStrategy != nil {
		options.NetworkStrategy = overrideDialerOptions.NetworkStrategy
	}
	if overrideDialerOptions.NetworkType != nil {
		options.NetworkType = *overrideDialerOptions.NetworkType
	}
	if overrideDialerOptions.FallbackNetworkType != nil {
		options.FallbackNetworkType = *overrideDialerOptions.FallbackNetworkType
	}
	if overrideDialerOptions.FallbackDelay != nil {
		options.FallbackDelay = *overrideDialerOptions.FallbackDelay
	}

	//nolint:staticcheck
	if overrideDialerOptions.DomainStrategy != nil {
		options.DomainStrategy = *overrideDialerOptions.DomainStrategy
	}
	return options
}

func applyOverride[T any](value *T, override *T) {
	if override != nil {
		*value = *override
	}
}

func overrideTLSOption(options *option.OutboundTLSOptions, override *option.OverrideTLSOptions) *option.OutboundTLSOptions {
	if override == nil {
		return options
	}
	if override.Enabled != nil && !*override.Enabled {
		return &option.OutboundTLSOptions{}
	}
	if options == nil {
		if override.Enabled == nil || !*override.Enabled {
			return nil
		}
		options = &option.OutboundTLSOptions{}
	}
	result := *options
	applyOverride(&result.Enabled, override.Enabled)
	applyOverride(&result.DisableSNI, override.DisableSNI)
	applyOverride(&result.ServerName, override.ServerName)
	applyOverride(&result.Insecure, override.Insecure)
	applyOverride(&result.ALPN, override.ALPN)
	applyOverride(&result.MinVersion, override.MinVersion)
	applyOverride(&result.MaxVersion, override.MaxVersion)
	applyOverride(&result.CipherSuites, override.CipherSuites)
	applyOverride(&result.CurvePreferences, override.CurvePreferences)
	applyOverride(&result.Certificate, override.Certificate)
	applyOverride(&result.CertificatePath, override.CertificatePath)
	applyOverride(&result.CertificatePublicKeySHA256, override.CertificatePublicKeySHA256)
	applyOverride(&result.ClientCertificate, override.ClientCertificate)
	applyOverride(&result.ClientCertificatePath, override.ClientCertificatePath)
	applyOverride(&result.ClientKey, override.ClientKey)
	applyOverride(&result.ClientKeyPath, override.ClientKeyPath)
	applyOverride(&result.CertificatePinSHA256, override.CertificatePinSHA256)
	applyOverride(&result.Fragment, override.Fragment)
	applyOverride(&result.FragmentFallbackDelay, override.FragmentFallbackDelay)
	applyOverride(&result.RecordFragment, override.RecordFragment)
	applyOverride(&result.KernelTx, override.KernelTx)
	applyOverride(&result.KernelRx, override.KernelRx)
	if override.ECH != nil {
		var nested option.OutboundECHOptions
		if result.ECH != nil {
			nested = *result.ECH
		}
		applyOverride(&nested.Enabled, override.ECH.Enabled)
		applyOverride(&nested.Config, override.ECH.Config)
		applyOverride(&nested.ConfigPath, override.ECH.ConfigPath)
		applyOverride(&nested.PQSignatureSchemesEnabled, override.ECH.PQSignatureSchemesEnabled)
		applyOverride(&nested.DynamicRecordSizingDisabled, override.ECH.DynamicRecordSizingDisabled)
		result.ECH = &nested
	}
	if override.UTLS != nil {
		var nested option.OutboundUTLSOptions
		if result.UTLS != nil {
			nested = *result.UTLS
		}
		applyOverride(&nested.Enabled, override.UTLS.Enabled)
		applyOverride(&nested.Fingerprint, override.UTLS.Fingerprint)
		result.UTLS = &nested
	}
	if override.Reality != nil {
		var nested option.OutboundRealityOptions
		if result.Reality != nil {
			nested = *result.Reality
		}
		applyOverride(&nested.Enabled, override.Reality.Enabled)
		applyOverride(&nested.PublicKey, override.Reality.PublicKey)
		applyOverride(&nested.ShortID, override.Reality.ShortID)
		result.Reality = &nested
	}
	return &result
}
