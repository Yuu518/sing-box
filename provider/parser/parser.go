package parser

import (
	"context"
	"slices"
	"strings"

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
	return parseSubscription(ctx, content, false, overrideDialerOptions, overrideTLSOptions, overrideAnyTLSOptions)
}

func ParseRemoteSubscription(ctx context.Context, content string, overrideDialerOptions *option.OverrideDialerOptions, overrideTLSOptions *option.OverrideTLSOptions, overrideAnyTLSOptions *option.OverrideAnyTLSOptions) ([]option.Outbound, []option.Endpoint, error) {
	return parseSubscription(ctx, content, true, overrideDialerOptions, overrideTLSOptions, overrideAnyTLSOptions)
}

func parseSubscription(ctx context.Context, content string, remote bool, overrideDialerOptions *option.OverrideDialerOptions, overrideTLSOptions *option.OverrideTLSOptions, overrideAnyTLSOptions *option.OverrideAnyTLSOptions) ([]option.Outbound, []option.Endpoint, error) {
	var pErr error
	for _, parser := range subscriptionParsers {
		outbounds, endpoints, err := parser(ctx, content)
		if len(outbounds) > 0 || len(endpoints) > 0 {
			if remote {
				sanitizeRemoteOutbounds(outbounds)
				sanitizeRemoteEndpoints(endpoints)
			}
			return overrideOutbounds(outbounds, overrideDialerOptions, overrideTLSOptions, overrideAnyTLSOptions),
				overrideEndpoints(endpoints, overrideDialerOptions, overrideTLSOptions),
				nil
		}
		pErr = E.Errors(pErr, err)
	}
	return nil, nil, E.Cause(pErr, "no servers found")
}

func sanitizeRemoteOutbounds(outbounds []option.Outbound) {
	for _, outbound := range outbounds {
		if wrapper, ok := outbound.Options.(option.DialerOptionsWrapper); ok {
			wrapper.ReplaceDialerOptions(sanitizeRemoteDialerOptions(wrapper.TakeDialerOptions()))
		}
		if wrapper, ok := outbound.Options.(option.OutboundTLSOptionsWrapper); ok {
			wrapper.ReplaceOutboundTLSOptions(sanitizeRemoteTLSOptions(wrapper.TakeOutboundTLSOptions()))
		}
		switch options := outbound.Options.(type) {
		case *option.TorOutboundOptions:
			options.ExecutablePath = ""
			options.DataDirectory = ""
			options.ExtraArgs = nil
			options.Options = nil
		case *option.SSHOutboundOptions:
			options.PrivateKeyPath = ""
		case *option.ShadowsocksOutboundOptions:
			if options.Plugin == "v2ray-plugin" && options.PluginOptions != "" {
				options.PluginOptions = sanitizeV2RayPluginOptions(options.PluginOptions)
			}
		case *option.Hysteria2OutboundOptions:
			if options.Realm != nil && options.Realm.HTTPClient != nil {
				sanitizeRemoteHTTPClientOptions(options.Realm.HTTPClient)
			}
		case *option.TailcatOutboundOptions:
			if options.HTTPClient != nil {
				sanitizeRemoteHTTPClientOptions(options.HTTPClient)
			}
		}
	}
}

func sanitizeRemoteEndpoints(endpoints []option.Endpoint) {
	for _, endpoint := range endpoints {
		if wrapper, ok := endpoint.Options.(option.DialerOptionsWrapper); ok {
			wrapper.ReplaceDialerOptions(sanitizeRemoteDialerOptions(wrapper.TakeDialerOptions()))
		}
		if wrapper, ok := endpoint.Options.(option.OutboundTLSOptionsWrapper); ok {
			wrapper.ReplaceOutboundTLSOptions(sanitizeRemoteTLSOptions(wrapper.TakeOutboundTLSOptions()))
		}
		switch options := endpoint.Options.(type) {
		case *option.OpenConnectEndpointOptions:
			if options.Token != nil {
				options.Token.SecretPath = ""
			}
			if options.CSD != nil {
				options.CSD.WrapperPath = ""
			}
			if options.HIP != nil {
				options.HIP.WrapperPath = ""
			}
			if options.TNCC != nil {
				options.TNCC.WrapperPath = ""
				for i := range options.TNCC.Certificates {
					options.TNCC.Certificates[i].CertificatePath = ""
				}
			}
			options.TLS.CertificateAuthorityPath = ""
			options.TLS.ClientCertificatePath = ""
			options.TLS.ClientKeyPath = ""
			options.TLS.MCACertificatePath = ""
			options.TLS.MCAKeyPath = ""
		case *option.OpenVPNClientEndpointOptions:
			options.StaticKeyPath = ""
			if options.TLS != nil {
				options.TLS.CertificatePath = ""
				options.TLS.ClientCertificatePath = ""
				options.TLS.ClientKeyPath = ""
				options.TLS.CRLPath = ""
				if options.TLS.ControlWrap != nil {
					options.TLS.ControlWrap.KeyPath = ""
				}
			}
		case *option.TailscaleEndpointOptions:
			options.StateDirectory = ""
			options.TaildropDirectory = ""
			options.SSHServer = nil
		}
	}
}

func sanitizeV2RayPluginOptions(pluginOptions string) string {
	var parts []string
	var current strings.Builder
	escaped := false
	for _, r := range pluginOptions {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			current.WriteRune(r)
			escaped = true
			continue
		}
		if r == ';' {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	filtered := parts[:0]
	for _, part := range parts {
		key, _, _ := strings.Cut(part, "=")
		if key == "cert" {
			continue
		}
		filtered = append(filtered, part)
	}
	return strings.Join(filtered, ";")
}

func sanitizeRemoteHTTPClientOptions(options *option.HTTPClientOptions) {
	if options == nil {
		return
	}
	options.DialerOptions = sanitizeRemoteDialerOptions(options.DialerOptions)
	options.TLS = sanitizeRemoteTLSOptions(options.TLS)
}

func sanitizeRemoteDialerOptions(options option.DialerOptions) option.DialerOptions {
	options.ProtectPath = ""
	options.NetNs = ""
	options.BindInterface = ""
	options.RoutingMark = 0
	options.Inet4BindAddress = nil
	options.Inet6BindAddress = nil
	return options
}

func sanitizeRemoteTLSOptions(options *option.OutboundTLSOptions) *option.OutboundTLSOptions {
	if options == nil {
		return nil
	}
	result := *options
	result.CertificatePath = ""
	result.ClientCertificatePath = ""
	result.ClientKeyPath = ""
	if result.ECH != nil {
		ech := *result.ECH
		ech.ConfigPath = ""
		result.ECH = &ech
	}
	return &result
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

func overrideEndpoints(endpoints []option.Endpoint, dialer *option.OverrideDialerOptions, tls *option.OverrideTLSOptions) []option.Endpoint {
	for _, endpoint := range endpoints {
		if wrapper, ok := endpoint.Options.(option.DialerOptionsWrapper); ok {
			wrapper.ReplaceDialerOptions(overrideDialerOption(wrapper.TakeDialerOptions(), dialer))
		}
		if wrapper, ok := endpoint.Options.(option.OutboundTLSOptionsWrapper); ok {
			wrapper.ReplaceOutboundTLSOptions(overrideTLSOption(wrapper.TakeOutboundTLSOptions(), tls))
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
		options.DetourLocalOnly = true
	}
	if overrideDialerOptions.BindInterface != nil {
		options.BindInterface = *overrideDialerOptions.BindInterface
	}
	if overrideDialerOptions.Inet4BindAddress != nil {
		addr := *overrideDialerOptions.Inet4BindAddress
		options.Inet4BindAddress = &addr
	}
	if overrideDialerOptions.Inet6BindAddress != nil {
		addr := *overrideDialerOptions.Inet6BindAddress
		options.Inet6BindAddress = &addr
	}
	if overrideDialerOptions.BindAddressNoPort != nil {
		options.BindAddressNoPort = *overrideDialerOptions.BindAddressNoPort
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
	if overrideDialerOptions.NetNs != nil {
		options.NetNs = *overrideDialerOptions.NetNs
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
	if overrideDialerOptions.DisableTCPKeepAlive != nil {
		options.DisableTCPKeepAlive = *overrideDialerOptions.DisableTCPKeepAlive
	}
	if overrideDialerOptions.TCPKeepAlive != nil {
		options.TCPKeepAlive = *overrideDialerOptions.TCPKeepAlive
	}
	if overrideDialerOptions.TCPKeepAliveInterval != nil {
		options.TCPKeepAliveInterval = *overrideDialerOptions.TCPKeepAliveInterval
	}
	if overrideDialerOptions.UDPFragment != nil {
		udpFragment := *overrideDialerOptions.UDPFragment
		options.UDPFragment = &udpFragment
	}
	if overrideDialerOptions.DomainResolver != nil {
		resolver := *overrideDialerOptions.DomainResolver
		if resolver.RewriteTTL != nil {
			ttl := *resolver.RewriteTTL
			resolver.RewriteTTL = &ttl
		}
		if resolver.ClientSubnet != nil {
			subnet := *resolver.ClientSubnet
			resolver.ClientSubnet = &subnet
		}
		options.DomainResolver = &resolver
	}
	if overrideDialerOptions.NetworkStrategy != nil {
		strategy := *overrideDialerOptions.NetworkStrategy
		options.NetworkStrategy = &strategy
	}
	if overrideDialerOptions.NetworkType != nil {
		options.NetworkType = slices.Clone(*overrideDialerOptions.NetworkType)
	}
	if overrideDialerOptions.FallbackNetworkType != nil {
		options.FallbackNetworkType = slices.Clone(*overrideDialerOptions.FallbackNetworkType)
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
	} else if !options.Enabled && (override.Enabled == nil || !*override.Enabled) {
		return options
	}
	result := *options
	applyOverride(&result.Enabled, override.Enabled)
	applyOverride(&result.Engine, override.Engine)
	applyOverride(&result.DisableSNI, override.DisableSNI)
	applyOverride(&result.ServerName, override.ServerName)
	applyOverride(&result.Insecure, override.Insecure)
	if override.ALPN != nil {
		result.ALPN = slices.Clone(*override.ALPN)
	}
	applyOverride(&result.MinVersion, override.MinVersion)
	applyOverride(&result.MaxVersion, override.MaxVersion)
	if override.CipherSuites != nil {
		result.CipherSuites = slices.Clone(*override.CipherSuites)
	}
	if override.CurvePreferences != nil {
		result.CurvePreferences = slices.Clone(*override.CurvePreferences)
	}
	if override.Certificate != nil || override.CertificatePath != nil {
		result.CertificateSHA256 = nil
		result.CertificatePublicKeySHA256 = nil
		result.CertificatePinSHA256 = ""
	}
	if override.Certificate != nil {
		result.Certificate = slices.Clone(*override.Certificate)
		if override.CertificatePath == nil {
			result.CertificatePath = ""
		}
	}
	if override.CertificatePath != nil {
		result.CertificatePath = *override.CertificatePath
		if override.Certificate == nil {
			result.Certificate = nil
		}
	}
	if override.CertificateSHA256 != nil || override.CertificatePublicKeySHA256 != nil {
		result.Certificate = nil
		result.CertificatePath = ""
		result.CertificatePinSHA256 = ""
	}
	if override.CertificateSHA256 != nil {
		result.CertificateSHA256 = slices.Clone(*override.CertificateSHA256)
	}
	if override.CertificatePublicKeySHA256 != nil {
		result.CertificatePublicKeySHA256 = slices.Clone(*override.CertificatePublicKeySHA256)
	}
	if override.CertificatePinSHA256 != nil {
		if *override.CertificatePinSHA256 != "" {
			result.Certificate = nil
			result.CertificatePath = ""
			result.CertificateSHA256 = nil
			result.CertificatePublicKeySHA256 = nil
		}
		result.CertificatePinSHA256 = *override.CertificatePinSHA256
	}
	if override.ClientCertificate != nil {
		result.ClientCertificate = slices.Clone(*override.ClientCertificate)
	}
	applyOverride(&result.ClientCertificatePath, override.ClientCertificatePath)
	if override.ClientKey != nil {
		result.ClientKey = slices.Clone(*override.ClientKey)
	}
	applyOverride(&result.ClientKeyPath, override.ClientKeyPath)
	applyOverride(&result.Fragment, override.Fragment)
	applyOverride(&result.FragmentFallbackDelay, override.FragmentFallbackDelay)
	applyOverride(&result.RecordFragment, override.RecordFragment)
	applyOverride(&result.Spoof, override.Spoof)
	applyOverride(&result.SpoofMethod, override.SpoofMethod)
	applyOverride(&result.KernelTx, override.KernelTx)
	applyOverride(&result.KernelRx, override.KernelRx)
	applyOverride(&result.HandshakeTimeout, override.HandshakeTimeout)
	if override.ECH != nil {
		var nested option.OutboundECHOptions
		if result.ECH != nil {
			nested = *result.ECH
		}
		applyOverride(&nested.Enabled, override.ECH.Enabled)
		if override.ECH.Config != nil {
			nested.Config = slices.Clone(*override.ECH.Config)
		}
		applyOverride(&nested.ConfigPath, override.ECH.ConfigPath)
		applyOverride(&nested.QueryServerName, override.ECH.QueryServerName)
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
