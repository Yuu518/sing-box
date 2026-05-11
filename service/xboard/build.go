package xboard

import (
	"reflect"
	"strings"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

const (
	tlsModeDisabled = iota
	tlsModeEnabled
	tlsModeReality
)

type inboundBuildOptions struct {
	listen     string
	listenUnix string
	tls        *option.InboundTLSOptions
	transport  *option.V2RayTransportOptions
	machine    bool
	machineTLS *option.InboundTLSOptions
}

func buildInboundOptions(template option.Inbound, listen string, listenUnix string, manualTLS *option.InboundTLSOptions, manualTransport *option.V2RayTransportOptions, config map[string]any, users []xboardUser) (option.Inbound, error) {
	inboundOptions, _, err := buildManagedInboundOptions(template, inboundBuildOptions{
		listen:     listen,
		listenUnix: listenUnix,
		tls:        manualTLS,
		transport:  manualTransport,
	}, config, users)
	return inboundOptions, err
}

func buildManagedInboundOptions(template option.Inbound, build inboundBuildOptions, config map[string]any, users []xboardUser) (option.Inbound, []string, error) {
	listen, listenUnix := resolveListen(build.listen, build.listenUnix, config)
	inboundOptions := template
	finalType := inboundOptions.Type
	if protocol, loaded := stringValue(config, "protocol"); loaded && protocol != "" {
		if protocol == "hysteria" {
			if version, _ := intValue(config, "version"); version == 2 {
				finalType = C.TypeHysteria2
			} else {
				finalType = C.TypeHysteria
			}
		} else {
			finalType = nodeType(protocol)
		}
	}
	if finalType == "" {
		return inboundOptions, nil, E.New("missing xboard inbound type")
	}
	inboundOptions.Type = finalType
	if inboundOptions.Options != nil {
		if template.Type != "" && template.Type != finalType {
			return inboundOptions, nil, E.New("xboard inbound type mismatch: template=", template.Type, " panel=", finalType)
		}
		inboundOptions.Options = cloneOptions(template.Options)
	} else {
		options, err := defaultInboundOptions(inboundOptions.Type, listen, listenUnix)
		if err != nil {
			return inboundOptions, nil, err
		}
		inboundOptions.Options = options
	}
	// server_port is the port the panel advertises to clients, which has no
	// relation to the local socket when listening on a UNIX socket.
	if listenUnix == "" {
		if port, loaded := intValue(config, "server_port"); loaded && port > 0 && port <= 65535 {
			setUint16Field(inboundOptions.Options, "ListenPort", uint16(port))
		}
	}
	var warnings []string
	if build.machine {
		warnings = applyMachineTLSConfig(inboundOptions.Type, inboundOptions.Options, config, build.machineTLS, build.tls)
	} else {
		applyTLSConfig(inboundOptions.Options, config)
		applyManualTLSConfig(inboundOptions.Options, build.tls)
	}
	applyTransportConfig(inboundOptions.Options, build.transport, config)
	return inboundOptions, warnings, applyUsersAndConfig(inboundOptions.Type, inboundOptions.Options, config, users)
}

func panelTLSMode(inboundType string, config map[string]any) int {
	switch inboundType {
	case C.TypeHysteria, C.TypeHysteria2, C.TypeTUIC, C.TypeAnyTLS:
		return tlsModeEnabled
	}
	mode, _ := intValue(config, "tls")
	switch {
	case mode <= 0:
		return tlsModeDisabled
	case mode == 2 && (inboundType == C.TypeVLESS || inboundType == C.TypeTrojan):
		return tlsModeReality
	default:
		return tlsModeEnabled
	}
}

func applyMachineTLSConfig(inboundType string, options any, config map[string]any, machineTLS *option.InboundTLSOptions, nodeTLS *option.InboundTLSOptions) []string {
	tlsOptions, loaded := options.(option.InboundTLSOptionsWrapper)
	if !loaded {
		return nil
	}
	mode := panelTLSMode(inboundType, config)
	if mode == tlsModeDisabled {
		tlsOptions.ReplaceInboundTLSOptions(nil)
		return nil
	}
	var (
		tlsValue option.InboundTLSOptions
		warnings []string
	)
	if machineTLS != nil {
		tlsValue = *machineTLS
	}
	tlsSettings, hasTLSSettings := mapValue(config, "tls_settings")
	if !hasTLSSettings {
		tlsSettings, _ = mapValue(config, "tls")
	}
	serverName, _ := stringValue(config, "server_name")
	if settingsServerName, _ := stringValue(tlsSettings, "server_name"); settingsServerName != "" && (serverName == "" || mode == tlsModeReality) {
		serverName = settingsServerName
	}
	if mode == tlsModeReality {
		clearTLSCertificate(&tlsValue)
		handshakePort := uint16(443)
		if port, loaded := intValue(tlsSettings, "server_port"); loaded && port > 0 && port <= 65535 {
			handshakePort = uint16(port)
		}
		reality := &option.InboundRealityOptions{Enabled: true}
		reality.Handshake.Server = serverName
		reality.Handshake.ServerPort = handshakePort
		reality.PrivateKey, _ = stringValue(tlsSettings, "private_key")
		if shortID, loaded := stringValue(tlsSettings, "short_id"); loaded {
			reality.ShortID = []string{shortID}
		}
		tlsValue.Reality = reality
	} else if certConfig, loaded := mapValue(config, "cert_config"); loaded {
		certMode, _ := stringValue(certConfig, "cert_mode")
		switch certMode {
		case "", "none":
		case "content", "file":
			certificate, _ := stringValue(certConfig, "cert_content")
			key, _ := stringValue(certConfig, "key_content")
			if strings.TrimSpace(certificate) == "" || strings.TrimSpace(key) == "" {
				warnings = append(warnings, "incomplete xboard cert_config content, using panel tls")
				break
			}
			clearTLSCertificate(&tlsValue)
			tlsValue.Certificate = splitLines(certificate)
			tlsValue.Key = splitLines(key)
		default:
			warnings = append(warnings, "unsupported xboard cert_mode "+certMode+", using panel tls")
		}
	}
	if serverName != "" {
		tlsValue.ServerName = serverName
	}
	mergeTLSOptions(&tlsValue, nodeTLS)
	tlsValue.Enabled = true
	tlsOptions.ReplaceInboundTLSOptions(&tlsValue)
	return warnings
}

func clearTLSCertificate(tlsValue *option.InboundTLSOptions) {
	tlsValue.Certificate = nil
	tlsValue.CertificatePath = ""
	tlsValue.Key = nil
	tlsValue.KeyPath = ""
	tlsValue.CertificateProvider = nil
	tlsValue.ACME = nil
}

func mergeTLSOptions(tlsValue *option.InboundTLSOptions, override *option.InboundTLSOptions) {
	if override == nil {
		return
	}
	if len(override.Certificate) > 0 || override.CertificatePath != "" || override.CertificateProvider != nil || override.ACME != nil {
		tlsValue.Certificate = nil
		tlsValue.CertificatePath = ""
		tlsValue.CertificateProvider = nil
		tlsValue.ACME = nil
	}
	if len(override.Key) > 0 || override.KeyPath != "" {
		tlsValue.Key = nil
		tlsValue.KeyPath = ""
	}
	target := reflect.ValueOf(tlsValue).Elem()
	source := reflect.ValueOf(override).Elem()
	for i := 0; i < source.NumField(); i++ {
		field := source.Field(i)
		if field.IsZero() {
			continue
		}
		target.Field(i).Set(field)
	}
}

func splitLines(content string) []string {
	return strings.Split(strings.ReplaceAll(strings.TrimSpace(content), "\r\n", "\n"), "\n")
}
