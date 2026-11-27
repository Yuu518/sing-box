package parser

import (
	"net"
	"net/netip"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
)

var (
	linkSchemeRegexp     = regexp.MustCompile(`^(.*?)(://)(.*?)([@?#].*)?$`)
	httpHeaderLineRegexp = regexp.MustCompile(`^[ \t]*([^:\s]+)[ \t]*:[ \t]*(.+?)[ \t]*$`)
	wsEarlyDataRegexp    = regexp.MustCompile(`^(.*?)(?:\?ed=(\d*))?$`)
)

func ParseSubscriptionLink(link string) (option.Outbound, error) {
	result := linkSchemeRegexp.FindStringSubmatch(link)
	if result == nil {
		return option.Outbound{}, E.New("invalid link")
	}

	scheme := strings.ToLower(result[1])
	normalizedLink := scheme + "://" + link[len(result[1])+3:]
	switch scheme {
	case "tuic":
		return parseTuicLink(normalizedLink)
	case "trojan":
		return parseTrojanLink(normalizedLink)
	case "vless":
		return parseVLESSLink(normalizedLink)
	case "hysteria":
		return parseHysteriaLink(normalizedLink)
	case "hy2", "hysteria2":
		return parseHysteria2Link(normalizedLink)
	case "anytls":
		return parseAnyTLSLink(normalizedLink)
	case "ss":
		return parseShadowsocksLink(normalizedLink)
	case "vmess":
		return parseVMessLink(normalizedLink)
	default:
		return option.Outbound{}, E.New("unsupported scheme: ", scheme)
	}
}

func urlUserPassword(user *url.Userinfo) string {
	if user == nil {
		return ""
	}
	if password, hasPassword := user.Password(); hasPassword {
		return user.Username() + ":" + password
	}
	return user.Username()
}

func parseALPNList(value string) []string {
	var alpn []string
	for part := range strings.SplitSeq(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			alpn = append(alpn, part)
		}
	}
	return alpn
}

func parseHeartbeatDuration(value string) badoption.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if d, err := time.ParseDuration(value); err == nil {
		return badoption.Duration(d)
	}
	if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
		return badoption.Duration(time.Duration(ms) * time.Millisecond)
	}
	return 0
}

func StringToType[T any](str string) T {
	var value T
	v := reflect.ValueOf(&value).Elem()
	switch any(value).(type) {
	case badoption.Duration:
		d, err := time.ParseDuration(str)
		if err != nil {
			v.SetInt(StringToType[int64](str))
		} else {
			v.SetInt(int64(d))
		}
		return value
	case badoption.HTTPHeader:
		headers := badoption.HTTPHeader{}
		for header := range strings.SplitSeq(str, "\n") {
			result := httpHeaderLineRegexp.FindStringSubmatch(header)
			if result != nil {
				key := result[1]
				headers[key] = strings.Split(result[2], ",")
			}
		}
		v.Set(reflect.ValueOf(headers))
		return value
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, _ := strconv.ParseInt(str, 10, 64)
		v.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i, _ := strconv.ParseUint(str, 10, 64)
		v.SetUint(i)
	case reflect.Float32, reflect.Float64:
		f, _ := strconv.ParseFloat(str, 64)
		v.SetFloat(f)
	case reflect.Bool:
		b, _ := strconv.ParseBool(str)
		v.SetBool(b)
	default:
		panic("unsupported type")
	}
	return value
}

func shadowsocksPluginName(plugin string) string {
	if before, _, ok := strings.Cut(plugin, ";"); ok {
		return before
	}
	return plugin
}

func shadowsocksPluginOptions(plugin string) string {
	if _, after, ok := strings.Cut(plugin, ";"); ok {
		return after
	}
	return ""
}

func v2rayTransportWsPath(WebsocketOptions *option.V2RayWebsocketOptions, path string) error {
	for _, character := range path {
		if character < 0x20 || character == 0x7f {
			return E.New("invalid control character in websocket path")
		}
	}
	result := wsEarlyDataRegexp.FindStringSubmatch(path)
	if result == nil {
		return E.New("invalid websocket path")
	}
	WebsocketOptions.Path = result[1]
	if result[2] != "" {
		WebsocketOptions.EarlyDataHeaderName = "Sec-WebSocket-Protocol"
		WebsocketOptions.MaxEarlyData = StringToType[uint32](result[2])
	}
	return nil
}

func v2rayTransportWs(host string, path string) (option.V2RayWebsocketOptions, error) {
	var WebsocketOptions option.V2RayWebsocketOptions
	if host != "" {
		WebsocketOptions.Headers = StringToType[badoption.HTTPHeader](F.ToString("Host: ", host))
	}
	if path != "" {
		if err := v2rayTransportWsPath(&WebsocketOptions, path); err != nil {
			return option.V2RayWebsocketOptions{}, err
		}
	}
	return WebsocketOptions, nil
}

func v2rayHostTLSServerName(server string, host string) string {
	// Some legacy links use the HTTP host as the certificate name when dialing an IP.
	if _, err := netip.ParseAddr(server); err != nil {
		return ""
	}
	if host == "" || strings.ContainsAny(host, ",:") || !M.IsDomainName(host) {
		return ""
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return ""
	}
	return host
}

func parseShadowsocksLink(link string) (option.Outbound, error) {
	rest := strings.TrimPrefix(link, "ss://")
	beforeFrag, rawFragment, _ := strings.Cut(rest, "#")
	tag := rawFragment
	if unescaped, err := url.PathUnescape(rawFragment); err == nil {
		tag = unescaped
	}
	authority, rawQuery, _ := strings.Cut(beforeFrag, "?")

	var userinfo, hostPort string
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		userinfo = authority[:at]
		hostPort = strings.TrimSuffix(authority[at+1:], "/")
		if !strings.Contains(userinfo, ":") {
			decoded, err := DecodeBase64URLSafe(userinfo)
			if err != nil {
				return option.Outbound{}, E.Cause(err, "decode shadowsocks user info")
			}
			userinfo = decoded
		} else if unescaped, err := url.PathUnescape(userinfo); err == nil {
			userinfo = unescaped
		}
	} else {
		decoded, err := DecodeBase64URLSafe(authority)
		if err != nil {
			return option.Outbound{}, E.Cause(err, "decode shadowsocks link")
		}
		if rawQuery == "" {
			decoded, rawQuery, _ = strings.Cut(decoded, "?")
		}
		at := strings.LastIndex(decoded, "@")
		if at < 0 {
			return option.Outbound{}, E.New("missing user info")
		}
		userinfo = decoded[:at]
		hostPort = strings.TrimSuffix(decoded[at+1:], "/")
	}

	method, password, ok := strings.Cut(userinfo, ":")
	if !ok || method == "" {
		return option.Outbound{}, E.New("missing user info")
	}
	if password == "" {
		return option.Outbound{}, E.New("bad user info")
	}
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return option.Outbound{}, E.Cause(err, "parse shadowsocks host:port")
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return option.Outbound{}, E.New("invalid shadowsocks port: ", portStr)
	}

	var plugin string
	if rawQuery != "" {
		if query, err := url.ParseQuery(rawQuery); err == nil {
			plugin = query.Get("plugin")
		}
	}

	var options option.ShadowsocksOutboundOptions
	options.ServerOptions.Server = host
	options.ServerOptions.ServerPort = uint16(port)
	options.Method = clashShadowsocksCipher(method)
	options.Password = password
	options.Plugin = clashPluginName(shadowsocksPluginName(plugin))
	options.PluginOptions = shadowsocksPluginOptions(plugin)

	outbound := option.Outbound{
		Type:    C.TypeShadowsocks,
		Tag:     tag,
		Options: &options,
	}
	return outbound, nil
}

func parseTuicLink(link string) (option.Outbound, error) {
	linkURL, err := url.Parse(link)
	if err != nil {
		return option.Outbound{}, err
	}
	if linkURL.User == nil || linkURL.User.Username() == "" {
		return option.Outbound{}, E.New("missing uuid")
	}
	var options option.TUICOutboundOptions
	TLSOptions := option.OutboundTLSOptions{
		Enabled: true,
		ECH:     &option.OutboundECHOptions{},
		UTLS:    &option.OutboundUTLSOptions{},
		Reality: &option.OutboundRealityOptions{},
	}
	options.UUID = linkURL.User.Username()
	options.Password, _ = linkURL.User.Password()
	options.ServerOptions.Server = linkURL.Hostname()
	TLSOptions.ServerName = linkURL.Hostname()
	options.ServerOptions.ServerPort = StringToType[uint16](linkURL.Port())
	for key, values := range linkURL.Query() {
		value := values[0]
		switch key {
		case "congestion_control":
			if value != "" && value != "cubic" {
				options.CongestionControl = value
			}
		case "udp_relay_mode":
			if value != "" {
				options.UDPRelayMode = value
			}
		case "udp_over_stream":
			if value == "true" || value == "1" {
				options.UDPOverStream = true
			}
		case "zero_rtt_handshake", "reduce_rtt":
			if value == "true" || value == "1" {
				options.ZeroRTTHandshake = true
			}
		case "heartbeat_interval":
			if value != "" {
				options.Heartbeat = parseHeartbeatDuration(value)
			}
		case "sni":
			if value != "" {
				TLSOptions.ServerName = value
			}
		case "insecure", "skip-cert-verify", "allow_insecure":
			if value == "1" || value == "true" {
				TLSOptions.Insecure = true
			}
		case "disable_sni":
			if value == "1" || value == "true" {
				TLSOptions.DisableSNI = true
			}
		case "alpn":
			if alpn := parseALPNList(value); len(alpn) > 0 {
				TLSOptions.ALPN = alpn
			}
		}
	}
	if options.UDPOverStream {
		options.UDPRelayMode = ""
	}
	outbound := option.Outbound{
		Type: C.TypeTUIC,
		Tag:  linkURL.Fragment,
	}
	options.TLS = &TLSOptions
	outbound.Options = &options
	return outbound, nil
}

func decodeVMessJSON(raw string) (map[string]string, error) {
	var rawMap map[string]any
	if err := json.Unmarshal([]byte(raw), &rawMap); err != nil {
		return nil, err
	}
	proxy := make(map[string]string, len(rawMap))
	for key, val := range rawMap {
		switch v := val.(type) {
		case string:
			proxy[key] = v
		case float64:
			proxy[key] = strconv.FormatFloat(v, 'f', -1, 64)
		case bool:
			proxy[key] = strconv.FormatBool(v)
		}
	}
	return proxy, nil
}

func parseVMessLink(link string) (option.Outbound, error) {
	var proxy map[string]string
	var err error
	rest := strings.TrimPrefix(link, "vmess://")
	rawBody, suffix, hasSuffix := strings.Cut(rest, "?")
	if !hasSuffix {
		rawBody, suffix, hasSuffix = strings.Cut(rest, "#")
		if hasSuffix {
			suffix = "#" + suffix
		}
	} else {
		suffix = "?" + suffix
	}
	if !strings.Contains(rawBody, "@") {
		decoded, decodeErr := DecodeBase64URLSafe(rawBody)
		if decodeErr == nil {
			proxy, err = decodeVMessJSON(decoded)
			if err != nil {
				link = "vmess://" + decoded + suffix
			}
		} else {
			proxy, err = decodeVMessJSON(rawBody)
		}
	} else {
		err = E.New("not json vmess link")
	}
	if err != nil {
		proxy = make(map[string]string)
		linkURL, err := url.Parse(link)
		if err != nil {
			return option.Outbound{}, err
		}
		if linkURL.User == nil || linkURL.User.Username() == "" {
			return option.Outbound{}, E.New("missing uuid")
		}
		if password, hasPassword := linkURL.User.Password(); hasPassword && password != "" {
			proxy["scy"] = linkURL.User.Username()
			proxy["id"] = password
		} else {
			proxy["id"] = linkURL.User.Username()
		}
		proxy["add"] = linkURL.Hostname()
		proxy["port"] = linkURL.Port()
		proxy["ps"] = linkURL.Fragment
		for key, values := range linkURL.Query() {
			value := values[0]
			switch key {
			case "type":
				if value == "http" {
					proxy["net"] = "tcp"
					proxy["type"] = "http"
				} else if value != "" {
					proxy["net"] = value
				}
			case "encryption":
				proxy["scy"] = value
			case "alterId":
				proxy["aid"] = value
			default:
				proxy[key] = value
			}
		}
	}
	outbound := option.Outbound{
		Type: C.TypeVMess,
	}
	options := option.VMessOutboundOptions{
		Security: "auto",
	}
	TLSOptions := option.OutboundTLSOptions{
		ECH:     &option.OutboundECHOptions{},
		UTLS:    &option.OutboundUTLSOptions{},
		Reality: &option.OutboundRealityOptions{},
	}
	TLSOptions.ServerName = proxy["add"]
	for key, value := range proxy {
		switch key {
		case "ps":
			outbound.Tag = value
		case "add":
			options.Server = value
		case "sni":
			if value != "" {
				TLSOptions.ServerName = value
			}
		case "port":
			options.ServerPort = StringToType[uint16](value)
		case "id":
			options.UUID = value
		case "scy":
			if value != "" {
				options.Security = value
			}
		case "aid":
			options.AlterId, _ = strconv.Atoi(value)
		case "packet_encoding":
			options.PacketEncoding = value
		case "xudp":
			if value == "1" || value == "true" {
				options.PacketEncoding = "xudp"
			}
		case "tls":
			if value == "1" || value == "true" || value == "tls" {
				TLSOptions.Enabled = true
			}
		case "insecure", "skip-cert-verify", "allowInsecure":
			if value == "1" || value == "true" {
				TLSOptions.Insecure = true
			}
		case "alpn":
			if alpn := parseALPNList(value); len(alpn) > 0 {
				TLSOptions.ALPN = alpn
			}
		case "fp":
			if value != "" {
				TLSOptions.UTLS.Enabled = true
				TLSOptions.UTLS.Fingerprint = value
			}
		case "net":
			if value == "" || value == "raw" || value == "none" {
				continue
			}
			Transport := option.V2RayTransportOptions{
				WebsocketOptions: option.V2RayWebsocketOptions{
					Headers: badoption.HTTPHeader{},
				},
				HTTPOptions: option.V2RayHTTPOptions{
					Host:    badoption.Listable[string]{},
					Headers: map[string]badoption.Listable[string]{},
				},
				GRPCOptions: option.V2RayGRPCOptions{},
			}
			switch value {
			case "ws":
				Transport.Type = C.V2RayTransportTypeWebsocket
				Transport.WebsocketOptions, err = v2rayTransportWs(proxy["host"], proxy["path"])
				if err != nil {
					return option.Outbound{}, err
				}
			case "h2":
				Transport.Type = C.V2RayTransportTypeHTTP
				TLSOptions.Enabled = true
				if host, exists := proxy["host"]; exists && host != "" {
					Transport.HTTPOptions.Host = []string{host}
				}
				if path, exists := proxy["path"]; exists && path != "" {
					Transport.HTTPOptions.Path = path
				}
			case "tcp":
				if proxy["type"] != "http" {
					continue
				}
				Transport.Type = C.V2RayTransportTypeHTTP
				if method, exists := proxy["method"]; exists {
					Transport.HTTPOptions.Method = method
				}
				if host, exists := proxy["host"]; exists && host != "" {
					Transport.HTTPOptions.Host = []string{host}
				}
				if path, exists := proxy["path"]; exists && path != "" {
					Transport.HTTPOptions.Path = path
				}
				if headers, exists := proxy["headers"]; exists {
					Transport.HTTPOptions.Headers = StringToType[badoption.HTTPHeader](headers)
				}
			case "grpc":
				Transport.Type = C.V2RayTransportTypeGRPC
				if serviceName := proxy["serviceName"]; serviceName != "" {
					Transport.GRPCOptions.ServiceName = serviceName
				} else if path := proxy["path"]; path != "" {
					Transport.GRPCOptions.ServiceName = path
				} else if host := proxy["host"]; host != "" {
					Transport.GRPCOptions.ServiceName = host
				}
			default:
				return option.Outbound{}, E.New("unsupported vmess transport: ", value)
			}
			options.Transport = &Transport
		case "tfo", "tcp-fast-open", "tcp_fast_open":
			if value == "1" || value == "true" {
				options.TCPFastOpen = true
			}
		}
	}
	if TLSOptions.Enabled && proxy["sni"] == "" {
		transportType := proxy["net"]
		if transportType == "h2" || transportType == "tcp" && proxy["type"] == "http" {
			transportType = "http"
		}
		if transportType == "ws" || transportType == "http" {
			if serverName := v2rayHostTLSServerName(proxy["add"], proxy["host"]); serverName != "" {
				TLSOptions.ServerName = serverName
			}
		}
	}
	if serverName := proxy["sni"]; serverName != "" {
		TLSOptions.ServerName = serverName
	}
	if TLSOptions.Enabled {
		options.TLS = &TLSOptions
	}
	outbound.Options = &options
	return outbound, nil
}

func parseVLESSLink(link string) (option.Outbound, error) {
	linkURL, err := url.Parse(link)
	if err != nil {
		return option.Outbound{}, err
	}
	if linkURL.User == nil || linkURL.User.Username() == "" {
		return option.Outbound{}, E.New("missing uuid")
	}
	var options option.VLESSOutboundOptions
	TLSOptions := option.OutboundTLSOptions{
		ECH:     &option.OutboundECHOptions{},
		UTLS:    &option.OutboundUTLSOptions{},
		Reality: &option.OutboundRealityOptions{},
	}
	options.UUID = linkURL.User.Username()
	options.Server = linkURL.Hostname()
	TLSOptions.ServerName = linkURL.Hostname()
	options.ServerPort = StringToType[uint16](linkURL.Port())
	proxy := map[string]string{}
	for key, values := range linkURL.Query() {
		proxy[key] = values[0]
	}
	for key, value := range proxy {
		switch key {
		case "type":
			if value == "" || value == "tcp" || value == "raw" || value == "none" {
				continue
			}
			Transport := option.V2RayTransportOptions{
				HTTPOptions: option.V2RayHTTPOptions{
					Host:    badoption.Listable[string]{},
					Headers: badoption.HTTPHeader{},
				},
				GRPCOptions: option.V2RayGRPCOptions{},
			}
			switch value {
			case "ws":
				Transport.Type = C.V2RayTransportTypeWebsocket
				Transport.WebsocketOptions, err = v2rayTransportWs(proxy["host"], proxy["path"])
				if err != nil {
					return option.Outbound{}, err
				}
			case "http", "h2":
				Transport.Type = C.V2RayTransportTypeHTTP
				if host, exists := proxy["host"]; exists && host != "" {
					Transport.HTTPOptions.Host = strings.Split(host, ",")
				}
				if path, exists := proxy["path"]; exists && path != "" {
					Transport.HTTPOptions.Path = path
				}
			case "grpc":
				Transport.Type = C.V2RayTransportTypeGRPC
				if serviceName, exists := proxy["serviceName"]; exists && serviceName != "" {
					Transport.GRPCOptions.ServiceName = serviceName
				}
			default:
				return option.Outbound{}, E.New("unsupported vless transport: ", value)
			}
			options.Transport = &Transport
		case "security":
			switch value {
			case "tls":
				TLSOptions.Enabled = true
			case "reality":
				TLSOptions.Enabled = true
				TLSOptions.Reality.Enabled = true
			}
		case "insecure", "skip-cert-verify", "allowInsecure":
			if value == "1" || value == "true" {
				TLSOptions.Insecure = true
			}
		case "sni", "peer":
			if value != "" {
				TLSOptions.ServerName = value
			}
		case "alpn":
			if alpn := parseALPNList(value); len(alpn) > 0 {
				TLSOptions.ALPN = alpn
			}
		case "fp":
			if value != "" {
				TLSOptions.UTLS.Enabled = true
				TLSOptions.UTLS.Fingerprint = value
			}
		case "flow":
			if value == "xtls-rprx-vision" {
				options.Flow = "xtls-rprx-vision"
			}
		case "pbk":
			TLSOptions.Reality.PublicKey = value
		case "sid":
			TLSOptions.Reality.ShortID = value
		case "tfo", "tcp-fast-open", "tcp_fast_open":
			if value == "1" || value == "true" {
				options.TCPFastOpen = true
			}
		}
	}
	if TLSOptions.Reality.Enabled && !TLSOptions.UTLS.Enabled {
		TLSOptions.UTLS.Enabled = true
	}
	if proxy["security"] == "tls" && proxy["sni"] == "" && proxy["peer"] == "" {
		transportType := proxy["type"]
		if transportType == "ws" || transportType == "http" || transportType == "h2" {
			if serverName := v2rayHostTLSServerName(options.Server, proxy["host"]); serverName != "" {
				TLSOptions.ServerName = serverName
			}
		}
	}
	if serverName := proxy["sni"]; serverName != "" {
		TLSOptions.ServerName = serverName
	}
	outbound := option.Outbound{
		Type: C.TypeVLESS,
		Tag:  linkURL.Fragment,
	}
	if TLSOptions.Enabled {
		options.TLS = &TLSOptions
	}
	outbound.Options = &options
	return outbound, nil
}

func parseTrojanLink(link string) (option.Outbound, error) {
	linkURL, err := url.Parse(link)
	if err != nil {
		return option.Outbound{}, err
	}
	password := urlUserPassword(linkURL.User)
	if password == "" {
		return option.Outbound{}, E.New("missing password")
	}
	var options option.TrojanOutboundOptions
	TLSOptions := option.OutboundTLSOptions{
		Enabled: true,
		ECH:     &option.OutboundECHOptions{},
		UTLS:    &option.OutboundUTLSOptions{},
		Reality: &option.OutboundRealityOptions{},
	}
	options.Server = linkURL.Hostname()
	TLSOptions.ServerName = linkURL.Hostname()
	options.ServerPort = StringToType[uint16](linkURL.Port())
	options.Password = password
	proxy := map[string]string{}
	for key, values := range linkURL.Query() {
		proxy[key] = values[0]
	}
	for key, value := range proxy {
		switch key {
		case "insecure", "allowInsecure", "skip-cert-verify":
			if value == "1" || value == "true" {
				TLSOptions.Insecure = true
			}
		case "sni", "peer":
			if value != "" {
				TLSOptions.ServerName = value
			}
		case "alpn":
			if alpn := parseALPNList(value); len(alpn) > 0 {
				TLSOptions.ALPN = alpn
			}
		case "fp":
			if value != "" {
				TLSOptions.UTLS.Enabled = true
				TLSOptions.UTLS.Fingerprint = value
			}
		case "type":
			if value == "" || value == "tcp" || value == "raw" || value == "none" {
				continue
			}
			Transport := option.V2RayTransportOptions{
				WebsocketOptions: option.V2RayWebsocketOptions{
					Headers: map[string]badoption.Listable[string]{},
				},
				HTTPOptions: option.V2RayHTTPOptions{
					Host:    badoption.Listable[string]{},
					Headers: map[string]badoption.Listable[string]{},
				},
				GRPCOptions: option.V2RayGRPCOptions{},
			}
			switch value {
			case "ws":
				Transport.Type = C.V2RayTransportTypeWebsocket
				Transport.WebsocketOptions, err = v2rayTransportWs(proxy["host"], proxy["path"])
				if err != nil {
					return option.Outbound{}, err
				}
			case "grpc":
				Transport.Type = C.V2RayTransportTypeGRPC
				if serviceName, exists := proxy["grpc-service-name"]; exists && serviceName != "" {
					Transport.GRPCOptions.ServiceName = serviceName
				} else {
					Transport.GRPCOptions.ServiceName = proxy["serviceName"]
				}
			default:
				return option.Outbound{}, E.New("unsupported trojan transport: ", value)
			}
			options.Transport = &Transport
		case "tfo", "tcp-fast-open", "tcp_fast_open":
			if value == "1" || value == "true" {
				options.TCPFastOpen = true
			}
		}
	}
	outbound := option.Outbound{
		Type: C.TypeTrojan,
		Tag:  linkURL.Fragment,
	}
	if serverName := proxy["sni"]; serverName != "" {
		TLSOptions.ServerName = serverName
	}
	options.TLS = &TLSOptions
	outbound.Options = &options
	return outbound, nil
}

func parseHysteriaLink(link string) (option.Outbound, error) {
	linkURL, err := url.Parse(link)
	if err != nil {
		return option.Outbound{}, err
	}
	var options option.HysteriaOutboundOptions
	TLSOptions := option.OutboundTLSOptions{
		Enabled: true,
		ECH:     &option.OutboundECHOptions{},
		UTLS:    &option.OutboundUTLSOptions{},
		Reality: &option.OutboundRealityOptions{},
	}
	options.Server = linkURL.Hostname()
	TLSOptions.ServerName = linkURL.Hostname()
	options.ServerPort = StringToType[uint16](linkURL.Port())
	for key, values := range linkURL.Query() {
		value := values[0]
		switch key {
		case "auth":
			options.AuthString = value
		case "peer", "sni":
			if value != "" {
				TLSOptions.ServerName = value
			}
		case "alpn":
			if alpn := parseALPNList(value); len(alpn) > 0 {
				TLSOptions.ALPN = alpn
			}
		case "ca":
			TLSOptions.CertificatePath = value
		case "ca_str":
			TLSOptions.Certificate = strings.Split(value, "\n")
		case "up":
			options.Up = clashSpeedToNetworkBytes(value)
		case "upmbps", "up_mbps":
			options.UpMbps, _ = strconv.Atoi(value)
		case "down":
			options.Down = clashSpeedToNetworkBytes(value)
		case "downmbps", "down_mbps":
			options.DownMbps, _ = strconv.Atoi(value)
		case "obfs", "obfsParam":
			options.Obfs = value
		case "insecure", "skip-cert-verify", "allowInsecure":
			if value == "1" || value == "true" {
				TLSOptions.Insecure = true
			}
		}
	}
	outbound := option.Outbound{
		Type: C.TypeHysteria,
		Tag:  linkURL.Fragment,
	}
	options.TLS = &TLSOptions
	outbound.Options = &options
	return outbound, nil
}

func parseHysteria2Link(link string) (option.Outbound, error) {
	linkURL, err := url.Parse(link)
	if err != nil {
		return option.Outbound{}, err
	}
	var options option.Hysteria2OutboundOptions
	TLSOptions := option.OutboundTLSOptions{
		Enabled: true,
		ECH:     &option.OutboundECHOptions{},
		UTLS:    &option.OutboundUTLSOptions{},
		Reality: &option.OutboundRealityOptions{},
	}
	Obfs := &option.Hysteria2Obfs{}
	options.ServerPort = uint16(443)
	options.Server = linkURL.Hostname()
	TLSOptions.ServerName = linkURL.Hostname()
	options.Password = urlUserPassword(linkURL.User)
	if linkURL.Port() != "" {
		options.ServerPort = StringToType[uint16](linkURL.Port())
	}
	for key, values := range linkURL.Query() {
		value := values[0]
		switch key {
		case "up":
			options.UpMbps = clashSpeedToIntMbps(value)
		case "down":
			options.DownMbps = clashSpeedToIntMbps(value)
		case "mport":
			options.ServerPorts = clashPorts(value)
		case "obfs":
			if value == "salamander" {
				Obfs.Type = "salamander"
				options.Obfs = Obfs
			}
		case "obfs-password":
			Obfs.Password = value
		case "sni":
			if value != "" {
				TLSOptions.ServerName = value
			}
		case "alpn":
			if alpn := parseALPNList(value); len(alpn) > 0 {
				TLSOptions.ALPN = alpn
			}
		case "pinSHA256":
			if value != "" {
				TLSOptions.CertificatePinSHA256 = value
			}
		case "insecure", "skip-cert-verify", "allowInsecure":
			if value == "1" || value == "true" {
				TLSOptions.Insecure = true
			}
		}
	}
	outbound := option.Outbound{
		Type: C.TypeHysteria2,
		Tag:  linkURL.Fragment,
	}
	options.TLS = &TLSOptions
	outbound.Options = &options
	return outbound, nil
}

func parseAnyTLSLink(link string) (option.Outbound, error) {
	linkURL, err := url.Parse(link)
	if err != nil {
		return option.Outbound{}, err
	}
	password := urlUserPassword(linkURL.User)
	if password == "" {
		return option.Outbound{}, E.New("missing password")
	}
	var options option.AnyTLSOutboundOptions
	TLSOptions := option.OutboundTLSOptions{
		Enabled: true,
		ECH:     &option.OutboundECHOptions{},
		UTLS:    &option.OutboundUTLSOptions{},
		Reality: &option.OutboundRealityOptions{},
	}
	options.Server = linkURL.Hostname()
	TLSOptions.ServerName = linkURL.Hostname()
	options.ServerPort = StringToType[uint16](linkURL.Port())
	options.Password = password
	proxy := map[string]string{}
	for key, values := range linkURL.Query() {
		proxy[key] = values[0]
	}
	for key, value := range proxy {
		switch key {
		case "insecure", "allowInsecure", "skip-cert-verify":
			if value == "1" || value == "true" {
				TLSOptions.Insecure = true
			}
		case "sni":
			if value != "" {
				TLSOptions.ServerName = value
			}
		case "alpn":
			if alpn := parseALPNList(value); len(alpn) > 0 {
				TLSOptions.ALPN = alpn
			}
		case "fp":
			if value != "" {
				TLSOptions.UTLS.Enabled = true
				TLSOptions.UTLS.Fingerprint = value
			}
		}
	}
	outbound := option.Outbound{
		Type: C.TypeAnyTLS,
		Tag:  linkURL.Fragment,
	}
	options.TLS = &TLSOptions
	outbound.Options = &options
	return outbound, nil
}
