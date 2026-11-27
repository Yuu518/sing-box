package parser

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"net/netip"
	"strings"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"

	"github.com/stretchr/testify/require"
)

func TestClashDurationUnitsAndSpeedParsing(t *testing.T) {
	echRaw := []byte("mock-ech-config-bytes")
	echB64 := base64.StdEncoding.EncodeToString(echRaw)
	yamlContent := `
proxies:
  - name: tuic-node
    type: tuic
    server: tuic.example.com
    port: "443"
    uuid: 11111111-1111-1111-1111-111111111111
    password: secret
    heartbeat-interval: 10000
    disable-mtu-discovery: true
    udp-relay-mode: native
    udp-over-stream: true
    fast-open: true
  - name: hy1-node
    type: hysteria
    server: hy1.example.com
    port: 443
    up: "50 Mbps"
    down: "100"
    hop-interval: 30
    auth: ` + base64.StdEncoding.EncodeToString([]byte("bin-auth")) + `
    fast-open: true
  - name: hy2-node
    type: hysteria2
    server: hy2.example.com
    port: 443
    password: pw
    up: "5 Mbps"
    down: "100 Mbps"
    hop-interval: 20
    ech-opts:
      enable: true
      config: ` + echB64 + `
  - name: anytls-node
    type: anytls
    server: anytls.example.com
    port: 443
    password: pw
    idle-session-check-interval: 30
    idle-session-timeout: 60
  - name: vmess-sni
    type: vmess
    server: 192.0.2.1
    port: 443
    uuid: 11111111-1111-1111-1111-111111111111
    alterId: 0
    cipher: auto
    tls: true
    sni: keep.example.com
    network: ws
    ws-opts:
      path: /ws
      v2ray-http-upgrade: true
      headers:
        host: upgrade.example.com
  - name: vless-http-scalar
    type: vless
    server: 192.0.2.2
    port: 443
    uuid: 11111111-1111-1111-1111-111111111111
    tls: true
    sni: vless-sni.example.com
    network: http
    http-opts:
      path: /api
      headers:
        Host: scalar.example.com
  - name: vless-xhttp-unsupported
    type: vless
    server: 192.0.2.3
    port: 443
    uuid: 11111111-1111-1111-1111-111111111111
    network: xhttp
  - name: ss-v2ray
    type: ss
    server: 192.0.2.4
    port: 443
    cipher: aes-128-gcm
    password: pw
    plugin: v2ray-plugin
    plugin-opts:
      mode: websocket
      tls: false
      mux: false
      host: a=b;c\d.example.com
      path: /ws
  - name: wg-node
    type: wireguard
    server: 192.0.2.5
    port: "51820"
    ip: 10.0.0.2
    private-key: priv
    public-key: pub
`
	outbounds, endpoints, err := ParseClashSubscription(context.Background(), yamlContent)
	require.NoError(t, err)
	require.Len(t, outbounds, 7)
	require.Len(t, endpoints, 1)

	tuic := outbounds[0].Options.(*option.TUICOutboundOptions)
	require.Equal(t, uint16(443), tuic.ServerPort)
	require.Equal(t, 10*time.Second, time.Duration(tuic.Heartbeat))
	require.True(t, tuic.DisablePathMTUDiscovery)
	require.True(t, tuic.UDPOverStream)
	require.Empty(t, tuic.UDPRelayMode)
	require.False(t, tuic.TCPFastOpen)

	hy1 := outbounds[1].Options.(*option.HysteriaOutboundOptions)
	require.Equal(t, 30*time.Second, time.Duration(hy1.HopInterval))
	require.NotNil(t, hy1.Up)
	require.NotNil(t, hy1.Down)
	require.Equal(t, []byte("bin-auth"), hy1.Auth)
	require.False(t, hy1.TCPFastOpen)

	hy2 := outbounds[2].Options.(*option.Hysteria2OutboundOptions)
	require.Equal(t, 20*time.Second, time.Duration(hy2.HopInterval))
	require.Equal(t, 5, hy2.UpMbps)
	require.Equal(t, 100, hy2.DownMbps)
	require.NotNil(t, hy2.TLS.ECH)
	require.True(t, hy2.TLS.ECH.Enabled)
	block, _ := pem.Decode([]byte(strings.Join(hy2.TLS.ECH.Config, "\n")))
	require.NotNil(t, block)
	require.Equal(t, "ECH CONFIGS", block.Type)
	require.Equal(t, echRaw, block.Bytes)

	anytls := outbounds[3].Options.(*option.AnyTLSOutboundOptions)
	require.Equal(t, 30*time.Second, time.Duration(anytls.IdleSessionCheckInterval))
	require.Equal(t, 60*time.Second, time.Duration(anytls.IdleSessionTimeout))

	vmess := outbounds[4].Options.(*option.VMessOutboundOptions)
	require.Equal(t, "keep.example.com", vmess.TLS.ServerName)
	require.Equal(t, C.V2RayTransportTypeHTTPUpgrade, vmess.Transport.Type)
	require.Equal(t, "upgrade.example.com", vmess.Transport.HTTPUpgradeOptions.Host)

	vless := outbounds[5].Options.(*option.VLESSOutboundOptions)
	require.Equal(t, "vless-sni.example.com", vless.TLS.ServerName)
	require.Equal(t, "/api", vless.Transport.HTTPOptions.Path)
	require.Equal(t, badoption.Listable[string]{"scalar.example.com"}, vless.Transport.HTTPOptions.Headers["Host"])

	ss := outbounds[6].Options.(*option.ShadowsocksOutboundOptions)
	require.NotContains(t, ss.PluginOptions, "tls")
	require.Contains(t, ss.PluginOptions, "mux=0")
	require.Contains(t, ss.PluginOptions, `host=a\=b\;c\\d.example.com`)
	require.Equal(t, clashPluginOptions("v2ray-plugin", map[string]any{
		"mode": "websocket", "tls": false, "mux": false, "host": `a=b;c\d.example.com`, "path": "/ws",
	}), ss.PluginOptions)

	wg := endpoints[0].Options.(*option.WireGuardEndpointOptions)
	require.Equal(t, uint16(51820), wg.Peers[0].Port)
	require.Equal(t, option.LegacyListable[netip.Prefix]{
		netip.MustParsePrefix("0.0.0.0/0"),
		netip.MustParsePrefix("::/0"),
	}, wg.Peers[0].AllowedIPs)
}

func TestLinkParserFixes(t *testing.T) {
	hy2, err := ParseSubscriptionLink("HYSTERIA2://user:pass@hy2.example.com:443?sni=&alpn=&up=50%20Mbps&down=100%20Mbps&allowInsecure=1#hy2")
	require.NoError(t, err)
	hy2Opts := hy2.Options.(*option.Hysteria2OutboundOptions)
	require.Equal(t, "user:pass", hy2Opts.Password)
	require.Equal(t, "hy2.example.com", hy2Opts.TLS.ServerName)
	require.Empty(t, hy2Opts.TLS.ALPN)
	require.Equal(t, 50, hy2Opts.UpMbps)
	require.Equal(t, 100, hy2Opts.DownMbps)
	require.True(t, hy2Opts.TLS.Insecure)

	trojan, err := ParseSubscriptionLink("trojan://user:secret@trojan.example.com:443?sni=&fp=&alpn=#t")
	require.NoError(t, err)
	trojanOpts := trojan.Options.(*option.TrojanOutboundOptions)
	require.Equal(t, "user:secret", trojanOpts.Password)
	require.Equal(t, "trojan.example.com", trojanOpts.TLS.ServerName)
	require.False(t, trojanOpts.TLS.UTLS.Enabled)
	require.Empty(t, trojanOpts.TLS.ALPN)

	anytls, err := ParseSubscriptionLink("anytls://user:pw@any.example.com:443?tfo=1&fp=&sni=&allowInsecure=1#a")
	require.NoError(t, err)
	anytlsOpts := anytls.Options.(*option.AnyTLSOutboundOptions)
	require.Equal(t, "user:pw", anytlsOpts.Password)
	require.False(t, anytlsOpts.TCPFastOpen)
	require.False(t, anytlsOpts.TLS.UTLS.Enabled)
	require.Equal(t, "any.example.com", anytlsOpts.TLS.ServerName)
	require.True(t, anytlsOpts.TLS.Insecure)

	tuic, err := ParseSubscriptionLink("tuic://11111111-1111-1111-1111-111111111111:pw@tuic.example.com:443?heartbeat_interval=10000&sni=&alpn=#tuic")
	require.NoError(t, err)
	tuicOpts := tuic.Options.(*option.TUICOutboundOptions)
	require.Equal(t, 10*time.Second, time.Duration(tuicOpts.Heartbeat))
	require.Equal(t, "tuic.example.com", tuicOpts.TLS.ServerName)
	require.Empty(t, tuicOpts.TLS.ALPN)

	hy1, err := ParseSubscriptionLink("hysteria://hy1.example.com:443?up=50%20Mbps&down=100%20Mbps&upmbps=50&downmbps=100&sni=#hy1")
	require.NoError(t, err)
	hy1Opts := hy1.Options.(*option.HysteriaOutboundOptions)
	require.NotNil(t, hy1Opts.Up)
	require.NotNil(t, hy1Opts.Down)
	require.Equal(t, 50, hy1Opts.UpMbps)
	require.Equal(t, 100, hy1Opts.DownMbps)
	require.Equal(t, "hy1.example.com", hy1Opts.TLS.ServerName)

	ssUserInfo := base64.RawURLEncoding.EncodeToString([]byte("2022-blake3-aes-128-gcm:keyA:keyB/+=#?@%"))
	ss, err := ParseSubscriptionLink("SS://" + ssUserInfo + "@ss.example.com:8388?plugin=obfs%3Bobfs%3Dhttp#SS%20Node")
	require.NoError(t, err)
	require.Equal(t, "SS Node", ss.Tag)
	ssOpts := ss.Options.(*option.ShadowsocksOutboundOptions)
	require.Equal(t, "2022-blake3-aes-128-gcm", ssOpts.Method)
	require.Equal(t, "keyA:keyB/+=#?@%", ssOpts.Password)
	require.Equal(t, "obfs-local", ssOpts.Plugin)
	require.Equal(t, "obfs=http", ssOpts.PluginOptions)

	vmessJSON := `{"add":"vmess.example.com","port":443,"id":"11111111-1111-1111-1111-111111111111","aid":0,"net":"tcp","tls":"tls","fp":"","alpn":"h2,http/1.1","ps":"node \"a\":1"}`
	vmess, err := ParseSubscriptionLink("vmess://" + base64.StdEncoding.EncodeToString([]byte(vmessJSON)))
	require.NoError(t, err)
	vmessOpts := vmess.Options.(*option.VMessOutboundOptions)
	require.Nil(t, vmessOpts.Transport)
	require.False(t, vmessOpts.TLS.UTLS.Enabled)
	require.Equal(t, badoption.Listable[string]{"h2", "http/1.1"}, vmessOpts.TLS.ALPN)

	vmessGRPCJSON := `{"add":"vmess.example.com","port":443,"id":"11111111-1111-1111-1111-111111111111","net":"grpc","path":"my-grpc-svc"}`
	vmessGRPC, err := ParseSubscriptionLink("vmess://" + base64.StdEncoding.EncodeToString([]byte(vmessGRPCJSON)))
	require.NoError(t, err)
	require.Equal(t, "my-grpc-svc", vmessGRPC.Options.(*option.VMessOutboundOptions).Transport.GRPCOptions.ServiceName)

	vmessURL, err := ParseSubscriptionLink("vmess://11111111-1111-1111-1111-111111111111@vmess.example.com:443?type=ws&path=/ws&host=ws.example.com#vmess-url")
	require.NoError(t, err)
	require.Equal(t, C.V2RayTransportTypeWebsocket, vmessURL.Options.(*option.VMessOutboundOptions).Transport.Type)

	vlessReality, err := ParseSubscriptionLink("VLESS://11111111-1111-1111-1111-111111111111@vless.example.com:443?security=reality&pbk=pub&sid=01&sni=reality.example.com#r")
	require.NoError(t, err)
	vlessOpts := vlessReality.Options.(*option.VLESSOutboundOptions)
	require.True(t, vlessOpts.TLS.Reality.Enabled)
	require.True(t, vlessOpts.TLS.UTLS.Enabled)

	_, err = ParseSubscriptionLink("vless://11111111-1111-1111-1111-111111111111@vless.example.com:443?type=xhttp#bad")
	require.Error(t, err)

	headers := StringToType[badoption.HTTPHeader]("User-Agent: Mozilla/5.0 Chrome/120")
	require.Equal(t, badoption.Listable[string]{"Mozilla/5.0 Chrome/120"}, headers["User-Agent"])
}

func TestRawBase64AndSIP008Fixes(t *testing.T) {
	_, err := DecodeBase64URLSafe("hello world not base64")
	require.Error(t, err)

	rawSub := "  # comment line\r\n  trojan://pw@example.com:443#node1  \r\n"
	outbounds, _, err := ParseRawSubscription(context.Background(), rawSub)
	require.NoError(t, err)
	require.Len(t, outbounds, 1)
	require.Equal(t, "node1", outbounds[0].Tag)

	_, _, err = ParseSIP008Subscription(context.Background(), `{}`)
	require.Error(t, err)

	sipOut, _, err := ParseSIP008Subscription(context.Background(), `{"servers":[{"remarks":"bad-port","server":"1.2.3.4","server_port":70000,"method":"aes-128-gcm","password":"pw"},{"remarks":"ok","server":"1.2.3.4","server_port":8388,"method":"aes-128-gcm","password":"pw","plugin":"obfs","plugin_opts":"obfs=http"}]}`)
	require.NoError(t, err)
	require.Len(t, sipOut, 1)
	require.Equal(t, "ok", sipOut[0].Tag)
	require.Equal(t, "obfs-local", sipOut[0].Options.(*option.ShadowsocksOutboundOptions).Plugin)
}
