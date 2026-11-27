package parser

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestTUICLinkHeartbeatDuration(t *testing.T) {
	var outbounds []option.Outbound
	require.NotPanics(t, func() {
		var err error
		outbounds, _, err = ParseSubscription(context.Background(), "tuic://11111111-1111-1111-1111-111111111111:password@example.com:443?heartbeat_interval=10s", nil, nil, nil)
		require.NoError(t, err)
	})
	require.Len(t, outbounds, 1)
	require.Equal(t, 10*time.Second, time.Duration(outbounds[0].Options.(*option.TUICOutboundOptions).Heartbeat))
}

func TestWebsocketLinkNewlinePath(t *testing.T) {
	for _, link := range []string{
		"vless://11111111-1111-1111-1111-111111111111@example.com:443?type=ws&path=%2Ffoo%0Abar",
		"trojan://password@example.com:443?type=ws&path=%2Ffoo%0Abar",
		"vmess://" + base64.RawURLEncoding.EncodeToString([]byte(`{"add":"example.com","port":443,"id":"11111111-1111-1111-1111-111111111111","net":"ws","path":"/foo\nbar"}`)),
	} {
		t.Run(link[:6], func(t *testing.T) {
			require.NotPanics(t, func() {
				_, _, err := ParseSubscription(context.Background(), link, nil, nil, nil)
				require.Error(t, err)
			})
		})
	}
}

func TestWebsocketLinkPreservesEarlyData(t *testing.T) {
	out, err := ParseSubscriptionLink("vless://11111111-1111-1111-1111-111111111111@example.com:443?type=ws&path=%2Fws%3Fed%3D2048")
	require.NoError(t, err)
	ws := out.Options.(*option.VLESSOutboundOptions).Transport.WebsocketOptions
	require.Equal(t, "/ws", ws.Path)
	require.EqualValues(t, 2048, ws.MaxEarlyData)
	require.Equal(t, "Sec-WebSocket-Protocol", ws.EarlyDataHeaderName)
}

func TestTrojanGRPCServiceNameDoesNotReplaceTLSName(t *testing.T) {
	out, err := ParseSubscriptionLink("trojan://password@proxy.example:443?type=grpc&serviceName=grpc-service")
	require.NoError(t, err)
	opts := out.Options.(*option.TrojanOutboundOptions)
	require.Equal(t, "grpc-service", opts.Transport.GRPCOptions.ServiceName)
	require.Equal(t, "proxy.example", opts.TLS.ServerName)
}

func TestVLESSGRPCServiceNameDoesNotReplaceTLSName(t *testing.T) {
	for _, suffix := range []string{"", "&sni=cert.example", "&peer=peer.example"} {
		t.Run(suffix, func(t *testing.T) {
			out, err := ParseSubscriptionLink("vless://11111111-1111-1111-1111-111111111111@proxy.example:443?security=tls&type=grpc&serviceName=grpc-service" + suffix)
			require.NoError(t, err)
			opts := out.Options.(*option.VLESSOutboundOptions)
			require.Equal(t, "grpc-service", opts.Transport.GRPCOptions.ServiceName)
			expected := map[string]string{"": "proxy.example", "&sni=cert.example": "cert.example", "&peer=peer.example": "peer.example"}[suffix]
			require.Equal(t, expected, opts.TLS.ServerName)
		})
	}
}
