package parser_test

import (
	"context"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/provider/parser"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"

	"github.com/stretchr/testify/require"
)

func TestBoxSubscriptionFiltersServerEndpointsAndSkipsUnknown(t *testing.T) {
	ctx := include.Context(context.Background())
	content := `{
		"outbounds": [
			{"type": "direct", "tag": "d"},
			{"type": "bridge", "tag": "b"},
			{"type": "unknown-future-protocol", "tag": "u"},
			{"type": "http", "tag": "valid-http", "server": "127.0.0.1", "server_port": 8080}
		],
		"endpoints": [
			{"type": "openvpn-server", "tag": "ovpn-srv", "address": ["10.0.0.1/24"]},
			{"type": "masque-server", "tag": "masq-srv", "address": ["10.0.1.1/24"]},
			{"type": "unknown-endpoint", "tag": "u-ep"},
			{"type": "masque-client", "tag": "valid-ep", "server": "example.com", "server_port": 443}
		]
	}`
	outbounds, endpoints, err := parser.ParseBoxSubscription(ctx, content)
	require.NoError(t, err)
	require.Len(t, outbounds, 1)
	require.Equal(t, "valid-http", outbounds[0].Tag)
	require.Len(t, endpoints, 1)
	require.Equal(t, "valid-ep", endpoints[0].Tag)
}

func TestParseRemoteSubscriptionSanitizesLocalPathsAndAppliesOverride(t *testing.T) {
	ctx := include.Context(context.Background())
	content := `{
		"outbounds": [
			{
				"type": "ssh",
				"tag": "ssh-node",
				"server": "1.2.3.4",
				"server_port": 22,
				"private_key_path": "/etc/secret/id_rsa",
				"protect_path": "/var/run/protect",
				"netns": "corp",
				"bind_interface": "eth0",
				"routing_mark": 123,
				"inet4_bind_address": "127.0.0.1",
				"detour": "sub-front"
			},
			{
				"type": "http",
				"tag": "http-node",
				"server": "example.com",
				"server_port": 443,
				"tls": {
					"enabled": true,
					"certificate_path": "/etc/ssl/custom.pem",
					"client_certificate_path": "/etc/ssl/client.pem",
					"client_key_path": "/etc/ssl/client.key",
					"ech": {"enabled": true, "config_path": "/etc/ssl/ech.pem"}
				}
			},
			{
				"type": "tor",
				"tag": "tor-node",
				"executable_path": "/bin/sh",
				"data_directory": "/tmp/evil",
				"extra_args": ["-c", "id"],
				"torrc": {"Log": "notice file /tmp/pwn"}
			}
		]
	}`

	localOutbounds, _, err := parser.ParseSubscription(ctx, content, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "/etc/secret/id_rsa", localOutbounds[0].Options.(*option.SSHOutboundOptions).PrivateKeyPath)
	require.Equal(t, "/etc/ssl/custom.pem", localOutbounds[1].Options.(*option.HTTPOutboundOptions).TLS.CertificatePath)

	remoteOutbounds, _, err := parser.ParseRemoteSubscription(ctx, content, nil, nil, nil)
	require.NoError(t, err)
	sshOpts := remoteOutbounds[0].Options.(*option.SSHOutboundOptions)
	require.Empty(t, sshOpts.PrivateKeyPath)
	require.Empty(t, sshOpts.ProtectPath)
	require.Empty(t, sshOpts.NetNs)
	require.Empty(t, sshOpts.BindInterface)
	require.Zero(t, sshOpts.RoutingMark)
	require.Nil(t, sshOpts.Inet4BindAddress)
	require.Equal(t, "sub-front", sshOpts.Detour)
	require.False(t, sshOpts.DetourLocalOnly)

	httpTLS := remoteOutbounds[1].Options.(*option.HTTPOutboundOptions).TLS
	require.Empty(t, httpTLS.CertificatePath)
	require.Empty(t, httpTLS.ClientCertificatePath)
	require.Empty(t, httpTLS.ClientKeyPath)
	require.Empty(t, httpTLS.ECH.ConfigPath)

	torOpts := remoteOutbounds[2].Options.(*option.TorOutboundOptions)
	require.Empty(t, torOpts.ExecutablePath)
	require.Empty(t, torOpts.DataDirectory)
	require.Nil(t, torOpts.ExtraArgs)
	require.Nil(t, torOpts.Options)

	bindAddr := badoption.Addr(netip.MustParseAddr("127.0.0.2"))
	overridden, _, err := parser.ParseRemoteSubscription(
		ctx,
		content,
		&option.OverrideDialerOptions{
			Detour:           common.Ptr("local-front"),
			BindInterface:    common.Ptr("wg0"),
			ProtectPath:      common.Ptr("/run/user-protect"),
			Inet4BindAddress: &bindAddr,
		},
		&option.OverrideTLSOptions{
			CertificatePath: common.Ptr("/etc/user/ca.pem"),
		},
		nil,
	)
	require.NoError(t, err)
	sshOverridden := overridden[0].Options.(*option.SSHOutboundOptions)
	require.Equal(t, "local-front", sshOverridden.Detour)
	require.True(t, sshOverridden.DetourLocalOnly)
	require.Equal(t, "wg0", sshOverridden.BindInterface)
	require.Equal(t, "/run/user-protect", sshOverridden.ProtectPath)
	require.Equal(t, &bindAddr, sshOverridden.Inet4BindAddress)
	require.Equal(t, "/etc/user/ca.pem", overridden[1].Options.(*option.HTTPOutboundOptions).TLS.CertificatePath)
}
