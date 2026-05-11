package option_test

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

func TestXboardRejectsStaticInboundConflict(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"inbounds": [
			{
				"type": "shadowsocks",
				"tag": "vless-in",
				"users": [
					{"name": "local", "password": "password"}
				]
			}
		],
		"services": [
			{
				"type": "xboard",
				"panels": [
					{"tag": "main", "url": "https://panel.example", "token": "token"}
				],
				"nodes": [
					{"inbound": "vless-in", "panel": "main", "node_id": 1, "node_type": "shadowsocks"}
				]
			}
		]
	}`), &options)
	require.ErrorContains(t, err, "conflicts with static inbound")
}

func TestXboardServiceManagedInboundCanOmitNodeType(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "xboard",
				"panels": [
					{"tag": "main", "url": "https://panel.example", "token": "token"}
				],
				"nodes": [
					{"inbound": "vless-in", "panel": "main", "node_id": 1}
				]
			}
		]
	}`), &options)
	require.NoError(t, err)
}

func TestXboardServiceManagedInboundValid(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "xboard",
				"panels": [
					{"tag": "main", "url": "https://panel.example", "token": "token"}
				],
				"nodes": [
					{"panel": "main", "node_id": 1, "node_type": "trojan"}
				]
			}
		]
	}`), &options)
	require.NoError(t, err)
}

func TestXboardMachinePanelCanOmitNodes(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "xboard",
				"panels": [
					{
						"tag": "main",
						"url": "https://panel.example",
						"token": "token",
						"machine_id": 3,
						"tls": {"certificate_path": "/etc/ssl/fullchain.pem", "key_path": "/etc/ssl/private.key"}
					}
				]
			}
		]
	}`), &options)
	require.NoError(t, err)
	xboardOptions := options.Services[0].Options.(*option.XboardServiceOptions)
	require.Equal(t, 3, xboardOptions.Panels[0].MachineID)
	require.Equal(t, "/etc/ssl/fullchain.pem", xboardOptions.Panels[0].TLS.CertificatePath)
}

func TestXboardMachinePanelAcceptsNodeOverrides(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "xboard",
				"panels": [
					{"tag": "main", "url": "https://panel.example", "token": "token", "machine_id": 3}
				],
				"nodes": [
					{"panel": "main", "node_id": 12, "tls": {"certificate_path": "/etc/ssl/other.pem"}}
				]
			}
		]
	}`), &options)
	require.NoError(t, err)
}

func TestXboardPanelWithoutMachineRequiresNodes(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "xboard",
				"panels": [
					{"tag": "main", "url": "https://panel.example", "token": "token"}
				]
			}
		]
	}`), &options)
	require.ErrorContains(t, err, "missing xboard nodes")
}

func TestXboardPanelTLSRequiresMachine(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "xboard",
				"panels": [
					{"tag": "main", "url": "https://panel.example", "token": "token", "tls": {"certificate_path": "/etc/ssl/fullchain.pem"}}
				],
				"nodes": [
					{"panel": "main", "node_id": 1}
				]
			}
		]
	}`), &options)
	require.ErrorContains(t, err, "xboard panel tls requires machine_id")
}
