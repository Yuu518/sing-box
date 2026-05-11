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
