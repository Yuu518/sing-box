package provider

import (
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestResolveOutboundTags(t *testing.T) {
	adapter := Adapter{
		providerTag: "subscription",
		logger:      log.NewNOPFactory().Logger(),
	}

	tags := adapter.resolveOutboundTags([]option.Outbound{
		{Tag: "node"},
		{Tag: "node"},
		{Tag: "node"},
		{},
	})

	require.Equal(t, []string{"node", "node (1)", "node (2)", "3"}, tags)
}

func TestResolveEndpointTags(t *testing.T) {
	adapter := Adapter{
		providerTag: "subscription",
		logger:      log.NewNOPFactory().Logger(),
	}

	tags := adapter.resolveEndpointTags([]option.Endpoint{
		{Tag: "node"},
		{Tag: "node"},
		{Tag: "node"},
		{},
	})

	require.Equal(t, []string{"node", "node (1)", "node (2)", "endpoint-3"}, tags)
}
