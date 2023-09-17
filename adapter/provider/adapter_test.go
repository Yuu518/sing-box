package provider

import (
	"testing"

	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

func TestResolveTagsPreservesOriginalNames(t *testing.T) {
	a := &Adapter{providerTag: "subscription"}
	require.Equal(t, []string{"香港节点", "original/name"}, a.resolveOutboundTags([]option.Outbound{
		{Tag: "香港节点"}, {Tag: "original/name"},
	}))
	require.Equal(t, []string{"WireGuard", "original/endpoint"}, a.resolveEndpointTags([]option.Endpoint{
		{Tag: "WireGuard"}, {Tag: "original/endpoint"},
	}))
}
