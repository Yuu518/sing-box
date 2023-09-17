package provider

import (
	"testing"

	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestEndpointToOutboundKeepsNode(t *testing.T) {
	manager, endpoints := newOwnershipTestManagers()
	provider := newOwnershipTestProvider("p", manager, endpoints)
	defer provider.Close()
	old := []option.Endpoint{{Tag: "shared", Type: "test", Options: &option.DirectOutboundOptions{}}}
	provider.UpdateEndpoints(nil, old)
	provider.Update(nil, ownershipTestOptions(), old, nil)
	require.Len(t, provider.Outbounds(), 1)
	_, found := manager.Outbound("shared")
	require.True(t, found)
	provider.Update(ownershipTestOptions(), nil, nil, old)
	require.Len(t, provider.Outbounds(), 1)
	_, found = endpoints.Get("shared")
	require.True(t, found)
}

func TestDetourSeesReplacedProviderNode(t *testing.T) {
	manager, endpoints := newOwnershipTestManagers()
	provider := newOwnershipTestProvider("p", manager, endpoints)
	defer provider.Close()
	old := ownershipTestOptions()
	provider.UpdateOutbounds(nil, old)
	detour := dialer.NewDetour(manager, "shared", true).(*dialer.DetourDialer)
	previous, err := detour.Dialer()
	require.NoError(t, err)
	next := ownershipTestOptions()
	next[0].Options.(*option.DirectOutboundOptions).BindInterface = "updated"
	provider.UpdateOutbounds(old, next)
	current, found := manager.Outbound("shared")
	require.True(t, found)
	require.NotSame(t, previous, current)
	resolved, err := detour.Dialer()
	require.NoError(t, err)
	require.Same(t, current, resolved)
	provider.UpdateOutbounds(next, nil)
	_, err = detour.Dialer()
	require.ErrorContains(t, err, "outbound detour not found")
	provider.UpdateOutbounds(nil, next)
	resolved, err = detour.Dialer()
	require.NoError(t, err)
	current, _ = manager.Outbound("shared")
	require.Same(t, current, resolved)
}
