package route

import (
	"context"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	R "github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type matchOnlyDNS struct {
	adapter.DNSRouter
	addresses []netip.Addr
}

func (d *matchOnlyDNS) Lookup(context.Context, string, adapter.DNSQueryOptions) ([]netip.Addr, error) {
	return d.addresses, nil
}

type matchOnlyFlowOutbounds struct {
	adapter.OutboundManager
	outbound adapter.Outbound
}

func (m *matchOnlyFlowOutbounds) Default() adapter.Outbound { return m.outbound }

type matchOnlyFlowOutbound struct{ adapter.FlowOutbound }

func (*matchOnlyFlowOutbound) Type() string { return "direct" }

func (*matchOnlyFlowOutbound) Tag() string { return "direct" }

func (*matchOnlyFlowOutbound) Network() []string { return []string{N.NetworkTCP, N.NetworkUDP} }

func (*matchOnlyFlowOutbound) PreMatchFlow(string, netip.Addr) adapter.PreMatchAction {
	return adapter.PreMatchFlow
}

func TestResolveMatchOnlyPreservesDialAddresses(t *testing.T) {
	dns := &matchOnlyDNS{addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")}}
	router := &Router{dns: dns, logger: logger.NOP()}
	dialAddresses := []netip.Addr{netip.MustParseAddr("192.0.2.1")}
	metadata := adapter.InboundContext{Destination: M.ParseSocksaddr("example.com:443"), DestinationAddresses: dialAddresses}

	require.NoError(t, router.actionResolve(context.Background(), &metadata, &R.RuleActionResolve{MatchOnly: true}))
	require.Equal(t, dialAddresses, metadata.DestinationAddresses)
	require.Equal(t, dns.addresses, metadata.CacheIPs)
	require.Equal(t, uint8(4), metadata.IPVersion)

	dns.addresses = []netip.Addr{netip.MustParseAddr("2001:db8::1")}
	require.NoError(t, router.actionResolve(context.Background(), &metadata, &R.RuleActionResolve{MatchOnly: true}))
	require.Equal(t, uint8(6), metadata.IPVersion)

	dns.addresses = []netip.Addr{netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("2001:db8::1")}
	require.NoError(t, router.actionResolve(context.Background(), &metadata, &R.RuleActionResolve{MatchOnly: true}))
	require.Zero(t, metadata.IPVersion)

	resolvedDial := []netip.Addr{netip.MustParseAddr("203.0.113.10")}
	dns.addresses = resolvedDial
	require.NoError(t, router.actionResolve(context.Background(), &metadata, &R.RuleActionResolve{}))
	require.Equal(t, resolvedDial, metadata.DestinationAddresses)
	require.Nil(t, metadata.CacheIPs)
	require.Equal(t, uint8(4), metadata.IPVersion)
}

func TestApplyRouteOptionsOverrideClearsCacheIPs(t *testing.T) {
	metadata := adapter.InboundContext{
		Destination:          M.ParseSocksaddr("example.com:443"),
		DestinationAddresses: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
		CacheIPs:             []netip.Addr{netip.MustParseAddr("10.0.0.1")},
	}
	applyRouteOptionsOverride(&metadata, &R.RuleActionRouteOptions{OverridePort: 8443})
	require.Equal(t, uint16(8443), metadata.Destination.Port)
	require.NotEmpty(t, metadata.DestinationAddresses)
	require.NotEmpty(t, metadata.CacheIPs)

	applyRouteOptionsOverride(&metadata, &R.RuleActionRouteOptions{OverrideAddress: M.ParseSocksaddr("other.example:0")})
	require.Equal(t, M.ParseSocksaddr("other.example:8443"), metadata.Destination)
	require.Nil(t, metadata.DestinationAddresses)
	require.Nil(t, metadata.CacheIPs)
}

func TestPreMatchFakeIPReusesCacheIPsForFlowOutbound(t *testing.T) {
	router := &Router{
		ctx:      context.Background(),
		logger:   logger.NOP(),
		outbound: &matchOnlyFlowOutbounds{outbound: &matchOnlyFlowOutbound{}},
	}
	packetDest := M.ParseSocksaddr("198.18.0.1:443")

	t.Run("reuses cache ips when destination addresses are empty", func(t *testing.T) {
		metadata := adapter.InboundContext{
			Network:     N.NetworkTCP,
			Source:      M.ParseSocksaddr("127.0.0.1:10000"),
			Destination: M.ParseSocksaddr("example.com:443"),
			FakeIP:      true,
			CacheIPs:    []netip.Addr{netip.MustParseAddr("2001:db8::1"), netip.MustParseAddr("110.242.68.66")},
		}
		result := router.preMatchFlow(context.Background(), &metadata, packetDest, nil, "")
		require.Equal(t, adapter.PreMatchFlow, result.Action)
		require.Equal(t, netip.MustParseAddrPort("110.242.68.66:443"), result.Destination)
	})

	t.Run("prefers destination addresses over cache ips", func(t *testing.T) {
		metadata := adapter.InboundContext{
			Network:              N.NetworkTCP,
			Source:               M.ParseSocksaddr("127.0.0.1:10000"),
			Destination:          M.ParseSocksaddr("example.com:443"),
			FakeIP:               true,
			DestinationAddresses: []netip.Addr{netip.MustParseAddr("192.0.2.10")},
			CacheIPs:             []netip.Addr{netip.MustParseAddr("110.242.68.66")},
		}
		result := router.preMatchFlow(context.Background(), &metadata, packetDest, nil, "")
		require.Equal(t, adapter.PreMatchFlow, result.Action)
		require.Equal(t, netip.MustParseAddrPort("192.0.2.10:443"), result.Destination)
	})

	t.Run("rejects when neither destination addresses nor cache ips are set", func(t *testing.T) {
		metadata := adapter.InboundContext{
			Network:     N.NetworkTCP,
			Source:      M.ParseSocksaddr("127.0.0.1:10000"),
			Destination: M.ParseSocksaddr("example.com:443"),
			FakeIP:      true,
		}
		result := router.preMatchFlow(context.Background(), &metadata, packetDest, nil, "")
		require.Equal(t, adapter.PreMatchReject, result.Action)
	})

	t.Run("rejects when cache ips do not match address family", func(t *testing.T) {
		metadata := adapter.InboundContext{
			Network:     N.NetworkTCP,
			Source:      M.ParseSocksaddr("127.0.0.1:10000"),
			Destination: M.ParseSocksaddr("example.com:443"),
			FakeIP:      true,
			CacheIPs:    []netip.Addr{netip.MustParseAddr("2001:db8::1")},
		}
		result := router.preMatchFlow(context.Background(), &metadata, packetDest, nil, "")
		require.Equal(t, adapter.PreMatchReject, result.Action)
	})
}
