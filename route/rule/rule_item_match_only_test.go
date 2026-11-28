package rule

import (
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

type preferredAddrOutbound struct {
	adapter.OutboundWithPreferredRoutes
	target netip.Addr
}

func (o *preferredAddrOutbound) PreferredDomain(*adapter.InboundContext, string) bool {
	return false
}

func (o *preferredAddrOutbound) PreferredAddress(_ *adapter.InboundContext, address netip.Addr) bool {
	return address == o.target
}

func TestIPRuleItemsMatchCacheIPs(t *testing.T) {
	t.Parallel()
	prefix := badoption.Prefixable(netip.MustParsePrefix("10.0.0.0/8"))
	cidrItem, err := NewIPCIDRItem(false, []*badoption.Prefixable{&prefix})
	require.NoError(t, err)
	privateItem := NewIPIsPrivateItem(false)
	acceptAnyItem := NewIPAcceptAnyItem()
	preferredItem := &PreferredByItem{
		outbounds: []adapter.OutboundWithPreferredRoutes{
			&preferredAddrOutbound{target: netip.MustParseAddr("10.0.0.1")},
		},
	}

	cacheOnly := &adapter.InboundContext{
		Destination: M.ParseSocksaddr("example.com:443"),
		CacheIPs:    []netip.Addr{netip.MustParseAddr("10.0.0.1")},
	}
	require.True(t, cidrItem.Match(cacheOnly))
	require.True(t, privateItem.Match(cacheOnly))
	require.True(t, acceptAnyItem.Match(cacheOnly))
	require.True(t, preferredItem.Match(cacheOnly))

	cacheOverridesOldDial := &adapter.InboundContext{
		Destination:          M.ParseSocksaddr("example.com:443"),
		DestinationAddresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		CacheIPs:             []netip.Addr{netip.MustParseAddr("8.8.8.8")},
	}
	require.False(t, cidrItem.Match(cacheOverridesOldDial))
	require.False(t, privateItem.Match(cacheOverridesOldDial))
	require.True(t, acceptAnyItem.Match(cacheOverridesOldDial))
	require.False(t, preferredItem.Match(cacheOverridesOldDial))
}
