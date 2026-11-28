package dns

import (
	"context"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"

	mDNS "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestDNSQueriesIgnoreConnectionCacheIPs(t *testing.T) {
	parent := &adapter.InboundContext{
		CacheIPs: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
	}
	ctx := adapter.WithContext(context.Background(), parent)

	router := raceTestRouter(t)
	message := new(mDNS.Msg)
	message.SetQuestion("query.example.", mDNS.TypeA)
	exchange, _, err := router.prepareExchange(ctx, message)
	require.NoError(t, err)
	require.Nil(t, exchange.metadata.CacheIPs)
	require.Equal(t, []netip.Addr{netip.MustParseAddr("10.0.0.1")}, parent.CacheIPs)

	selected := &fakeDNSTransport{tag: "selected", address: netip.MustParseAddr("192.0.2.1")}
	fallback := &fakeDNSTransport{tag: "final", address: netip.MustParseAddr("192.0.2.2")}
	lookupRouter := raceTestRouter(t, selected, fallback)
	prefix := badoption.Prefixable(netip.MustParsePrefix("10.0.0.0/8"))
	rawRule := routeRule("selected", false)
	rawRule.DefaultOptions.RawDefaultDNSRule = option.RawDefaultDNSRule{IPCIDR: badoption.Listable[*badoption.Prefixable]{&prefix}}
	lookupRouter.rules = raceTestRules(t, []option.DNSRule{rawRule})

	addresses, err := lookupRouter.Lookup(ctx, "query.example", adapter.DNSQueryOptions{Strategy: C.DomainStrategyIPv4Only})
	require.NoError(t, err)
	require.Equal(t, []netip.Addr{fallback.address}, addresses)
	require.Zero(t, selected.queryCount.Load())
	require.Positive(t, fallback.queryCount.Load())
	require.Equal(t, []netip.Addr{netip.MustParseAddr("10.0.0.1")}, parent.CacheIPs)
}
