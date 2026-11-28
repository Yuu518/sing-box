package adapter

import (
	"net"
	"net/netip"
	"testing"

	M "github.com/sagernet/sing/common/metadata"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestDNSResponseAddressesUnmapsHTTPSIPv4Hints(t *testing.T) {
	t.Parallel()

	ipv4Hint := net.ParseIP("1.1.1.1")
	require.NotNil(t, ipv4Hint)

	response := &dns.Msg{
		MsgHdr: dns.MsgHdr{
			Response: true,
			Rcode:    dns.RcodeSuccess,
		},
		Answer: []dns.RR{
			&dns.HTTPS{
				SVCB: dns.SVCB{
					Hdr: dns.RR_Header{
						Name:   dns.Fqdn("example.com"),
						Rrtype: dns.TypeHTTPS,
						Class:  dns.ClassINET,
						Ttl:    60,
					},
					Priority: 1,
					Target:   ".",
					Value: []dns.SVCBKeyValue{
						&dns.SVCBIPv4Hint{Hint: []net.IP{ipv4Hint}},
					},
				},
			},
		},
	}

	addresses := DNSResponseAddresses(response)
	require.Equal(t, []netip.Addr{netip.MustParseAddr("1.1.1.1")}, addresses)
	require.True(t, addresses[0].Is4())
}

func TestInboundContextDomainHost(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		metadata InboundContext
		expected string
	}{
		{
			name: "destination fqdn takes precedence over sniff host and domain",
			metadata: InboundContext{
				Destination: M.ParseSocksaddr("origin.example:443"),
				SniffHost:   "sniff.example",
				Domain:      "reverse.example",
			},
			expected: "origin.example",
		},
		{
			name: "fakeip destination fqdn takes precedence over sniff host",
			metadata: InboundContext{
				Destination: M.ParseSocksaddr("fakeip.example:443"),
				SniffHost:   "sniff.example",
				FakeIP:      true,
			},
			expected: "fakeip.example",
		},
		{
			name: "sniff host takes precedence over reverse mapped domain on ip destination",
			metadata: InboundContext{
				Destination: M.ParseSocksaddr("1.1.1.1:443"),
				SniffHost:   "sniff.example",
				Domain:      "reverse.example",
			},
			expected: "sniff.example",
		},
		{
			name: "falls back to reverse mapped domain",
			metadata: InboundContext{
				Destination: M.ParseSocksaddr("1.1.1.1:443"),
				Domain:      "reverse.example",
			},
			expected: "reverse.example",
		},
		{
			name: "empty when only ip destination is present",
			metadata: InboundContext{
				Destination: M.ParseSocksaddr("1.1.1.1:443"),
			},
			expected: "",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, testCase.metadata.DomainHost())
		})
	}
}

func TestInboundContextMatchDestinationAddresses(t *testing.T) {
	t.Parallel()
	dialAddrs := []netip.Addr{netip.MustParseAddr("192.0.2.1")}
	cacheAddrs := []netip.Addr{netip.MustParseAddr("10.0.0.1")}

	require.Nil(t, (&InboundContext{}).MatchDestinationAddresses())
	require.Equal(t, dialAddrs, (&InboundContext{DestinationAddresses: dialAddrs}).MatchDestinationAddresses())
	require.Equal(t, cacheAddrs, (&InboundContext{CacheIPs: cacheAddrs}).MatchDestinationAddresses())
	require.Equal(t, cacheAddrs, (&InboundContext{DestinationAddresses: dialAddrs, CacheIPs: cacheAddrs}).MatchDestinationAddresses())
}
