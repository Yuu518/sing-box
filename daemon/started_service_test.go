package daemon

import (
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func TestBuildConnectionProtoDomainHost(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		metadata adapter.InboundContext
		expected string
	}{
		{
			name: "destination fqdn over sniff host",
			metadata: adapter.InboundContext{
				Destination: M.ParseSocksaddr("origin.example:443"),
				SniffHost:   "sniff.example.com",
				Domain:      "reverse.example.com",
			},
			expected: "origin.example",
		},
		{
			name: "sniff host over reverse domain",
			metadata: adapter.InboundContext{
				Destination: M.ParseSocksaddr("1.1.1.1:443"),
				SniffHost:   "sniff.example.com",
				Domain:      "reverse.example.com",
			},
			expected: "sniff.example.com",
		},
		{
			name: "reverse domain fallback",
			metadata: adapter.InboundContext{
				Destination: M.ParseSocksaddr("1.1.1.1:443"),
				Domain:      "reverse.example.com",
			},
			expected: "reverse.example.com",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			metadata := &trafficcontrol.TrackerMetadata{
				Metadata: testCase.metadata,
				Upload:   new(atomic.Int64),
				Download: new(atomic.Int64),
			}
			require.Equal(t, testCase.expected, buildConnectionProto(metadata).Domain)
		})
	}
}
