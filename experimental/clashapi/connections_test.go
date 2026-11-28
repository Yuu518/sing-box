package clashapi

import (
	"encoding/json"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func TestConnectionObjectMarshalJSONMetadata(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name                  string
		metadata              adapter.InboundContext
		expectedHost          string
		expectedSniffHost     string
		expectedDestinationIP string
	}{
		{
			name: "real ip with sniff host only",
			metadata: adapter.InboundContext{
				Destination: M.ParseSocksaddr("1.1.1.1:443"),
				SniffHost:   "sniff.example.com",
			},
			expectedHost:          "",
			expectedSniffHost:     "sniff.example.com",
			expectedDestinationIP: "1.1.1.1",
		},
		{
			name: "real ip with reverse domain and sniff host",
			metadata: adapter.InboundContext{
				Destination: M.ParseSocksaddr("1.1.1.1:443"),
				Domain:      "reverse.example.com",
				SniffHost:   "sniff.example.com",
			},
			expectedHost:          "reverse.example.com",
			expectedSniffHost:     "sniff.example.com",
			expectedDestinationIP: "1.1.1.1",
		},
		{
			name: "fqdn with resolved destination address and sniff host",
			metadata: adapter.InboundContext{
				Destination:          M.ParseSocksaddr("origin.example.com:443"),
				DestinationAddresses: []netip.Addr{netip.MustParseAddr("93.184.216.34")},
				SniffHost:            "sniff.example.com",
			},
			expectedHost:          "origin.example.com",
			expectedSniffHost:     "sniff.example.com",
			expectedDestinationIP: "93.184.216.34",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			obj := connectionObject(trafficcontrol.TrackerMetadata{
				Metadata: testCase.metadata,
				Upload:   new(atomic.Int64),
				Download: new(atomic.Int64),
			})
			raw, err := json.Marshal(obj)
			require.NoError(t, err)
			var parsed struct {
				Metadata struct {
					Host          string `json:"host"`
					SniffHost     string `json:"sniffHost"`
					DestinationIP string `json:"destinationIP"`
				} `json:"metadata"`
			}
			require.NoError(t, json.Unmarshal(raw, &parsed))
			require.Equal(t, testCase.expectedHost, parsed.Metadata.Host)
			require.Equal(t, testCase.expectedSniffHost, parsed.Metadata.SniffHost)
			require.Equal(t, testCase.expectedDestinationIP, parsed.Metadata.DestinationIP)
		})
	}
}
