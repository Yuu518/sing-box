package remote

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

type filterTestEndpoint struct {
	cacheTestOutbound
}

func (*filterTestEndpoint) Start(adapter.StartStage) error { return nil }

func TestProviderRegexpFiltersOutboundsAndEndpoints(t *testing.T) {
	var options option.ProviderRemoteOptions
	require.NoError(t, json.Unmarshal([]byte(`{"include":"^(?!.*(?i:DMIT)).*🇺🇸","exclude":"(?<=US )blocked"}`), &options))
	registry := endpoint.NewRegistry()
	endpoint.Register[option.WireGuardEndpointOptions](registry, "wireguard", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.WireGuardEndpointOptions) (adapter.Endpoint, error) {
		return &filterTestEndpoint{cacheTestOutbound: cacheTestOutbound{Adapter: outbound.NewAdapter("wireguard", tag, nil, nil)}}, nil
	})
	provider := newCacheTestProviderWithEndpoints(t, nil, options, registry)
	content := `{"outbounds":[
		{"type":"http","tag":"🇺🇸 US premium","server":"example.com","server_port":443},
		{"type":"http","tag":"🇺🇸 dMiT","server":"example.com","server_port":443},
		{"type":"http","tag":"🇺🇸 US blocked","server":"example.com","server_port":443},
		{"type":"http","tag":"🇯🇵 JP premium","server":"example.com","server_port":443}
	],"endpoints":[
		{"type":"wireguard","tag":"🇺🇸 US endpoint"},
		{"type":"wireguard","tag":"🇺🇸 dMiT endpoint"},
		{"type":"wireguard","tag":"🇺🇸 US blocked endpoint"},
		{"type":"wireguard","tag":"🇯🇵 JP endpoint"}
	]}`
	require.NoError(t, provider.updateProviderFromContent(content))
	require.Len(t, provider.lastOutOpts, 1)
	require.Equal(t, "🇺🇸 US premium", provider.lastOutOpts[0].Tag)
	require.Len(t, provider.lastEPOpts, 1)
	require.Equal(t, "🇺🇸 US endpoint", provider.lastEPOpts[0].Tag)
	require.Len(t, provider.Outbounds(), 2)
	require.Equal(t, "🇺🇸 US premium", provider.Outbounds()[0].Tag())
	require.Equal(t, "🇺🇸 US endpoint", provider.Outbounds()[1].Tag())
}

func TestProviderFilterTimeoutPreservesMembers(t *testing.T) {
	for _, field := range []string{"include", "exclude"} {
		t.Run(field, func(t *testing.T) {
			provider := newCacheTestProvider(t, nil, option.ProviderRemoteOptions{})
			require.NoError(t, provider.updateProviderFromContent(rawTestSubscription))
			previous := provider.Outbounds()
			expression := newTestRegexp(t, `^(a+)+$`).Build()
			expression.MatchTimeout = -time.Second
			if field == "include" {
				provider.include = expression
			} else {
				provider.exclude = expression
			}
			content := strings.Replace(rawTestSubscription, `"tag":"b"`, `"tag":"`+strings.Repeat("a", 100)+`!"`, 1)
			require.ErrorContains(t, provider.updateProviderFromContent(content), "match "+field)
			require.Equal(t, previous, provider.Outbounds())
			require.Equal(t, "a", provider.lastOutOpts[0].Tag)
			require.Equal(t, "b", provider.lastOutOpts[1].Tag)
		})
	}
}
