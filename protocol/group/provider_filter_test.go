package group

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"

	"github.com/dlclark/regexp2/v2"
	"github.com/stretchr/testify/require"
)

func TestProviderGroupRegexpFilters(t *testing.T) {
	for _, groupType := range []string{"selector", "urltest"} {
		t.Run(groupType, func(t *testing.T) {
			manager := &providerUpdateTestOutboundManager{
				outbounds: map[string]adapter.Outbound{"static": &providerUpdateTestOutbound{tag: "static"}},
				fallback:  &providerUpdateTestOutbound{tag: "fallback"},
			}
			provider := &lifecycleTestProvider{}
			for _, tag := range []string{"🇺🇸 US premium", "🇺🇸 dMiT", "🇺🇸 US blocked", "🇯🇵 JP premium"} {
				provider.members = append(provider.members, &providerUpdateTestOutbound{tag: tag})
			}
			ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
			ctx = service.ContextWithPtr(ctx, urltest.NewHistoryStorage())
			ctx = service.ContextWith[adapter.ProviderManager](ctx, &lifecycleTestProviderManager{provider: provider})
			logger := log.NewNOPFactory().NewLogger("test")
			var options option.GroupCommonOption
			require.NoError(t, json.Unmarshal([]byte(`{"outbounds":["static"],"providers":["p"],"include":"^(?!.*(?i:DMIT)).*🇺🇸","exclude":"(?<=US )blocked"}`), &options))
			var all func() []string
			var update func(string) error
			var setFilters func(exclude, include *regexp2.Regexp)
			if groupType == "selector" {
				raw, err := NewSelector(ctx, nil, logger, "test", option.SelectorOutboundOptions{GroupCommonOption: options})
				require.NoError(t, err)
				selector := raw.(*Selector)
				require.NoError(t, selector.Start())
				all, update = selector.All, selector.onProviderUpdated
				setFilters = func(exclude, include *regexp2.Regexp) { selector.exclude, selector.include = exclude, include }
			} else {
				raw, err := NewURLTest(ctx, nil, logger, "test", option.URLTestOutboundOptions{GroupCommonOption: options})
				require.NoError(t, err)
				urlTest := raw.(*URLTest)
				require.NoError(t, urlTest.Start())
				t.Cleanup(func() { require.NoError(t, urlTest.Close()) })
				all, update = urlTest.All, urlTest.onProviderUpdated
				setFilters = func(exclude, include *regexp2.Regexp) { urlTest.exclude, urlTest.include = exclude, include }
			}
			require.Equal(t, []string{"static", "🇺🇸 US premium"}, all())
			provider.members = append(provider.members, &providerUpdateTestOutbound{tag: "🇺🇸 US new"})
			require.NoError(t, update("p"))
			expected := []string{"static", "🇺🇸 US premium", "🇺🇸 US new"}
			require.Equal(t, expected, all())
			expression := regexp2.MustCompile(`^(a+)+$`)
			expression.MatchTimeout = -time.Second
			for _, field := range []string{"include", "exclude"} {
				if field == "include" {
					setFilters(options.Exclude.Build(), expression)
				} else {
					setFilters(expression, options.Include.Build())
				}
				provider.members = []adapter.Outbound{&providerUpdateTestOutbound{tag: strings.Repeat("a", 100) + "!"}}
				require.ErrorContains(t, update("p"), "filter provider p")
				require.Equal(t, expected, all())
			}
		})
	}
}
