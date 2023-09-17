package group

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type lifecycleTestProvider struct {
	adapter.Provider
	members   []adapter.Outbound
	callbacks list.List[adapter.ProviderUpdateCallback]
}

func (p *lifecycleTestProvider) Outbounds() []adapter.Outbound { return p.members }
func (p *lifecycleTestProvider) RegisterCallback(callback adapter.ProviderUpdateCallback) *list.Element[adapter.ProviderUpdateCallback] {
	return p.callbacks.PushBack(callback)
}

type lifecycleTestProviderManager struct {
	adapter.ProviderManager
	provider adapter.Provider
}

func (m *lifecycleTestProviderManager) Get(string) (adapter.Provider, bool) { return m.provider, true }

type lifecycleTestOutbound struct {
	outbound.Adapter
	adapter.Outbound
}

// Explicit methods avoid ambiguity with the embedded mock dialer interface.
func (o *lifecycleTestOutbound) Tag() string            { return o.Adapter.Tag() }
func (o *lifecycleTestOutbound) Type() string           { return o.Adapter.Type() }
func (o *lifecycleTestOutbound) Network() []string      { return o.Adapter.Network() }
func (o *lifecycleTestOutbound) Dependencies() []string { return o.Adapter.Dependencies() }

func newLifecycleTestSelector(t *testing.T, defaultTag string, loaded bool) (*Selector, *lifecycleTestProvider, *providerUpdateTestOutboundManager) {
	t.Helper()
	manager := &providerUpdateTestOutboundManager{outbounds: make(map[string]adapter.Outbound)}
	for _, tag := range []string{"test-fallback", "a", "b"} {
		manager.outbounds[tag] = &lifecycleTestOutbound{Adapter: outbound.NewAdapter("test", tag, nil, nil)}
	}
	manager.fallback = manager.outbounds["test-fallback"]
	provider := &lifecycleTestProvider{}
	if loaded {
		provider.members = []adapter.Outbound{manager.outbounds["a"], manager.outbounds["b"]}
	}
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
	ctx = service.ContextWith[adapter.ProviderManager](ctx, &lifecycleTestProviderManager{provider: provider})
	raw, err := NewSelector(ctx, nil, log.NewNOPFactory().NewLogger("test"), "test", option.SelectorOutboundOptions{GroupCommonOption: option.GroupCommonOption{Providers: []string{"p"}}, Default: defaultTag})
	require.NoError(t, err)
	return raw.(*Selector), provider, manager
}

func TestSelectorWaitsForProviderDefault(t *testing.T) {
	selector, provider, manager := newLifecycleTestSelector(t, "b", false)
	require.NoError(t, selector.Start())
	require.Equal(t, "test-fallback", selector.Now())
	provider.members = []adapter.Outbound{manager.outbounds["a"], manager.outbounds["b"]}
	require.NoError(t, selector.onProviderUpdated("p"))
	require.NoError(t, selector.PostStart())
	require.Equal(t, "b", selector.Now())
}

func TestSelectorStillRejectsMissingDefaultAfterProvidersLoad(t *testing.T) {
	selector, _, _ := newLifecycleTestSelector(t, "missing", true)
	require.NoError(t, selector.Start())
	require.ErrorContains(t, selector.PostStart(), "default outbound not found")
}

func TestSelectorUsesAlreadyLoadedProvider(t *testing.T) {
	selector, _, _ := newLifecycleTestSelector(t, "", true)
	require.NoError(t, selector.Start())
	require.Equal(t, []string{"a", "b"}, selector.All())
	require.NoError(t, selector.PostStart())
	require.Equal(t, "a", selector.Now())
}

func TestSelectorRefreshPreservesSelectionWithoutCache(t *testing.T) {
	selector, provider, manager := newLifecycleTestSelector(t, "", true)
	require.NoError(t, selector.Start())
	require.NoError(t, selector.PostStart())
	require.True(t, selector.SelectOutbound("b"))
	require.NoError(t, selector.onProviderUpdated("p"))
	require.Equal(t, "b", selector.Now())
	replacement := &lifecycleTestOutbound{Adapter: outbound.NewAdapter("test", "b", nil, nil)}
	provider.members = []adapter.Outbound{manager.outbounds["a"], replacement}
	require.NoError(t, selector.onProviderUpdated("p"))
	require.Same(t, replacement, selector.selected.Load())
	provider.members = []adapter.Outbound{manager.outbounds["a"]}
	require.NoError(t, selector.onProviderUpdated("p"))
	require.Equal(t, "a", selector.Now())
}
