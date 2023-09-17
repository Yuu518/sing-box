package provider

import (
	"context"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type ownershipTestOutbound struct {
	outbound.Adapter
	N.Dialer
}

func (*ownershipTestOutbound) Close() error                   { return nil }
func (*ownershipTestOutbound) Start(adapter.StartStage) error { return nil }

func newOwnershipTestManagers() (*outbound.Manager, *endpoint.Manager) {
	logger := log.NewNOPFactory().NewLogger("test")
	registry := outbound.NewRegistry()
	outbound.Register[option.DirectOutboundOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.DirectOutboundOptions) (adapter.Outbound, error) {
		return &ownershipTestOutbound{Adapter: outbound.NewAdapter("test", tag, []string{N.NetworkTCP}, nil)}, nil
	})
	endpointRegistry := endpoint.NewRegistry()
	endpoint.Register[option.DirectOutboundOptions](endpointRegistry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.DirectOutboundOptions) (adapter.Endpoint, error) {
		return &ownershipTestOutbound{Adapter: outbound.NewAdapter("test", tag, []string{N.NetworkTCP}, nil)}, nil
	})
	endpoints := endpoint.NewManager(logger, endpointRegistry)
	return outbound.NewManager(logger, registry, endpoints, ""), endpoints
}

func newOwnershipTestProvider(tag string, manager adapter.OutboundManager, endpoints adapter.EndpointManager) *Adapter {
	logger := log.NewNOPFactory()
	provider := NewAdapter(context.Background(), nil, manager, endpoints, logger, logger.NewLogger(tag), tag, "test", option.ProviderHealthCheckOptions{})
	return &provider
}

func ownershipTestOptions() []option.Outbound {
	return []option.Outbound{{Tag: "shared", Type: "test", Options: &option.DirectOutboundOptions{}}}
}

func TestProviderRejectsStaticTagCollision(t *testing.T) {
	manager, endpoints := newOwnershipTestManagers()
	require.NoError(t, manager.Create(context.Background(), nil, log.NewNOPFactory().NewLogger("test"), "shared", "test", &option.DirectOutboundOptions{}))
	original, _ := manager.Outbound("shared")
	provider := newOwnershipTestProvider("p", manager, endpoints)
	provider.UpdateOutbounds(nil, ownershipTestOptions())
	provider.UpdateEndpoints(nil, []option.Endpoint{{Tag: "shared", Type: "test", Options: &option.DirectOutboundOptions{}}})
	require.Empty(t, provider.Outbounds())
	require.NoError(t, provider.Close())
	current, found := manager.Outbound("shared")
	require.True(t, found)
	require.Same(t, original, current)
}

func TestConcurrentProvidersCannotReplaceOrRemoveEachOther(t *testing.T) {
	manager, endpoints := newOwnershipTestManagers()
	first := newOwnershipTestProvider("first", manager, endpoints)
	second := newOwnershipTestProvider("second", manager, endpoints)
	var tasks sync.WaitGroup
	for _, provider := range []*Adapter{first, second} {
		tasks.Go(func() { provider.UpdateOutbounds(nil, ownershipTestOptions()) })
	}
	tasks.Wait()
	winner, loser := first, second
	if len(first.Outbounds()) == 0 {
		winner, loser = second, first
	}
	require.Len(t, winner.Outbounds(), 1)
	require.Empty(t, loser.Outbounds())
	original := winner.Outbounds()[0]
	loser.UpdateOutbounds(ownershipTestOptions(), nil)
	require.NoError(t, loser.Close())
	current, found := manager.Outbound("shared")
	require.True(t, found)
	require.Same(t, original, current)
	winner.UpdateOutbounds(ownershipTestOptions(), ownershipTestOptions())
	require.Same(t, original, winner.Outbounds()[0])
	require.NoError(t, winner.Close())
	_, found = manager.Outbound("shared")
	require.False(t, found)
}

func TestProviderCloseDoesNotRemoveExternalReplacement(t *testing.T) {
	manager, endpoints := newOwnershipTestManagers()
	provider := newOwnershipTestProvider("p", manager, endpoints)
	provider.UpdateOutbounds(nil, ownershipTestOptions())
	previous := provider.Outbounds()[0]
	require.NoError(t, manager.Create(context.Background(), nil, log.NewNOPFactory().NewLogger("test"), "shared", "test", &option.DirectOutboundOptions{}))
	current, _ := manager.Outbound("shared")
	require.NotSame(t, previous, current)
	require.ErrorContains(t, manager.Create(adapter.ContextWithProviderUpdate(context.Background(), previous), nil, log.NewNOPFactory().NewLogger("test"), "shared", "test", &option.DirectOutboundOptions{}), "owned by another")
	require.NoError(t, provider.Close())
	afterClose, found := manager.Outbound("shared")
	require.True(t, found)
	require.Same(t, current, afterClose)
}

func TestProviderRejectsEndpointCollision(t *testing.T) {
	manager, endpoints := newOwnershipTestManagers()
	first := newOwnershipTestProvider("first", manager, endpoints)
	second := newOwnershipTestProvider("second", manager, endpoints)
	opts := []option.Endpoint{{Tag: "shared", Type: "test", Options: &option.DirectOutboundOptions{}}}
	first.UpdateEndpoints(nil, opts)
	second.UpdateEndpoints(nil, opts)
	second.UpdateOutbounds(nil, ownershipTestOptions())
	require.Len(t, first.Outbounds(), 1)
	require.Empty(t, second.Outbounds())
	require.NoError(t, second.Close())
	_, found := endpoints.Get("shared")
	require.True(t, found)
	require.NoError(t, first.Close())
	_, found = endpoints.Get("shared")
	require.False(t, found)
}

func TestProviderCloseAfterManagersClose(t *testing.T) {
	manager, endpoints := newOwnershipTestManagers()
	provider := newOwnershipTestProvider("p", manager, endpoints)
	provider.UpdateOutbounds(nil, ownershipTestOptions())
	provider.UpdateEndpoints(nil, []option.Endpoint{{Tag: "endpoint", Type: "test", Options: &option.DirectOutboundOptions{}}})
	require.NoError(t, manager.Start(adapter.StartStateInitialize))
	require.NoError(t, endpoints.Start(adapter.StartStateInitialize))
	require.NoError(t, endpoints.Close())
	require.NoError(t, manager.Close())
	require.NotPanics(t, func() { require.NoError(t, provider.Close()) })
}
