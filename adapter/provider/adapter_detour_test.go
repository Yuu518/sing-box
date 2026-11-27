package provider_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	providerAdapter "github.com/sagernet/sing-box/adapter/provider"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type chainTestOptions struct {
	option.DialerOptions
	blockOnDial bool
}

type chainTestOutbound struct {
	outbound.Adapter
	dialer      N.Dialer
	startOrder  *[]string
	startMu     *sync.Mutex
	blockOnDial bool
	dialStarted chan struct{}
}

func (o *chainTestOutbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateStart {
		if o.dialer != nil {
			if err := dialer.InitializeDetour(o.dialer); err != nil {
				return err
			}
		}
		if o.startOrder != nil {
			o.startMu.Lock()
			*o.startOrder = append(*o.startOrder, o.Tag())
			o.startMu.Unlock()
		}
	}
	return nil
}

func (o *chainTestOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if o.blockOnDial {
		if o.dialStarted != nil {
			select {
			case <-o.dialStarted:
			default:
				close(o.dialStarted)
			}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if o.dialer != nil {
		return o.dialer.DialContext(ctx, network, destination)
	}
	return nil, errors.New("no dialer")
}

func (o *chainTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unsupported")
}

type testOutboundManager struct {
	adapter.OutboundManager
}

func (m *testOutboundManager) Outbound(string) (adapter.Outbound, bool) {
	return nil, false
}

func (m *testOutboundManager) Outbounds() []adapter.Outbound {
	return nil
}

func TestAdapterTopologicalDetourAndCycleRejection(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	registry := outbound.NewRegistry()
	var (
		startMu    sync.Mutex
		startOrder []string
	)
	outbound.Register[chainTestOptions](registry, "chain-test", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options chainTestOptions) (adapter.Outbound, error) {
		var d N.Dialer
		var err error
		if options.Detour != "" {
			d, err = dialer.NewWithOptions(dialer.Options{
				Context:                 ctx,
				Options:                 options.DialerOptions,
				DisableEmptyDirectCheck: true,
			})
			if err != nil {
				return nil, err
			}
		}
		return &chainTestOutbound{
			Adapter:    outbound.NewAdapterWithDialerOptions("chain-test", tag, []string{N.NetworkTCP}, options.DialerOptions),
			dialer:     d,
			startOrder: &startOrder,
			startMu:    &startMu,
		}, nil
	})
	service.MustRegister[adapter.OutboundRegistry](ctx, registry)
	service.MustRegister[adapter.OutboundManager](ctx, &testOutboundManager{})

	logFactory := log.NewNOPFactory()
	a := providerAdapter.NewAdapter(ctx, nil, logFactory, logFactory.NewLogger("provider"), "p", "remote", option.ProviderHealthCheckOptions{})
	defer func() { _ = a.Close() }()

	require.NoError(t, a.StartStage(adapter.StartStateInitialize))
	require.NoError(t, a.StartStage(adapter.StartStateStart))

	a.Update([]option.Outbound{
		{Type: "chain-test", Tag: "landing", Options: &chainTestOptions{DialerOptions: option.DialerOptions{Detour: "front"}}},
		{Type: "chain-test", Tag: "front", Options: &chainTestOptions{}},
		{Type: "chain-test", Tag: "self-loop", Options: &chainTestOptions{DialerOptions: option.DialerOptions{Detour: "self-loop"}}},
		{Type: "chain-test", Tag: "cycle-a", Options: &chainTestOptions{DialerOptions: option.DialerOptions{Detour: "cycle-b"}}},
		{Type: "chain-test", Tag: "cycle-b", Options: &chainTestOptions{DialerOptions: option.DialerOptions{Detour: "cycle-a"}}},
	}, nil)

	outbounds := a.Outbounds()
	require.Len(t, outbounds, 2)
	_, hasLanding := a.Outbound("landing")
	_, hasFront := a.Outbound("front")
	require.True(t, hasLanding)
	require.True(t, hasFront)
	_, hasSelf := a.Outbound("self-loop")
	_, hasCycleA := a.Outbound("cycle-a")
	_, hasCycleB := a.Outbound("cycle-b")
	require.False(t, hasSelf)
	require.False(t, hasCycleA)
	require.False(t, hasCycleB)
	require.Equal(t, []string{"front", "landing"}, startOrder)
}

func TestAdapterCloseCancelsRunningHealthCheck(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	registry := outbound.NewRegistry()
	dialStarted := make(chan struct{})
	outbound.Register[chainTestOptions](registry, "chain-test", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options chainTestOptions) (adapter.Outbound, error) {
		return &chainTestOutbound{
			Adapter:     outbound.NewAdapter("chain-test", tag, []string{N.NetworkTCP}, nil),
			blockOnDial: options.blockOnDial,
			dialStarted: dialStarted,
		}, nil
	})
	service.MustRegister[adapter.OutboundRegistry](ctx, registry)
	service.MustRegisterPtr(ctx, urltest.NewHistoryStorage())

	logFactory := log.NewNOPFactory()
	a := providerAdapter.NewAdapter(ctx, nil, logFactory, logFactory.NewLogger("provider"), "p", "remote", option.ProviderHealthCheckOptions{
		Enabled: true,
		Timeout: option.ProviderHealthCheckOptions{}.Timeout,
	})
	a.Update([]option.Outbound{
		{Type: "chain-test", Tag: "slow", Options: &chainTestOptions{blockOnDial: true}},
	}, nil)

	require.NoError(t, a.StartStage(adapter.StartStateInitialize))
	require.NoError(t, a.StartStage(adapter.StartStateStart))
	require.NoError(t, a.StartStage(adapter.StartStatePostStart))

	select {
	case <-dialStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("health check did not start")
	}

	done := make(chan error, 1)
	go func() {
		done <- a.Close()
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("adapter close timed out")
	}
}

func TestManagerRejectsDuplicateAndClearsOnClose(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	registry := providerAdapter.NewRegistry()
	providerAdapter.Register[struct{}](registry, "stub", func(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, _ struct{}) (adapter.Provider, error) {
		return &stubProvider{
			Adapter: providerAdapter.NewAdapter(ctx, router, logFactory, logFactory.NewLogger(tag), tag, "stub", option.ProviderHealthCheckOptions{}),
		}, nil
	})
	logFactory := log.NewNOPFactory()
	manager := providerAdapter.NewManager(ctx, logFactory.NewLogger("provider"), registry)
	require.NoError(t, manager.Create(ctx, nil, logFactory, "dup", "stub", &struct{}{}))
	require.ErrorContains(t, manager.Create(ctx, nil, logFactory, "dup", "stub", &struct{}{}), "duplicate outbound provider tag")

	scope := adapter.NewScope(ctx, logFactory.NewLogger("test"))
	require.NoError(t, manager.Start(adapter.StartStateInitialize, scope))
	_, found := manager.Get("dup")
	require.True(t, found)
	require.NoError(t, scope.Close())
	_, found = manager.Get("dup")
	require.False(t, found)
}

type stubProvider struct {
	providerAdapter.Adapter
}

func (p *stubProvider) UpdatedAt() time.Time {
	return time.Time{}
}
