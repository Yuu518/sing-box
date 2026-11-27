package dialer

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type detourTestOutbound struct {
	outbound.Adapter
	dial func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
}

func (o *detourTestOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return o.dial(ctx, network, destination)
}

func (o *detourTestOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unsupported")
}

type detourTestManager struct {
	adapter.OutboundManager
	byTag map[string]adapter.Outbound
}

func (m *detourTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	ob, ok := m.byTag[tag]
	return ob, ok
}

type detourTestScope struct {
	byTag map[string]adapter.Outbound
}

func (s *detourTestScope) Outbound(tag string) (adapter.Outbound, bool) {
	ob, ok := s.byTag[tag]
	return ob, ok
}

func TestDetourDialerCycleDetection(t *testing.T) {
	manager := &detourTestManager{byTag: make(map[string]adapter.Outbound)}
	var group adapter.Outbound
	nodeDialer := NewDetour(manager, "group", true)
	node := &detourTestOutbound{
		Adapter: outbound.NewAdapter("http", "node", []string{N.NetworkTCP}, []string{"group"}),
		dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			return nodeDialer.DialContext(ctx, network, destination)
		},
	}
	group = &detourTestOutbound{
		Adapter: outbound.NewAdapter("selector", "group", []string{N.NetworkTCP}, nil),
		dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			return node.DialContext(ctx, network, destination)
		},
	}
	manager.byTag["group"] = group

	_, err := group.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:80"))
	require.ErrorContains(t, err, "circular outbound detour")
}

func TestDetourLocalOnlyIgnoresProviderScope(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	localCalled := false
	providerCalled := false
	localNode := &detourTestOutbound{
		Adapter: outbound.NewAdapter("http", "HK", []string{N.NetworkTCP}, nil),
		dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			localCalled = true
			return nil, errors.New("local reached")
		},
	}
	providerNode := &detourTestOutbound{
		Adapter: outbound.NewAdapter("http", "HK", []string{N.NetworkTCP}, nil),
		dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			providerCalled = true
			return nil, errors.New("provider reached")
		},
	}
	service.MustRegister[adapter.OutboundManager](ctx, &detourTestManager{
		byTag: map[string]adapter.Outbound{"HK": localNode},
	})
	ctx = adapter.ContextWithOutboundScope(ctx, &detourTestScope{
		byTag: map[string]adapter.Outbound{"HK": providerNode},
	})

	d, err := NewWithOptions(Options{
		Context: ctx,
		Options: option.DialerOptions{
			Detour:          "HK",
			DetourLocalOnly: true,
		},
		DisableEmptyDirectCheck: true,
	})
	require.NoError(t, err)
	_, err = d.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:80"))
	require.ErrorContains(t, err, "local reached")
	require.True(t, localCalled)
	require.False(t, providerCalled)
}
