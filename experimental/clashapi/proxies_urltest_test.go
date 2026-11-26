package clashapi

import (
	"context"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"

	"github.com/stretchr/testify/require"
)

type stubClashOutbound struct {
	outbound.Adapter
}

func (s *stubClashOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, nil
}

func (s *stubClashOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, nil
}

type stubClashOutboundManager struct {
	adapter.OutboundManager
	outbounds []adapter.Outbound
}

func (m *stubClashOutboundManager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	return nil
}
func (m *stubClashOutboundManager) Outbounds() []adapter.Outbound { return m.outbounds }
func (m *stubClashOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	for _, ob := range m.outbounds {
		if ob.Tag() == tag {
			return ob, true
		}
	}
	return nil, false
}
func (m *stubClashOutboundManager) Default() adapter.Outbound { return nil }
func (m *stubClashOutboundManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	return nil
}

func TestProxyInfoURLTestAndFallbackType(t *testing.T) {
	leaf := &stubClashOutbound{Adapter: outbound.NewAdapter("stub", "a", []string{N.NetworkTCP}, nil)}
	manager := &stubClashOutboundManager{outbounds: []adapter.Outbound{leaf}}
	history := urltest.NewHistoryStorage()
	ctx := service.ContextWithDefaultRegistry(context.Background())
	ctx = pause.WithDefaultManager(ctx)
	service.MustRegister[adapter.OutboundManager](ctx, manager)
	service.MustRegisterPtr(ctx, history)

	normalRaw, err := group.NewURLTest(ctx, nil, log.NewNOPFactory().Logger(), "auto", option.URLTestOutboundOptions{
		Outbounds: []string{"a"},
	})
	require.NoError(t, err)
	normal := normalRaw.(*group.URLTest)
	require.NoError(t, normal.Start(adapter.StartStateStart, nil))

	fallbackRaw, err := group.NewURLTest(ctx, nil, log.NewNOPFactory().Logger(), "fb", option.URLTestOutboundOptions{
		Outbounds: []string{"a"},
		Fallback:  &option.URLTestFallbackOptions{Enabled: true},
	})
	require.NoError(t, err)
	fallback := fallbackRaw.(*group.URLTest)
	require.NoError(t, fallback.Start(adapter.StartStateStart, nil))

	server := &Server{
		ctx:            ctx,
		outbound:       manager,
		urlTestHistory: history,
	}

	normalInfo := proxyInfo(server, normal)
	normalType, _ := normalInfo.Get("type")
	require.Equal(t, "URLTest", normalType)

	fallbackInfo := proxyInfo(server, fallback)
	fallbackType, _ := fallbackInfo.Get("type")
	require.Equal(t, "Fallback", fallbackType)
}
