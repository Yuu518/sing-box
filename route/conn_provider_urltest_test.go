package route

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type stubProviderOutbound struct {
	outbound.Adapter
}

func (s *stubProviderOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("dial failed")
}

func (s *stubProviderOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("listen failed")
}

type stubURLTestGroup struct {
	adapter.URLTestGroup
	tag    string
	checks atomic.Int32
}

func (s *stubURLTestGroup) Tag() string            { return s.tag }
func (s *stubURLTestGroup) Type() string           { return "urltest" }
func (s *stubURLTestGroup) Network() []string      { return []string{N.NetworkTCP} }
func (s *stubURLTestGroup) Dependencies() []string { return nil }
func (s *stubURLTestGroup) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("dial failed")
}
func (s *stubURLTestGroup) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("listen failed")
}

var _ adapter.URLTestGroup = (*stubURLTestGroup)(nil)

func (s *stubURLTestGroup) PerformUpdateCheck()  { s.checks.Add(1) }
func (s *stubURLTestGroup) ReportFailure(string) { s.checks.Add(1) }

func TestDeleteURLTestHistoryClearsProviderOutbound(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	history := urltest.NewHistoryStorage()
	service.MustRegisterPtr(ctx, history)

	providerNode := &stubProviderOutbound{
		Adapter: outbound.NewAdapter("http", "node-1", []string{N.NetworkTCP}, nil),
	}
	history.AddProviderOutbound(providerNode)
	history.StoreURLTestHistoryForOutbound(providerNode, &adapter.URLTestHistory{
		Time:  time.Now(),
		Delay: 42,
	})
	require.NotNil(t, history.LoadURLTestHistoryForOutbound(providerNode))

	group := &stubURLTestGroup{tag: "auto"}
	manager := NewConnectionManager(ctx, log.NewNOPFactory().NewLogger("connection"))
	manager.deleteURLTestHistory([]adapter.Outbound{group, providerNode}, errors.New("connection refused"))

	require.Nil(t, history.LoadURLTestHistoryForOutbound(providerNode))
	require.Equal(t, int32(1), group.checks.Load())
}
