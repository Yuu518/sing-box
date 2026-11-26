package route

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
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

type stubEarlyConn struct {
	net.Conn
	writeErr error
}

func (c *stubEarlyConn) NeedHandshakeForWrite() bool {
	return true
}

func (c *stubEarlyConn) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(b), nil
}

type stubReadErrorConn struct {
	net.Conn
	readErr error
}

func (c *stubReadErrorConn) Read(b []byte) (int, error) {
	return 0, c.readErr
}

type stubRouteURLTestGroup struct {
	adapter.URLTestGroup
	tag      string
	mu       sync.Mutex
	reported []string
}

func newStubRouteURLTestGroup(tag string) *stubRouteURLTestGroup {
	return &stubRouteURLTestGroup{
		tag: tag,
	}
}

func (g *stubRouteURLTestGroup) Tag() string {
	return g.tag
}

func (g *stubRouteURLTestGroup) Type() string {
	return "urltest"
}

func (g *stubRouteURLTestGroup) Network() []string {
	return []string{N.NetworkTCP, N.NetworkUDP}
}

func (g *stubRouteURLTestGroup) Dependencies() []string {
	return nil
}

func (g *stubRouteURLTestGroup) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("unused")
}

func (g *stubRouteURLTestGroup) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unused")
}

func (g *stubRouteURLTestGroup) All() []string {
	return nil
}

func (g *stubRouteURLTestGroup) Selected(network string) adapter.Outbound {
	return nil
}

func (g *stubRouteURLTestGroup) AttachConnection(closer io.Closer) func() {
	return func() {}
}

func (g *stubRouteURLTestGroup) URLTest(ctx context.Context) (map[string]uint16, error) {
	return nil, nil
}

func (g *stubRouteURLTestGroup) PerformUpdateCheck() {
}

func (g *stubRouteURLTestGroup) ReportFailure(tag string) {
	g.mu.Lock()
	g.reported = append(g.reported, tag)
	g.mu.Unlock()
}

func (g *stubRouteURLTestGroup) Reported() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := make([]string, len(g.reported))
	copy(result, g.reported)
	return result
}

type stubRouteLeafOutbound struct {
	outbound.Adapter
}

func (s *stubRouteLeafOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("unused")
}

func (s *stubRouteLeafOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unused")
}

type stubRouteOutboundManager struct {
	adapter.OutboundManager
	outbounds []adapter.Outbound
}

func (m *stubRouteOutboundManager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	return nil
}
func (m *stubRouteOutboundManager) Outbounds() []adapter.Outbound { return m.outbounds }
func (m *stubRouteOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	for _, ob := range m.outbounds {
		if ob.Tag() == tag {
			return ob, true
		}
	}
	return nil, false
}
func (m *stubRouteOutboundManager) Default() adapter.Outbound { return nil }
func (m *stubRouteOutboundManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	return nil
}

func TestKickWriteHandshakeWriteFailureNotifiesAllURLTestGroups(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	history := urltest.NewHistoryStorage()
	history.StoreURLTestHistory("proxy-a", &adapter.URLTestHistory{Time: time.Now(), Delay: 50})

	chainGroup := newStubRouteURLTestGroup("chain-group")
	sharedGroup := newStubRouteURLTestGroup("shared-group")
	leaf := &stubRouteLeafOutbound{Adapter: outbound.NewAdapter("stub", "proxy-a", []string{N.NetworkTCP}, nil)}
	manager := &stubRouteOutboundManager{outbounds: []adapter.Outbound{chainGroup, sharedGroup, leaf}}

	service.MustRegister[adapter.OutboundManager](ctx, manager)
	service.MustRegisterPtr(ctx, history)

	cm := NewConnectionManager(ctx, log.NewNOPFactory().Logger())
	srcClient, srcServer := net.Pipe()
	defer srcClient.Close()
	defer srcServer.Close()
	go func() {
		_, _ = srcClient.Write([]byte("hello"))
	}()
	dstClient, dstServer := net.Pipe()
	defer dstClient.Close()
	defer dstServer.Close()
	earlyDst := &stubEarlyConn{Conn: dstServer, writeErr: errors.New("proxy handshake failed")}

	var done atomic.Bool
	closed, _ := cm.kickWriteHandshake(ctx, srcServer, earlyDst, true, false, []adapter.Outbound{chainGroup, leaf}, &done, nil)
	require.True(t, closed)
	require.Nil(t, history.LoadURLTestHistory("proxy-a"))
	require.Equal(t, []string{"proxy-a"}, chainGroup.Reported())
	require.Equal(t, []string{"proxy-a"}, sharedGroup.Reported())
}

func TestKickWriteHandshakeSourceReadFailureDoesNotDeleteHistory(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	history := urltest.NewHistoryStorage()
	history.StoreURLTestHistory("proxy-a", &adapter.URLTestHistory{Time: time.Now(), Delay: 50})

	chainGroup := newStubRouteURLTestGroup("chain-group")
	leaf := &stubRouteLeafOutbound{Adapter: outbound.NewAdapter("stub", "proxy-a", []string{N.NetworkTCP}, nil)}
	manager := &stubRouteOutboundManager{outbounds: []adapter.Outbound{chainGroup, leaf}}

	service.MustRegister[adapter.OutboundManager](ctx, manager)
	service.MustRegisterPtr(ctx, history)

	cm := NewConnectionManager(ctx, log.NewNOPFactory().Logger())
	srcClient, srcServer := net.Pipe()
	defer srcClient.Close()
	defer srcServer.Close()
	dstClient, dstServer := net.Pipe()
	defer dstClient.Close()
	defer dstServer.Close()

	badSrc := &stubReadErrorConn{Conn: srcServer, readErr: errors.New("client reset")}
	earlyDst := &stubEarlyConn{Conn: dstServer}

	var done atomic.Bool
	closed, _ := cm.kickWriteHandshake(ctx, badSrc, earlyDst, true, false, []adapter.Outbound{chainGroup, leaf}, &done, nil)
	require.True(t, closed)
	require.NotNil(t, history.LoadURLTestHistory("proxy-a"))
	require.Empty(t, chainGroup.Reported())
}
