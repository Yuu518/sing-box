package route

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	commonDialer "github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

var _ commonDialer.ConcurrentNetworkDialer = (*connectionTestDialer)(nil)

type connectionTestDialer struct {
	access           sync.Mutex
	serialDialed     bool
	concurrentDialed bool
	conns            []net.Conn
}

func (d *connectionTestDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	d.access.Lock()
	d.serialDialed = true
	d.access.Unlock()
	return d.newConn(), nil
}

func (d *connectionTestDialer) DialConcurrentNetwork(context.Context, string, M.Socksaddr, []netip.Addr, *C.NetworkStrategy, []C.InterfaceType, []C.InterfaceType, time.Duration) (net.Conn, error) {
	d.access.Lock()
	d.concurrentDialed = true
	d.access.Unlock()
	return d.newConn(), nil
}

func (d *connectionTestDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	panic("unexpected ListenPacket")
}

func (d *connectionTestDialer) newConn() net.Conn {
	conn, peer := net.Pipe()
	d.access.Lock()
	d.conns = append(d.conns, conn, peer)
	d.access.Unlock()
	return conn
}

func (d *connectionTestDialer) closeAll() {
	d.access.Lock()
	defer d.access.Unlock()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
}

func (d *connectionTestDialer) status() (serialDialed bool, concurrentDialed bool) {
	d.access.Lock()
	defer d.access.Unlock()
	return d.serialDialed, d.concurrentDialed
}

func TestConnectionManagerUsesConcurrentDialForTCPDestinationAddresses(t *testing.T) {
	t.Parallel()

	ctx := newConnectionTestContext(t)
	testDialer := &connectionTestDialer{}
	defer testDialer.closeAll()
	clientConn, inboundConn := net.Pipe()
	defer clientConn.Close()
	defer inboundConn.Close()

	connectionManager := NewConnectionManager(log.NewNOPFactory().NewLogger("connection"), true)
	connectionManager.NewConnection(ctx, testDialer, inboundConn, adapter.InboundContext{
		Destination: M.ParseSocksaddrHostPort("example.com", 443),
		DestinationAddresses: []netip.Addr{
			netip.MustParseAddr("127.0.0.11"),
			netip.MustParseAddr("127.0.0.12"),
		},
	}, nil)

	serialDialed, concurrentDialed := testDialer.status()
	require.False(t, serialDialed)
	require.True(t, concurrentDialed)
}

func TestConnectionManagerKeepsSerialDialByDefault(t *testing.T) {
	t.Parallel()

	ctx := newConnectionTestContext(t)
	testDialer := &connectionTestDialer{}
	defer testDialer.closeAll()
	clientConn, inboundConn := net.Pipe()
	defer clientConn.Close()
	defer inboundConn.Close()

	connectionManager := NewConnectionManager(log.NewNOPFactory().NewLogger("connection"), false)
	connectionManager.NewConnection(ctx, testDialer, inboundConn, adapter.InboundContext{
		Destination: M.ParseSocksaddrHostPort("example.com", 443),
		DestinationAddresses: []netip.Addr{
			netip.MustParseAddr("127.0.0.13"),
			netip.MustParseAddr("127.0.0.14"),
		},
	}, nil)

	serialDialed, concurrentDialed := testDialer.status()
	require.True(t, serialDialed)
	require.False(t, concurrentDialed)
}

func newConnectionTestContext(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}
