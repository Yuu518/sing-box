package route

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

type connectionTestDialer struct {
	access       sync.Mutex
	destinations []M.Socksaddr
	conns        []net.Conn
}

func (d *connectionTestDialer) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	d.access.Lock()
	d.destinations = append(d.destinations, destination)
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

func (d *connectionTestDialer) dialedDestinations() []M.Socksaddr {
	d.access.Lock()
	defer d.access.Unlock()
	return append([]M.Socksaddr(nil), d.destinations...)
}

func TestConnectionManagerUsesResolvedTCPDestinationAddress(t *testing.T) {
	t.Parallel()

	ctx := newConnectionTestContext(t)
	testDialer := &connectionTestDialer{}
	defer testDialer.closeAll()
	clientConn, inboundConn := net.Pipe()
	defer clientConn.Close()
	defer inboundConn.Close()

	connectionManager := NewConnectionManager(log.NewNOPFactory().NewLogger("connection"))
	connectionManager.NewConnection(ctx, testDialer, inboundConn, adapter.InboundContext{
		Destination: M.ParseSocksaddrHostPort("example.com", 443),
		DestinationAddresses: []netip.Addr{
			netip.MustParseAddr("127.0.0.11"),
			netip.MustParseAddr("127.0.0.12"),
		},
	}, nil)

	require.Equal(t, []M.Socksaddr{M.ParseSocksaddrHostPort("127.0.0.11", 443)}, testDialer.dialedDestinations())
}

func TestConnectionManagerPreservesUnresolvedTCPDestinationDomain(t *testing.T) {
	t.Parallel()

	ctx := newConnectionTestContext(t)
	testDialer := &connectionTestDialer{}
	defer testDialer.closeAll()
	clientConn, inboundConn := net.Pipe()
	defer clientConn.Close()
	defer inboundConn.Close()

	connectionManager := NewConnectionManager(log.NewNOPFactory().NewLogger("connection"))
	connectionManager.NewConnection(ctx, testDialer, inboundConn, adapter.InboundContext{
		Destination: M.ParseSocksaddrHostPort("example.com", 443),
	}, nil)

	require.Equal(t, []M.Socksaddr{M.ParseSocksaddrHostPort("example.com", 443)}, testDialer.dialedDestinations())
}

func newConnectionTestContext(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}
