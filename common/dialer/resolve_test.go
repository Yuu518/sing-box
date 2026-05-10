package dialer

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxLog "github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type resolveTestRouter struct {
	adapter.DNSRouter
	addresses []netip.Addr
}

func (r *resolveTestRouter) Lookup(context.Context, string, adapter.DNSQueryOptions) ([]netip.Addr, error) {
	return r.addresses, nil
}

type resolveTestNetworkManager struct {
	adapter.NetworkManager
	defaultOptions adapter.NetworkOptions
}

type resolveTestLogFactory struct {
	boxLog.Factory
	logger boxLog.ContextLogger
}

type resolveTestLogger struct {
	*concurrentTestLogger
}

func (l *resolveTestLogger) InfoContext(ctx context.Context, args ...any) {
	if boxLog.OverrideLevelFromContext(boxLog.LevelInfo, ctx) == boxLog.LevelDebug {
		l.DebugContext(ctx, args...)
		return
	}
	l.concurrentTestLogger.InfoContext(ctx, args...)
}

func (f *resolveTestLogFactory) NewLogger(string) boxLog.ContextLogger {
	return f.logger
}

func (m *resolveTestNetworkManager) DefaultOptions() adapter.NetworkOptions {
	return m.defaultOptions
}

func TestResolveDialerUsesConcurrentTCPDialFromNetworkOptions(t *testing.T) {
	slowAddress := netip.MustParseAddr("127.0.0.21")
	fastAddress := netip.MustParseAddr("127.0.0.22")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		slowAddress: {delay: 50 * time.Millisecond},
		fastAddress: {delay: time.Millisecond},
	}}
	testLogger := &resolveTestLogger{newConcurrentTestLogger()}
	ctx := newResolveTestContext([]netip.Addr{slowAddress, fastAddress}, true)
	ctx = service.ContextWith[boxLog.Factory](ctx, &resolveTestLogFactory{logger: testLogger})
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.22:443", conn.RemoteAddr().String())
	require.GreaterOrEqual(t, testDialer.MaxActive(), 2)
	debugMessages, infoMessages := testLogger.Messages()
	require.Equal(t, []string{"concurrent dial tcp example.com:443 with [127.0.0.21 127.0.0.22]"}, debugMessages)
	require.Equal(t, []string{"concurrent dial tcp example.com:443 connected 127.0.0.22:443"}, infoMessages)
	require.NoError(t, conn.Close())
}

func TestResolveDialerKeepsUDPDialSerialWhenConcurrentEnabled(t *testing.T) {
	firstAddress := netip.MustParseAddr("127.0.0.23")
	secondAddress := netip.MustParseAddr("127.0.0.24")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		firstAddress:  {delay: time.Millisecond},
		secondAddress: {delay: time.Millisecond},
	}}
	ctx := newResolveTestContext([]netip.Addr{firstAddress, secondAddress}, true)
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkUDP, M.ParseSocksaddrHostPort("example.com", 443))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.23:443", conn.RemoteAddr().String())
	require.Equal(t, 1, testDialer.MaxActive())
	require.NoError(t, conn.Close())
}

func newResolveTestContext(addresses []netip.Addr, concurrentDial bool) context.Context {
	ctx := service.ContextWith[adapter.DNSRouter](context.Background(), &resolveTestRouter{addresses: addresses})
	return service.ContextWith[adapter.NetworkManager](ctx, &resolveTestNetworkManager{
		defaultOptions: adapter.NetworkOptions{ConcurrentDial: concurrentDial},
	})
}
