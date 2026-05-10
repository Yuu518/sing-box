package dialer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	L "github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type concurrentTestDialer struct {
	behaviors map[netip.Addr]concurrentTestBehavior
	access    sync.Mutex
	active    int
	maxActive int
}

type concurrentTestBehavior struct {
	delay         time.Duration
	err           error
	ignoreContext bool
	closed        chan struct{}
}

type concurrentTestLogger struct {
	L.ContextLogger
	access        sync.Mutex
	debugMessages []string
	infoMessages  []string
}

func newConcurrentTestLogger() *concurrentTestLogger {
	return &concurrentTestLogger{ContextLogger: L.NOP()}
}

func (l *concurrentTestLogger) DebugContext(_ context.Context, args ...any) {
	l.access.Lock()
	defer l.access.Unlock()
	l.debugMessages = append(l.debugMessages, fmt.Sprint(args...))
}

func (l *concurrentTestLogger) InfoContext(_ context.Context, args ...any) {
	l.access.Lock()
	defer l.access.Unlock()
	l.infoMessages = append(l.infoMessages, fmt.Sprint(args...))
}

func (l *concurrentTestLogger) Messages() ([]string, []string) {
	l.access.Lock()
	defer l.access.Unlock()
	return append([]string(nil), l.debugMessages...), append([]string(nil), l.infoMessages...)
}

func (d *concurrentTestDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	d.access.Lock()
	d.active++
	if d.active > d.maxActive {
		d.maxActive = d.active
	}
	d.access.Unlock()
	defer func() {
		d.access.Lock()
		d.active--
		d.access.Unlock()
	}()

	behavior := d.behaviors[destination.Addr]
	if behavior.ignoreContext {
		time.Sleep(behavior.delay)
	} else {
		select {
		case <-time.After(behavior.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if behavior.err != nil {
		return nil, behavior.err
	}
	return &concurrentTestConn{
		remoteAddr: concurrentTestAddr(destination.String()),
		closed:     behavior.closed,
	}, nil
}

func (d *concurrentTestDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unexpected ListenPacket")
}

func (d *concurrentTestDialer) MaxActive() int {
	d.access.Lock()
	defer d.access.Unlock()
	return d.maxActive
}

type concurrentTestConn struct {
	remoteAddr net.Addr
	closeOnce  sync.Once
	closed     chan struct{}
}

func (c *concurrentTestConn) Read([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func (c *concurrentTestConn) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func (c *concurrentTestConn) Close() error {
	c.closeOnce.Do(func() {
		if c.closed != nil {
			close(c.closed)
		}
	})
	return nil
}

func (c *concurrentTestConn) LocalAddr() net.Addr {
	return concurrentTestAddr("local")
}

func (c *concurrentTestConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *concurrentTestConn) SetDeadline(time.Time) error {
	return nil
}

func (c *concurrentTestConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *concurrentTestConn) SetWriteDeadline(time.Time) error {
	return nil
}

type concurrentTestAddr string

func (a concurrentTestAddr) Network() string {
	return "tcp"
}

func (a concurrentTestAddr) String() string {
	return string(a)
}

func TestDialConcurrentNetworkFastestSuccessWins(t *testing.T) {
	t.Parallel()

	fastAddress := netip.MustParseAddr("127.0.0.2")
	slowAddress := netip.MustParseAddr("127.0.0.3")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		fastAddress: {delay: time.Millisecond},
		slowAddress: {delay: 50 * time.Millisecond},
	}}

	conn, err := DialConcurrentNetwork(context.Background(), testDialer, nil, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443), []netip.Addr{
		slowAddress,
		fastAddress,
	}, nil, nil, nil, 0)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.2:443", conn.RemoteAddr().String())
	require.GreaterOrEqual(t, testDialer.MaxActive(), 2)
}

func TestDialConcurrentNetworkWaitsAfterFastFailure(t *testing.T) {
	t.Parallel()

	failedAddress := netip.MustParseAddr("127.0.0.4")
	successAddress := netip.MustParseAddr("127.0.0.5")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		failedAddress:  {delay: time.Millisecond, err: errors.New("dial failed")},
		successAddress: {delay: 10 * time.Millisecond},
	}}

	conn, err := DialConcurrentNetwork(context.Background(), testDialer, nil, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443), []netip.Addr{
		failedAddress,
		successAddress,
	}, nil, nil, nil, 0)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.5:443", conn.RemoteAddr().String())
}

func TestDialConcurrentNetworkAllFailed(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first failed")
	secondErr := errors.New("second failed")
	firstAddress := netip.MustParseAddr("127.0.0.6")
	secondAddress := netip.MustParseAddr("127.0.0.7")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		firstAddress:  {delay: time.Millisecond, err: firstErr},
		secondAddress: {delay: time.Millisecond, err: secondErr},
	}}

	conn, err := DialConcurrentNetwork(context.Background(), testDialer, nil, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443), []netip.Addr{
		firstAddress,
		secondAddress,
	}, nil, nil, nil, 0)
	require.Nil(t, conn)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
}

func TestDialConcurrentNetworkClosesLateSuccessAfterReturn(t *testing.T) {
	t.Parallel()

	winnerAddress := netip.MustParseAddr("127.0.0.8")
	loserAddress := netip.MustParseAddr("127.0.0.9")
	loserClosed := make(chan struct{})
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		winnerAddress: {delay: time.Millisecond},
		loserAddress:  {delay: 10 * time.Millisecond, ignoreContext: true, closed: loserClosed},
	}}

	conn, err := DialConcurrentNetwork(context.Background(), testDialer, nil, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443), []netip.Addr{
		winnerAddress,
		loserAddress,
	}, nil, nil, nil, 0)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.8:443", conn.RemoteAddr().String())

	select {
	case <-loserClosed:
	case <-time.After(time.Second):
		t.Fatal("late loser connection was not closed")
	}
}

func TestDialConcurrentNetworkSingleAddressUsesSingleDial(t *testing.T) {
	t.Parallel()

	address := netip.MustParseAddr("127.0.0.10")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		address: {delay: time.Millisecond},
	}}

	conn, err := DialConcurrentNetwork(context.Background(), testDialer, nil, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443), []netip.Addr{
		address,
	}, nil, nil, nil, 0)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.10:443", conn.RemoteAddr().String())
	require.Equal(t, 1, testDialer.MaxActive())
}
