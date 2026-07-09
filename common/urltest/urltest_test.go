package urltest_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

const (
	testHandshakeDelay = 300 * time.Millisecond
	testRequestDelay   = 50 * time.Millisecond
)

type fixedDialer struct {
	target string
}

func (d *fixedDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", d.target)
}

func (d *fixedDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

var _ N.Dialer = (*fixedDialer)(nil)

// startTestTarget answers the first request slowly and later ones quickly, so a
// measurement that includes the first request is distinguishable from one that
// only covers a reused connection.
func startTestTarget(t *testing.T, keepAlive bool) (*fixedDialer, *atomic.Int32) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if requests.Add(1) == 1 {
			time.Sleep(testHandshakeDelay)
		} else {
			time.Sleep(testRequestDelay)
		}
		if !keepAlive {
			writer.Header().Set("Connection", "close")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	return &fixedDialer{target: server.Listener.Addr().String()}, &requests
}

func testContext(unifiedDelay bool) context.Context {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	service.MustRegister[urltest.UnifiedDelay](ctx, urltest.UnifiedDelay(unifiedDelay))
	return ctx
}

func TestURLTest(t *testing.T) {
	t.Parallel()
	dialer, requests := startTestTarget(t, true)
	delay, err := urltest.URLTest(testContext(false), "http://test.invalid/", dialer)
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
	require.Greater(t, delay, uint16(testHandshakeDelay/time.Millisecond))
}

func TestURLTestUnifiedDelay(t *testing.T) {
	t.Parallel()
	dialer, requests := startTestTarget(t, true)
	delay, err := urltest.URLTest(testContext(true), "http://test.invalid/", dialer)
	require.NoError(t, err)
	require.EqualValues(t, 2, requests.Load())
	require.Less(t, delay, uint16(testHandshakeDelay/time.Millisecond))
}

// A target that refuses to keep the connection alive cannot be measured without
// the handshake; the outbound must still be reported as alive.
func TestURLTestUnifiedDelayWithoutKeepAlive(t *testing.T) {
	t.Parallel()
	dialer, requests := startTestTarget(t, false)
	delay, err := urltest.URLTest(testContext(true), "http://test.invalid/", dialer)
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load())
	require.Greater(t, delay, uint16(testHandshakeDelay/time.Millisecond))
}
