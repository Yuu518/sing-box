package outbound

import (
	"context"
	"net"
	"testing"

	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type recordingDialer struct {
	calls  int
	closed bool
}

func (d *recordingDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	d.calls++
	return nil, nil
}

func (d *recordingDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	d.calls++
	return nil, nil
}

func (d *recordingDialer) Close() error {
	d.closed = true
	return nil
}

func TestProviderFallbackOnlyDialsProviderConnections(t *testing.T) {
	bootstrap := &recordingDialer{}
	fallback := NewProviderFallback("fallback", bootstrap)
	require.Equal(t, C.TypeBlock, fallback.Type())
	_, err := fallback.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("127.0.0.1:80"))
	require.ErrorContains(t, err, "no available outbound in provider")
	_, err = fallback.ListenPacket(context.Background(), M.ParseSocksaddr("127.0.0.1:53"))
	require.ErrorContains(t, err, "no available outbound in provider")
	require.Zero(t, bootstrap.calls)
	providerContext := interrupt.ContextWithIsProviderConnection(context.Background())
	_, err = fallback.DialContext(providerContext, N.NetworkTCP, M.ParseSocksaddr("127.0.0.1:80"))
	require.NoError(t, err)
	_, err = fallback.ListenPacket(providerContext, M.ParseSocksaddr("127.0.0.1:53"))
	require.NoError(t, err)
	require.Equal(t, 2, bootstrap.calls)
	require.NoError(t, common.Close(fallback))
	require.True(t, bootstrap.closed)
}
