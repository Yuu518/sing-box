package outbound

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type providerFallbackOutbound struct {
	Adapter
	bootstrap N.Dialer
}

func NewProviderFallback(tag string, bootstrap N.Dialer) adapter.Outbound {
	return &providerFallbackOutbound{
		Adapter:   NewAdapter(C.TypeBlock, tag, []string{N.NetworkTCP, N.NetworkUDP}, nil),
		bootstrap: bootstrap,
	}
}

func (o *providerFallbackOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if interrupt.IsProviderConnectionFromContext(ctx) {
		return o.bootstrap.DialContext(ctx, network, destination)
	}
	return nil, E.New("no available outbound in provider")
}

func (o *providerFallbackOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if interrupt.IsProviderConnectionFromContext(ctx) {
		return o.bootstrap.ListenPacket(ctx, destination)
	}
	return nil, E.New("no available outbound in provider")
}

func (o *providerFallbackOutbound) Close() error {
	return common.Close(o.bootstrap)
}
