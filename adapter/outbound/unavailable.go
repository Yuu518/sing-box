package outbound

import (
	"context"
	"net"

	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

// unavailableOutbound keeps routing safe while a provider has no usable default.
// It reports an ordinary connection error until a replacement is registered.
type unavailableOutbound struct {
	Adapter
}

func (o *unavailableOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, E.New("default outbound is not available")
}

func (o *unavailableOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, E.New("default outbound is not available")
}
