package dialer

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type DirectDialer interface {
	IsEmpty() bool
}

type DetourDialer struct {
	outboundManager         adapter.OutboundManager
	scope                   adapter.OutboundScope
	detour                  string
	defaultOutbound         bool
	disableEmptyDirectCheck bool
}

func NewDetour(outboundManager adapter.OutboundManager, detour string, disableEmptyDirectCheck bool) N.Dialer {
	return &DetourDialer{
		outboundManager:         outboundManager,
		detour:                  detour,
		disableEmptyDirectCheck: disableEmptyDirectCheck,
	}
}

func NewDefaultOutboundDetour(outboundManager adapter.OutboundManager) N.Dialer {
	return &DetourDialer{
		outboundManager: outboundManager,
		defaultOutbound: true,
	}
}

func InitializeDetour(dialer N.Dialer) error {
	detourDialer, isDetour := common.Cast[*DetourDialer](dialer)
	if !isDetour {
		return nil
	}
	return common.Error(detourDialer.Dialer())
}

type detourStackKey struct{}

type detourStackNode struct {
	dialer *DetourDialer
	target adapter.Outbound
	prev   *detourStackNode
}

func (d *DetourDialer) Dialer() (N.Dialer, error) {
	return d.resolve()
}

func (d *DetourDialer) resolve() (adapter.Outbound, error) {
	// Provider updates can replace or remove a node without changing its tag.
	// Resolve it for each new connection instead of retaining a closed instance.
	var dialer adapter.Outbound
	if d.detour != "" {
		var loaded bool
		if d.scope != nil {
			dialer, loaded = d.scope.Outbound(d.detour)
		}
		if !loaded {
			dialer, loaded = d.outboundManager.Outbound(d.detour)
		}
		if !loaded {
			return nil, E.New("outbound detour not found: ", d.detour)
		}
	} else {
		dialer = d.outboundManager.Default()
	}
	if dialer == nil {
		return nil, E.New("default outbound is not available")
	}
	if !d.defaultOutbound && !d.disableEmptyDirectCheck {
		if directDialer, isDirect := dialer.(DirectDialer); isDirect {
			if directDialer.IsEmpty() {
				return nil, E.New("detour to an empty direct outbound makes no sense")
			}
		}
	}
	return dialer, nil
}

func (d *DetourDialer) enterDetour(ctx context.Context, target adapter.Outbound) (context.Context, error) {
	stack, _ := ctx.Value(detourStackKey{}).(*detourStackNode)
	for node := stack; node != nil; node = node.prev {
		if node.dialer == d || node.target == target {
			return nil, E.New("circular outbound detour: ", target.Tag())
		}
	}
	return context.WithValue(ctx, detourStackKey{}, &detourStackNode{
		dialer: d,
		target: target,
		prev:   stack,
	}), nil
}

func (d *DetourDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	dialer, err := d.resolve()
	if err != nil {
		return nil, err
	}
	ctx, err = d.enterDetour(ctx, dialer)
	if err != nil {
		return nil, err
	}
	return dialer.DialContext(ctx, network, destination)
}

func (d *DetourDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	dialer, err := d.resolve()
	if err != nil {
		return nil, err
	}
	ctx, err = d.enterDetour(ctx, dialer)
	if err != nil {
		return nil, err
	}
	return dialer.ListenPacket(ctx, destination)
}

func (d *DetourDialer) Upstream() any {
	detour, _ := d.Dialer()
	return detour
}
