package daemon

import (
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common"
	N "github.com/sagernet/sing/common/network"
)

// Resolve the currently selected leaf without looping on malformed groups.
func (i *Instance) selectedOutbound(outbound adapter.Outbound) adapter.Outbound {
	seen := make(map[string]bool)
	for outbound != nil {
		if seen[outbound.Tag()] {
			return nil
		}
		seen[outbound.Tag()] = true
		group, ok := outbound.(adapter.OutboundGroup)
		if !ok {
			return outbound
		}
		outbound, _ = i.outboundManager.Outbound(group.Now())
	}
	return nil
}

func (i *Instance) outboundInfo(outbound adapter.Outbound) *GroupItem {
	item := &GroupItem{Tag: outbound.Tag(), Type: outbound.Type()}
	leaf := i.selectedOutbound(outbound)
	if leaf == nil {
		return item
	}
	item.Udp = common.Contains(leaf.Network(), N.NetworkUDP)
	if history := i.urlTestHistoryStorage.LoadURLTestHistory(leaf.Tag()); history != nil {
		item.UrlTestTime = history.Time.Unix()
		item.UrlTestDelay = int32(history.Delay)
	}
	i.probeAccess.Lock()
	result := i.ipv6Results[leaf.Tag()]
	i.probeAccess.Unlock()
	// A replacement node with the same tag must not inherit an old result.
	item.Ipv6 = result.outbound == leaf && result.available
	return item
}
