package tun

import (
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/x/list"

	"github.com/stretchr/testify/require"
	"go4.org/netipx"
)

type routeUpdateTestRuleSet struct {
	adapter.RuleSet
	extract      func() []*netipx.IPSet
	unregistered int
}

func (s *routeUpdateTestRuleSet) ExtractIPSet() []*netipx.IPSet {
	return s.extract()
}

func (s *routeUpdateTestRuleSet) UnregisterCallback(*list.Element[adapter.RuleSetUpdateCallback]) {
	s.unregistered++
}

type routeUpdateTestRedirect struct {
	tun.AutoRedirect
	update func() error
	close  func() error
}

func (r *routeUpdateTestRedirect) UpdateRouteAddressSet() error { return r.update() }
func (r *routeUpdateTestRedirect) Close() error                 { return r.close() }

func TestRuleSetUpdatesSerializeAddressSnapshots(t *testing.T) {
	t.Parallel()
	makeSet := func(prefix string) *netipx.IPSet {
		var builder netipx.IPSetBuilder
		builder.AddPrefix(netip.MustParsePrefix(prefix))
		result, err := builder.IPSet()
		require.NoError(t, err)
		return result
	}
	oldSet, newSet := makeSet("192.0.2.0/24"), makeSet("198.51.100.0/24")
	firstSnapshot := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondSnapshot := make(chan struct{})
	var reads atomic.Int32
	ruleSet := &routeUpdateTestRuleSet{extract: func() []*netipx.IPSet {
		if reads.Add(1) == 1 {
			close(firstSnapshot)
			<-releaseFirst
			return []*netipx.IPSet{oldSet}
		}
		close(secondSnapshot)
		return []*netipx.IPSet{newSet}
	}}
	inbound := &Inbound{routeRuleSet: []adapter.RuleSet{ruleSet}}
	inbound.autoRedirect = &routeUpdateTestRedirect{update: func() error {
		// Real redirect implementations can read the published prefixes.
		inbound.routeAddressSetPrefixes()
		return nil
	}}
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(firstDone); inbound.updateRouteAddressSet(ruleSet) }()
	<-firstSnapshot
	go func() { defer close(secondDone); inbound.updateRouteAddressSet(ruleSet) }()
	select {
	case <-secondSnapshot:
		<-secondDone
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	<-firstDone
	<-secondDone
	include, _ := inbound.routeAddressSetPrefixes()
	require.Equal(t, []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}, include)
}

func TestRuleSetUpdateCallbackAfterInboundClose(t *testing.T) {
	t.Parallel()
	ruleSet := &routeUpdateTestRuleSet{extract: func() []*netipx.IPSet { return nil }}
	var updates int
	inbound := &Inbound{
		routeRuleSet:                []adapter.RuleSet{ruleSet},
		routeRuleSetCallback:        []*list.Element[adapter.RuleSetUpdateCallback]{nil},
		routeExcludeRuleSet:         []adapter.RuleSet{ruleSet},
		routeExcludeRuleSetCallback: []*list.Element[adapter.RuleSetUpdateCallback]{nil},
		autoRedirect: &routeUpdateTestRedirect{
			update: func() error { updates++; return nil },
			close:  func() error { return nil },
		},
	}
	require.NoError(t, inbound.Close())
	// An update may already have copied the callback before unregistration.
	inbound.updateRouteAddressSet(ruleSet)
	require.Zero(t, updates)
	require.Equal(t, 2, ruleSet.unregistered)
}

func TestInboundCloseWaitsForRuleSetCallback(t *testing.T) {
	t.Parallel()
	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	redirectClosed := make(chan struct{})
	inbound := &Inbound{autoRedirect: &routeUpdateTestRedirect{
		update: func() error {
			close(updateStarted)
			<-releaseUpdate
			return nil
		},
		close: func() error { close(redirectClosed); return nil },
	}}
	updateDone := make(chan struct{})
	go func() { defer close(updateDone); inbound.updateRouteAddressSet(nil) }()
	<-updateStarted
	closeDone := make(chan error, 1)
	go func() { closeDone <- inbound.Close() }()
	select {
	case <-redirectClosed:
		t.Error("redirect closed while an update was still using it")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseUpdate)
	<-updateDone
	require.NoError(t, <-closeDone)
}
