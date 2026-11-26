package group

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service/pause"
)

func (s *URLTest) Fallback() bool {
	return s.fallback
}

func (s *URLTest) ReportFailure(tag string) {
	if s.group == nil {
		return
	}
	s.group.reportFailure(tag)
}

func NotifyOutboundFailure(outboundManager adapter.OutboundManager, history *urltest.HistoryStorage, chain []adapter.Outbound, failedTag string) {
	if failedTag == "" {
		return
	}
	if history != nil {
		history.DeleteURLTestHistory(failedTag)
	}
	notified := make(map[adapter.URLTestGroup]struct{})
	for _, it := range slices.Backward(chain) {
		urlTestGroup, isURLTestGroup := it.(adapter.URLTestGroup)
		if !isURLTestGroup {
			continue
		}
		if _, seen := notified[urlTestGroup]; seen {
			continue
		}
		notified[urlTestGroup] = struct{}{}
		urlTestGroup.ReportFailure(failedTag)
	}
	if outboundManager == nil {
		return
	}
	for _, detour := range outboundManager.Outbounds() {
		urlTestGroup, isURLTestGroup := detour.(adapter.URLTestGroup)
		if !isURLTestGroup {
			continue
		}
		if _, seen := notified[urlTestGroup]; seen {
			continue
		}
		notified[urlTestGroup] = struct{}{}
		urlTestGroup.ReportFailure(failedTag)
	}
}

func NotifyOutboundTestResult(outboundManager adapter.OutboundManager, tag string, testErr error) {
	if outboundManager == nil {
		return
	}
	for _, detour := range outboundManager.Outbounds() {
		urlTestGroup, isURLTestGroup := detour.(adapter.URLTestGroup)
		if !isURLTestGroup {
			continue
		}
		if !GroupContains(outboundManager, urlTestGroup, tag, map[string]bool{detour.Tag(): true}) {
			continue
		}
		if testErr != nil {
			urlTestGroup.ReportFailure(tag)
		} else {
			urlTestGroup.PerformUpdateCheck()
		}
	}
}

func GroupContains(outboundManager adapter.OutboundManager, outboundGroup adapter.OutboundGroup, tag string, visited map[string]bool) bool {
	for _, memberTag := range outboundGroup.All() {
		if memberTag == tag {
			return true
		}
		member, loaded := outboundManager.Outbound(memberTag)
		if !loaded {
			continue
		}
		if RealTag(member, N.NetworkTCP) == tag || RealTag(member, N.NetworkUDP) == tag {
			return true
		}
		memberGroup, isGroup := member.(adapter.OutboundGroup)
		if !isGroup || visited[memberTag] {
			continue
		}
		visited[memberTag] = true
		if GroupContains(outboundManager, memberGroup, tag, visited) {
			return true
		}
	}
	return false
}

type urlTestFailover struct {
	fallback                  bool
	maxDelay                  time.Duration
	fallbackPauseCallback     *list.Element[pause.Callback]
	probeAccess               sync.Mutex
	fallbackTicker            *time.Ticker
	fallbackProbing           map[string]struct{}
	fallbackStates            map[string]*fallbackProbeState
	lastFailureCheck          atomic.Int64
	fallbackProbeInterval     time.Duration
	fallbackProbeTimeout      time.Duration
	fallbackFlapWindow        time.Duration
	fallbackFlapMaxBackoff    time.Duration
	fallbackFailureMaxBackoff time.Duration
	fallbackFailureBurstLimit int
	failureCheckCooldown      time.Duration
	urlTestFunc               func(ctx context.Context, link string, detour N.Dialer) (uint16, error)
}

func (g *URLTestGroup) initFailover() {
	probeInterval := min(defaultFallbackProbeInterval, g.interval)
	if probeInterval <= 0 {
		probeInterval = defaultFallbackProbeInterval
	}
	g.fallbackProbing = make(map[string]struct{})
	g.fallbackStates = make(map[string]*fallbackProbeState)
	g.fallbackProbeInterval = probeInterval
	g.fallbackProbeTimeout = C.TCPConnectTimeout
	g.fallbackFlapWindow = defaultFallbackFlapWindow
	g.fallbackFlapMaxBackoff = min(g.interval, defaultFallbackFlapMaxBackoff)
	g.fallbackFailureMaxBackoff = min(g.interval, defaultFallbackFailureMaxBackoff)
	g.fallbackFailureBurstLimit = defaultFallbackFailureBurstLimit
	g.failureCheckCooldown = defaultFailureCheckCooldown
	g.urlTestFunc = urltest.URLTest
}

func (g *URLTestGroup) configureFallback(fallback bool, maxDelay time.Duration) error {
	if maxDelay < 0 {
		return E.New("max_delay must not be negative")
	}
	g.fallback = fallback
	g.maxDelay = maxDelay
	return nil
}

func (g *URLTestGroup) selectOutbound(network string, failedOutbound adapter.Outbound) (adapter.Outbound, bool) {
	if g.fallback {
		return g.selectFallbackOutbound(network, failedOutbound)
	}
	var (
		minDelay         uint16
		minOutbound      adapter.Outbound
		selectedOutbound adapter.Outbound
	)
	switch network {
	case N.NetworkTCP:
		selectedOutbound = g.selectedOutboundTCP.Load()
	case N.NetworkUDP:
		selectedOutbound = g.selectedOutboundUDP.Load()
	}
	if selectedOutbound != nil && selectedOutbound != failedOutbound {
		if history := g.history.LoadURLTestHistory(RealTag(selectedOutbound, network)); history != nil {
			minOutbound = selectedOutbound
			minDelay = history.Delay
		}
	}
	for _, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) {
			continue
		}
		history := g.history.LoadURLTestHistory(RealTag(detour, network))
		if history == nil {
			continue
		}
		if minOutbound == nil {
			minDelay = history.Delay
			minOutbound = detour
		} else if selectedOutbound != nil && minOutbound == selectedOutbound {
			if uint32(minDelay) > uint32(history.Delay)+uint32(g.tolerance) {
				minDelay = history.Delay
				minOutbound = detour
			}
		} else if history.Delay < minDelay {
			minDelay = history.Delay
			minOutbound = detour
		}
	}
	if minOutbound == nil {
		return g.selectWithoutHistory(network, failedOutbound), false
	}
	return minOutbound, true
}

func (g *URLTestGroup) selectFallbackOutbound(network string, failedOutbound adapter.Outbound) (adapter.Outbound, bool) {
	var (
		bestExceededDelay    uint16
		bestExceededOutbound adapter.Outbound
	)
	for _, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) {
			continue
		}
		history := g.history.LoadURLTestHistory(RealTag(detour, network))
		if history == nil {
			continue
		}
		if g.maxDelay == 0 || time.Duration(history.Delay)*time.Millisecond <= g.maxDelay {
			return detour, true
		}
		if bestExceededOutbound == nil || history.Delay < bestExceededDelay {
			bestExceededDelay = history.Delay
			bestExceededOutbound = detour
		}
	}
	if bestExceededOutbound != nil {
		return bestExceededOutbound, true
	}
	return g.selectWithoutHistory(network, failedOutbound), false
}

func (g *URLTestGroup) selectWithoutHistory(network string, failedOutbound adapter.Outbound) adapter.Outbound {
	startIndex := 0
	if failedOutbound != nil {
		if idx := slices.Index(g.outbounds, failedOutbound); idx >= 0 {
			startIndex = (idx + 1) % len(g.outbounds)
		}
	}
	for offset := range len(g.outbounds) {
		detour := g.outbounds[(startIndex+offset)%len(g.outbounds)]
		if !common.Contains(detour.Network(), network) {
			continue
		}
		return detour
	}
	return nil
}

func (g *URLTestGroup) handleBatchResult(tag string, delay uint16, err error, inFlight map[string]bool) {
	if err != nil {
		selectedTCP := g.selectedOutboundTCP.Load()
		selectedUDP := g.selectedOutboundUDP.Load()
		var (
			failedTCP adapter.Outbound
			failedUDP adapter.Outbound
		)
		if selectedTCP != nil && RealTag(selectedTCP, N.NetworkTCP) == tag {
			failedTCP = selectedTCP
		}
		if selectedUDP != nil && RealTag(selectedUDP, N.NetworkUDP) == tag {
			failedUDP = selectedUDP
		}
		if failedTCP == nil && failedUDP == nil {
			return
		}
		if selected, _ := g.performUpdateCheckWithFailed(failedTCP, failedUDP); selected {
			g.history.NotifyUpdated()
		}
		return
	}
	if !g.shouldSwitchMidBatchOnSuccess(tag, delay, inFlight) {
		return
	}
	if selected, _ := g.performUpdateCheckWithFailed(nil, nil); selected {
		g.history.NotifyUpdated()
	}
}

func (g *URLTestGroup) shouldSwitchMidBatchOnSuccess(tag string, delay uint16, inFlight map[string]bool) bool {
	if g.fallback {
		return g.shouldSwitchMidBatchFallback(tag, delay, inFlight, N.NetworkTCP) ||
			g.shouldSwitchMidBatchFallback(tag, delay, inFlight, N.NetworkUDP)
	}
	return g.selectedUnavailableAndNotInFlight(N.NetworkTCP, inFlight) ||
		g.selectedUnavailableAndNotInFlight(N.NetworkUDP, inFlight)
}

func (g *URLTestGroup) shouldSwitchMidBatchFallback(tag string, delay uint16, inFlight map[string]bool, network string) bool {
	var selectedOutbound adapter.Outbound
	if network == N.NetworkUDP {
		selectedOutbound = g.selectedOutboundUDP.Load()
	} else {
		selectedOutbound = g.selectedOutboundTCP.Load()
	}
	candidateIdx := -1
	for i, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) {
			continue
		}
		if RealTag(detour, network) == tag || detour.Tag() == tag {
			candidateIdx = i
			break
		}
	}
	if candidateIdx < 0 {
		return false
	}
	if selectedOutbound == nil {
		for i := range candidateIdx {
			detour := g.outbounds[i]
			if !common.Contains(detour.Network(), network) {
				continue
			}
			if g.isOutboundInFlight(detour, network, inFlight) {
				return false
			}
		}
		return true
	}
	selectedIdx := slices.Index(g.outbounds, selectedOutbound)
	if selectedIdx >= 0 && candidateIdx < selectedIdx && (g.maxDelay == 0 || time.Duration(delay)*time.Millisecond <= g.maxDelay) {
		return true
	}
	if g.selectedUnavailableAndNotInFlight(network, inFlight) {
		for i := range candidateIdx {
			detour := g.outbounds[i]
			if !common.Contains(detour.Network(), network) {
				continue
			}
			if g.isOutboundInFlight(detour, network, inFlight) {
				return false
			}
		}
		return true
	}
	return false
}

func (g *URLTestGroup) selectedUnavailableAndNotInFlight(network string, inFlight map[string]bool) bool {
	var selectedOutbound adapter.Outbound
	if network == N.NetworkUDP {
		selectedOutbound = g.selectedOutboundUDP.Load()
	} else {
		selectedOutbound = g.selectedOutboundTCP.Load()
	}
	if selectedOutbound == nil {
		return false
	}
	if g.history.LoadURLTestHistory(RealTag(selectedOutbound, network)) != nil {
		return false
	}
	return !g.isOutboundInFlight(selectedOutbound, network, inFlight)
}

func (g *URLTestGroup) isOutboundInFlight(detour adapter.Outbound, network string, inFlight map[string]bool) bool {
	if inFlight[detour.Tag()] {
		return true
	}
	realTag := RealTag(detour, network)
	if realTag != "" && inFlight[realTag] {
		return true
	}
	return false
}

func (g *URLTestGroup) reportFailure(failedTag string) {
	selectedTCP := g.selectedOutboundTCP.Load()
	selectedUDP := g.selectedOutboundUDP.Load()
	var (
		failedTCP adapter.Outbound
		failedUDP adapter.Outbound
	)
	if selectedTCP != nil && RealTag(selectedTCP, N.NetworkTCP) == failedTag {
		failedTCP = selectedTCP
	}
	if selectedUDP != nil && RealTag(selectedUDP, N.NetworkUDP) == failedTag {
		failedUDP = selectedUDP
	}
	if failedTCP == nil && failedUDP == nil {
		if g.containsTag(failedTag) {
			if g.performUpdateCheck() {
				g.history.NotifyUpdated()
			}
		}
		return
	}
	if g.fallback {
		g.recordFlapFailure(failedTag)
	}
	selected, hasHistory := g.performUpdateCheckWithFailed(failedTCP, failedUDP)
	if selected {
		g.history.NotifyUpdated()
	}
	if !hasHistory {
		g.triggerFailureBackgroundCheck()
	}
}

func (g *URLTestGroup) containsTag(tag string) bool {
	return g.containsTagVisited(g.outbounds, tag, make(map[string]bool))
}

func (g *URLTestGroup) containsTagVisited(outbounds []adapter.Outbound, tag string, visited map[string]bool) bool {
	for _, detour := range outbounds {
		detourTag := detour.Tag()
		if detourTag == tag {
			return true
		}
		if visited[detourTag] {
			continue
		}
		visited[detourTag] = true
		nestedGroup, isGroup := detour.(adapter.OutboundGroup)
		if !isGroup || g.outbound == nil {
			continue
		}
		members := common.FilterNotNil(common.Map(nestedGroup.All(), func(it string) adapter.Outbound {
			member, _ := g.outbound.Outbound(it)
			return member
		}))
		if g.containsTagVisited(members, tag, visited) {
			return true
		}
	}
	return false
}

func (g *URLTestGroup) triggerFailureBackgroundCheck() {
	if !g.started.Load() || g.closed.Load() || g.checking.Load() {
		return
	}
	if g.pause != nil && (g.pause.IsDevicePaused() || g.pause.IsNetworkPaused()) {
		return
	}
	now := time.Now().UnixNano()
	last := g.lastFailureCheck.Load()
	if last != 0 && time.Duration(now-last) < g.failureCheckCooldown {
		return
	}
	if !g.lastFailureCheck.CompareAndSwap(last, now) {
		return
	}
	go g.CheckOutbounds(g.ctx, false)
}

func (g *URLTestGroup) performUpdateCheckWithFailed(failedTCP adapter.Outbound, failedUDP adapter.Outbound) (bool, bool) {
	g.updateAccess.Lock()
	var (
		updated    bool
		selected   bool
		hasHistory bool
	)
	selectedOutboundTCP := g.selectedOutboundTCP.Load()
	outboundTCP, existsTCP := g.selectOutbound(N.NetworkTCP, failedTCP)
	if existsTCP {
		hasHistory = true
	}
	if outboundTCP != nil && outboundTCP != selectedOutboundTCP {
		if selectedOutboundTCP != nil {
			updated = true
		}
		g.selectedOutboundTCP.Store(outboundTCP)
		selected = true
	}
	selectedOutboundUDP := g.selectedOutboundUDP.Load()
	outboundUDP, existsUDP := g.selectOutbound(N.NetworkUDP, failedUDP)
	if existsUDP {
		hasHistory = true
	}
	if outboundUDP != nil && outboundUDP != selectedOutboundUDP {
		if selectedOutboundUDP != nil {
			updated = true
		}
		g.selectedOutboundUDP.Store(outboundUDP)
		selected = true
	}
	g.updateAccess.Unlock()
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	if g.fallback {
		g.ensureFallbackProbeLoop()
	}
	return selected, hasHistory
}
