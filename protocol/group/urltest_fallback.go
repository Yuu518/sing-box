package group

import (
	"context"
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service/pause"
)

const (
	defaultFallbackProbeInterval     = time.Second
	defaultFallbackFlapWindow        = 10 * time.Second
	defaultFallbackFlapMaxBackoff    = 30 * time.Second
	defaultFallbackFailureMaxBackoff = 60 * time.Second
	defaultFallbackFailureBurstLimit = 5
	defaultFailureCheckCooldown      = C.TCPConnectTimeout
)

type fallbackProbeState struct {
	consecutiveFailures int
	lastRecovered       time.Time
	flapBackoff         time.Duration
	nextProbe           time.Time
}

func (g *URLTestGroup) onTouch() {
	if g.fallback {
		g.ensureFallbackProbeLoop()
	}
}

func (g *URLTestGroup) stopFallbackProbeLocked() {
	if g.fallbackTicker != nil {
		g.fallbackTicker.Stop()
		g.fallbackTicker = nil
		if g.pause != nil && g.fallbackPauseCallback != nil {
			g.pause.UnregisterCallback(g.fallbackPauseCallback)
		}
		g.fallbackPauseCallback = nil
	}
}

func (g *URLTestGroup) fallbackProbeStateLocked(tag string) *fallbackProbeState {
	state, loaded := g.fallbackStates[tag]
	if !loaded {
		state = &fallbackProbeState{}
		g.fallbackStates[tag] = state
	}
	return state
}

func (g *URLTestGroup) recordFlapFailure(tag string) {
	now := time.Now()
	g.probeAccess.Lock()
	defer g.probeAccess.Unlock()
	state, loaded := g.fallbackStates[tag]
	if !loaded || state.lastRecovered.IsZero() {
		return
	}
	if now.Sub(state.lastRecovered) <= g.fallbackFlapWindow {
		if state.flapBackoff == 0 {
			state.flapBackoff = g.fallbackProbeInterval * 2
		} else {
			state.flapBackoff *= 2
		}
		if state.flapBackoff > g.fallbackFlapMaxBackoff {
			state.flapBackoff = g.fallbackFlapMaxBackoff
		}
		state.nextProbe = now.Add(state.flapBackoff)
	} else {
		state.flapBackoff = 0
		state.lastRecovered = time.Time{}
		state.consecutiveFailures = 0
		state.nextProbe = time.Time{}
	}
}

func (g *URLTestGroup) recordProbeFailure(tag string) {
	now := time.Now()
	g.probeAccess.Lock()
	defer g.probeAccess.Unlock()
	state := g.fallbackProbeStateLocked(tag)
	state.consecutiveFailures++
	backoff := g.fallbackProbeInterval
	if state.consecutiveFailures > g.fallbackFailureBurstLimit {
		shift := state.consecutiveFailures - g.fallbackFailureBurstLimit
		if shift > 10 {
			shift = 10
		}
		backoff = g.fallbackProbeInterval << shift
		if backoff > g.fallbackFailureMaxBackoff {
			backoff = g.fallbackFailureMaxBackoff
		}
	}
	if state.flapBackoff > backoff {
		backoff = state.flapBackoff
	}
	state.nextProbe = now.Add(backoff)
}

func (g *URLTestGroup) recordProbeRecovery(tag string) {
	now := time.Now()
	g.probeAccess.Lock()
	defer g.probeAccess.Unlock()
	state := g.fallbackProbeStateLocked(tag)
	state.consecutiveFailures = 0
	state.nextProbe = time.Time{}
	state.lastRecovered = now
}

func (g *URLTestGroup) resetFallbackProbeStates() {
	g.probeAccess.Lock()
	clear(g.fallbackStates)
	g.probeAccess.Unlock()
}

func (g *URLTestGroup) fallbackRecoveryTargets() []adapter.Outbound {
	if !g.fallback {
		return nil
	}
	seen := make(map[string]struct{})
	var targets []adapter.Outbound
	appendTarget := func(detour adapter.Outbound) {
		tag := detour.Tag()
		if _, exists := seen[tag]; exists {
			return
		}
		seen[tag] = struct{}{}
		targets = append(targets, detour)
	}
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		var selectedOutbound adapter.Outbound
		if network == N.NetworkUDP {
			selectedOutbound = g.selectedOutboundUDP.Load()
		} else {
			selectedOutbound = g.selectedOutboundTCP.Load()
		}
		var (
			supported  []adapter.Outbound
			anyHistory bool
		)
		for _, detour := range g.outbounds {
			if !common.Contains(detour.Network(), network) {
				continue
			}
			supported = append(supported, detour)
			if g.history.LoadURLTestHistory(RealTag(detour, network)) != nil {
				anyHistory = true
			}
		}
		if len(supported) == 0 {
			continue
		}
		if !anyHistory {
			for _, detour := range supported {
				appendTarget(detour)
			}
			continue
		}
		if selectedOutbound == nil {
			continue
		}
		selectedIdx := slices.Index(g.outbounds, selectedOutbound)
		if selectedIdx <= 0 {
			continue
		}
		for i := range selectedIdx {
			detour := g.outbounds[i]
			if !common.Contains(detour.Network(), network) {
				continue
			}
			if g.history.LoadURLTestHistory(RealTag(detour, network)) == nil {
				appendTarget(detour)
			}
		}
	}
	return targets
}

func (g *URLTestGroup) ensureFallbackProbeLoop() {
	if !g.fallback || !g.started.Load() || g.closed.Load() {
		return
	}
	targets := g.fallbackRecoveryTargets()
	g.access.Lock()
	defer g.access.Unlock()
	if g.closed.Load() {
		return
	}
	if len(targets) == 0 || g.ticker == nil {
		g.stopFallbackProbeLocked()
		return
	}
	if g.fallbackTicker != nil {
		return
	}
	ticker := time.NewTicker(g.fallbackProbeInterval)
	g.fallbackTicker = ticker
	if g.pause != nil {
		g.fallbackPauseCallback = pause.RegisterTicker(g.pause, ticker, g.fallbackProbeInterval, nil)
	}
	go g.loopFallbackProbe(ticker, g.close)
}

func (g *URLTestGroup) stopFallbackProbeTicker(ticker *time.Ticker) {
	g.access.Lock()
	if g.fallbackTicker == ticker {
		g.stopFallbackProbeLocked()
	}
	g.access.Unlock()
}

func (g *URLTestGroup) loopFallbackProbe(ticker *time.Ticker, closeChan <-chan struct{}) {
	for {
		select {
		case <-closeChan:
			return
		case <-ticker.C:
		}
		if time.Since(g.lastActive.Load()) > g.idleTimeout {
			g.stopFallbackProbeTicker(ticker)
			return
		}
		g.access.Lock()
		active := !g.closed.Load() && g.ticker != nil && g.fallbackTicker == ticker
		g.access.Unlock()
		if !active {
			return
		}
		targets := g.fallbackRecoveryTargets()
		if len(targets) == 0 {
			g.stopFallbackProbeTicker(ticker)
			return
		}
		g.probeFallbackTargets(targets)
	}
}

func (g *URLTestGroup) probeFallbackTargets(targets []adapter.Outbound) {
	visited := make(map[string]bool)
	var (
		leaves       []adapter.Outbound
		nestedGroups []adapter.URLTestGroup
	)
	for _, target := range targets {
		g.collectUnavailableLeaves(target, visited, &nestedGroups, &leaves)
	}
	now := time.Now()
	for _, leaf := range leaves {
		tag := leaf.Tag()
		g.probeAccess.Lock()
		if _, probing := g.fallbackProbing[tag]; probing {
			g.probeAccess.Unlock()
			continue
		}
		if state, loaded := g.fallbackStates[tag]; loaded && !state.nextProbe.IsZero() && now.Before(state.nextProbe) {
			g.probeAccess.Unlock()
			continue
		}
		g.fallbackProbing[tag] = struct{}{}
		g.probeAccess.Unlock()
		go g.probeFallbackLeaf(leaf, nestedGroups)
	}
}

func (g *URLTestGroup) collectUnavailableLeaves(detour adapter.Outbound, visited map[string]bool, nestedGroups *[]adapter.URLTestGroup, leaves *[]adapter.Outbound) {
	tag := detour.Tag()
	if visited[tag] {
		return
	}
	visited[tag] = true
	if urlTestGroup, isURLTest := detour.(adapter.URLTestGroup); isURLTest {
		*nestedGroups = append(*nestedGroups, urlTestGroup)
	}
	if outboundGroup, isGroup := detour.(adapter.OutboundGroup); isGroup {
		if g.outbound == nil {
			return
		}
		for _, memberTag := range outboundGroup.All() {
			member, loaded := g.outbound.Outbound(memberTag)
			if !loaded {
				continue
			}
			g.collectUnavailableLeaves(member, visited, nestedGroups, leaves)
		}
		return
	}
	if g.history.LoadURLTestHistory(tag) != nil {
		return
	}
	*leaves = append(*leaves, detour)
}

func (g *URLTestGroup) probeFallbackLeaf(detour adapter.Outbound, nestedGroups []adapter.URLTestGroup) {
	tag := detour.Tag()
	defer func() {
		g.probeAccess.Lock()
		delete(g.fallbackProbing, tag)
		g.probeAccess.Unlock()
	}()
	if g.closed.Load() || g.ctx.Err() != nil {
		return
	}
	probeCtx, cancel := context.WithTimeout(g.ctx, g.fallbackProbeTimeout)
	defer cancel()
	testChan := make(chan urlTestResult, 1)
	go func() {
		delay, testErr := g.urlTestFunc(probeCtx, g.link, detour)
		testChan <- urlTestResult{delay, testErr}
	}()
	var testResult urlTestResult
	select {
	case testResult = <-testChan:
	case <-probeCtx.Done():
		testResult.err = probeCtx.Err()
	case <-g.close:
		return
	}
	if g.closed.Load() || g.ctx.Err() != nil {
		return
	}
	if testResult.err != nil {
		g.logger.Debug("outbound ", tag, " unavailable: ", testResult.err)
		g.recordProbeFailure(tag)
		return
	}
	g.logger.Debug("outbound ", tag, " available: ", testResult.delay, "ms")
	g.recordProbeRecovery(tag)
	g.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{
		Time:  time.Now(),
		Delay: testResult.delay,
	})
	for _, nested := range slices.Backward(nestedGroups) {
		nested.PerformUpdateCheck()
	}
	if g.performUpdateCheck() {
		g.history.NotifyUpdated()
	}
}
