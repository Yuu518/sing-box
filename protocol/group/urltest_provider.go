package group

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
)

func (s *URLTest) start() error {
	s.providerAccess.Lock()
	defer s.providerAccess.Unlock()
	if s.useAllProviders {
		var providerTags []string
		for _, provider := range s.provider.Providers() {
			providerTags = append(providerTags, provider.Tag())
			s.providers[provider.Tag()] = provider
		}
		s.providerTags = providerTags
	} else {
		for i, tag := range s.providerTags {
			provider, loaded := s.provider.Get(tag)
			if !loaded {
				return E.New("outbound provider ", i, " not found: ", tag)
			}
			s.providers[tag] = provider
		}
	}
	if len(s.tags)+len(s.providerTags) == 0 {
		return E.New("missing outbound and provider tags")
	}
	tags, outbounds, cache, err := collectProviderOutbounds("", s.Dependencies(), s.outbound, s.providers, s.providerTags, s.outboundsCache, s.exclude, s.include)
	if err != nil {
		return err
	}
	s.outboundsCache = cache
	group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, tags, outbounds, s.link, s.interval, s.tolerance, s.idleTimeout, s.interruptExternalConnections)
	if err != nil {
		return err
	}
	err = group.configureFallback(s.fallback, s.maxDelay)
	if err != nil {
		return err
	}
	s.group = group
	if len(s.providerTags) > 0 || s.useAllProviders {
		s.providerCallback = s.provider.RegisterCallback(s.onProviderUpdated)
	}
	return nil
}

func (s *URLTest) close() error {
	s.providerAccess.Lock()
	s.closed = true
	if s.providerCallback != nil {
		s.provider.UnregisterCallback(s.providerCallback)
		s.providerCallback = nil
	}
	s.providerAccess.Unlock()
	return common.Close(
		common.PtrOrNil(s.group),
	)
}

func (s *URLTest) isGroupActive() bool {
	if !s.group.started.Load() {
		return false
	}
	return time.Since(s.group.lastActive.Load()) <= s.group.idleTimeout
}

func (s *URLTest) onProviderUpdated(tag string) error {
	s.providerAccess.Lock()
	if s.closed {
		s.providerAccess.Unlock()
		return nil
	}
	providerTags, relevant := refreshProvider(s.provider, s.providers, s.providerTags, s.useAllProviders, tag)
	if !relevant {
		s.providerAccess.Unlock()
		return nil
	}
	s.providerTags = providerTags
	tags, outbounds, outboundsCache, err := collectProviderOutbounds(
		tag,
		s.Dependencies(),
		s.outbound,
		s.providers,
		s.providerTags,
		s.outboundsCache,
		s.exclude,
		s.include,
	)
	if err != nil {
		s.providerAccess.Unlock()
		return E.Cause(err, s.Tag())
	}
	s.outboundsCache = outboundsCache
	s.group.replaceOutbounds(tags, outbounds)
	s.providerAccess.Unlock()
	if s.isGroupActive() && !s.group.closed.Load() {
		s.group.access.Lock()
		if s.group.ticker != nil {
			s.group.ticker.Reset(s.group.interval)
		}
		s.group.access.Unlock()
		s.providerUpdateCheck.Schedule(func() {
			if s.group.closed.Load() {
				return
			}
			// A check on the previous instances may still be in flight. Wait for
			// it, then replace its results instead of accepting its cached tags.
			_, _ = s.group.urlTestWait(s.ctx, true)
		})
	}
	return nil
}

func (g *URLTestGroup) urlTestWait(ctx context.Context, force bool) (map[string]uint16, error) {
	g.checkLock.Lock()
	defer g.checkLock.Unlock()
	return g.urlTestLocked(ctx, force)
}

func (g *URLTestGroup) loadOutbounds() []adapter.Outbound {
	g.outboundsAccess.RLock()
	defer g.outboundsAccess.RUnlock()
	return g.outbounds
}

func (g *URLTestGroup) loadMembers() ([]string, []adapter.Outbound) {
	g.outboundsAccess.RLock()
	defer g.outboundsAccess.RUnlock()
	return g.tags, g.outbounds
}

func (g *URLTestGroup) storeOutbounds(tags []string, outbounds []adapter.Outbound) {
	g.outboundsAccess.Lock()
	g.tags = tags
	g.outbounds = outbounds
	g.outboundsAccess.Unlock()
}

func (g *URLTestGroup) replaceOutbounds(tags []string, outbounds []adapter.Outbound) {
	g.updateAccess.Lock()
	selectedOutboundTCP := g.selectedOutboundTCP.Load()
	selectedOutboundUDP := g.selectedOutboundUDP.Load()
	g.storeOutbounds(tags, outbounds)
	if !containsOutbound(outbounds, selectedOutboundTCP) {
		g.selectedOutboundTCP.Store(nil)
	}
	if !containsOutbound(outbounds, selectedOutboundUDP) {
		g.selectedOutboundUDP.Store(nil)
	}
	if g.selectedOutboundTCP.Load() == nil {
		if outbound, _ := g.Select(N.NetworkTCP); outbound != nil {
			g.selectedOutboundTCP.Store(outbound)
		}
	}
	if g.selectedOutboundUDP.Load() == nil {
		if outbound, _ := g.Select(N.NetworkUDP); outbound != nil {
			g.selectedOutboundUDP.Store(outbound)
		}
	}
	updated := (selectedOutboundTCP != nil && g.selectedOutboundTCP.Load() != selectedOutboundTCP) ||
		(selectedOutboundUDP != nil && g.selectedOutboundUDP.Load() != selectedOutboundUDP)
	selected := g.selectedOutboundTCP.Load() != selectedOutboundTCP || g.selectedOutboundUDP.Load() != selectedOutboundUDP
	g.updateAccess.Unlock()
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	if g.fallback {
		g.ensureFallbackProbeLoop()
	}
	if selected && g.history != nil {
		g.history.NotifyUpdated()
	}
}

func containsOutbound(outbounds []adapter.Outbound, selected adapter.Outbound) bool {
	if selected == nil {
		return true
	}
	for _, outbound := range outbounds {
		if outbound == selected {
			return true
		}
	}
	return false
}
