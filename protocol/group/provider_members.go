package group

import (
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/outboundfilter"
	"github.com/sagernet/sing-box/common/tagname"
	E "github.com/sagernet/sing/common/exceptions"

	"github.com/dlclark/regexp2/v2"
)

type providerUpdateCheckScheduler struct {
	access  sync.Mutex
	running bool
	pending bool
}

func (c *providerUpdateCheckScheduler) Schedule(check func()) {
	c.access.Lock()
	c.pending = true
	if c.running {
		c.access.Unlock()
		return
	}
	c.running = true
	c.access.Unlock()
	go func() {
		for {
			c.access.Lock()
			c.pending = false
			c.access.Unlock()
			check()
			c.access.Lock()
			if !c.pending {
				c.running = false
				c.access.Unlock()
				return
			}
			c.access.Unlock()
		}
	}()
}

func collectProviderOutbounds(
	updatedTag string,
	directTags []string,
	outboundManager adapter.OutboundManager,
	providers map[string]adapter.Provider,
	providerTags []string,
	outboundsCache map[string][]adapter.Outbound,
	exclude *regexp2.Regexp,
	include *regexp2.Regexp,
) ([]string, []adapter.Outbound, map[string][]adapter.Outbound, error) {
	newCache := make(map[string][]adapter.Outbound, len(outboundsCache))
	for providerTag, cachedOutbounds := range outboundsCache {
		newCache[providerTag] = cachedOutbounds
	}
	var (
		tags      = make([]string, 0, len(directTags))
		outbounds = make([]adapter.Outbound, 0, len(directTags))
	)
	for i, tag := range directTags {
		detour, loaded := outboundManager.Outbound(tag)
		if !loaded {
			return nil, nil, nil, E.New("outbound ", i, " not found: ", tag)
		}
		tags = append(tags, tag)
		outbounds = append(outbounds, detour)
	}
	for _, providerTag := range providerTags {
		if updatedTag != "" && providerTag != updatedTag {
			if cachedOutbounds := newCache[providerTag]; cachedOutbounds != nil {
				for _, detour := range cachedOutbounds {
					tags = append(tags, detour.Tag())
				}
				outbounds = append(outbounds, cachedOutbounds...)
				continue
			}
		}
		provider := providers[providerTag]
		if provider == nil {
			delete(newCache, providerTag)
			continue
		}
		cachedOutbounds := make([]adapter.Outbound, 0)
		for _, detour := range provider.Outbounds() {
			tag := detour.Tag()
			matched, err := outboundfilter.Match(tag, exclude, include)
			if err != nil {
				return nil, nil, nil, E.Cause(err, "filter provider ", providerTag)
			}
			if !matched {
				continue
			}
			tags = append(tags, tag)
			outbounds = append(outbounds, detour)
			cachedOutbounds = append(cachedOutbounds, detour)
		}
		newCache[providerTag] = cachedOutbounds
	}
	if len(tags) == 0 {
		detour, err := outboundManager.ProviderFallback()
		if err != nil {
			return nil, nil, nil, E.Cause(err, "create provider fallback")
		}
		tags = append(tags, detour.Tag())
		outbounds = append(outbounds, detour)
	}
	return tagname.Deduplicate(tags), outbounds, newCache, nil
}

func findOutboundTag(tags []string, outbounds []adapter.Outbound, outbound adapter.Outbound) string {
	if outbound == nil {
		return ""
	}
	for i, it := range outbounds {
		if it == outbound {
			return tags[i]
		}
	}
	return ""
}

func outboundMap(tags []string, outbounds []adapter.Outbound) map[string]adapter.Outbound {
	outboundByTag := make(map[string]adapter.Outbound, len(tags))
	for i, tag := range tags {
		outboundByTag[tag] = outbounds[i]
	}
	return outboundByTag
}

func Members(outboundGroup adapter.OutboundGroup) ([]string, []adapter.Outbound) {
	var (
		tags      []string
		outbounds []adapter.Outbound
	)
	for _, tag := range outboundGroup.All() {
		member, loaded := outboundGroup.Member(tag)
		if !loaded {
			continue
		}
		tags = append(tags, tag)
		outbounds = append(outbounds, member)
	}
	return tags, outbounds
}

func FindMember(outboundManager adapter.OutboundManager, tag string) (adapter.Outbound, bool) {
	for _, detour := range outboundManager.Outbounds() {
		outboundGroup, isGroup := detour.(adapter.OutboundGroup)
		if !isGroup {
			continue
		}
		if member, loaded := outboundGroup.Member(tag); loaded {
			return member, true
		}
	}
	return nil, false
}

func refreshProvider(manager adapter.ProviderManager, providers map[string]adapter.Provider, providerTags []string, useAllProviders bool, tag string) ([]string, bool) {
	relevant := slices.Contains(providerTags, tag)
	if manager == nil {
		return providerTags, relevant
	}
	provider, loaded := manager.Get(tag)
	if !relevant {
		if !useAllProviders || !loaded {
			return providerTags, false
		}
		providerTags = append(slices.Clip(providerTags), tag)
	}
	if loaded {
		providers[tag] = provider
	} else {
		delete(providers, tag)
	}
	return providerTags, true
}
