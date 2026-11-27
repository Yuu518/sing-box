package group

import (
	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

func (s *Selector) start() error {
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
	if len(s.Dependencies())+len(s.providerTags) == 0 {
		return E.New("missing outbound and provider tags")
	}
	tags, outbounds, cache, err := collectProviderOutbounds("", s.Dependencies(), s.outbound, s.providers, s.providerTags, s.outboundsCache, s.exclude, s.include)
	if err != nil {
		return err
	}
	outboundByTag := outboundMap(tags, outbounds)
	selectedTag, err := s.outboundSelect(outboundByTag, tags, len(s.providerTags) > 0)
	if err != nil {
		return err
	}
	s.stateAccess.Lock()
	s.tags, s.outbounds, s.outboundsCache = tags, outboundByTag, cache
	s.selectedTag = selectedTag
	s.selected.Store(outboundByTag[selectedTag])
	s.stateAccess.Unlock()
	if len(s.providerTags) > 0 || s.useAllProviders {
		s.providerCallback = s.provider.RegisterCallback(s.onProviderUpdated)
	}
	return nil
}

func (s *Selector) close() error {
	s.providerAccess.Lock()
	defer s.providerAccess.Unlock()
	if s.providerCallback != nil {
		s.provider.UnregisterCallback(s.providerCallback)
		s.providerCallback = nil
	}
	return nil
}

func (s *Selector) postStart() error {
	s.providerAccess.Lock()
	defer s.providerAccess.Unlock()
	s.stateAccess.Lock()
	defer s.stateAccess.Unlock()
	selectedTag, err := s.outboundSelect(s.outbounds, s.tags, false)
	if err != nil {
		return err
	}
	s.selectedTag = selectedTag
	s.selected.Store(s.outbounds[selectedTag])
	s.selectionReady = true
	return nil
}

func (s *Selector) onProviderUpdated(tag string) error {
	s.providerAccess.Lock()
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
	outboundByTag := outboundMap(tags, outbounds)
	s.stateAccess.Lock()
	selectedTag, err := s.outboundSelect(outboundByTag, tags, true)
	if err != nil {
		s.stateAccess.Unlock()
		s.providerAccess.Unlock()
		return err
	}
	detour := outboundByTag[selectedTag]
	s.tags, s.outbounds, s.outboundsCache = tags, outboundByTag, outboundsCache
	s.selectedTag = selectedTag
	previous := s.selected.Swap(detour)
	s.stateAccess.Unlock()
	s.providerAccess.Unlock()
	if previous != detour {
		s.interruptGroup.Interrupt(s.interruptExternalConnections)
		if s.history != nil {
			s.history.NotifyUpdated()
		}
	}
	return nil
}

func (s *Selector) outboundSelect(outbounds map[string]adapter.Outbound, tags []string, allowMissingDefault bool) (string, error) {
	if s.selectionReady && s.selectedTag != "" {
		if _, found := outbounds[s.selectedTag]; found {
			return s.selectedTag, nil
		}
	}
	if s.Tag() != "" {
		cacheFile := service.FromContext[adapter.CacheFile](s.ctx)
		if cacheFile != nil {
			selected := cacheFile.LoadSelected(s.Tag())
			if selected != "" {
				if _, loaded := outbounds[selected]; loaded {
					return selected, nil
				}
			}
		}
	}

	if s.defaultTag != "" {
		if _, loaded := outbounds[s.defaultTag]; loaded {
			return s.defaultTag, nil
		}
		if !allowMissingDefault {
			return "", E.New("default outbound not found: ", s.defaultTag)
		}
	}

	return tags[0], nil
}
