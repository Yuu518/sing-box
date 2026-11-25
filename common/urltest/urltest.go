package urltest

import (
	"context"
	"crypto/tls"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/common/observable"
)

const retiredProviderOutboundTTL = 5 * time.Minute

type HistoryStorage struct {
	access                   sync.RWMutex
	delayHistory             map[string]*adapter.URLTestHistory
	providerHistory          map[adapter.Outbound]*adapter.URLTestHistory
	providerOutbounds        map[adapter.Outbound]struct{}
	retiredProviderOutbounds map[adapter.Outbound]time.Time
	updateHooks              []*observable.Subscriber[struct{}]
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{
		delayHistory:             make(map[string]*adapter.URLTestHistory),
		providerHistory:          make(map[adapter.Outbound]*adapter.URLTestHistory),
		providerOutbounds:        make(map[adapter.Outbound]struct{}),
		retiredProviderOutbounds: make(map[adapter.Outbound]time.Time),
	}
}

func (s *HistoryStorage) AddProviderOutbound(outbound adapter.Outbound) {
	s.access.Lock()
	defer s.access.Unlock()
	s.providerOutbounds[outbound] = struct{}{}
}

func (s *HistoryStorage) pruneRetiredLocked(now time.Time) {
	for retired, removedAt := range s.retiredProviderOutbounds {
		if now.Sub(removedAt) > retiredProviderOutboundTTL {
			delete(s.retiredProviderOutbounds, retired)
		}
	}
}

func (s *HistoryStorage) RemoveProviderOutbound(outbound adapter.Outbound) {
	s.access.Lock()
	defer s.access.Unlock()
	if _, loaded := s.providerOutbounds[outbound]; !loaded {
		return
	}
	delete(s.providerOutbounds, outbound)
	delete(s.providerHistory, outbound)
	now := time.Now()
	s.pruneRetiredLocked(now)
	s.retiredProviderOutbounds[outbound] = now
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadURLTestHistoryForOutbound(outbound adapter.Outbound) *adapter.URLTestHistory {
	if s == nil || outbound == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	if _, loaded := s.providerOutbounds[outbound]; loaded {
		return s.providerHistory[outbound]
	}
	if _, retired := s.retiredProviderOutbounds[outbound]; retired {
		return nil
	}
	return s.delayHistory[outbound.Tag()]
}

func (s *HistoryStorage) StoreURLTestHistoryForOutbound(outbound adapter.Outbound, history *adapter.URLTestHistory) bool {
	if s == nil || outbound == nil {
		return false
	}
	s.access.Lock()
	defer s.access.Unlock()
	if len(s.retiredProviderOutbounds) > 0 {
		s.pruneRetiredLocked(time.Now())
	}
	if _, loaded := s.providerOutbounds[outbound]; loaded {
		if history == nil {
			delete(s.providerHistory, outbound)
		} else {
			s.providerHistory[outbound] = history
		}
	} else {
		if _, retired := s.retiredProviderOutbounds[outbound]; retired {
			return false
		}
		if history == nil {
			delete(s.delayHistory, outbound.Tag())
		} else {
			s.delayHistory[outbound.Tag()] = history
		}
	}
	s.notifyUpdated()
	return true
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = append(s.updateHooks, hook)
}

func (s *HistoryStorage) NotifyUpdated() {
	s.access.RLock()
	defer s.access.RUnlock()
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadURLTestHistory(tag string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return s.delayHistory[tag]
}

func (s *HistoryStorage) DeleteURLTestHistory(tag string) {
	s.access.Lock()
	delete(s.delayHistory, tag)
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) StoreURLTestHistory(tag string, history *adapter.URLTestHistory) {
	s.access.Lock()
	s.delayHistory[tag] = history
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) UpdateURLTestHistory(stored map[string]*adapter.URLTestHistory, deleted []string) {
	s.access.Lock()
	defer s.access.Unlock()
	for _, tag := range deleted {
		delete(s.delayHistory, tag)
	}
	maps.Copy(s.delayHistory, stored)
}

func (s *HistoryStorage) UpdateURLTestHistoryForOutbounds(stored map[adapter.Outbound]*adapter.URLTestHistory, deleted []adapter.Outbound) map[adapter.Outbound]bool {
	if s == nil {
		return nil
	}
	s.access.Lock()
	defer s.access.Unlock()
	if len(s.retiredProviderOutbounds) > 0 {
		s.pruneRetiredLocked(time.Now())
	}
	for _, outbound := range deleted {
		if outbound == nil {
			continue
		}
		if _, loaded := s.providerOutbounds[outbound]; loaded {
			delete(s.providerHistory, outbound)
		} else if _, retired := s.retiredProviderOutbounds[outbound]; !retired {
			delete(s.delayHistory, outbound.Tag())
		}
	}
	storedOutbounds := make(map[adapter.Outbound]bool, len(stored))
	for outbound, history := range stored {
		if outbound == nil {
			continue
		}
		if _, loaded := s.providerOutbounds[outbound]; loaded {
			s.providerHistory[outbound] = history
			storedOutbounds[outbound] = true
		} else if _, retired := s.retiredProviderOutbounds[outbound]; !retired {
			s.delayHistory[outbound.Tag()] = history
			storedOutbounds[outbound] = true
		}
	}
	return storedOutbounds
}

func (s *HistoryStorage) notifyUpdated() {
	for _, updateHook := range s.updateHooks {
		updateHook.Emit(struct{}{})
	}
}

func (s *HistoryStorage) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = nil
	return nil
}

func URLTest(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	ctx, cancel := context.WithTimeout(ctx, C.TCPTimeout)
	defer cancel()
	var requestStart time.Time
	requestCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			requestStart = time.Now()
		},
	})
	req, err := http.NewRequestWithContext(requestCtx, http.MethodHead, link, nil)
	if err != nil {
		return 0, err
	}
	var dialed atomic.Bool
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
				if dialed.Swap(true) {
					return nil, E.New("connection is not reusable")
				}
				return detour.DialContext(ctx, network, M.ParseSocksaddr(addr))
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	delay := time.Since(requestStart)
	resp.Body.Close()
	if !resp.Close {
		resp, err = client.Do(req)
		if err == nil {
			delay = time.Since(requestStart)
			resp.Body.Close()
		} else if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}
	return uint16(max(1, delay.Milliseconds())), nil
}
