package urltest

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
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

func (s *HistoryStorage) RemoveProviderOutbound(outbound adapter.Outbound) {
	s.access.Lock()
	defer s.access.Unlock()
	if _, loaded := s.providerOutbounds[outbound]; !loaded {
		return
	}
	delete(s.providerOutbounds, outbound)
	delete(s.providerHistory, outbound)
	now := time.Now()
	for retired, removedAt := range s.retiredProviderOutbounds {
		if now.Sub(removedAt) > retiredProviderOutboundTTL {
			delete(s.retiredProviderOutbounds, retired)
		}
	}
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
	s.access.Lock()
	defer s.access.Unlock()
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
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, link, nil)
	if err != nil {
		return 0, err
	}
	var dialed atomic.Bool
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
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
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	delay := time.Since(start)
	if resp.Close {
		return uint16(delay / time.Millisecond), nil
	}

	start = time.Now()
	resp, err = client.Do(req)
	if err == nil {
		resp.Body.Close()
		delay = time.Since(start)
	} else if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return uint16(delay / time.Millisecond), nil
}
