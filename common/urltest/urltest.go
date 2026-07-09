package urltest

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/common/observable"
	"github.com/sagernet/sing/service"
)

type UnifiedDelay bool

type HistoryStorage struct {
	access       sync.RWMutex
	delayHistory map[string]*adapter.URLTestHistory
	updateHooks  []*observable.Subscriber[struct{}]
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{
		delayHistory: make(map[string]*adapter.URLTestHistory),
	}
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
	unifiedDelay := bool(service.FromContext[UnifiedDelay](ctx))
	multiplexOutbound, isMultiplexOutbound := common.Cast[adapter.OutboundWithMultiplex](detour)
	if isMultiplexOutbound && multiplexOutbound.MultiplexEnabled() {
		_, err := urlTest(ctx, link, detour, false)
		if err != nil {
			return 0, err
		}
		unifiedDelay = false
	}
	return urlTest(ctx, link, detour, unifiedDelay)
}

func urlTest(ctx context.Context, link string, detour N.Dialer, unifiedDelay bool) (t uint16, err error) {
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return
	}
	hostname := linkURL.Hostname()
	port := linkURL.Port()
	if port == "" {
		switch linkURL.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}

	start := time.Now()
	instance, err := detour.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(hostname, port))
	if err != nil {
		return
	}
	defer instance.Close()
	if N.NeedHandshakeForWrite(instance) {
		start = time.Now()
	}
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return
	}
	var dialed atomic.Bool
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if dialed.Swap(true) {
					return nil, E.New("connection is not reusable")
				}
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: C.TCPTimeout,
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return
	}
	resp.Body.Close()
	if unifiedDelay {
		unifiedStart := time.Now()
		var unifiedResponse *http.Response
		unifiedResponse, err = client.Do(req.WithContext(ctx))
		if err != nil {
			if logFactory := service.FromContext[log.Factory](ctx); logFactory != nil {
				logFactory.NewLogger("urltest").DebugContext(ctx, "unified delay unavailable for ", link, ": ", err)
			}
			err = nil
		} else {
			unifiedResponse.Body.Close()
			start = unifiedStart
		}
	}
	t = uint16(time.Since(start) / time.Millisecond)
	return
}
