package daemon

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/protocol/group"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type apiURLTestOptions struct {
	url     string
	timeout time.Duration
	ipv6    bool
}

func parseAPIURLTestOptions(link string, timeoutMs int32, ipv6 bool) (apiURLTestOptions, error) {
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	u, err := url.Parse(link)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return apiURLTestOptions{}, status.Error(codes.InvalidArgument, "invalid URL test URL")
	}
	if timeoutMs < 0 {
		return apiURLTestOptions{}, status.Error(codes.InvalidArgument, "negative URL test timeout")
	}
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = C.TCPTimeout
	}
	return apiURLTestOptions{link, timeout, ipv6}, nil
}

type apiIPv6Result struct {
	outbound  adapter.Outbound
	supported bool
}

type apiURLTester struct {
	ctx      context.Context
	cancel   context.CancelFunc
	instance *Instance
	access   sync.Mutex
	active   map[string]bool
	ipv6     map[string]apiIPv6Result
	slots    chan struct{}
}

func newAPIURLTester(instance *Instance) *apiURLTester {
	ctx, cancel := context.WithCancel(instance.ctx)
	return &apiURLTester{
		ctx: ctx, cancel: cancel, instance: instance,
		active: make(map[string]bool), ipv6: make(map[string]apiIPv6Result),
		slots: make(chan struct{}, 10),
	}
}

func (i *Instance) apiOutbound(tag string) (adapter.Outbound, bool) {
	if i.outboundManager != nil {
		if outbound, loaded := i.outboundManager.Outbound(tag); loaded {
			return outbound, true
		}
	}
	if i.endpointManager != nil {
		if endpoint, loaded := i.endpointManager.Get(tag); loaded {
			return endpoint, true
		}
	}
	return nil, false
}

func (i *Instance) apiGroupItem(outbound adapter.Outbound) *GroupItem {
	item := &GroupItem{Tag: outbound.Tag(), Type: outbound.Type()}
	seen := make(map[string]bool)
	for {
		if seen[outbound.Tag()] {
			return item
		}
		seen[outbound.Tag()] = true
		outboundGroup, isGroup := outbound.(adapter.OutboundGroup)
		if !isGroup {
			break
		}
		var loaded bool
		outbound, loaded = i.apiOutbound(outboundGroup.Now())
		if !loaded {
			return item
		}
	}
	item.Udp = slices.Contains(outbound.Network(), N.NetworkUDP)
	if history := i.urlTestHistoryStorage.LoadURLTestHistory(outbound.Tag()); history != nil {
		item.UrlTestTime = history.Time.Unix()
		item.UrlTestDelay = int32(history.Delay)
	}
	if i.apiURLTest != nil {
		i.apiURLTest.access.Lock()
		result := i.apiURLTest.ipv6[outbound.Tag()]
		item.Ipv6 = result.outbound == outbound && result.supported
		i.apiURLTest.access.Unlock()
	}
	return item
}

func (t *apiURLTester) prepare(roots []adapter.Outbound) (func(context.Context, apiURLTestOptions), error) {
	var outbounds []adapter.Outbound
	var groups []*group.URLTest
	seen := make(map[string]bool)
	visiting := make(map[string]bool)
	var visit func(adapter.Outbound) error
	visit = func(outbound adapter.Outbound) error {
		tag := outbound.Tag()
		if visiting[tag] {
			return status.Error(codes.FailedPrecondition, "cyclic outbound group: "+tag)
		}
		if seen[tag] {
			return nil
		}
		seen[tag], visiting[tag] = true, true
		defer delete(visiting, tag)
		if outboundGroup, ok := outbound.(adapter.OutboundGroup); ok {
			for _, memberTag := range outboundGroup.All() {
				if member, loaded := t.instance.apiOutbound(memberTag); loaded {
					if err := visit(member); err != nil {
						return err
					}
				}
			}
			if urlTestGroup, ok := outbound.(*group.URLTest); ok {
				groups = append(groups, urlTestGroup)
			}
		} else {
			outbounds = append(outbounds, outbound)
		}
		return nil
	}
	for _, outbound := range roots {
		if err := visit(outbound); err != nil {
			return nil, err
		}
	}
	t.access.Lock()
	defer t.access.Unlock()
	if t.ctx.Err() != nil {
		return nil, status.Error(codes.FailedPrecondition, "service stopped")
	}
	for _, outbound := range outbounds {
		if t.active[outbound.Tag()] {
			return nil, status.Error(codes.Aborted, "outbound is being tested: "+outbound.Tag())
		}
	}
	for _, outbound := range outbounds {
		t.active[outbound.Tag()] = true
	}
	return func(ctx context.Context, options apiURLTestOptions) {
		defer func() {
			t.access.Lock()
			for _, outbound := range outbounds {
				delete(t.active, outbound.Tag())
			}
			t.access.Unlock()
		}()
		var tasks sync.WaitGroup
	testLoop:
		for _, outbound := range outbounds {
			select {
			case t.slots <- struct{}{}:
			case <-ctx.Done():
				break testLoop
			}
			tasks.Add(1)
			go func() {
				defer tasks.Done()
				defer func() { <-t.slots }()
				t.test(ctx, outbound, options)
			}()
		}
		tasks.Wait()
		if ctx.Err() == nil {
			for _, outboundGroup := range groups {
				if current, loaded := t.instance.apiOutbound(outboundGroup.Tag()); loaded && current == outboundGroup {
					outboundGroup.PerformUpdateCheck()
				}
			}
		}
	}, nil
}

func (t *apiURLTester) test(ctx context.Context, outbound adapter.Outbound, options apiURLTestOptions) {
	delay, err := probeAPIURL(ctx, options, outbound, false)
	if ctx.Err() != nil {
		return
	}
	var history *adapter.URLTestHistory
	if err == nil {
		history = &adapter.URLTestHistory{Time: time.Now(), Delay: delay}
	}
	if !t.instance.urlTestHistoryStorage.StoreURLTestHistoryForOutbound(outbound, history) {
		return
	}
	if options.ipv6 {
		_, err = probeAPIURL(ctx, options, outbound, true)
		if ctx.Err() != nil {
			return
		}
		t.access.Lock()
		if current, loaded := t.instance.apiOutbound(outbound.Tag()); loaded && current == outbound {
			t.ipv6[outbound.Tag()] = apiIPv6Result{outbound, err == nil}
		}
		t.access.Unlock()
		t.instance.urlTestHistoryStorage.NotifyUpdated()
	}
}

func probeAPIURL(ctx context.Context, options apiURLTestOptions, outbound N.Dialer, ipv6 bool) (uint16, error) {
	ctx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, options.url, nil)
	if err != nil {
		return 0, err
	}
	var dialed atomic.Bool
	transport := &http.Transport{
		DialContext: func(_ context.Context, network, address string) (net.Conn, error) {
			if dialed.Swap(true) {
				return nil, errors.New("connection is not reusable")
			}
			destination := M.ParseSocksaddr(address)
			if !ipv6 {
				return outbound.DialContext(ctx, network, destination)
			}
			if destination.IsDomain() {
				router := service.FromContext[adapter.DNSRouter](ctx)
				if router == nil {
					return nil, errors.New("missing DNS router")
				}
				addresses, lookupErr := router.Lookup(ctx, destination.Fqdn, adapter.DNSQueryOptions{Strategy: C.DomainStrategyIPv6Only})
				if lookupErr != nil {
					return nil, lookupErr
				}
				var lastErr error
				for _, address := range addresses {
					if !address.Is6() || address.Is4In6() {
						continue
					}
					conn, dialErr := outbound.DialContext(ctx, network, M.Socksaddr{Addr: address, Port: destination.Port})
					if dialErr == nil {
						return conn, nil
					}
					lastErr = dialErr
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}
				}
				if lastErr != nil {
					return nil, lastErr
				}
				return nil, errors.New("no IPv6 address for URL test")
			}
			if !destination.Addr.Is6() || destination.Addr.Is4In6() {
				return nil, errors.New("URL test target is not IPv6")
			}
			return outbound.DialContext(ctx, network, destination)
		},
		TLSClientConfig: &tls.Config{Time: ntp.TimeFuncFromContext(ctx), RootCAs: adapter.RootPoolFromContext(ctx)},
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer transport.CloseIdleConnections()
	start := time.Now()
	response, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	response.Body.Close()
	delay := time.Since(start)
	if !response.Close && !ipv6 {
		start = time.Now()
		response, err = client.Do(req)
		if err == nil {
			response.Body.Close()
			delay = time.Since(start)
		} else if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}
	return uint16(min(max(delay.Milliseconds(), 1), 65535)), nil
}
