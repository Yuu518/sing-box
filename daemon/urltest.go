package daemon

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-anytls"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultProbeURL = "https://www.gstatic.com/generate_204"

type probeOptions struct {
	url     string
	timeout time.Duration
	ipv6    bool
}

type outboundIPv6Result struct {
	outbound  adapter.Outbound
	available bool
}

func parseProbeOptions(link string, timeoutMs int32, ipv6 bool) (probeOptions, error) {
	if link == "" {
		link = defaultProbeURL
	}
	u, err := url.Parse(link)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return probeOptions{}, status.Error(codes.InvalidArgument, "URL test requires an HTTP or HTTPS URL without credentials")
	}
	if timeoutMs < 0 {
		return probeOptions{}, status.Error(codes.InvalidArgument, "URL test timeout must not be negative")
	}
	timeout := C.TCPTimeout
	if timeoutMs > 0 {
		timeout = time.Duration(timeoutMs) * time.Millisecond
	}
	return probeOptions{link, timeout, ipv6}, nil
}

// Combine the request lifetime with the service instance, keeping its registered
// TLS, clock and unified-delay settings. Detached URLTest jobs use the service
// context instead of the short-lived unary RPC context.
func (i *Instance) probeContext(request context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(i.ctx)
	stop := context.AfterFunc(request, cancel)
	return ctx, func() { stop(); cancel() }
}

func (i *Instance) probeOutbounds(ctx context.Context, roots []adapter.Outbound, options probeOptions) error {
	i.probeAccess.Lock()
	if i.probeSlot == nil {
		i.probeSlot = make(chan struct{}, 1)
	}
	slot := i.probeSlot
	i.probeAccess.Unlock()
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	// Serialize manual batches per instance and deduplicate nested groups. Native
	// periodic health checks retain their own schedule and shared delay history.
	seen := make(map[string]bool)
	var leaves []adapter.Outbound
	var expand func(adapter.Outbound)
	expand = func(outbound adapter.Outbound) {
		if outbound == nil || seen[outbound.Tag()] {
			return
		}
		seen[outbound.Tag()] = true
		if nested, ok := outbound.(adapter.OutboundGroup); ok {
			for _, tag := range nested.All() {
				child, _ := i.outboundManager.Outbound(tag)
				expand(child)
			}
		} else {
			leaves = append(leaves, outbound)
		}
	}
	for _, outbound := range roots {
		expand(outbound)
	}
	jobs := make(chan adapter.Outbound)
	var workers sync.WaitGroup
	for range min(10, len(leaves)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for outbound := range jobs {
				i.probeOutbound(ctx, outbound, options)
			}
		}()
	}
dispatch:
	for _, outbound := range leaves {
		select {
		case jobs <- outbound:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	workers.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Children must select their new leaf before their parents read its history.
	// Include dependent groups outside the requested roots (e.g. provider tests).
	clear(seen)
	var refresh func(adapter.Outbound)
	refresh = func(outbound adapter.Outbound) {
		if outbound == nil || seen[outbound.Tag()] {
			return
		}
		seen[outbound.Tag()] = true
		if nested, ok := outbound.(adapter.OutboundGroup); ok {
			for _, tag := range nested.All() {
				child, _ := i.outboundManager.Outbound(tag)
				refresh(child)
			}
		}
		if urlGroup, ok := outbound.(*group.URLTest); ok {
			urlGroup.PerformUpdateCheck()
		}
	}
	for _, outbound := range i.outboundManager.Outbounds() {
		refresh(outbound)
	}
	return nil
}

func (i *Instance) probeOutbound(ctx context.Context, outbound adapter.Outbound, options probeOptions) {
	probeCtx, cancel := context.WithTimeout(ctx, options.timeout)
	delay, err := probeURL(probeCtx, options.url, outbound)
	cancel()
	if ctx.Err() != nil || !i.currentProbeOutbound(outbound) {
		return
	}
	if err != nil {
		i.urlTestHistoryStorage.DeleteURLTestHistory(outbound.Tag())
	} else {
		i.urlTestHistoryStorage.StoreURLTestHistory(outbound.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: delay})
	}
	if !options.ipv6 {
		return
	}
	probeCtx, cancel = context.WithTimeout(ctx, options.timeout)
	// Pin the destination to a literal IPv6 address, while retaining the HTTPS
	// hostname for certificate verification. The daemon does not resolve or dial
	// this target directly: the outbound may reach its proxy over IPv4, then ask
	// that proxy to connect to this IPv6 address.
	_, err = probeHTTP(probeCtx, "https://one.one.one.one/cdn-cgi/trace", ipv6ProbeDialer{Dialer: outbound}, false)
	cancel()
	if ctx.Err() != nil || !i.currentProbeOutbound(outbound) {
		return
	}
	i.probeAccess.Lock()
	if i.ipv6Results == nil {
		i.ipv6Results = make(map[string]outboundIPv6Result)
	}
	i.ipv6Results[outbound.Tag()] = outboundIPv6Result{outbound, err == nil}
	i.probeAccess.Unlock()
	i.urlTestHistoryStorage.NotifyUpdated()
}

func (i *Instance) currentProbeOutbound(outbound adapter.Outbound) bool {
	current, ok := i.outboundManager.Outbound(outbound.Tag())
	if !ok && i.endpointManager != nil {
		current, ok = i.endpointManager.Get(outbound.Tag())
	}
	return ok && current == outbound
}

type ipv6ProbeDialer struct{ N.Dialer }

func (d ipv6ProbeDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	destination.Addr = netip.MustParseAddr("2606:4700:4700::1111")
	destination.Fqdn = ""
	return d.Dialer.DialContext(ctx, network, destination)
}

func probeURL(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
	unifiedDelay := bool(service.FromContext[urltest.UnifiedDelay](ctx))
	multiplexOutbound, isMultiplexOutbound := common.Cast[adapter.OutboundWithMultiplex](detour)
	if isMultiplexOutbound && multiplexOutbound.MultiplexEnabled() {
		warmContext := adapter.ContextWithKeepSession(ctx)
		warmContext = mux.ContextWithKeepSession(warmContext)
		warmContext = anytls.ContextWithKeepSession(warmContext)
		warmContext = contextWithQUICKeepSession(warmContext)
		warmContext = snell.ContextWithKeepSession(warmContext)
		_, err := probeHTTP(warmContext, link, detour, false)
		if err != nil {
			return 0, err
		}
		unifiedDelay = false
	}
	return probeHTTP(ctx, link, detour, unifiedDelay)
}

func probeHTTP(ctx context.Context, link string, detour N.Dialer, unifiedDelay bool) (t uint16, err error) {
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
		// The caller bounds the complete probe, including dialing and warm-up.
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
	t = uint16(min(max(time.Since(start)/time.Millisecond, 1), 65535))
	return
}
