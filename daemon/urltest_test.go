package daemon

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type probeTestOutbound struct {
	adapter.Outbound
	tag       string
	dial      func(context.Context, string, M.Socksaddr) (net.Conn, error)
	multiplex bool
}

func (o *probeTestOutbound) Tag() string            { return o.tag }
func (o *probeTestOutbound) Type() string           { return "test" }
func (o *probeTestOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *probeTestOutbound) MultiplexEnabled() bool { return o.multiplex }
func (o *probeTestOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return o.dial(ctx, network, destination)
}

type probeTestManager struct {
	adapter.OutboundManager
	sync.RWMutex
	outbounds map[string]adapter.Outbound
	order     []adapter.Outbound
}

func (m *probeTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	m.RLock()
	defer m.RUnlock()
	outbound, ok := m.outbounds[tag]
	return outbound, ok
}
func (m *probeTestManager) Outbounds() []adapter.Outbound {
	m.RLock()
	defer m.RUnlock()
	if m.order != nil {
		return m.order
	}
	var result []adapter.Outbound
	for _, outbound := range m.outbounds {
		result = append(result, outbound)
	}
	return result
}

type probeTestGroup struct {
	*probeTestOutbound
	tags []string
	now  string
}

func (g *probeTestGroup) All() []string { return g.tags }
func (g *probeTestGroup) Now() string   { return g.now }

func probeTestInstance(t *testing.T, outbounds ...adapter.Outbound) *Instance {
	t.Helper()
	ctx, cancel := context.WithCancel(service.ContextWithDefaultRegistry(context.Background()))
	t.Cleanup(cancel)
	manager := &probeTestManager{outbounds: make(map[string]adapter.Outbound)}
	for _, outbound := range outbounds {
		manager.outbounds[outbound.Tag()] = outbound
	}
	history := urltest.NewHistoryStorage()
	t.Cleanup(func() { _ = history.Close() })
	return &Instance{ctx: ctx, outboundManager: manager, urlTestHistoryStorage: history}
}

func TestProbeOptions(t *testing.T) {
	options, err := parseProbeOptions("", 0, false)
	require.NoError(t, err)
	require.Equal(t, defaultProbeURL, options.url)
	require.Equal(t, 15*time.Second, options.timeout)
	for _, link := range []string{"file:///tmp/test", "https://", "http://user:pass@example.com", "http://example.com:bad"} {
		_, err = parseProbeOptions(link, 1000, false)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	_, err = parseProbeOptions("https://example.com", -1, true)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestProbeTimeoutAndURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}
		if r.Method != http.MethodHead || r.URL.RequestURI() != "/custom?probe=1" {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	var requests atomic.Int32
	outbound := &probeTestOutbound{tag: "node", dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		requests.Add(1)
		require.Equal(t, "proxy-only.invalid", destination.Fqdn)
		return (&net.Dialer{}).DialContext(ctx, "tcp4", server.Listener.Addr().String())
	}}
	instance := probeTestInstance(t, outbound)
	options := probeOptions{"http://proxy-only.invalid/custom?probe=1", time.Second, false}
	require.NoError(t, instance.probeOutbounds(instance.ctx, []adapter.Outbound{outbound}, options))
	require.NotNil(t, instance.urlTestHistoryStorage.LoadURLTestHistory("node"))
	options.url = "http://proxy-only.invalid/slow"
	options.timeout = 30 * time.Millisecond
	started := time.Now()
	require.NoError(t, instance.probeOutbounds(instance.ctx, []adapter.Outbound{outbound}, options))
	require.Less(t, time.Since(started), time.Second)
	require.Nil(t, instance.urlTestHistoryStorage.LoadURLTestHistory("node"))
	require.EqualValues(t, 2, requests.Load())
}

func TestProbeTimeoutLongerThanCoreDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("verifies a request exceeding the core's fixed 15 second HTTP timeout")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(15200 * time.Millisecond):
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	outbound := &probeTestOutbound{tag: "node", dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp4", server.Listener.Addr().String())
	}}
	instance := probeTestInstance(t, outbound)
	ctx, cancel := context.WithTimeout(instance.ctx, 20*time.Second)
	defer cancel()
	delay, err := probeURL(ctx, "http://probe.invalid", outbound)
	require.NoError(t, err)
	require.GreaterOrEqual(t, delay, uint16(15000))
}

type probeTestCertificates struct {
	adapter.CertificateStore
	pool *x509.CertPool
}

func (s probeTestCertificates) Pool() *x509.CertPool { return s.pool }

func TestProbeIPv6ThroughIPv4ProxyAndClearFailure(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"one.one.one.one"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	var tlsRequests atomic.Int32
	tlsServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tlsRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	tlsServer.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	tlsServer.StartTLS()
	defer tlsServer.Close()
	latencyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer latencyServer.Close()
	var ipv6Fails atomic.Bool
	outbound := &probeTestOutbound{tag: "node", dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		address := latencyServer.Listener.Addr().String()
		if destination.Addr.Is6() {
			require.Empty(t, destination.Fqdn)
			require.Equal(t, "2606:4700:4700::1111", destination.Addr.String())
			if ipv6Fails.Load() {
				return nil, net.ErrClosed
			}
			address = tlsServer.Listener.Addr().String()
		}
		// Simulates an IPv4 proxy connection carrying an IPv6 destination.
		return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
	}}
	instance := probeTestInstance(t, outbound)
	service.MustRegister[adapter.CertificateStore](instance.ctx, probeTestCertificates{pool: pool})
	options := probeOptions{"http://latency.invalid", time.Second, true}
	require.NoError(t, instance.probeOutbounds(instance.ctx, []adapter.Outbound{outbound}, options))
	require.True(t, instance.outboundInfo(outbound).Ipv6)
	require.EqualValues(t, 1, tlsRequests.Load())
	ipv6Fails.Store(true)
	require.NoError(t, instance.probeOutbounds(instance.ctx, []adapter.Outbound{outbound}, options))
	require.False(t, instance.outboundInfo(outbound).Ipv6)
	require.NotNil(t, instance.urlTestHistoryStorage.LoadURLTestHistory("node"), "IPv6 failure must not erase successful latency")
	options.ipv6 = false
	require.NoError(t, instance.probeOutbounds(instance.ctx, []adapter.Outbound{outbound}, options))
	require.EqualValues(t, 1, tlsRequests.Load())
}

func TestProbeNestedGroupsDeduplicateAndCancel(t *testing.T) {
	var calls atomic.Int32
	outbound := &probeTestOutbound{tag: "node", dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		calls.Add(1)
		return nil, net.ErrClosed
	}}
	a := &probeTestGroup{probeTestOutbound: &probeTestOutbound{tag: "a"}, tags: []string{"b", "node"}, now: "b"}
	b := &probeTestGroup{probeTestOutbound: &probeTestOutbound{tag: "b"}, tags: []string{"a", "node"}, now: "node"}
	instance := probeTestInstance(t, outbound, a, b)
	options := probeOptions{defaultProbeURL, time.Second, false}
	require.NoError(t, instance.probeOutbounds(instance.ctx, []adapter.Outbound{a, b, outbound}, options))
	require.EqualValues(t, 1, calls.Load())
	instance.ipv6Results = map[string]outboundIPv6Result{"node": {outbound, true}}
	require.True(t, instance.outboundInfo(a).Ipv6)
	require.True(t, instance.outboundInfo(a).Udp)
	manager := instance.outboundManager.(*probeTestManager)
	manager.outbounds["node"] = &probeTestOutbound{tag: "node"}
	require.False(t, instance.outboundInfo(a).Ipv6, "replaced nodes must not inherit IPv6 results")
	ctx, cancel := context.WithCancel(instance.ctx)
	cancel()
	instance.probeSlot <- struct{}{}
	require.ErrorIs(t, instance.probeOutbounds(ctx, nil, options), context.Canceled)
	<-instance.probeSlot
}

func TestProbeUnifiedDelayAndMultiplexWarmup(t *testing.T) {
	for _, multiplex := range []bool{false, true} {
		t.Run(map[bool]string{false: "unified", true: "multiplex"}[multiplex], func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			outbound := &probeTestOutbound{tag: "node", multiplex: multiplex, dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp4", server.Listener.Addr().String())
			}}
			instance := probeTestInstance(t, outbound)
			service.MustRegister[urltest.UnifiedDelay](instance.ctx, true)
			ctx, cancel := context.WithTimeout(instance.ctx, time.Second)
			defer cancel()
			_, err := probeURL(ctx, "http://probe.invalid", outbound)
			require.NoError(t, err)
			require.EqualValues(t, 2, requests.Load(), "multiplex warm-up must replace, not duplicate, unified-delay warm-up")
		})
	}
}

func TestProbeRefreshesNestedURLTestSelection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	fast := &probeTestOutbound{tag: "fast", dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp4", server.Listener.Addr().String())
	}}
	slow := &probeTestOutbound{tag: "slow"}
	other := &probeTestOutbound{tag: "other"}
	instance := probeTestInstance(t, fast, slow, other)
	manager := instance.outboundManager.(*probeTestManager)
	service.MustRegister[adapter.OutboundManager](instance.ctx, manager)
	service.MustRegisterPtr(instance.ctx, instance.urlTestHistoryStorage)
	newGroup := func(tag string, tags []string) *group.URLTest {
		outbound, err := group.NewURLTest(instance.ctx, nil, log.NewNOPFactory().Logger(), tag, option.URLTestOutboundOptions{GroupCommonOption: option.GroupCommonOption{Outbounds: tags}, Tolerance: 1})
		require.NoError(t, err)
		urlGroup := outbound.(*group.URLTest)
		manager.outbounds[tag] = urlGroup
		require.NoError(t, urlGroup.Start())
		t.Cleanup(func() { require.NoError(t, urlGroup.Close()) })
		return urlGroup
	}
	child := newGroup("child", []string{"fast", "slow"})
	parent := newGroup("parent", []string{"child", "other"})
	// Deliberately enumerate the parent first, as manager ordering is unrelated
	// to dependency order.
	manager.order = []adapter.Outbound{parent, child, fast, slow, other}
	for tag, delay := range map[string]uint16{"fast": 200, "slow": 100, "other": 50} {
		instance.urlTestHistoryStorage.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: time.Now(), Delay: delay})
	}
	child.PerformUpdateCheck()
	parent.PerformUpdateCheck()
	require.Equal(t, "slow", child.Now())
	require.Equal(t, "other", parent.Now())
	require.NoError(t, instance.probeOutbounds(instance.ctx, []adapter.Outbound{fast}, probeOptions{"http://probe.invalid", time.Second, false}))
	require.Equal(t, "fast", child.Now())
	require.Equal(t, "child", parent.Now())
}

func TestProbeBoundsConcurrentNodes(t *testing.T) {
	started := make(chan struct{}, 12)
	release := make(chan struct{})
	var roots []adapter.Outbound
	for n := range 12 {
		roots = append(roots, &probeTestOutbound{tag: big.NewInt(int64(n)).String(), dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
			started <- struct{}{}
			select {
			case <-release:
				return nil, net.ErrClosed
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}})
	}
	instance := probeTestInstance(t, roots...)
	ctx, cancel := context.WithTimeout(instance.ctx, 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- instance.probeOutbounds(ctx, roots, probeOptions{defaultProbeURL, time.Second, false}) }()
	for range 10 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("more than ten concurrent probes")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	require.Len(t, started, 2)
}
