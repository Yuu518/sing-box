package daemon

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testProxyProvider struct {
	adapter.Provider
	tag       string
	outbounds []adapter.Outbound
	updated   atomic.Int64
}

func (p *testProxyProvider) Tag() string                   { return p.tag }
func (p *testProxyProvider) Type() string                  { return "remote" }
func (p *testProxyProvider) Outbounds() []adapter.Outbound { return p.outbounds }
func (p *testProxyProvider) UpdatedAt() time.Time {
	if stamp := p.updated.Load(); stamp > 0 {
		return time.UnixMilli(stamp)
	}
	return time.Time{}
}
func (p *testProxyProvider) SubscriptionInfo() adapter.SubscriptionInfo {
	return adapter.SubscriptionInfo{Upload: 10, Download: 20, Total: 100, Expire: 2000000000}
}

type testUpdatableProxyProvider struct {
	*testProxyProvider
	err error
}

func (p *testUpdatableProxyProvider) Update() error {
	if p.err != nil {
		return p.err
	}
	p.updated.Store(time.Now().UnixMilli())
	return nil
}

type testProxyProviderManager struct {
	adapter.ProviderManager
	providers []adapter.Provider
}

func (m *testProxyProviderManager) Providers() []adapter.Provider { return m.providers }
func (m *testProxyProviderManager) Get(tag string) (adapter.Provider, bool) {
	for _, provider := range m.providers {
		if provider.Tag() == tag {
			return provider, true
		}
	}
	return nil, false
}

func testProviderClient(t *testing.T, instance *Instance) (*StartedService, StartedServiceClient) {
	t.Helper()
	s := NewStartedService(ServiceOptions{Context: instance.ctx})
	s.instance = instance
	s.serviceStatus = &ServiceStatus{Status: ServiceStatus_STARTED}
	instance.urlTestHistoryStorage.AddUpdateHook(s.urlTestSubscriber)
	t.Cleanup(s.Close)
	listener := bufconn.Listen(1 << 20)
	server := NewServer(s, "")
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///daemon-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return s, NewStartedServiceClient(conn)
}

func TestProxyProviderRPCAndStream(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	outbound := &probeTestOutbound{tag: "node", dial: func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp4", server.Listener.Addr().String())
	}}
	instance := probeTestInstance(t, outbound)
	provider := &testUpdatableProxyProvider{testProxyProvider: &testProxyProvider{tag: "subscription", outbounds: []adapter.Outbound{outbound}}}
	instance.providerManager = &testProxyProviderManager{providers: []adapter.Provider{provider}}
	s, client := testProviderClient(t, instance)
	ctx, cancel := context.WithTimeout(instance.ctx, 5*time.Second)
	defer cancel()
	version, err := client.GetVersion(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.True(t, version.ProxyProvidersSupported)
	stream, err := client.SubscribeProxyProviders(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	initial, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, initial.Providers, 1)
	item := initial.Providers[0]
	require.True(t, item.Updatable)
	require.Zero(t, item.UpdatedAt)
	require.True(t, item.Outbounds[0].Udp)
	require.EqualValues(t, 2000000000, item.Subscription.Expire)
	require.EqualValues(t, 100, item.Subscription.Total)
	_, err = client.UpdateProxyProvider(ctx, &ProxyProviderRequest{Tag: "subscription"})
	require.NoError(t, err)
	updated, err := stream.Recv()
	require.NoError(t, err)
	require.Greater(t, updated.Providers[0].UpdatedAt, int64(1000000000000), "timestamps must use milliseconds")
	_, err = client.HealthCheckProxyProvider(ctx, &ProxyProviderHealthCheckRequest{Tag: "subscription", Url: "http://probe.invalid", TimeoutMs: 500})
	require.NoError(t, err)
	tested, err := stream.Recv()
	require.NoError(t, err)
	require.Positive(t, tested.Providers[0].Outbounds[0].UrlTestDelay)
	require.EqualValues(t, 1, requests.Load())
	// Metadata-only background refreshes must also reach the subscription.
	provider.updated.Add(1000)
	background, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, provider.updated.Load(), background.Providers[0].UpdatedAt)
	s.serviceAccess.Lock()
	s.updateStatus(ServiceStatus_IDLE)
	s.serviceAccess.Unlock()
	stopped, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, stopped.Providers)
	s.serviceAccess.Lock()
	s.updateStatus(ServiceStatus_STARTED)
	s.serviceAccess.Unlock()
	restarted, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, restarted.Providers, 1)
	cancel()
	_, err = stream.Recv()
	require.Equal(t, codes.Canceled, status.Code(err))
}

func TestProxyProviderRPCErrors(t *testing.T) {
	instance := probeTestInstance(t)
	readonly := &testProxyProvider{tag: "inline"}
	broken := &testUpdatableProxyProvider{testProxyProvider: &testProxyProvider{tag: "broken"}, err: errors.New("fetch failed")}
	instance.providerManager = &testProxyProviderManager{providers: []adapter.Provider{readonly, broken}}
	s, client := testProviderClient(t, instance)
	ctx, cancel := context.WithTimeout(instance.ctx, 3*time.Second)
	defer cancel()
	for tag, code := range map[string]codes.Code{"missing": codes.NotFound, "inline": codes.FailedPrecondition, "broken": codes.Unavailable} {
		_, err := client.UpdateProxyProvider(ctx, &ProxyProviderRequest{Tag: tag})
		require.Equal(t, code, status.Code(err))
	}
	_, err := client.HealthCheckProxyProvider(ctx, &ProxyProviderHealthCheckRequest{Tag: "inline", Url: "file:///test"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	s.serviceAccess.Lock()
	s.updateStatus(ServiceStatus_IDLE)
	s.serviceAccess.Unlock()
	_, err = client.UpdateProxyProvider(ctx, &ProxyProviderRequest{Tag: "broken"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
