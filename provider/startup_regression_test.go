package provider_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestPreserveStaticCompatible(t *testing.T) {
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	defer cancel()
	instance, err := box.New(box.Options{Context: ctx, Options: option.Options{
		Log:       &option.LogOptions{Disabled: true},
		Outbounds: []option.Outbound{{Type: "http", Tag: "Compatible", Options: &option.HTTPOutboundOptions{ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: 8080}}}},
		Route:     &option.RouteOptions{Final: "Compatible"},
	}})
	require.NoError(t, err)
	defer func() { cancel(); _ = instance.Close() }()
	require.NoError(t, instance.Start())
	manager := service.FromContext[adapter.OutboundManager](ctx)
	node, found := manager.Outbound("Compatible")
	require.True(t, found)
	require.Equal(t, "http", node.Type())
	require.Same(t, node, manager.Default())
	require.Len(t, manager.Outbounds(), 1)
}

func TestRemoteProviderLoadsBeforeDependentOutboundStarts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"outbounds":[{"type":"http","tag":"node","server":"127.0.0.1","server_port":8080}]}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	defer cancel()
	var options option.Options
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"selector","tag":"select","providers":["p"]},{"type":"http","tag":"chain","server":"127.0.0.1","server_port":8081,"detour":"node"}],"outbound_providers":[{"type":"remote","tag":"p","url":%q}],"route":{"final":"select"}}`, server.URL)
	require.NoError(t, json.UnmarshalContext(ctx, []byte(config), &options))
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	require.NoError(t, err)
	defer func() { cancel(); _ = instance.Close() }()
	require.NoError(t, instance.Start())
	manager := service.FromContext[adapter.OutboundManager](ctx)
	selected, found := manager.Outbound("select")
	require.True(t, found)
	require.Equal(t, "node", selected.(adapter.OutboundGroup).Now())
}

func TestProviderFallbackAvoidsUserNamespaces(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	defer cancel()
	registry := service.FromContext[adapter.EndpointRegistry](ctx).(*endpoint.Registry)
	endpoint.Register[option.DirectOutboundOptions](registry, "test-endpoint", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.DirectOutboundOptions) (adapter.Endpoint, error) {
		return &stageCountingEndpoint{Adapter: outbound.NewAdapter("test-endpoint", tag, []string{N.NetworkTCP}, nil), stages: make(map[adapter.StartStage]int)}, nil
	})
	var options option.Options
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"http","tag":"Compatible","server":"127.0.0.1","server_port":8080},{"type":"http","tag":"__provider_fallback__","server":"127.0.0.1","server_port":8081},{"type":"selector","tag":"select","providers":["p"]},{"type":"urltest","tag":"auto","providers":["p"],"url":%q}],"endpoints":[{"type":"test-endpoint","tag":"__provider_fallback__-2"}],"outbound_providers":[{"type":"inline","tag":"p"}],"route":{"final":"select"}}`, server.URL)
	require.NoError(t, json.UnmarshalContext(ctx, []byte(config), &options))
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	require.NoError(t, err)
	defer func() { cancel(); _ = instance.Close() }()
	require.NoError(t, instance.Start())
	manager := service.FromContext[adapter.OutboundManager](ctx)
	compatible, found := manager.Outbound("Compatible")
	require.True(t, found)
	require.Equal(t, "http", compatible.Type())
	fallback, err := manager.ProviderFallback()
	require.NoError(t, err)
	require.Equal(t, "direct", fallback.Type())
	require.NotContains(t, []string{"Compatible", "__provider_fallback__", "__provider_fallback__-2"}, fallback.Tag())
	for _, tag := range []string{"select", "auto"} {
		group, found := manager.Outbound(tag)
		require.True(t, found)
		require.Equal(t, []string{fallback.Tag()}, group.(adapter.OutboundGroup).All())
	}
}

type stageCountingEndpoint struct {
	outbound.Adapter
	N.Dialer
	stages map[adapter.StartStage]int
}

func (e *stageCountingEndpoint) Start(stage adapter.StartStage) error { e.stages[stage]++; return nil }
func (e *stageCountingEndpoint) Close() error                         { return nil }

func TestProviderEndpointStartsEachStageOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	defer cancel()
	stages := make(map[adapter.StartStage]int)
	registry := service.FromContext[adapter.EndpointRegistry](ctx).(*endpoint.Registry)
	endpoint.Register[option.DirectOutboundOptions](registry, "test-endpoint", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.DirectOutboundOptions) (adapter.Endpoint, error) {
		return &stageCountingEndpoint{Adapter: outbound.NewAdapter("test-endpoint", tag, []string{N.NetworkTCP}, nil), stages: stages}, nil
	})
	path := filepath.Join(t.TempDir(), "provider.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"endpoints":[{"type":"test-endpoint","tag":"ep"}]}`), 0o600))
	instance, err := box.New(box.Options{Context: ctx, Options: option.Options{
		Log:       &option.LogOptions{Disabled: true},
		Providers: []option.Provider{{Type: "local", Tag: "p", Options: &option.ProviderLocalOptions{Path: path}}},
	}})
	require.NoError(t, err)
	defer func() { cancel(); _ = instance.Close() }()
	require.NoError(t, instance.Start())
	for _, stage := range adapter.ListStartStages {
		t.Run(stage.String(), func(t *testing.T) { require.Equal(t, 1, stages[stage]) })
	}
}

type bootstrapEndpoint struct {
	stageCountingEndpoint
	ready bool
}

func (e *bootstrapEndpoint) Start(stage adapter.StartStage) error {
	if stage == adapter.StartStatePostStart {
		e.ready = true
	}
	return e.stageCountingEndpoint.Start(stage)
}

func (e *bootstrapEndpoint) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if !e.ready {
		return nil, errors.New("endpoint has not started")
	}
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func TestRemoteProviderCanDownloadThroughEarlierProviderEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"outbounds":[{"type":"http","tag":"node","server":"127.0.0.1","server_port":8080}]}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	defer cancel()
	registry := service.FromContext[adapter.EndpointRegistry](ctx).(*endpoint.Registry)
	stages := make(map[adapter.StartStage]int)
	endpoint.Register[option.DirectOutboundOptions](registry, "test-endpoint", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.DirectOutboundOptions) (adapter.Endpoint, error) {
		return &bootstrapEndpoint{stageCountingEndpoint: stageCountingEndpoint{Adapter: outbound.NewAdapter("test-endpoint", tag, []string{N.NetworkTCP}, nil), stages: stages}}, nil
	})
	path := filepath.Join(t.TempDir(), "provider.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"endpoints":[{"type":"test-endpoint","tag":"download"}]}`), 0o600))
	encodedPath, err := json.Marshal(path)
	require.NoError(t, err)
	var options option.Options
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbound_providers":[{"type":"local","tag":"local","path":%s},{"type":"remote","tag":"remote","url":%q,"http_client":{"detour":"download"}}]}`, encodedPath, server.URL)
	require.NoError(t, json.UnmarshalContext(ctx, []byte(config), &options))
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	require.NoError(t, err)
	defer func() { cancel(); _ = instance.Close() }()
	require.NoError(t, instance.Start())
	provider, found := service.FromContext[adapter.ProviderManager](ctx).Get("remote")
	require.True(t, found)
	require.Len(t, provider.Outbounds(), 1)
	for _, stage := range adapter.ListStartStages {
		require.Equal(t, 1, stages[stage], stage.String())
	}
}

func TestProviderBackgroundTasksStart(t *testing.T) {
	for _, kind := range []string{"local", "remote"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(include.Context(context.Background()))
			defer cancel()
			const initial = `{"outbounds":[{"type":"http","tag":"initial","server":"127.0.0.1","server_port":1}]}`
			const updated = `{"outbounds":[{"type":"http","tag":"updated","server":"127.0.0.1","server_port":1}]}`
			path := filepath.Join(t.TempDir(), "provider.json")
			var config option.Provider
			if kind == "local" {
				require.NoError(t, os.WriteFile(path, []byte(initial), 0o600))
				config = option.Provider{Type: kind, Tag: "p", Options: &option.ProviderLocalOptions{Path: path}}
			} else {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, updated) }))
				defer server.Close()
				require.NoError(t, os.WriteFile(path, []byte("# sing-box provider cache v1\n"+initial), 0o600))
				oldTime := time.Now().Add(-48 * time.Hour)
				require.NoError(t, os.Chtimes(path, oldTime, oldTime))
				config = option.Provider{Type: kind, Tag: "p", Options: &option.ProviderRemoteOptions{URL: server.URL, Path: path}}
			}
			instance, err := box.New(box.Options{Context: ctx, Options: option.Options{Log: &option.LogOptions{Disabled: true}, Providers: []option.Provider{config}}})
			require.NoError(t, err)
			defer func() { cancel(); _ = instance.Close() }()
			require.NoError(t, instance.Start())
			p, found := service.FromContext[adapter.ProviderManager](ctx).Get("p")
			require.True(t, found)
			require.NotPanics(t, func() { _, err := p.HealthCheck(ctx); require.NoError(t, err) })
			if kind == "local" {
				require.NoError(t, os.WriteFile(path, []byte(updated), 0o600))
			}
			require.Eventually(t, func() bool { _, found := p.Outbound("updated"); return found }, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestStaticOutboundDetourToProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	defer cancel()
	path := filepath.Join(t.TempDir(), "provider.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"outbounds":[{"type":"http","tag":"node","server":"127.0.0.1","server_port":8080}]}`), 0o600))
	instance, err := box.New(box.Options{Context: ctx, Options: option.Options{
		Log:       &option.LogOptions{Disabled: true},
		Outbounds: []option.Outbound{{Type: "http", Tag: "chain", Options: &option.HTTPOutboundOptions{DialerOptions: option.DialerOptions{Detour: "node"}, ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: 8081}}}},
		Providers: []option.Provider{{Type: "local", Tag: "p", Options: &option.ProviderLocalOptions{Path: path}}},
	}})
	require.NoError(t, err)
	defer func() { cancel(); _ = instance.Close() }()
	require.NoError(t, instance.Start())
}
