package remote

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/hash"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

const rawTestSubscription = `{"outbounds":[{"type":"http","tag":"a","server":"a.example","server_port":443,"detour":"source"},{"type":"http","tag":"b","server":"b.example","server_port":443,"detour":"source"}]}`

type cacheTestOutbound struct {
	outbound.Adapter
	N.Dialer
	options option.HTTPOutboundOptions
}

func newTestRegexp(t *testing.T, pattern string) *option.Regexp {
	t.Helper()
	content, err := json.Marshal(pattern)
	require.NoError(t, err)
	var expression option.Regexp
	require.NoError(t, json.Unmarshal(content, &expression))
	return &expression
}

func (*cacheTestOutbound) Close() error { return nil }

type cacheTestTransport func(*http.Request) (*http.Response, error)

func (f cacheTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type cacheTestStore struct {
	adapter.CacheFile
	saved *adapter.SavedBinary
}

func (s *cacheTestStore) LoadSubscription(string) *adapter.SavedBinary {
	if s.saved == nil {
		return nil
	}
	copy := *s.saved
	return &copy
}

func (s *cacheTestStore) SaveSubscription(_ string, saved *adapter.SavedBinary) error {
	copy := *saved
	s.saved = &copy
	return nil
}

func newCacheTestProvider(t *testing.T, cache *cacheTestStore, options option.ProviderRemoteOptions) *ProviderRemote {
	t.Helper()
	return newCacheTestProviderWithEndpoints(t, cache, options, endpoint.NewRegistry())
}

func newCacheTestProviderWithEndpoints(t *testing.T, cache *cacheTestStore, options option.ProviderRemoteOptions, endpointRegistry *endpoint.Registry) *ProviderRemote {
	t.Helper()
	ctx := context.Background()
	logger := log.NewNOPFactory()
	registry := outbound.NewRegistry()
	outbound.Register[option.HTTPOutboundOptions](registry, "http", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options option.HTTPOutboundOptions) (adapter.Outbound, error) {
		return &cacheTestOutbound{Adapter: outbound.NewAdapter("http", tag, []string{N.NetworkTCP}, nil), options: options}, nil
	})
	endpoints := endpoint.NewManager(logger.NewLogger("test"), endpointRegistry)
	manager := outbound.NewManager(logger.NewLogger("test"), registry, endpoints, "")
	ctx = service.ContextWith[option.OutboundOptionsRegistry](ctx, registry)
	ctx = service.ContextWith[option.EndpointOptionsRegistry](ctx, endpointRegistry)
	ctx = service.ContextWith[adapter.OutboundManager](ctx, manager)
	ctx = service.ContextWith[adapter.EndpointManager](ctx, endpoints)
	options.URL = "https://example.com/subscription"
	raw, err := NewProviderRemote(ctx, nil, logger, "test", options)
	require.NoError(t, err)
	provider := raw.(*ProviderRemote)
	provider.cacheFile = cache
	t.Cleanup(func() { provider.cancel(); _ = provider.Adapter.Close() })
	return provider
}

func setCacheTestResponse(provider *ProviderRemote, status int, content string, info string) {
	provider.httpClient = &http.Client{Transport: cacheTestTransport(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Etag", "subscription-v1")
		if info != "" {
			header.Set("subscription-userinfo", info)
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: header, Body: io.NopCloser(strings.NewReader(content))}, nil
	})}
}

func TestCacheReappliesCurrentFiltersAndOverrides(t *testing.T) {
	for _, fileCache := range []bool{false, true} {
		t.Run(map[bool]string{false: "database", true: "file"}[fileCache], func(t *testing.T) {
			cache := &cacheTestStore{}
			options := option.ProviderRemoteOptions{
				Include:        newTestRegexp(t, "^(?!b$).*"),
				OverrideDialer: &option.OverrideDialerOptions{Detour: common.Ptr("old")},
			}
			if fileCache {
				options.Path = filepath.Join(t.TempDir(), "provider.cache")
			}
			first := newCacheTestProvider(t, cache, options)
			setCacheTestResponse(first, http.StatusOK, rawTestSubscription, "")
			require.NoError(t, first.fetch(first.ctx, true))
			require.Len(t, first.Outbounds(), 1)
			require.Equal(t, "a", first.Outbounds()[0].Tag())
			options.Include = newTestRegexp(t, "^(?=b$).*")
			options.OverrideDialer.Detour = common.Ptr("new")
			restarted := newCacheTestProvider(t, cache, options)
			loaded, err := restarted.loadCacheFile()
			require.NoError(t, err)
			require.True(t, loaded)
			require.Len(t, restarted.Outbounds(), 1)
			node := restarted.Outbounds()[0].(*cacheTestOutbound)
			require.Equal(t, "b", node.Tag())
			require.Equal(t, "new", node.options.Detour)
			setCacheTestResponse(restarted, http.StatusNotModified, "", "")
			require.NoError(t, restarted.fetch(restarted.ctx, false))
			options.Include, options.OverrideDialer = nil, nil
			unfiltered := newCacheTestProvider(t, cache, options)
			loaded, err = unfiltered.loadCacheFile()
			require.NoError(t, err)
			require.True(t, loaded)
			require.Len(t, unfiltered.Outbounds(), 2)
			for _, node := range unfiltered.Outbounds() {
				require.Equal(t, "source", node.(*cacheTestOutbound).options.Detour)
			}
		})
	}
}

func TestNotModifiedCacheHashMatchesFile(t *testing.T) {
	cache := &cacheTestStore{}
	options := option.ProviderRemoteOptions{Path: filepath.Join(t.TempDir(), "provider.cache")}
	provider := newCacheTestProvider(t, cache, options)
	setCacheTestResponse(provider, http.StatusOK, rawTestSubscription, "upload=1; download=2; total=100; expire=200;")
	require.NoError(t, provider.fetch(provider.ctx, true))
	setCacheTestResponse(provider, http.StatusNotModified, "", "upload=1; download=3; total=100; expire=200;")
	require.NoError(t, provider.fetch(provider.ctx, false))
	content, err := os.ReadFile(options.Path)
	require.NoError(t, err)
	require.True(t, cache.saved.Hash.Equal(hash.MakeHash(content)))
	restarted := newCacheTestProvider(t, cache, options)
	loaded, err := restarted.loadCacheFile()
	require.NoError(t, err)
	require.True(t, loaded)
	require.EqualValues(t, 3, restarted.SubscriptionInfo().Download)
	setCacheTestResponse(restarted, http.StatusNotModified, "", "")
	require.NoError(t, restarted.fetch(restarted.ctx, false))
	require.EqualValues(t, 3, restarted.SubscriptionInfo().Download)
}

type cacheTestBody struct {
	io.Reader
	closed bool
}

func (b *cacheTestBody) Close() error { b.closed = true; return nil }

func TestDownloadErrorsCloseBodyAndPreserveETag(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusInternalServerError, http.StatusNotModified, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			provider := newCacheTestProvider(t, &cacheTestStore{}, option.ProviderRemoteOptions{})
			body := &cacheTestBody{Reader: strings.NewReader(`{"outbounds":null}`)}
			provider.httpClient = &http.Client{Transport: cacheTestTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: http.Header{"Etag": {"invalid"}}, Body: body}, nil
			})}
			require.Error(t, provider.fetch(provider.ctx, true))
			require.True(t, body.closed)
			require.Empty(t, provider.lastEtag)
		})
	}
}

func TestLegacyFilteredCacheRequiresRefetch(t *testing.T) {
	provider := newCacheTestProvider(t, &cacheTestStore{saved: &adapter.SavedBinary{Content: []byte(rawTestSubscription)}}, option.ProviderRemoteOptions{})
	loaded, err := provider.loadCacheFile()
	require.False(t, loaded)
	require.ErrorContains(t, err, "legacy provider cache")
}
