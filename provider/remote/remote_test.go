package remote

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	providerAdapter "github.com/sagernet/sing-box/adapter/provider"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

type subscriptionCacheStub struct {
	adapter.CacheFile
	saved *adapter.SavedBinary
}

func (c *subscriptionCacheStub) LoadSubscription(string) *adapter.SavedBinary {
	return c.saved
}

func TestProviderURLHash(t *testing.T) {
	t.Parallel()

	const providerURL = "https://example.com/provider"
	provider, err := NewProvider(
		context.Background(),
		nil,
		log.NewNOPFactory(),
		"test",
		option.ProviderRemoteOptions{URL: providerURL},
	)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte(providerURL)), provider.(*Provider).urlHash)
}

func TestProviderRejectsCacheFromDifferentURL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logFactory := log.NewNOPFactory()
	logger := logFactory.NewLogger("test")
	providerURLHash := sha256.Sum256([]byte("https://example.com/new-provider"))
	provider := &Provider{
		Adapter: providerAdapter.NewAdapter(
			ctx,
			nil,
			logFactory,
			logger,
			"test",
			C.ProviderTypeRemote,
			option.ProviderHealthCheckOptions{},
		),
		ctx:     ctx,
		logger:  logger,
		urlHash: providerURLHash,
		cacheFile: &subscriptionCacheStub{saved: &adapter.SavedBinary{
			Content: []byte("invalid provider content must not be parsed"),
			URLHash: []byte("different URL hash"),
		}},
	}

	loaded, err := provider.loadCacheFile()
	require.NoError(t, err)
	require.False(t, loaded)
}

func TestParseInfoAndCommentInfo(t *testing.T) {
	t.Parallel()

	info, ok := parseInfo("Upload=1.5e3; DOWNLOAD=2048; total=10000; expire=1.7e9;")
	require.True(t, ok)
	require.Equal(t, int64(1500), info.Upload)
	require.Equal(t, int64(2048), info.Download)
	require.Equal(t, int64(10000), info.Total)
	require.Equal(t, int64(1700000000), info.Expire)

	_, ok = parseCommentInfo("ss://YWVzLTEyOC1nY206cGFzcw@1.2.3.4:443#total=100")
	require.False(t, ok)

	info, ok = parseCommentInfo("# upload=1; download=2; total=3; expire=4;")
	require.True(t, ok)
	require.Equal(t, adapter.SubscriptionInfo{Upload: 1, Download: 2, Total: 3, Expire: 4}, info)
}

type testRoundTripFunc func(*http.Request) (*http.Response, error)

func (f testRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestProviderRemoteFetchTimeoutAndSizeLimit(t *testing.T) {
	t.Parallel()

	cache := &cacheTestStore{}
	p := newCacheTestProvider(t, cache, option.ProviderRemoteOptions{})
	p.fetchTimeout = 50 * time.Millisecond
	p.httpClient = &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	err := p.fetch(p.ctx, false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, p.updating.Load())

	p.fetchTimeout = 5 * time.Second
	p.httpClient = &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(strings.Repeat("a", maxSubscriptionSize+1))),
		}, nil
	})}
	err = p.fetch(p.ctx, false)
	require.ErrorContains(t, err, "exceeds limit")
}

func TestProviderRemoteConcurrentUpdateAndClose(t *testing.T) {
	t.Parallel()

	cache := &cacheTestStore{}
	p := newCacheTestProvider(t, cache, option.ProviderRemoteOptions{
		Path: filepath.Join(t.TempDir(), "provider.cache"),
	})
	p.updateInterval = time.Hour
	setCacheTestResponse(p, http.StatusOK, rawTestSubscription, "upload=1; download=2; total=100; expire=200;")
	require.NoError(t, p.fetch(p.ctx, true))
	require.NoError(t, p.PostStart())

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Update()
			_ = p.UpdatedAt()
			_, _ = p.SubscriptionInfo()
		}()
	}
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, p.Close())
	wg.Wait()
}
