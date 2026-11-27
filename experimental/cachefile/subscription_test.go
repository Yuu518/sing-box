package cachefile

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

func TestSubscriptionCacheSafety(t *testing.T) {
	ctx := context.Background()
	cache := New(ctx, log.NewNOPFactory().NewLogger("cache-file"), option.CacheFileOptions{
		Enabled: true,
		Path:    filepath.Join(t.TempDir(), "cache.db"),
	})

	require.NotPanics(t, func() {
		require.Nil(t, cache.LoadSubscription("missing"))
		require.Error(t, cache.SaveSubscription("missing", &adapter.SavedBinary{Content: []byte("data")}))
	})

	scope := adapter.NewScope(ctx, log.NewNOPFactory().NewLogger("test"))
	defer func() {
		_ = scope.Close()
	}()
	require.NoError(t, cache.Start(adapter.StartStateInitialize, scope))

	saved := &adapter.SavedBinary{
		Content:     []byte("payload"),
		LastUpdated: time.Now().Truncate(time.Second),
		LastEtag:    "etag-1",
		URLHash:     []byte("hash-1"),
		ContentHash: []byte("content-hash-1"),
	}
	require.NoError(t, cache.SaveSubscription("sub", saved))
	loaded := cache.LoadSubscription("sub")
	require.NotNil(t, loaded)
	require.Equal(t, saved.Content, loaded.Content)
	require.Equal(t, saved.LastEtag, loaded.LastEtag)
	require.Equal(t, saved.URLHash, loaded.URLHash)
	require.Equal(t, saved.ContentHash, loaded.ContentHash)
}
