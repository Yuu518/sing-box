package rule

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"

	"github.com/stretchr/testify/require"
)

type ruleSetTestCache struct {
	adapter.CacheFile
	data []byte
	err  error
}

func (c *ruleSetTestCache) LoadRuleSet(string) *adapter.SavedBinary {
	if c.data == nil {
		return nil
	}
	var result adapter.SavedBinary
	if err := result.UnmarshalBinary(c.data); err != nil {
		panic(err)
	}
	return &result
}

func (c *ruleSetTestCache) SaveRuleSet(_ string, data *adapter.SavedBinary) error {
	if c.err != nil {
		return c.err
	}
	var err error
	c.data, err = data.MarshalBinary()
	return err
}

type ruleSetTestFileManager struct {
	filemanager.Manager
	writeErr  error
	renameErr error
}

func (m *ruleSetTestFileManager) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if flag&os.O_CREATE != 0 && m.writeErr != nil {
		return nil, m.writeErr
	}
	return m.Manager.OpenFile(name, flag, perm)
}

func (m *ruleSetTestFileManager) Rename(oldPath, newPath string) error {
	if m.renameErr != nil {
		return m.renameErr
	}
	return m.Manager.Rename(oldPath, newPath)
}

func TestRemoteRuleSetPersistenceFailureRecovery(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"file", "metadata", "rename", "database-only"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ctx := filemanager.WithDefault(t.Context(), "", root, os.Getuid(), os.Getgid())
			manager := &ruleSetTestFileManager{Manager: service.FromContext[filemanager.Manager](ctx)}
			ctx = service.ContextWith[filemanager.Manager](ctx, manager)
			cache := new(ruleSetTestCache)
			path := filepath.Join(root, "rules.json")
			if failure == "database-only" {
				path = ""
			}
			oldContent := `{"version":4,"rules":[{"domain":["old.example"]}]}`
			newContent := `{"version":4,"rules":[{"domain":["new.example"]}]}`
			content, etag := oldContent, `"v1"`
			ruleSet, err := NewRemoteRuleSet(ctx, logger.NOP(), "test", option.RuleSet{
				Type: C.RuleSetTypeRemote, Format: C.RuleSetFormatSource, Path: path,
				RemoteOptions: option.RemoteRuleSet{URL: "https://example.com/rules.json"},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
			ruleSet.IncRef()
			ruleSet.cacheFile = cache
			var requestETags []string
			ruleSet.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				requestETags = append(requestETags, request.Header.Get("If-None-Match"))
				status, body := http.StatusOK, content
				if request.Header.Get("If-None-Match") == etag {
					status, body = http.StatusNotModified, ""
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Etag": []string{etag}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			var callbacks int
			ruleSet.RegisterCallback(func(adapter.RuleSet) { callbacks++ })
			require.NoError(t, ruleSet.Update(t.Context()))
			updatedAt := ruleSet.UpdatedTime()
			savedBefore := cache.LoadRuleSet("test")
			content, etag = newContent, `"v2"`
			writeErr := errors.New("injected cache failure")
			switch failure {
			case "file":
				manager.writeErr = writeErr
			case "metadata", "database-only":
				cache.err = writeErr
			case "rename":
				manager.renameErr = writeErr
			}
			require.ErrorIs(t, ruleSet.Update(t.Context()), writeErr)
			require.Equal(t, updatedAt, ruleSet.UpdatedTime())
			require.Equal(t, 1, callbacks)
			require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "old.example"}))
			require.False(t, ruleSet.Match(&adapter.InboundContext{Domain: "new.example"}))
			require.Equal(t, savedBefore, cache.LoadRuleSet("test"))
			if path != "" && failure != "metadata" {
				persisted, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, oldContent, string(persisted))
			}
			manager.writeErr, manager.renameErr, cache.err = nil, nil, nil
			require.NoError(t, ruleSet.Update(t.Context()))
			require.NotEqual(t, `"v2"`, requestETags[len(requestETags)-1])
			require.Equal(t, 2, callbacks)
			restored, err := NewRemoteRuleSet(ctx, logger.NOP(), "test", ruleSetTestOptions(path))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, restored.Close()) })
			restored.cacheFile = cache
			loaded, err := restored.loadCacheFile()
			require.NoError(t, err)
			require.True(t, loaded)
			require.True(t, restored.Match(&adapter.InboundContext{Domain: "new.example"}))
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			if path == "" {
				require.Empty(t, entries)
			} else {
				require.Len(t, entries, 1, "temporary cache files must be removed")
			}
		})
	}
}

func ruleSetTestOptions(path string) option.RuleSet {
	return option.RuleSet{
		Type: C.RuleSetTypeRemote, Format: C.RuleSetFormatSource, Path: path,
		RemoteOptions: option.RemoteRuleSet{URL: "https://example.com/rules.json"},
	}
}

func TestRemoteRuleSetFallsBackFromMismatchedCacheFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "rules.json")
	content := []byte(`{"version":4,"rules":[{"domain":["cached.example"]}]}`)
	require.NoError(t, os.WriteFile(path, content, 0o600))
	cache := new(ruleSetTestCache)
	require.NoError(t, cache.SaveRuleSet("test", &adapter.SavedBinary{
		Content:  content,
		LastEtag: `"v1"`,
	}))
	ruleSet, err := NewRemoteRuleSet(t.Context(), logger.NOP(), "test", ruleSetTestOptions(path))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
	ruleSet.cacheFile = cache
	loaded, err := ruleSet.loadCacheFile()
	require.NoError(t, err)
	require.True(t, loaded)
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "cached.example"}))

	require.NoError(t, os.WriteFile(path, []byte(`{"version":4,"rules":[{"domain":["changed.example"]}]}`), 0o600))
	loaded, err = ruleSet.loadCacheFile()
	require.NoError(t, err)
	require.True(t, loaded)
	require.True(t, ruleSet.cacheDirty)
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "cached.example"}))
	require.False(t, ruleSet.Match(&adapter.InboundContext{Domain: "changed.example"}))
}

type ruleSetOfflineTransport struct {
	adapter.HTTPTransport
}

func TestRemoteRuleSetRejectsInvalidDatabaseFallback(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "rules.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":4,"rules":[{"domain":["example.com"]}]}`), 0o600))
	for _, content := range []string{"", "invalid"} {
		ruleSet, err := NewRemoteRuleSet(t.Context(), logger.NOP(), "test", ruleSetTestOptions(path))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
		cache := new(ruleSetTestCache)
		require.NoError(t, cache.SaveRuleSet("test", &adapter.SavedBinary{Content: []byte(content)}))
		ruleSet.cacheFile = cache
		loaded, err := ruleSet.loadCacheFile()
		require.Error(t, err)
		require.False(t, loaded)
		require.Zero(t, ruleSet.RuleCount())
	}
}

func (*ruleSetOfflineTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("offline")
}

func (*ruleSetOfflineTransport) CloseIdleConnections() {}

type ruleSetOfflineHTTPManager struct {
	adapter.HTTPClientManager
}

func (*ruleSetOfflineHTTPManager) DefaultTransport() adapter.HTTPTransport {
	return &ruleSetOfflineTransport{}
}

func TestRemoteRuleSetRestartAfterMetadataFailure(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "rules.json")
	cache := new(ruleSetTestCache)
	ctx := service.ContextWith[adapter.CacheFile](t.Context(), cache)
	ctx = service.ContextWith[adapter.HTTPClientManager](ctx, &ruleSetOfflineHTTPManager{})
	ruleSet, err := NewRemoteRuleSet(ctx, logger.NOP(), "test", ruleSetTestOptions(path))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
	ruleSet.IncRef()
	ruleSet.cacheFile = cache
	content := `{"version":4,"rules":[{"domain":["old.example"]}]}`
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Empty(t, request.Header.Get("If-None-Match"))
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": []string{`"v1"`}}, Body: io.NopCloser(strings.NewReader(content))}, nil
	})
	ruleSet.httpClient = &http.Client{Transport: transport}
	require.NoError(t, ruleSet.Update(ctx))
	oldCache := append([]byte(nil), cache.data...)
	content = `{"version":4,"rules":[{"domain":["new.example"]}]}`
	cache.err = errors.New("database write failed")
	ruleSet.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": []string{`"v2"`}}, Body: io.NopCloser(strings.NewReader(content))}, nil
	})}
	require.ErrorIs(t, ruleSet.Update(ctx), cache.err)
	require.Equal(t, oldCache, cache.data)
	require.NoError(t, ruleSet.Close())
	cache.err = nil
	restored, err := NewRemoteRuleSet(ctx, logger.NOP(), "test", ruleSetTestOptions(path))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restored.Close()) })
	restored.IncRef()
	start := adapter.NewHTTPStartContext()
	defer start.Close()
	require.NoError(t, restored.StartContext(ctx, start))
	require.True(t, restored.Match(&adapter.InboundContext{Domain: "old.example"}))
	require.False(t, restored.Match(&adapter.InboundContext{Domain: "new.example"}))
	require.Equal(t, oldCache, cache.data)

	restored.httpClient = &http.Client{Transport: transport}
	require.NoError(t, restored.Update(ctx))
	require.True(t, restored.Match(&adapter.InboundContext{Domain: "new.example"}))
	require.False(t, restored.cacheDirty)
	persisted, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte(content), persisted)
	require.Equal(t, persisted, cache.LoadRuleSet("test").Content)
}

func TestRemoteRuleSetCloseCancelsManualUpdate(t *testing.T) {
	t.Parallel()
	ruleSet, err := NewRemoteRuleSet(t.Context(), logger.NOP(), "test", ruleSetTestOptions(""))
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	ruleSet.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(started)
		select {
		case <-request.Context().Done():
			return nil, request.Context().Err()
		case <-release:
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"version":4,"rules":[{"domain":["example.com"]}]}`))}, nil
		}
	})}
	go func() { result <- ruleSet.Update(t.Context()) }()
	<-started
	err = ruleSet.Close()
	close(release)
	require.NoError(t, err)
	require.ErrorIs(t, <-result, context.Canceled)
	require.Zero(t, ruleSet.RuleCount())
	require.ErrorIs(t, ruleSet.Update(t.Context()), context.Canceled)
}

func TestRemoteRuleSetNotModifiedMetadataFailure(t *testing.T) {
	t.Parallel()
	ruleSet, err := NewRemoteRuleSet(t.Context(), logger.NOP(), "test", ruleSetTestOptions(""))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
	cache := new(ruleSetTestCache)
	ruleSet.cacheFile = cache
	var requestETags []string
	ruleSet.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestETags = append(requestETags, request.Header.Get("If-None-Match"))
		status, content := http.StatusOK, `{"version":4,"rules":[{"domain":["example.com"]}]}`
		if request.Header.Get("If-None-Match") == `"v1"` {
			status, content = http.StatusNotModified, ""
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Etag": []string{`"v1"`}}, Body: io.NopCloser(strings.NewReader(content))}, nil
	})}
	require.NoError(t, ruleSet.Update(t.Context()))
	updatedAt := ruleSet.UpdatedTime()
	cache.err = errors.New("injected metadata failure")
	require.ErrorIs(t, ruleSet.Update(t.Context()), cache.err)
	require.Equal(t, updatedAt, ruleSet.UpdatedTime())
	cache.err = nil
	require.NoError(t, ruleSet.Update(t.Context()))
	require.Equal(t, []string{"", `"v1"`, ""}, requestETags)
	content := append([]byte(nil), cache.LoadRuleSet("test").Content...)
	require.NoError(t, ruleSet.Update(t.Context()))
	require.Equal(t, `"v1"`, requestETags[3])
	require.Equal(t, content, cache.LoadRuleSet("test").Content)
}

func TestRemoteRuleSetCancelledResponseIsNotPublished(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		lifecycle bool
	}{
		{"request", false},
		{"lifecycle", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ruleSet, err := NewRemoteRuleSet(t.Context(), logger.NOP(), "test", ruleSetTestOptions(""))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ruleSet.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if testCase.lifecycle {
					ruleSet.cancel()
				} else {
					cancel()
				}
				// A custom transport may complete successfully while cancellation races with its response.
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"version":4,"rules":[{"domain":["example.com"]}]}`))}, nil
			})}
			require.ErrorIs(t, ruleSet.Update(ctx), context.Canceled)
			require.Zero(t, ruleSet.RuleCount())
			require.True(t, ruleSet.UpdatedTime().IsZero())
		})
	}
}
