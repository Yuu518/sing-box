package rule

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
)

func TestLocalRuleSetManualUpdate(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "rules.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":4,"rules":[{"domain":["old.example"]}]}`), 0o600))
	ruleSet, err := NewLocalRuleSet(t.Context(), logger.NOP(), "test", option.RuleSet{
		Type: C.RuleSetTypeLocal, Format: C.RuleSetFormatSource, Path: path,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
	var callbacks int
	ruleSet.RegisterCallback(func(adapter.RuleSet) { callbacks++ })
	require.NoError(t, os.WriteFile(path, []byte(`{"version":4,"rules":[{"domain":["new.example","other.example"]}]}`), 0o600))
	require.NoError(t, ruleSet.Update(t.Context()))
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "new.example"}))
	require.False(t, ruleSet.Match(&adapter.InboundContext{Domain: "old.example"}))
	require.EqualValues(t, 2, ruleSet.RuleCount())
	require.Equal(t, 1, callbacks)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, info.ModTime(), ruleSet.UpdatedTime())

	updatedAt := ruleSet.UpdatedTime()
	require.NoError(t, os.WriteFile(path, []byte(`invalid`), 0o600))
	require.Error(t, ruleSet.Update(t.Context()))
	require.Equal(t, updatedAt, ruleSet.UpdatedTime())
	require.Equal(t, 1, callbacks)
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "new.example"}))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, ruleSet.Update(ctx), context.Canceled)
}

func TestInlineRuleSetUpdate(t *testing.T) {
	t.Parallel()
	ruleSet, err := NewLocalRuleSet(t.Context(), logger.NOP(), "test", option.RuleSet{
		Type: C.RuleSetTypeInline,
		InlineOptions: option.PlainRuleSet{Rules: []option.HeadlessRule{{
			Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{Domain: []string{"example.com"}},
		}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
	require.NoError(t, ruleSet.Update(t.Context()))
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "example.com"}))
}

func TestLocalRuleSetSerializesManualAndFileReload(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "rules.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":4,"rules":[{"domain":["example.com"]}]}`), 0o600))
	ruleSet, err := NewLocalRuleSet(t.Context(), logger.NOP(), "test", option.RuleSet{
		Type: C.RuleSetTypeLocal, Format: C.RuleSetFormatSource, Path: path,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	ruleSet.RegisterCallback(func(adapter.RuleSet) {
		entered <- struct{}{}
		<-release
	})
	manualDone := make(chan error, 1)
	go func() { manualDone <- ruleSet.Update(t.Context()) }()
	<-entered
	fileDone := make(chan error, 1)
	go func() { fileDone <- ruleSet.reloadFile(t.Context(), path) }()
	select {
	case <-entered:
		t.Error("file reload overlapped the manual update callback")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-manualDone)
	require.NoError(t, <-fileDone)
}
