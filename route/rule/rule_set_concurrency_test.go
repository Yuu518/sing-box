package rule

import (
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
)

func TestRuleSetConcurrentReloadAndMatch(t *testing.T) {
	t.Parallel()
	ruleSet := &LocalRuleSet{abstractRuleSet: abstractRuleSet{ctx: t.Context(), logger: logger.NOP()}}
	options := []option.HeadlessRule{{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{Domain: []string{"example.com"}}}}
	require.NoError(t, ruleSet.reloadRules(options, ruleSet))
	ruleSet.IncRef()
	item := &RuleSetItem{setList: []adapter.RuleSet{ruleSet}}
	var group sync.WaitGroup
	group.Go(func() {
		for range 500 {
			if err := ruleSet.reloadRules(options, ruleSet); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for range 4 {
		group.Go(func() {
			for range 500 {
				metadata := &adapter.InboundContext{Domain: "example.com"}
				if !item.Match(metadata) || !item.matchWithOuterGroups(metadata, ruleGroupMatch{}) {
					t.Error("concurrent update lost a matching rule")
					return
				}
				_ = ruleSet.String()
				ruleSet.ExtractIPSet()
			}
		})
	}
	group.Wait()
}

func TestRuleSetConcurrentCleanupAndReaders(t *testing.T) {
	t.Parallel()
	ruleSet := &LocalRuleSet{abstractRuleSet: abstractRuleSet{ctx: t.Context(), logger: logger.NOP()}}
	options := []option.HeadlessRule{{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{Domain: []string{"example.com"}}}}
	var group sync.WaitGroup
	group.Go(func() {
		for range 500 {
			if err := ruleSet.reloadRules(options, ruleSet); err != nil {
				t.Error(err)
				return
			}
			ruleSet.Cleanup()
		}
	})
	group.Go(func() {
		for range 500 {
			ruleSet.Match(&adapter.InboundContext{Domain: "example.com"})
			ruleSet.mergeableRule()
			_ = ruleSet.String()
			ruleSet.ExtractIPSet()
		}
	})
	group.Wait()
}

func TestRuleSetZeroValueAndZeroAllocHotPath(t *testing.T) {
	var empty LocalRuleSet
	require.False(t, empty.Match(&adapter.InboundContext{Domain: "example.com"}))
	require.Nil(t, empty.mergeableRule())
	require.Zero(t, empty.RuleCount())
	require.Equal(t, adapter.RuleSetMetadata{}, empty.Metadata())
	require.Empty(t, empty.ExtractIPSet())
	require.Empty(t, empty.String())
	empty.Cleanup()
	empty.closeRules()

	ruleSet := &LocalRuleSet{abstractRuleSet: abstractRuleSet{ctx: t.Context(), logger: logger.NOP()}}
	options := []option.HeadlessRule{{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{Network: []string{"tcp"}}}}
	require.NoError(t, ruleSet.reloadRules(options, ruleSet))
	first := ruleSet.loadContent()
	second := ruleSet.loadContent()
	require.Same(t, first, second)

	metadata := &adapter.InboundContext{Network: "tcp"}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !ruleSet.Match(metadata) {
			t.Fatal("expected match")
		}
		_ = ruleSet.loadContent()
		_ = ruleSet.mergeableRule()
		_ = ruleSet.Metadata()
		_ = ruleSet.RuleCount()
	}))
}
