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
				ruleSet.String()
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
			ruleSet.String()
			ruleSet.ExtractIPSet()
		}
	})
	group.Wait()
}
