package rule

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/srs"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/x/list"

	"go4.org/netipx"
)

type abstractRuleSet struct {
	ctx         context.Context
	logger      logger.ContextLogger
	tag         string
	access      sync.RWMutex
	sType       string
	path        string
	format      string
	rules       []adapter.HeadlessRule
	ruleCount   uint64
	metadata    adapter.RuleSetMetadata
	lastUpdated time.Time
	callbacks   list.List[adapter.RuleSetUpdateCallback]
	refs        atomic.Int32
	closed      bool
}

type compiledRuleSet struct {
	rules     []adapter.HeadlessRule
	ruleCount uint64
	metadata  adapter.RuleSetMetadata
}

func (s *abstractRuleSet) Name() string {
	return s.tag
}

func (s *abstractRuleSet) Type() string {
	return s.sType
}

func (s *abstractRuleSet) Format() string {
	return s.format
}

func (s *abstractRuleSet) RuleCount() uint64 {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.ruleCount
}

func (s *abstractRuleSet) UpdatedTime() time.Time {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.lastUpdated
}

func (s *abstractRuleSet) setUpdatedTime(updatedAt time.Time) {
	s.access.Lock()
	defer s.access.Unlock()
	s.lastUpdated = updatedAt
}

func (s *abstractRuleSet) String() string {
	return strings.Join(F.MapToString(s.rulesSnapshot()), " ")
}

func (s *abstractRuleSet) rulesSnapshot() []adapter.HeadlessRule {
	s.access.RLock()
	defer s.access.RUnlock()
	// Published rules and their backing slice are immutable.
	return s.rules
}

func (s *abstractRuleSet) Metadata() adapter.RuleSetMetadata {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.metadata
}

func (s *abstractRuleSet) ExtractIPSet() []*netipx.IPSet {
	return common.FlatMap(s.rulesSnapshot(), extractIPSetFromRule)
}

func (s *abstractRuleSet) IncRef() {
	s.refs.Add(1)
}

func (s *abstractRuleSet) DecRef() {
	if s.refs.Add(-1) < 0 {
		panic("rule-set: negative refs")
	}
}

func (s *abstractRuleSet) Cleanup() {
	s.access.Lock()
	defer s.access.Unlock()
	if s.refs.Load() == 0 {
		s.rules = nil
	}
}

func (s *abstractRuleSet) closeRules() {
	s.access.Lock()
	defer s.access.Unlock()
	s.closed = true
	s.rules = nil
}

func (s *abstractRuleSet) RegisterCallback(callback adapter.RuleSetUpdateCallback) *list.Element[adapter.RuleSetUpdateCallback] {
	s.access.Lock()
	defer s.access.Unlock()
	return s.callbacks.PushBack(callback)
}

func (s *abstractRuleSet) UnregisterCallback(element *list.Element[adapter.RuleSetUpdateCallback]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.callbacks.Remove(element)
}

func (s *abstractRuleSet) loadBytes(content []byte, ruleset adapter.RuleSet) error {
	rules, err := s.parseBytes(content)
	if err != nil {
		return err
	}
	return s.applyRules(rules, ruleset, time.Time{})
}

func (s *abstractRuleSet) parseBytes(content []byte) (*compiledRuleSet, error) {
	var (
		ruleSet option.PlainRuleSetCompat
		err     error
	)
	switch s.format {
	case C.RuleSetFormatSource:
		ruleSet, err = json.UnmarshalExtended[option.PlainRuleSetCompat](content)
		if err != nil {
			return nil, err
		}
	case C.RuleSetFormatBinary:
		ruleSet, err = srs.Read(bytes.NewReader(content), false)
		if err != nil {
			return nil, err
		}
	default:
		return nil, E.New("unknown rule-set format: ", s.format)
	}
	plainRuleSet, err := mmapRuleSet(s.ctx, s.logger, s.tag, ruleSet).Upgrade()
	if err != nil {
		return nil, err
	}
	return s.compileRules(plainRuleSet.Rules)
}

func (s *abstractRuleSet) reloadRules(headlessRules []option.HeadlessRule, ruleSet adapter.RuleSet) error {
	rules, err := s.compileRules(headlessRules)
	if err != nil {
		return err
	}
	return s.applyRules(rules, ruleSet, time.Time{})
}

func (s *abstractRuleSet) compileRules(headlessRules []option.HeadlessRule) (*compiledRuleSet, error) {
	rules := make([]adapter.HeadlessRule, len(headlessRules))
	var ruleCount uint64
	for i, ruleOptions := range headlessRules {
		rule, err := NewHeadlessRule(s.ctx, ruleOptions)
		if err != nil {
			return nil, E.Cause(err, "parse rule_set.rules.[", i, "]")
		}
		rules[i] = rule
		ruleCount += rule.RuleCount()
	}
	metadata := buildRuleSetMetadata(headlessRules)
	err := validateRuleSetMetadataUpdate(s.ctx, s.tag, metadata)
	if err != nil {
		return nil, err
	}
	return &compiledRuleSet{rules: rules, ruleCount: ruleCount, metadata: metadata}, nil
}

func (s *abstractRuleSet) applyRules(rules *compiledRuleSet, ruleSet adapter.RuleSet, updatedAt time.Time) error {
	s.access.Lock()
	if s.closed {
		s.access.Unlock()
		return os.ErrClosed
	}
	s.rules = rules.rules
	s.ruleCount = rules.ruleCount
	s.metadata = rules.metadata
	if !updatedAt.IsZero() {
		s.lastUpdated = updatedAt
	}
	callbacks := s.callbacks.Array()
	s.access.Unlock()
	for _, callback := range callbacks {
		callback(ruleSet)
	}
	return nil
}

func (s *abstractRuleSet) Match(metadata *adapter.InboundContext) bool {
	return matchAnyHeadlessRule(s.rulesSnapshot(), metadata)
}

func (s *abstractRuleSet) mergeableRule() *DefaultHeadlessRule {
	return mergeableRuleIn(s.rulesSnapshot())
}
