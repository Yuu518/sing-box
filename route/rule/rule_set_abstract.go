package rule

import (
	"bytes"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/srs"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/x/list"

	"go4.org/netipx"
)

var emptyRuleSetContent = &ruleSetContent{}

type abstractRuleSet struct {
	ctx         context.Context
	logger      logger.ContextLogger
	tag         string
	access      sync.Mutex
	sType       string
	path        string
	format      string
	content     atomic.Pointer[ruleSetContent]
	lastUpdated time.Time
	loadedAt    time.Time
	callbacks   list.List[adapter.RuleSetUpdateCallback]
	refs        atomic.Int32
	cleaned     bool
	closed      bool
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
	return s.loadContent().ruleCount
}

func (s *abstractRuleSet) UpdatedTime() time.Time {
	s.access.Lock()
	defer s.access.Unlock()
	if s.lastUpdated.IsZero() {
		return s.loadedAt
	}
	return s.lastUpdated
}

func (s *abstractRuleSet) lastUpdatedTime() time.Time {
	s.access.Lock()
	defer s.access.Unlock()
	return s.lastUpdated
}

func (s *abstractRuleSet) setUpdatedTime(updatedAt time.Time) {
	s.access.Lock()
	defer s.access.Unlock()
	s.lastUpdated = updatedAt
}

func (s *abstractRuleSet) String() string {
	return s.loadContent().String()
}

func (s *abstractRuleSet) Metadata() adapter.RuleSetMetadata {
	return s.loadContent().metadata
}

func (s *abstractRuleSet) ExtractIPSet() []*netipx.IPSet {
	return common.FlatMap(s.loadContent().rules, extractIPSetFromRule)
}

func (s *abstractRuleSet) IncRef() {
	s.refs.Add(1)
}

func (s *abstractRuleSet) DecRef() {
	if s.refs.Add(-1) < 0 {
		panic("rule-set: negative refs")
	}
}

func (s *abstractRuleSet) clearRulesLocked() {
	current := s.loadContent()
	if current.rules == nil {
		return
	}
	s.content.Store(&ruleSetContent{
		ruleCount: current.ruleCount,
		metadata:  current.metadata,
	})
}

func (s *abstractRuleSet) Cleanup() {
	s.access.Lock()
	defer s.access.Unlock()
	s.cleaned = true
	if s.refs.Load() == 0 {
		s.clearRulesLocked()
	}
}

func (s *abstractRuleSet) releaseUnreferencedRules() {
	s.access.Lock()
	defer s.access.Unlock()
	if s.cleaned && s.refs.Load() == 0 {
		s.clearRulesLocked()
	}
}

func (s *abstractRuleSet) closeRules() {
	s.access.Lock()
	defer s.access.Unlock()
	s.closed = true
	s.clearRulesLocked()
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

func (s *abstractRuleSet) parseBytes(content []byte) (*ruleSetContent, error) {
	var (
		ruleSet option.PlainRuleSetCompat
		err     error
	)
	switch s.format {
	case C.RuleSetFormatSource, "":
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
	ruleSet, err = mmapRuleSet(s.ctx, s.logger, s.tag, ruleSet)
	if err != nil {
		return nil, err
	}
	plainRuleSet, err := ruleSet.Upgrade()
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

func (s *abstractRuleSet) compileRules(headlessRules []option.HeadlessRule) (*ruleSetContent, error) {
	rules := make([]adapter.HeadlessRule, len(headlessRules))
	var ruleCount uint64
	for i, ruleOptions := range headlessRules {
		rule, err := NewHeadlessRule(s.ctx, ruleOptions)
		if err != nil {
			return nil, E.Cause(err, "parse rule_set.rules.[", i, "]")
		}
		rules[i] = rule
		ruleCount += headlessRuleCount(rule)
	}
	metadata := buildRuleSetMetadata(headlessRules)
	err := validateRuleSetMetadataUpdate(s.ctx, s.tag, metadata)
	if err != nil {
		return nil, err
	}
	return &ruleSetContent{rules: rules, ruleCount: ruleCount, metadata: metadata}, nil
}

func (s *abstractRuleSet) applyRules(rules *ruleSetContent, ruleSet adapter.RuleSet, updatedAt time.Time) error {
	s.access.Lock()
	if s.closed {
		s.access.Unlock()
		return os.ErrClosed
	}
	s.content.Store(rules)
	s.loadedAt = time.Now()
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
	return s.loadContent().Match(metadata)
}

func (s *abstractRuleSet) loadContent() *ruleSetContent {
	if content := s.content.Load(); content != nil {
		return content
	}
	return emptyRuleSetContent
}

func (s *abstractRuleSet) mergeableRule() *DefaultHeadlessRule {
	return s.loadContent().mergeableRule()
}
