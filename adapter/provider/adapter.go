package provider

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tagname"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/service"
)

type Adapter struct {
	ctx              context.Context
	router           adapter.Router
	outboundRegistry adapter.OutboundRegistry
	endpointRegistry adapter.EndpointRegistry
	logFactory       log.Factory
	logger           log.ContextLogger
	providerType     string
	providerTag      string
	history          *urltest.HistoryStorage
	providerManager  adapter.ProviderManager

	updateAccess sync.Mutex
	closed       bool
	started      bool
	stage        adapter.StartStage

	membersAccess sync.RWMutex
	members       []*member
	memberByTag   map[string]*member

	healthCheckAccess sync.Mutex
	ticker            *time.Ticker
	done              chan struct{}
	checking          atomic.Bool

	link     string
	enabled  bool
	timeout  time.Duration
	interval time.Duration
}

type member struct {
	outbound    adapter.Outbound
	endpoint    bool
	memberType  string
	options     any
	started     bool
	startedTill adapter.StartStage
}

type memberSpec struct {
	tag        string
	endpoint   bool
	memberType string
	options    any
}

func NewAdapter(ctx context.Context, router adapter.Router, logFactory log.Factory, logger log.ContextLogger, providerTag string, providerType string, options option.ProviderHealthCheckOptions) Adapter {
	timeout := time.Duration(options.Timeout)
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	interval := time.Duration(options.Interval)
	if interval == 0 {
		interval = 10 * time.Minute
	}
	if interval < time.Minute {
		interval = time.Minute
	}
	return Adapter{
		ctx:              ctx,
		router:           router,
		outboundRegistry: service.FromContext[adapter.OutboundRegistry](ctx),
		endpointRegistry: service.FromContext[adapter.EndpointRegistry](ctx),
		logFactory:       logFactory,
		logger:           logger,
		providerType:     providerType,
		providerTag:      providerTag,
		history:          service.PtrFromContext[urltest.HistoryStorage](ctx),
		providerManager:  service.FromContext[adapter.ProviderManager](ctx),
		memberByTag:      make(map[string]*member),

		enabled:  options.Enabled,
		link:     options.URL,
		timeout:  timeout,
		interval: interval,
	}
}

func (a *Adapter) Type() string {
	return a.providerType
}

func (a *Adapter) Tag() string {
	return a.providerTag
}

func (a *Adapter) Outbounds() []adapter.Outbound {
	a.membersAccess.RLock()
	defer a.membersAccess.RUnlock()
	outbounds := make([]adapter.Outbound, 0, len(a.members))
	for _, it := range a.members {
		outbounds = append(outbounds, it.outbound)
	}
	return outbounds
}

func (a *Adapter) Outbound(tag string) (adapter.Outbound, bool) {
	a.membersAccess.RLock()
	defer a.membersAccess.RUnlock()
	it, loaded := a.memberByTag[tag]
	if !loaded {
		return nil, false
	}
	return it.outbound, true
}

func (a *Adapter) StartStage(stage adapter.StartStage) error {
	a.updateAccess.Lock()
	defer a.updateAccess.Unlock()
	if a.closed {
		return nil
	}
	a.started = true
	a.stage = stage
	a.membersAccess.RLock()
	members := append([]*member(nil), a.members...)
	a.membersAccess.RUnlock()
	var failed []*member
	for _, it := range members {
		through, _ := a.memberTarget(it.endpoint)
		err := a.startMember(it, through)
		if err != nil {
			a.logger.Error(E.Cause(err, "start ", it.outbound.Tag()), ", remove this node")
			failed = append(failed, it)
		}
	}
	if len(failed) > 0 {
		a.membersAccess.Lock()
		a.members = common.Filter(a.members, func(it *member) bool {
			return !common.Contains(failed, it)
		})
		for _, it := range failed {
			delete(a.memberByTag, it.outbound.Tag())
		}
		a.membersAccess.Unlock()
		a.UpdateGroups()
		a.closeMembers(failed)
	}
	if stage == adapter.StartStatePostStart && a.enabled {
		a.startHealthCheck()
	}
	return nil
}

func (a *Adapter) Update(outbounds []option.Outbound, endpoints []option.Endpoint) {
	a.updateAccess.Lock()
	defer a.updateAccess.Unlock()
	if a.closed {
		return
	}
	specs := a.resolveMembers(outbounds, endpoints)
	a.membersAccess.RLock()
	current := a.memberByTag
	a.membersAccess.RUnlock()
	var (
		members     = make([]*member, 0, len(specs))
		memberByTag = make(map[string]*member, len(specs))
		reused      = make(map[*member]bool)
	)
	for _, spec := range specs {
		previous := current[spec.tag]
		if previous != nil && previous.endpoint == spec.endpoint && previous.memberType == spec.memberType && reflect.DeepEqual(previous.options, spec.options) {
			reused[previous] = true
			members = append(members, previous)
			memberByTag[spec.tag] = previous
			continue
		}
		created, err := a.createMember(spec)
		if err != nil {
			a.logger.Warn(E.Cause(err, "create ", spec.tag), ", skip this node")
			continue
		}
		if a.history != nil {
			a.history.AddProviderOutbound(created.outbound)
		}
		members = append(members, created)
		memberByTag[spec.tag] = created
	}
	var retired []*member
	for _, previous := range current {
		if !reused[previous] {
			retired = append(retired, previous)
		}
	}
	a.membersAccess.Lock()
	a.members = members
	a.memberByTag = memberByTag
	a.membersAccess.Unlock()
	a.UpdateGroups()
	a.closeMembers(retired)
	if a.enabled && a.started && a.stage >= adapter.StartStatePostStart {
		go a.HealthCheck(a.ctx)
	}
}

func (a *Adapter) resolveMembers(outbounds []option.Outbound, endpoints []option.Endpoint) []memberSpec {
	specs := make([]memberSpec, 0, len(outbounds)+len(endpoints))
	for i, it := range outbounds {
		tag := it.Tag
		if tag == "" {
			tag = F.ToString(a.providerTag, "/", i)
		}
		specs = append(specs, memberSpec{tag: tag, memberType: it.Type, options: it.Options})
	}
	for i, it := range endpoints {
		tag := it.Tag
		if tag == "" {
			tag = F.ToString(a.providerTag, "/endpoint-", i)
		}
		specs = append(specs, memberSpec{tag: tag, endpoint: true, memberType: it.Type, options: it.Options})
	}
	tags := tagname.Deduplicate(common.Map(specs, func(it memberSpec) string { return it.tag }))
	for i := range specs {
		if tags[i] != specs[i].tag {
			a.logger.Warn("duplicate node name ", specs[i].tag, " in provider, renamed to ", tags[i])
			specs[i].tag = tags[i]
		}
	}
	return specs
}

func (a *Adapter) createMember(spec memberSpec) (*member, error) {
	ctx := adapter.ContextWithOutboundScope(a.ctx, a)
	ctx = adapter.WithContext(ctx, &adapter.InboundContext{
		Outbound: spec.tag,
	})
	var (
		outbound adapter.Outbound
		err      error
	)
	if spec.endpoint {
		if a.endpointRegistry == nil {
			return nil, E.New("missing endpoint registry")
		}
		logger := a.logFactory.NewLogger(F.ToString("endpoint/", spec.memberType, "[", spec.tag, "]"))
		outbound, err = a.endpointRegistry.Create(ctx, a.router, logger, spec.tag, spec.memberType, spec.options)
	} else {
		if a.outboundRegistry == nil {
			return nil, E.New("missing outbound registry")
		}
		logger := a.logFactory.NewLogger(F.ToString("outbound/", spec.memberType, "[", spec.tag, "]"))
		outbound, err = a.outboundRegistry.CreateOutbound(ctx, a.router, logger, spec.tag, spec.memberType, spec.options)
	}
	if err != nil {
		return nil, err
	}
	created := &member{
		outbound:   outbound,
		endpoint:   spec.endpoint,
		memberType: spec.memberType,
		options:    spec.options,
	}
	if through, ok := a.memberTarget(spec.endpoint); ok {
		err = a.startMember(created, through)
		if err != nil {
			common.Close(outbound)
			return nil, err
		}
	}
	return created, nil
}

func (a *Adapter) memberTarget(endpoint bool) (adapter.StartStage, bool) {
	if !a.started {
		return 0, false
	}
	if endpoint && a.stage == adapter.StartStateStart {
		return adapter.StartStatePostStart, true
	}
	return a.stage, true
}

func (a *Adapter) startMember(it *member, through adapter.StartStage) error {
	kind := "outbound/"
	if it.endpoint {
		kind = "endpoint/"
	}
	name := F.ToString(kind, it.outbound.Type(), "[", it.outbound.Tag(), "]")
	for _, stage := range adapter.ListStartStages {
		if stage > through {
			break
		}
		if it.started && stage <= it.startedTill {
			continue
		}
		done := adapter.LogElapsed(a.logger, stage, " ", name)
		err := adapter.LegacyStart(it.outbound, stage)
		done()
		if err != nil {
			return E.Cause(err, stage, " ", name)
		}
		it.started = true
		it.startedTill = stage
	}
	return nil
}

func (a *Adapter) closeMembers(members []*member) error {
	var err error
	for _, it := range members {
		if a.history != nil {
			a.history.RemoveProviderOutbound(it.outbound)
		}
		closeErr := common.Close(it.outbound)
		if closeErr != nil {
			a.logger.Error(E.Cause(closeErr, "close ", it.outbound.Tag()))
			err = E.Append(err, closeErr, func(err error) error {
				return E.Cause(err, "close ", it.outbound.Tag())
			})
		}
	}
	return err
}

func (a *Adapter) UpdateGroups() {
	if a.providerManager != nil {
		a.providerManager.UpdateGroups(a.providerTag)
	}
}

func (a *Adapter) Close() error {
	a.updateAccess.Lock()
	defer a.updateAccess.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	a.healthCheckAccess.Lock()
	if a.ticker != nil {
		a.ticker.Stop()
		close(a.done)
	}
	a.healthCheckAccess.Unlock()
	a.membersAccess.Lock()
	members := a.members
	a.members = nil
	a.memberByTag = make(map[string]*member)
	a.membersAccess.Unlock()
	return a.closeMembers(members)
}

func (a *Adapter) startHealthCheck() {
	a.healthCheckAccess.Lock()
	defer a.healthCheckAccess.Unlock()
	if a.ticker != nil {
		return
	}
	a.ticker = time.NewTicker(a.interval)
	a.done = make(chan struct{})
	go a.loopCheck(a.ticker, a.done)
}

func (a *Adapter) loopCheck(ticker *time.Ticker, done chan struct{}) {
	a.healthcheck(a.ctx)
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			a.healthcheck(a.ctx)
		}
	}
}

func (a *Adapter) HealthCheck(ctx context.Context) (map[string]uint16, error) {
	a.healthCheckAccess.Lock()
	if a.ticker != nil {
		select {
		case <-a.done:
		default:
			a.ticker.Reset(a.interval)
		}
	}
	a.healthCheckAccess.Unlock()
	return a.healthcheck(ctx)
}

func (a *Adapter) healthcheck(ctx context.Context) (map[string]uint16, error) {
	result := make(map[string]uint16)
	if a.history == nil {
		return result, E.New("missing URL test history storage")
	}
	if a.checking.Swap(true) {
		return result, nil
	}
	defer a.checking.Store(false)
	outbounds := a.Outbounds()
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	var resultAccess sync.Mutex
	for _, detour := range outbounds {
		tag := detour.Tag()
		b.Go(tag, func() (any, error) {
			testCtx, cancel := context.WithTimeout(ctx, a.timeout)
			defer cancel()
			t, err := urltest.URLTest(testCtx, a.link, detour)
			if err != nil {
				a.logger.Debug("outbound ", tag, " unavailable: ", err)
				a.history.StoreURLTestHistoryForOutbound(detour, nil)
				return nil, nil
			}
			a.logger.Debug("outbound ", tag, " available: ", t, "ms")
			stored := a.history.StoreURLTestHistoryForOutbound(detour, &adapter.URLTestHistory{
				Time:  time.Now(),
				Delay: t,
			})
			if stored {
				resultAccess.Lock()
				result[tag] = t
				resultAccess.Unlock()
			}
			return nil, nil
		})
	}
	b.Wait()
	return result, nil
}
