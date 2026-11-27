package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

type Adapter struct {
	ctx              context.Context
	cancel           context.CancelFunc
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

	membersAccess    sync.RWMutex
	members          []*member
	memberByTag      map[string]*member
	memberByName     map[string]*member
	memberByOutbound map[adapter.Outbound]*member
	stagingByName    map[string]*member

	healthCheckAccess sync.Mutex
	pause             pause.Manager
	pauseCallback     *list.Element[pause.Callback]
	ticker            *time.Ticker
	done              chan struct{}
	checking          atomic.Bool

	link     string
	enabled  bool
	timeout  time.Duration
	interval time.Duration
}

type member struct {
	name           string
	outbound       adapter.Outbound
	endpoint       bool
	memberType     string
	options        any
	domainResolver string
	scope          *adapter.Scope
	started        bool
	startedTill    adapter.StartStage
}

func (it *member) close() error {
	if it.scope == nil {
		return nil
	}
	return it.scope.Close()
}

type memberSpec struct {
	name       string
	tag        string
	endpoint   bool
	memberType string
	options    any
}

type memberScope struct {
	adapter *Adapter
}

func (s memberScope) Outbound(name string) (adapter.Outbound, bool) {
	s.adapter.membersAccess.RLock()
	defer s.adapter.membersAccess.RUnlock()
	if s.adapter.stagingByName != nil {
		if it, loaded := s.adapter.stagingByName[name]; loaded {
			return it.outbound, true
		}
	}
	it, loaded := s.adapter.memberByName[name]
	if !loaded {
		return nil, false
	}
	return it.outbound, true
}

func NewAdapter(ctx context.Context, router adapter.Router, logFactory log.Factory, logger log.ContextLogger, providerTag string, providerType string, options option.ProviderHealthCheckOptions) Adapter {
	ctx, cancel := context.WithCancel(ctx)
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
		cancel:           cancel,
		router:           router,
		outboundRegistry: service.FromContext[adapter.OutboundRegistry](ctx),
		endpointRegistry: service.FromContext[adapter.EndpointRegistry](ctx),
		logFactory:       logFactory,
		logger:           logger,
		providerType:     providerType,
		providerTag:      providerTag,
		history:          service.PtrFromContext[urltest.HistoryStorage](ctx),
		providerManager:  service.FromContext[adapter.ProviderManager](ctx),
		pause:            service.FromContext[pause.Manager](ctx),
		memberByTag:      make(map[string]*member),
		memberByName:     make(map[string]*member),
		memberByOutbound: make(map[adapter.Outbound]*member),

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

func (a *Adapter) OutboundScope() adapter.OutboundScope {
	return memberScope{a}
}

func (a *Adapter) OutboundDomainResolver(outbound adapter.Outbound) string {
	a.membersAccess.RLock()
	defer a.membersAccess.RUnlock()
	if it, loaded := a.memberByOutbound[outbound]; loaded {
		return it.domainResolver
	}
	return ""
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
	if a.cancel != nil {
		a.cancel()
	}
	a.healthCheckAccess.Lock()
	if a.ticker != nil {
		a.ticker.Stop()
		if a.pause != nil && a.pauseCallback != nil {
			a.pause.UnregisterCallback(a.pauseCallback)
			a.pauseCallback = nil
		}
		close(a.done)
	}
	a.healthCheckAccess.Unlock()
	a.membersAccess.Lock()
	members := a.members
	a.members = nil
	a.memberByTag = make(map[string]*member)
	a.memberByName = make(map[string]*member)
	a.memberByOutbound = make(map[adapter.Outbound]*member)
	a.stagingByName = nil
	a.membersAccess.Unlock()
	return a.closeMembers(members)
}
