package provider

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
)

// Provider tags share the outbound and endpoint namespaces. Serialize provider
// reconciliation across both managers, including removal and shutdown.
var resourceAccess sync.Mutex

type Adapter struct {
	ctx             context.Context
	outbound        adapter.OutboundManager
	endpoint        adapter.EndpointManager
	router          adapter.Router
	logFactory      log.Factory
	logger          log.ContextLogger
	providerType    string
	providerTag     string
	outboundsAccess sync.RWMutex
	outbounds       []adapter.Outbound
	outboundsByTag  map[string]adapter.Outbound
	endpoints       []adapter.Outbound
	endpointsByTag  map[string]adapter.Outbound
	ticker          *time.Ticker
	checking        atomic.Bool
	history         *urltest.HistoryStorage
	callbackAccess  sync.Mutex
	callbacks       list.List[adapter.ProviderUpdateCallback]

	link     string
	enabled  bool
	timeout  time.Duration
	interval time.Duration
}

func NewAdapter(ctx context.Context, router adapter.Router, outbound adapter.OutboundManager, endpoint adapter.EndpointManager, logFactory log.Factory, logger log.ContextLogger, providerTag string, providerType string, options option.ProviderHealthCheckOptions) Adapter {
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
		ctx:          ctx,
		outbound:     outbound,
		endpoint:     endpoint,
		router:       router,
		logFactory:   logFactory,
		logger:       logger,
		providerType: providerType,
		providerTag:  providerTag,

		enabled:  options.Enabled,
		link:     options.URL,
		timeout:  timeout,
		interval: interval,
	}
}

func (a *Adapter) Start() error {
	a.history = service.PtrFromContext[urltest.HistoryStorage](a.ctx)
	if a.history == nil {
		return E.New("missing URL test history storage")
	}
	for _, outbound := range a.Outbounds() {
		a.history.SetCurrentOutbound(outbound)
	}
	if a.enabled {
		a.ticker = time.NewTicker(a.interval)
		go a.loopCheck()
	}
	return nil
}

func (a *Adapter) Type() string {
	return a.providerType
}

func (a *Adapter) Tag() string {
	return a.providerTag
}

func (a *Adapter) Outbounds() []adapter.Outbound {
	a.outboundsAccess.RLock()
	defer a.outboundsAccess.RUnlock()
	outbounds := make([]adapter.Outbound, 0, len(a.outbounds)+len(a.endpoints))
	outbounds = append(outbounds, a.outbounds...)
	outbounds = append(outbounds, a.endpoints...)
	return outbounds
}

func (a *Adapter) Outbound(tag string) (adapter.Outbound, bool) {
	a.outboundsAccess.RLock()
	defer a.outboundsAccess.RUnlock()
	if detour, ok := a.outboundsByTag[tag]; ok {
		return detour, true
	}
	detour, ok := a.endpointsByTag[tag]
	return detour, ok
}

func (a *Adapter) resolveOutboundTags(newOpts []option.Outbound) []string {
	tags := make([]string, len(newOpts))
	seen := make(map[string]bool)
	for i, opt := range newOpts {
		var baseTag string
		if opt.Tag != "" {
			baseTag = opt.Tag
		} else {
			baseTag = F.ToString(a.providerTag, "/", i)
		}
		tag := baseTag
		for n := 2; seen[tag]; n++ {
			tag = F.ToString(baseTag, " (", n, ")")
		}
		if tag != baseTag {
			a.logger.Warn("duplicate outbound tag ", baseTag, " in provider, renamed to ", tag)
		}
		seen[tag] = true
		tags[i] = tag
	}
	return tags
}

func (a *Adapter) UpdateOutbounds(oldOpts []option.Outbound, newOpts []option.Outbound) {
	resourceAccess.Lock()
	defer resourceAccess.Unlock()
	a.updateOutbounds(oldOpts, newOpts)
	a.checkUpdatedOutbounds()
}

// Update reconciles both namespaces before creating replacements, so changing
// a node between an outbound and an endpoint does not conflict with its old self.
func (a *Adapter) Update(oldOutbounds, newOutbounds []option.Outbound, oldEndpoints, newEndpoints []option.Endpoint) {
	resourceAccess.Lock()
	defer resourceAccess.Unlock()
	a.removeUseless(a.resolveOutboundTags(newOutbounds))
	a.removeUselessEndpoints(a.resolveEndpointTags(newEndpoints))
	a.updateOutbounds(oldOutbounds, newOutbounds)
	a.updateEndpoints(oldEndpoints, newEndpoints)
	a.checkUpdatedOutbounds()
}

func (a *Adapter) checkUpdatedOutbounds() {
	if a.enabled && a.history != nil {
		go a.HealthCheck(a.ctx)
	}
}

func (a *Adapter) updateOutbounds(oldOpts []option.Outbound, newOpts []option.Outbound) {
	newTags := a.resolveOutboundTags(newOpts)
	oldTags := a.resolveOutboundTags(oldOpts)
	var (
		oldOptByTag    = make(map[string]option.Outbound)
		outbounds      = make([]adapter.Outbound, 0, len(newOpts))
		outboundsByTag = make(map[string]adapter.Outbound)
	)
	for i, opt := range oldOpts {
		oldOptByTag[oldTags[i]] = opt
	}
	activeTags := a.activeOutboundTags()
	a.removeUseless(newTags)
	for i, opt := range newOpts {
		tag := newTags[i]
		outbound, exist := a.outbound.Outbound(tag)
		owned := activeTags[tag]
		active := owned != nil
		if current, found := a.outbound.Outbound(tag); found && current != owned {
			a.logger.Warn("node tag is owned by another configuration: ", tag)
			continue
		}
		if !exist || !active || !reflect.DeepEqual(opt, oldOptByTag[tag]) {
			err := a.outbound.Create(
				adapter.WithContext(adapter.ContextWithProviderUpdate(a.ctx, outbound), &adapter.InboundContext{
					Outbound: tag,
				}),
				a.router,
				a.logFactory.NewLogger(F.ToString("outbound/", opt.Type, "[", tag, "]")),
				tag,
				opt.Type,
				opt.Options,
			)
			if err != nil {
				a.logger.Warn(err, " in ", tag, ", skip create this outbound")
				if active {
					if closeErr := a.removeMember(a.outbound, owned); closeErr != nil {
						a.logger.Error(closeErr, "close outbound [", tag, "]")
					}
				}
				continue
			}
			outbound, _ = a.outbound.Outbound(tag)
		}
		if a.history != nil {
			a.history.SetCurrentOutbound(outbound)
		}
		outbounds = append(outbounds, outbound)
		outboundsByTag[tag] = outbound
	}
	a.outboundsAccess.Lock()
	a.outbounds = outbounds
	a.outboundsByTag = outboundsByTag
	a.outboundsAccess.Unlock()
}

func (a *Adapter) HealthCheck(ctx context.Context) (map[string]uint16, error) {
	if a.ticker != nil {
		a.ticker.Reset(a.interval)
	}
	return a.healthcheck(ctx)
}

func (a *Adapter) RegisterCallback(callback adapter.ProviderUpdateCallback) *list.Element[adapter.ProviderUpdateCallback] {
	a.callbackAccess.Lock()
	defer a.callbackAccess.Unlock()
	return a.callbacks.PushBack(callback)
}

func (a *Adapter) UnregisterCallback(element *list.Element[adapter.ProviderUpdateCallback]) {
	a.callbackAccess.Lock()
	defer a.callbackAccess.Unlock()
	a.callbacks.Remove(element)
}

func (a *Adapter) UpdateGroups() {
	a.callbackAccess.Lock()
	callbacks := make([]adapter.ProviderUpdateCallback, 0, a.callbacks.Len())
	for element := a.callbacks.Front(); element != nil; element = element.Next() {
		callbacks = append(callbacks, element.Value)
	}
	a.callbackAccess.Unlock()
	for _, callback := range callbacks {
		callback(a.providerTag)
	}
}

func (a *Adapter) Close() error {
	resourceAccess.Lock()
	defer resourceAccess.Unlock()
	if a.ticker != nil {
		a.ticker.Stop()
	}
	a.outboundsAccess.Lock()
	outbounds := a.outbounds
	endpoints := a.endpoints
	a.outbounds = nil
	a.outboundsByTag = nil
	a.endpoints = nil
	a.endpointsByTag = nil
	a.outboundsAccess.Unlock()
	var err error
	for _, ob := range outbounds {
		if err2 := a.removeMember(a.outbound, ob); err2 != nil {
			err = E.Append(err, err2, func(err error) error {
				return E.Cause(err, "close outbound [", ob.Tag(), "]")
			})
		}
	}
	for _, ep := range endpoints {
		if err2 := a.removeMember(a.endpoint, ep); err2 != nil {
			err = E.Append(err, err2, func(err error) error {
				return E.Cause(err, "close endpoint [", ep.Tag(), "]")
			})
		}
	}
	return err
}

func (a *Adapter) loopCheck() {
	a.healthcheck(a.ctx)
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-a.ticker.C:
			a.healthcheck(a.ctx)
		}
	}
}

func (a *Adapter) healthcheck(ctx context.Context) (map[string]uint16, error) {
	result := make(map[string]uint16)
	if a.checking.Swap(true) {
		return result, nil
	}
	defer a.checking.Store(false)
	outbounds := a.Outbounds()
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	var resultAccess sync.Mutex
	checked := make(map[string]bool)
	for _, detour := range outbounds {
		tag := detour.Tag()
		if checked[tag] {
			continue
		}
		checked[tag] = true
		b.Go(tag, func() (any, error) {
			ctx, cancel := context.WithTimeout(a.ctx, a.timeout)
			defer cancel()
			t, err := urltest.URLTest(ctx, a.link, detour)
			if err != nil {
				a.logger.Debug("outbound ", tag, " unavailable: ", err)
				a.history.StoreURLTestHistoryForOutbound(detour, nil)
			} else {
				a.logger.Debug("outbound ", tag, " available: ", t, "ms")
				stored := a.history.StoreURLTestHistoryForOutbound(detour, &adapter.URLTestHistory{
					Time:  time.Now(),
					Delay: t,
				})
				if !stored {
					return nil, nil
				}
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

func (a *Adapter) resolveEndpointTags(newOpts []option.Endpoint) []string {
	tags := make([]string, len(newOpts))
	seen := make(map[string]bool)
	for i, opt := range newOpts {
		var baseTag string
		if opt.Tag != "" {
			baseTag = opt.Tag
		} else {
			baseTag = F.ToString(a.providerTag, "/endpoint-", i)
		}
		tag := baseTag
		for n := 2; seen[tag]; n++ {
			tag = F.ToString(baseTag, " (", n, ")")
		}
		if tag != baseTag {
			a.logger.Warn("duplicate endpoint tag ", baseTag, " in provider, renamed to ", tag)
		}
		seen[tag] = true
		tags[i] = tag
	}
	return tags
}

func (a *Adapter) UpdateEndpoints(oldOpts []option.Endpoint, newOpts []option.Endpoint) {
	resourceAccess.Lock()
	defer resourceAccess.Unlock()
	a.updateEndpoints(oldOpts, newOpts)
	a.checkUpdatedOutbounds()
}

func (a *Adapter) updateEndpoints(oldOpts []option.Endpoint, newOpts []option.Endpoint) {
	newTags := a.resolveEndpointTags(newOpts)
	oldTags := a.resolveEndpointTags(oldOpts)
	var (
		oldOptByTag    = make(map[string]option.Endpoint)
		endpoints      []adapter.Outbound
		endpointsByTag = make(map[string]adapter.Outbound)
	)
	for i, opt := range oldOpts {
		oldOptByTag[oldTags[i]] = opt
	}
	activeTags := a.activeEndpointTags()
	a.removeUselessEndpoints(newTags)
	for i, opt := range newOpts {
		tag := newTags[i]
		ep, exist := a.endpoint.Get(tag)
		owned := activeTags[tag]
		active := owned != nil
		if current, found := a.outbound.Outbound(tag); found && current != owned {
			a.logger.Warn("node tag is owned by another configuration: ", tag)
			continue
		}
		if !exist || !active || !reflect.DeepEqual(opt, oldOptByTag[tag]) {
			err := a.endpoint.Create(
				adapter.WithContext(adapter.ContextWithProviderUpdate(a.ctx, ep), &adapter.InboundContext{
					Outbound: tag,
				}),
				a.router,
				a.logFactory.NewLogger(F.ToString("endpoint/", opt.Type, "[", tag, "]")),
				tag,
				opt.Type,
				opt.Options,
			)
			if err != nil {
				a.logger.Warn(err, " in ", tag, ", skip create this endpoint")
				if active {
					if closeErr := a.removeMember(a.endpoint, owned); closeErr != nil {
						a.logger.Error(closeErr, "close endpoint [", tag, "]")
					}
				}
				continue
			}
			ep, _ = a.endpoint.Get(tag)
		}
		if a.history != nil {
			a.history.SetCurrentOutbound(ep)
		}
		endpoints = append(endpoints, ep)
		endpointsByTag[tag] = ep
	}
	a.outboundsAccess.Lock()
	a.endpoints = endpoints
	a.endpointsByTag = endpointsByTag
	a.outboundsAccess.Unlock()
}

func (a *Adapter) removeUselessEndpoints(newTags []string) {
	exists := make(map[string]bool)
	for _, tag := range newTags {
		exists[tag] = true
	}
	a.outboundsAccess.RLock()
	snap := make([]adapter.Outbound, len(a.endpoints))
	copy(snap, a.endpoints)
	a.outboundsAccess.RUnlock()
	for _, ep := range snap {
		tag := ep.Tag()
		if !exists[tag] {
			if err := a.removeMember(a.endpoint, ep); err != nil {
				a.logger.Error(err, "close endpoint [", tag, "]")
			}
		}
	}
}

func (a *Adapter) activeOutboundTags() map[string]adapter.Outbound {
	a.outboundsAccess.RLock()
	defer a.outboundsAccess.RUnlock()
	tags := make(map[string]adapter.Outbound, len(a.outbounds))
	for _, outbound := range a.outbounds {
		tags[outbound.Tag()] = outbound
	}
	return tags
}

func (a *Adapter) activeEndpointTags() map[string]adapter.Outbound {
	a.outboundsAccess.RLock()
	defer a.outboundsAccess.RUnlock()
	tags := make(map[string]adapter.Outbound, len(a.endpoints))
	for _, endpoint := range a.endpoints {
		tags[endpoint.Tag()] = endpoint
	}
	return tags
}

func (a *Adapter) removeUseless(newTags []string) {
	a.outboundsAccess.RLock()
	if len(a.outbounds) == 0 {
		a.outboundsAccess.RUnlock()
		return
	}
	exists := make(map[string]bool)
	for _, tag := range newTags {
		exists[tag] = true
	}
	snap := make([]adapter.Outbound, len(a.outbounds))
	copy(snap, a.outbounds)
	a.outboundsAccess.RUnlock()
	for _, opt := range snap {
		if !exists[opt.Tag()] {
			if err := a.removeMember(a.outbound, opt); err != nil {
				a.logger.Error(err, "close outbound [", opt.Tag(), "]")
			}
		}
	}
}

func removeProviderMember(manager any, member adapter.Outbound) error {
	if remover, ok := manager.(adapter.ProviderMemberRemover); ok {
		return remover.RemoveIfSame(member)
	}
	return E.New("manager does not support provider member ownership")
}

func (a *Adapter) removeMember(manager any, member adapter.Outbound) error {
	err := removeProviderMember(manager, member)
	if a.history != nil {
		a.history.RemoveCurrentOutbound(member)
	}
	return err
}
