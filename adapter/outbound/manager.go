package outbound

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/taskmonitor"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
)

var _ adapter.OutboundManager = (*Manager)(nil)

type Manager struct {
	logger                  log.ContextLogger
	registry                adapter.OutboundRegistry
	endpoint                adapter.EndpointManager
	defaultTag              string
	access                  sync.RWMutex
	started                 bool
	stage                   adapter.StartStage
	outbounds               []adapter.Outbound
	outboundByTag           map[string]adapter.Outbound
	dependByTag             map[string][]string
	defaultOutbound         adapter.Outbound
	unavailableDefault      adapter.Outbound
	defaultOutboundFallback func() (adapter.Outbound, error)
	providerFallback        adapter.Outbound
	providerFallbackFactory func(tag string) (adapter.Outbound, error)
	startingProviders       bool
	startupOutbounds        map[adapter.Outbound]bool
}

func NewManager(logger logger.ContextLogger, registry adapter.OutboundRegistry, endpoint adapter.EndpointManager, defaultTag string) *Manager {
	return &Manager{
		logger:             logger,
		registry:           registry,
		endpoint:           endpoint,
		defaultTag:         defaultTag,
		outboundByTag:      make(map[string]adapter.Outbound),
		dependByTag:        make(map[string][]string),
		unavailableDefault: &unavailableOutbound{Adapter: NewAdapter("unavailable", defaultTag, []string{"tcp", "udp", "icmp"}, nil)},
	}
}

func (m *Manager) Initialize(defaultOutboundFallback func() (adapter.Outbound, error)) {
	m.defaultOutboundFallback = defaultOutboundFallback
}

func (m *Manager) InitializeProviderFallback(factory func(tag string) (adapter.Outbound, error)) {
	m.providerFallbackFactory = factory
}

func (m *Manager) ProviderFallback() (adapter.Outbound, error) {
	m.access.Lock()
	defer m.access.Unlock()
	if m.providerFallback != nil && m.outboundByTag[m.providerFallback.Tag()] == m.providerFallback {
		return m.providerFallback, nil
	}
	if m.providerFallbackFactory == nil {
		return nil, E.New("provider fallback is not initialized")
	}
	const baseTag = "__provider_fallback__"
	tag := baseTag
	for suffix := 2; ; suffix++ {
		_, endpointExists := m.endpoint.Get(tag)
		if m.outboundByTag[tag] == nil && !endpointExists {
			break
		}
		tag = baseTag + "-" + strconv.Itoa(suffix)
	}
	fallback, err := m.providerFallbackFactory(tag)
	if err != nil {
		return nil, err
	}
	m.providerFallback = fallback
	m.outbounds = append(m.outbounds, fallback)
	m.outboundByTag[tag] = fallback
	return fallback, nil
}

// StartAvailable starts the outbounds usable for downloading providers. Missing
// dependencies are checked after the providers have registered their members.
func (m *Manager) StartAvailable() error {
	m.access.Lock()
	if m.startingProviders {
		outbounds := append([]adapter.Outbound(nil), m.outbounds...)
		m.access.Unlock()
		return m.startOutbounds(append(outbounds, common.Map(m.endpoint.Endpoints(), func(it adapter.Endpoint) adapter.Outbound { return it })...))
	}
	m.startingProviders = true
	m.startupOutbounds = make(map[adapter.Outbound]bool)
	m.access.Unlock()
	return m.Start(adapter.StartStateStart)
}

func (m *Manager) StartRemaining() error {
	m.access.Lock()
	m.startingProviders = false
	err := m.initializeDefault()
	outbounds := append([]adapter.Outbound(nil), m.outbounds...)
	m.access.Unlock()
	if err != nil {
		return err
	}
	err = m.startOutbounds(append(outbounds, common.Map(m.endpoint.Endpoints(), func(it adapter.Endpoint) adapter.Outbound { return it })...))
	m.access.Lock()
	m.startupOutbounds = nil
	m.access.Unlock()
	return err
}

// Caller holds access.
func (m *Manager) initializeDefault() error {
	if m.defaultTag != "" && m.defaultOutbound == nil {
		defaultEndpoint, loaded := m.endpoint.Get(m.defaultTag)
		if !loaded {
			if m.stage == adapter.StartStateInitialize || m.startingProviders {
				return nil
			}
			return E.New("default outbound not found: ", m.defaultTag)
		}
		m.defaultOutbound = defaultEndpoint
	}
	if m.defaultOutbound == nil {
		directOutbound, err := m.defaultOutboundFallback()
		if err != nil {
			return E.Cause(err, "create direct outbound for fallback")
		}
		m.outbounds = append(m.outbounds, directOutbound)
		m.outboundByTag[directOutbound.Tag()] = directOutbound
		m.defaultOutbound = directOutbound
	}
	return nil
}

func (m *Manager) Start(stage adapter.StartStage) error {
	m.access.Lock()
	if m.started && m.stage >= stage {
		panic("already started")
	}
	m.started = true
	m.stage = stage
	if stage == adapter.StartStateInitialize || stage == adapter.StartStateStart {
		if err := m.initializeDefault(); err != nil {
			m.access.Unlock()
			return err
		}
	}
	outbounds := append([]adapter.Outbound(nil), m.outbounds...)
	m.access.Unlock()
	if stage == adapter.StartStateStart {
		return m.startOutbounds(append(outbounds, common.Map(m.endpoint.Endpoints(), func(it adapter.Endpoint) adapter.Outbound { return it })...))
	}
	for _, outbound := range outbounds {
		name := "outbound/" + outbound.Type() + "[" + outbound.Tag() + "]"
		done := adapter.LogElapsed(m.logger, stage, " ", name)
		err := adapter.LegacyStart(outbound, stage)
		done()
		if err != nil {
			return E.Cause(err, stage, " ", name)
		}
	}
	return nil
}

func (m *Manager) startOutbounds(outbounds []adapter.Outbound) error {
	monitor := taskmonitor.New(m.logger, C.StartTimeout)
	started := make(map[string]bool)
	for _, outbound := range outbounds {
		if m.startupOutbounds[outbound] {
			started[outbound.Tag()] = true
		}
	}
	for {
		canContinue := false
	startOne:
		for _, outboundToStart := range outbounds {
			outboundTag := outboundToStart.Tag()
			if started[outboundTag] {
				continue
			}
			dependencies := outboundToStart.Dependencies()
			for _, dependency := range dependencies {
				if !started[dependency] {
					continue startOne
				}
			}
			started[outboundTag] = true
			canContinue = true
			name := "outbound/" + outboundToStart.Type() + "[" + outboundTag + "]"
			if endpoint, found := m.endpoint.Get(outboundTag); found && endpoint == outboundToStart {
				monitor.Start("start ", name)
				err := m.endpoint.StartEndpoint(endpoint)
				monitor.Finish()
				if err != nil {
					return err
				}
			} else if starter, isStarter := outboundToStart.(adapter.Lifecycle); isStarter {
				done := adapter.LogElapsed(m.logger, "start ", name)
				monitor.Start("start ", name)
				err := starter.Start(adapter.StartStateStart)
				monitor.Finish()
				done()
				if err != nil {
					return E.Cause(err, "start ", name)
				}
			} else if starter, isStarter := outboundToStart.(interface {
				Start() error
			}); isStarter {
				done := adapter.LogElapsed(m.logger, "start ", name)
				monitor.Start("start ", name)
				err := starter.Start()
				monitor.Finish()
				done()
				if err != nil {
					return E.Cause(err, "start ", name)
				}
			}
			if m.startupOutbounds != nil {
				m.startupOutbounds[outboundToStart] = true
			}
		}
		if len(started) == len(outbounds) {
			break
		}
		if canContinue {
			continue
		}
		if m.startingProviders {
			return nil
		}
		currentOutbound := common.Find(outbounds, func(it adapter.Outbound) bool {
			return !started[it.Tag()]
		})
		var lintOutbound func(oTree []string, oCurrent adapter.Outbound) error
		lintOutbound = func(oTree []string, oCurrent adapter.Outbound) error {
			problemOutboundTag := common.Find(oCurrent.Dependencies(), func(it string) bool {
				return !started[it]
			})
			if common.Contains(oTree, problemOutboundTag) {
				return E.New("circular outbound dependency: ", strings.Join(oTree, " -> "), " -> ", problemOutboundTag)
			}
			problemOutbound := common.Find(outbounds, func(it adapter.Outbound) bool {
				return it.Tag() == problemOutboundTag
			})
			if problemOutbound == nil {
				return E.New("dependency[", problemOutboundTag, "] not found for outbound[", oCurrent.Tag(), "]")
			}
			return lintOutbound(append(oTree, problemOutboundTag), problemOutbound)
		}
		return lintOutbound([]string{currentOutbound.Tag()}, currentOutbound)
	}
	return nil
}

func (m *Manager) Close() error {
	monitor := taskmonitor.New(m.logger, C.StopTimeout)
	m.access.Lock()
	if !m.started {
		m.access.Unlock()
		return nil
	}
	m.started = false
	outbounds := m.outbounds
	m.outbounds = nil
	clear(m.outboundByTag)
	clear(m.dependByTag)
	m.defaultOutbound = nil
	m.providerFallback = nil
	m.startupOutbounds = nil
	m.access.Unlock()
	var err error
	for _, outbound := range outbounds {
		if closer, isCloser := outbound.(io.Closer); isCloser {
			name := "outbound/" + outbound.Type() + "[" + outbound.Tag() + "]"
			done := adapter.LogElapsed(m.logger, "close ", name)
			monitor.Start("close ", name)
			err = E.Append(err, closer.Close(), func(err error) error {
				return E.Cause(err, "close ", name)
			})
			monitor.Finish()
			done()
		}
	}
	return nil
}

func (m *Manager) Outbounds() []adapter.Outbound {
	m.access.RLock()
	defer m.access.RUnlock()
	return m.outbounds
}

func (m *Manager) Outbound(tag string) (adapter.Outbound, bool) {
	m.access.RLock()
	outbound, found := m.outboundByTag[tag]
	m.access.RUnlock()
	if found {
		return outbound, true
	}
	return m.endpoint.Get(tag)
}

func (m *Manager) Default() adapter.Outbound {
	if m.defaultTag != "" {
		if outbound, found := m.Outbound(m.defaultTag); found {
			return outbound
		}
	}
	m.access.RLock()
	defer m.access.RUnlock()
	if m.defaultOutbound != nil {
		if current, found := m.outboundByTag[m.defaultOutbound.Tag()]; found && current == m.defaultOutbound {
			return current
		}
		if current, found := m.endpoint.Get(m.defaultOutbound.Tag()); found && current == m.defaultOutbound {
			return current
		}
	}
	if len(m.outbounds) > 0 {
		return m.outbounds[0]
	}
	return m.unavailableDefault
}

func (m *Manager) Remove(tag string) error {
	return m.remove(tag, nil)
}

func (m *Manager) RemoveIfSame(member adapter.Outbound) error {
	return m.remove(member.Tag(), member)
}

func (m *Manager) remove(tag string, expected adapter.Outbound) error {
	m.access.Lock()
	defer m.access.Unlock()
	outbound, found := m.outboundByTag[tag]
	if expected != nil && outbound != expected {
		return nil
	}
	if !found {
		return os.ErrInvalid
	}
	dependBy := m.dependByTag[tag]
	if expected == nil && len(dependBy) > 0 {
		return E.New("outbound[", tag, "] is depended by ", strings.Join(dependBy, ", "))
	}
	delete(m.outboundByTag, tag)
	index := common.Index(m.outbounds, func(it adapter.Outbound) bool {
		return it == outbound
	})
	if index == -1 {
		panic("invalid inbound index")
	}
	m.outbounds = append(m.outbounds[:index], m.outbounds[index+1:]...)
	started := m.started
	if m.defaultOutbound == outbound {
		if len(m.outbounds) > 0 {
			m.defaultOutbound = m.outbounds[0]
			m.logger.Info("updated default outbound to ", m.defaultOutbound.Tag())
		} else {
			m.defaultOutbound = nil
		}
	}
	m.removeDependencies(outbound)
	if started {
		return common.Close(outbound)
	}
	return nil
}

func (m *Manager) removeDependencies(outbound adapter.Outbound) {
	for _, dependency := range outbound.Dependencies() {
		dependents := common.Filter(m.dependByTag[dependency], func(it string) bool {
			return it != outbound.Tag()
		})
		if len(dependents) == 0 {
			delete(m.dependByTag, dependency)
		} else {
			m.dependByTag[dependency] = dependents
		}
	}
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, inboundType string, options any) error {
	if tag == "" {
		return os.ErrInvalid
	}
	outbound, err := m.registry.CreateOutbound(ctx, router, logger, tag, inboundType, options)
	if err != nil {
		return err
	}
	m.access.RLock()
	started, currentStage := m.started, m.stage
	if m.startingProviders {
		currentStage = adapter.StartStateInitialize
	}
	m.access.RUnlock()
	if started {
		name := "outbound/" + outbound.Type() + "[" + outbound.Tag() + "]"
		for _, stage := range adapter.ListStartStages {
			if stage > currentStage {
				break
			}
			done := adapter.LogElapsed(m.logger, stage, " ", name)
			err = adapter.LegacyStart(outbound, stage)
			done()
			if err != nil {
				common.Close(outbound)
				return E.Cause(err, stage, " ", name)
			}
		}
	}
	m.access.Lock()
	defer m.access.Unlock()
	if expected, conditional := adapter.ProviderUpdateFromContext(ctx); conditional && m.outboundByTag[tag] != expected {
		common.Close(outbound)
		return E.New("outbound tag is owned by another configuration: ", tag)
	}
	if existsOutbound, loaded := m.outboundByTag[tag]; loaded {
		if m.started {
			err = common.Close(existsOutbound)
			if err != nil {
				return E.Cause(err, "close outbound/", existsOutbound.Type(), "[", existsOutbound.Tag(), "]")
			}
		}
		if m.defaultOutbound == existsOutbound {
			m.defaultOutbound = outbound
		}
		m.removeDependencies(existsOutbound)
		existsIndex := common.Index(m.outbounds, func(it adapter.Outbound) bool {
			return it == existsOutbound
		})
		if existsIndex == -1 {
			panic("invalid inbound index")
		}
		m.outbounds = append(m.outbounds[:existsIndex], m.outbounds[existsIndex+1:]...)
	}
	m.outbounds = append(m.outbounds, outbound)
	m.outboundByTag[tag] = outbound
	dependencies := outbound.Dependencies()
	for _, dependency := range dependencies {
		m.dependByTag[dependency] = append(m.dependByTag[dependency], tag)
	}
	if tag == m.defaultTag || (m.defaultTag == "" && m.defaultOutbound == nil) {
		m.defaultOutbound = outbound
		if m.started {
			m.logger.Info("updated default outbound to ", outbound.Tag())
		}
	}
	return nil
}
