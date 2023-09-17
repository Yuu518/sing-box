package endpoint

import (
	"context"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/taskmonitor"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.EndpointManager = (*Manager)(nil)

type Manager struct {
	logger        log.ContextLogger
	registry      adapter.EndpointRegistry
	access        sync.Mutex
	started       bool
	stage         adapter.StartStage
	endpoints     []adapter.Endpoint
	endpointByTag map[string]adapter.Endpoint
	endpointState map[adapter.Endpoint]*endpointState
}

type endpointState struct {
	started  bool
	stage    adapter.StartStage
	provider bool
}

func NewManager(logger log.ContextLogger, registry adapter.EndpointRegistry) *Manager {
	return &Manager{
		logger:        logger,
		registry:      registry,
		endpointByTag: make(map[string]adapter.Endpoint),
		endpointState: make(map[adapter.Endpoint]*endpointState),
	}
}

func (m *Manager) Start(stage adapter.StartStage) error {
	m.access.Lock()
	defer m.access.Unlock()
	if m.started && m.stage >= stage {
		panic("already started")
	}
	m.started = true
	m.stage = stage
	if stage == adapter.StartStateStart {
		// started with outbound manager
		return nil
	}
	for _, endpoint := range m.endpoints {
		if err := m.startEndpoint(endpoint, stage); err != nil {
			return err
		}
	}
	return nil
}

// StartEndpoint is called in dependency order by the outbound manager.
// Provider endpoints must be usable by later subscription downloads, including
// endpoints such as WireGuard that cannot dial until PostStart.
func (m *Manager) StartEndpoint(endpoint adapter.Endpoint) error {
	m.access.Lock()
	defer m.access.Unlock()
	stage := adapter.StartStateStart
	if m.endpointState[endpoint].provider {
		stage = adapter.StartStatePostStart
	}
	return m.startEndpoint(endpoint, stage)
}

// Caller holds access. Each instance receives each stage at most once.
func (m *Manager) startEndpoint(endpoint adapter.Endpoint, through adapter.StartStage) error {
	state := m.endpointState[endpoint]
	for _, stage := range adapter.ListStartStages {
		if stage > through {
			break
		}
		if state.started && stage <= state.stage {
			continue
		}
		name := "endpoint/" + endpoint.Type() + "[" + endpoint.Tag() + "]"
		done := adapter.LogElapsed(m.logger, stage, " ", name)
		err := adapter.LegacyStart(endpoint, stage)
		done()
		if err != nil {
			return E.Cause(err, stage, " ", name)
		}
		state.started = true
		state.stage = stage
	}
	return nil
}

func (m *Manager) Close() error {
	m.access.Lock()
	defer m.access.Unlock()
	if !m.started {
		return nil
	}
	m.started = false
	endpoints := m.endpoints
	m.endpoints = nil
	clear(m.endpointByTag)
	clear(m.endpointState)
	monitor := taskmonitor.New(m.logger, C.StopTimeout)
	var err error
	for _, endpoint := range endpoints {
		name := "endpoint/" + endpoint.Type() + "[" + endpoint.Tag() + "]"
		done := adapter.LogElapsed(m.logger, "close ", name)
		monitor.Start("close ", name)
		err = E.Append(err, endpoint.Close(), func(err error) error {
			return E.Cause(err, "close ", name)
		})
		monitor.Finish()
		done()
	}
	return nil
}

func (m *Manager) Endpoints() []adapter.Endpoint {
	m.access.Lock()
	defer m.access.Unlock()
	return m.endpoints
}

func (m *Manager) Get(tag string) (adapter.Endpoint, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	endpoint, found := m.endpointByTag[tag]
	return endpoint, found
}

func (m *Manager) Remove(tag string) error {
	return m.remove(tag, nil)
}

func (m *Manager) RemoveIfSame(member adapter.Outbound) error {
	return m.remove(member.Tag(), member)
}

func (m *Manager) remove(tag string, expected adapter.Outbound) error {
	m.access.Lock()
	endpoint, found := m.endpointByTag[tag]
	if expected != nil && endpoint != expected {
		m.access.Unlock()
		return nil
	}
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.endpointByTag, tag)
	delete(m.endpointState, endpoint)
	index := common.Index(m.endpoints, func(it adapter.Endpoint) bool {
		return it == endpoint
	})
	if index == -1 {
		panic("invalid endpoint index")
	}
	m.endpoints = append(m.endpoints[:index], m.endpoints[index+1:]...)
	started := m.started
	m.access.Unlock()
	if started {
		return endpoint.Close()
	}
	return nil
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	endpoint, err := m.registry.Create(ctx, router, logger, tag, outboundType, options)
	if err != nil {
		return err
	}
	m.access.Lock()
	defer m.access.Unlock()
	expected, conditional := adapter.ProviderUpdateFromContext(ctx)
	if conditional && m.endpointByTag[tag] != expected {
		endpoint.Close()
		return E.New("endpoint tag is owned by another configuration: ", tag)
	}
	m.endpointState[endpoint] = &endpointState{provider: conditional}
	if m.started {
		if err = m.startEndpoint(endpoint, m.stage); err != nil {
			delete(m.endpointState, endpoint)
			endpoint.Close()
			return err
		}
	}
	if existsEndpoint, loaded := m.endpointByTag[tag]; loaded {
		if m.started {
			err = existsEndpoint.Close()
			if err != nil {
				delete(m.endpointState, endpoint)
				endpoint.Close()
				return E.Cause(err, "close endpoint/", existsEndpoint.Type(), "[", existsEndpoint.Tag(), "]")
			}
		}
		delete(m.endpointState, existsEndpoint)
		existsIndex := common.Index(m.endpoints, func(it adapter.Endpoint) bool {
			return it == existsEndpoint
		})
		if existsIndex == -1 {
			panic("invalid endpoint index")
		}
		m.endpoints = append(m.endpoints[:existsIndex], m.endpoints[existsIndex+1:]...)
	}
	m.endpoints = append(m.endpoints, endpoint)
	m.endpointByTag[tag] = endpoint
	return nil
}
