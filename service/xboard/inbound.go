package xboard

import (
	"context"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

type inboundController interface {
	Get(tag string) (adapter.Inbound, bool)
	Remove(tag string) error
	Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, inboundType string, options any) error
}

var _ inboundController = (*managedInbounds)(nil)

type managedInbound struct {
	inbound adapter.Inbound
	scope   *adapter.Scope
}

type managedInbounds struct {
	registry  adapter.InboundRegistry
	lifecycle sync.Mutex
	access    sync.Mutex
	scope     *adapter.Scope
	stage     adapter.StartStage
	inbounds  map[string]*managedInbound
}

func newManagedInbounds(registry adapter.InboundRegistry) *managedInbounds {
	return &managedInbounds{
		registry: registry,
		inbounds: make(map[string]*managedInbound),
	}
}

func inboundName(inbound adapter.Inbound) string {
	return "inbound/" + inbound.Type() + "[" + inbound.Tag() + "]"
}

func (m *managedInbounds) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.access.Lock()
	if m.scope == nil {
		scope.Add(m.close)
	}
	m.scope = scope
	m.stage = stage
	entries := make([]*managedInbound, 0, len(m.inbounds))
	for _, entry := range m.inbounds {
		entries = append(entries, entry)
	}
	m.access.Unlock()
	for _, entry := range entries {
		err := entry.scope.Start(inboundName(entry.inbound), entry.inbound, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *managedInbounds) close() error {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.access.Lock()
	entries := m.inbounds
	m.inbounds = make(map[string]*managedInbound)
	m.access.Unlock()
	var err error
	for _, entry := range entries {
		err = E.Errors(err, entry.scope.Close())
	}
	return err
}

func (m *managedInbounds) Get(tag string) (adapter.Inbound, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	entry, loaded := m.inbounds[tag]
	if !loaded {
		return nil, false
	}
	return entry.inbound, true
}

func (m *managedInbounds) Remove(tag string) error {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.access.Lock()
	entry, loaded := m.inbounds[tag]
	delete(m.inbounds, tag)
	m.access.Unlock()
	if !loaded {
		return os.ErrInvalid
	}
	return entry.scope.Close()
}

func (m *managedInbounds) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, inboundType string, options any) error {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.access.Lock()
	scope := m.scope
	stage := m.stage
	_, loaded := m.inbounds[tag]
	m.access.Unlock()
	if scope == nil {
		return E.New("xboard service not started")
	}
	if loaded {
		return E.New("duplicate inbound tag: ", tag)
	}
	inbound, err := m.registry.Create(ctx, router, logger, tag, inboundType, options)
	if err != nil {
		return err
	}
	inboundScope := adapter.NewScope(scope.Context(), logger)
	for _, startStage := range adapter.ListStartStages {
		if startStage > stage {
			break
		}
		err = inboundScope.Start(inboundName(inbound), inbound, startStage)
		if err != nil {
			return E.Errors(err, inboundScope.Close())
		}
	}
	m.access.Lock()
	m.inbounds[tag] = &managedInbound{inbound: inbound, scope: inboundScope}
	m.access.Unlock()
	return nil
}
