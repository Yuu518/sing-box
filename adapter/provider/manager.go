package provider

import (
	"context"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tagname"
	"github.com/sagernet/sing-box/common/taskmonitor"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
)

var _ adapter.ProviderManager = (*Manager)(nil)

type Manager struct {
	ctx           context.Context
	logger        log.ContextLogger
	registry      adapter.ProviderRegistry
	access        sync.Mutex
	started       bool
	stage         adapter.StartStage
	providers     []adapter.Provider
	providerByTag map[string]adapter.Provider

	callbackAccess sync.Mutex
	callbacks      list.List[adapter.ProviderUpdateCallback]

	tagAccess      sync.Mutex
	tagsByProvider map[string]map[string]string
}

func NewManager(ctx context.Context, logger logger.ContextLogger, registry adapter.ProviderRegistry) *Manager {
	return &Manager{
		ctx:            ctx,
		logger:         logger,
		registry:       registry,
		providerByTag:  make(map[string]adapter.Provider),
		tagsByProvider: make(map[string]map[string]string),
	}
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateInitialize {
		scope.Add(m.Close)
	}
	m.access.Lock()
	if m.started && m.stage >= stage {
		panic("already started")
	}
	m.started = true
	m.stage = stage
	providers := append([]adapter.Provider(nil), m.providers...)
	m.access.Unlock()
	var startContext *adapter.HTTPStartContext
	if stage == adapter.StartStateStart {
		startContext = adapter.NewHTTPStartContext()
		defer startContext.Close()
	}
	for _, provider := range providers {
		err := m.startProviderStage(provider, stage, startContext)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) startProviderStage(provider adapter.Provider, stage adapter.StartStage, startContext *adapter.HTTPStartContext) error {
	if stageStarter, ok := provider.(interface {
		StartStage(stage adapter.StartStage) error
	}); ok {
		err := stageStarter.StartStage(stage)
		if err != nil {
			return E.Cause(err, stage, " provider/", provider.Type(), "[", provider.Tag(), "]")
		}
	}
	switch stage {
	case adapter.StartStateStart:
		if contextStarter, ok := provider.(interface {
			StartContext(ctx context.Context, startContext *adapter.HTTPStartContext) error
		}); ok {
			err := contextStarter.StartContext(m.ctx, startContext)
			if err != nil {
				return E.Cause(err, stage, " provider/", provider.Type(), "[", provider.Tag(), "]")
			}
		}
	case adapter.StartStatePostStart:
		if postStarter, ok := provider.(interface{ PostStart() error }); ok {
			err := postStarter.PostStart()
			if err != nil {
				return E.Cause(err, stage, " provider/", provider.Type(), "[", provider.Tag(), "]")
			}
		}
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
	providers := m.providers
	m.providers = nil
	m.access.Unlock()
	m.tagAccess.Lock()
	clear(m.tagsByProvider)
	m.tagAccess.Unlock()
	var err error
	for _, provider := range providers {
		if closer, isCloser := provider.(io.Closer); isCloser {
			monitor.Start("close provider/", provider.Type(), "[", provider.Tag(), "]")
			err = E.Append(err, closer.Close(), func(err error) error {
				return E.Cause(err, "close provider/", provider.Type(), "[", provider.Tag(), "]")
			})
			monitor.Finish()
		}
	}
	return err
}

func (m *Manager) Providers() []adapter.Provider {
	m.access.Lock()
	defer m.access.Unlock()
	return slices.Clone(m.providers)
}

func (m *Manager) Get(tag string) (adapter.Provider, bool) {
	m.access.Lock()
	provider, found := m.providerByTag[tag]
	m.access.Unlock()
	return provider, found
}

func (m *Manager) Remove(tag string) error {
	m.access.Lock()
	provider, found := m.providerByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.providerByTag, tag)
	m.providers = slices.DeleteFunc(slices.Clone(m.providers), func(it adapter.Provider) bool {
		return it == provider
	})
	started := m.started
	m.access.Unlock()
	defer m.releaseTags(tag)
	if started {
		m.UpdateGroups(tag)
		return common.Close(provider)
	}
	return nil
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, providerType string, options any) error {
	if tag == "" {
		return os.ErrInvalid
	}
	m.access.Lock()
	_, replacing := m.providerByTag[tag]
	m.access.Unlock()
	provider, err := m.registry.CreateProvider(ctx, router, logFactory, tag, providerType, options)
	if err != nil {
		if !replacing {
			m.releaseTags(tag)
		}
		return err
	}
	m.access.Lock()
	started, currentStage := m.started, m.stage
	m.access.Unlock()
	if started {
		err = m.startProvider(provider, currentStage)
		if err != nil {
			common.Close(provider)
			if !replacing {
				m.releaseTags(tag)
			}
			return err
		}
	}
	m.access.Lock()
	existsProvider, loaded := m.providerByTag[tag]
	providers := slices.Clone(m.providers)
	if loaded {
		providers = slices.DeleteFunc(providers, func(it adapter.Provider) bool {
			return it == existsProvider
		})
	}
	m.providers = append(providers, provider)
	m.providerByTag[tag] = provider
	started = m.started
	m.access.Unlock()
	if !started {
		return nil
	}
	m.UpdateGroups(tag)
	if loaded {
		err = common.Close(existsProvider)
		if err != nil {
			return E.Cause(err, "close provider/", existsProvider.Type(), "[", existsProvider.Tag(), "]")
		}
	}
	return nil
}

func (m *Manager) startProvider(provider adapter.Provider, through adapter.StartStage) error {
	var startContext *adapter.HTTPStartContext
	defer func() {
		if startContext != nil {
			startContext.Close()
		}
	}()
	for _, stage := range adapter.ListStartStages {
		if stage > through {
			break
		}
		if stage == adapter.StartStateStart {
			startContext = adapter.NewHTTPStartContext()
		}
		err := m.startProviderStage(provider, stage, startContext)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) RegisterCallback(callback adapter.ProviderUpdateCallback) *list.Element[adapter.ProviderUpdateCallback] {
	m.callbackAccess.Lock()
	defer m.callbackAccess.Unlock()
	return m.callbacks.PushBack(callback)
}

func (m *Manager) UnregisterCallback(element *list.Element[adapter.ProviderUpdateCallback]) {
	m.callbackAccess.Lock()
	defer m.callbackAccess.Unlock()
	m.callbacks.Remove(element)
}

func (m *Manager) UpdateGroups(tag string) {
	m.callbackAccess.Lock()
	callbacks := make([]adapter.ProviderUpdateCallback, 0, m.callbacks.Len())
	for element := m.callbacks.Front(); element != nil; element = element.Next() {
		callbacks = append(callbacks, element.Value)
	}
	m.callbackAccess.Unlock()
	for _, callback := range callbacks {
		err := callback(tag)
		if err != nil {
			m.logger.Error(E.Cause(err, "update group for provider[", tag, "]"))
		}
	}
}

func (m *Manager) AllocateTags(providerTag string, names []string) []string {
	taken := make(map[string]bool)
	if outboundManager := service.FromContext[adapter.OutboundManager](m.ctx); outboundManager != nil {
		for _, outbound := range outboundManager.Outbounds() {
			taken[outbound.Tag()] = true
		}
	}
	if endpointManager := service.FromContext[adapter.EndpointManager](m.ctx); endpointManager != nil {
		for _, endpoint := range endpointManager.Endpoints() {
			taken[endpoint.Tag()] = true
		}
	}
	m.tagAccess.Lock()
	defer m.tagAccess.Unlock()
	for owner, tagByName := range m.tagsByProvider {
		if owner == providerTag {
			continue
		}
		for _, tag := range tagByName {
			taken[tag] = true
		}
	}
	tags := tagname.Allocate(names, m.tagsByProvider[providerTag], taken)
	tagByName := make(map[string]string, len(names))
	for i, name := range names {
		tagByName[name] = tags[i]
	}
	m.tagsByProvider[providerTag] = tagByName
	return tags
}

func (m *Manager) releaseTags(providerTag string) {
	m.tagAccess.Lock()
	delete(m.tagsByProvider, providerTag)
	m.tagAccess.Unlock()
}
