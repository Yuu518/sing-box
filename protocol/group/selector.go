package group

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"

	"github.com/dlclark/regexp2/v2"
)

func RegisterSelector(registry *outbound.Registry) {
	outbound.Register[option.SelectorOutboundOptions](registry, C.TypeSelector, NewSelector)
}

var (
	_ adapter.OutboundGroup = (*Selector)(nil)
	_ adapter.Referrer      = (*Selector)(nil)
)

type Selector struct {
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	logger                       logger.ContextLogger
	tags                         []string
	defaultTag                   string
	outbounds                    map[string]adapter.Outbound
	selected                     common.TypedValue[adapter.Outbound]
	selectedTag                  string
	history                      *urltest.HistoryStorage
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	stateAccess                  sync.RWMutex
	providerAccess               sync.Mutex
	selectionReady               bool

	provider       adapter.ProviderManager
	providers      map[string]adapter.Provider
	outboundsCache map[string][]adapter.Outbound

	providerTags    []string
	exclude         *regexp2.Regexp
	include         *regexp2.Regexp
	useAllProviders bool

	providerCallback *list.Element[adapter.ProviderUpdateCallback]
}

func NewSelector(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SelectorOutboundOptions) (adapter.Outbound, error) {
	outbound := &Selector{
		Adapter:                      outbound.NewAdapter(C.TypeSelector, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		defaultTag:                   options.Default,
		outbounds:                    make(map[string]adapter.Outbound),
		history:                      service.PtrFromContext[urltest.HistoryStorage](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: options.InterruptExistConnections,

		provider:       service.FromContext[adapter.ProviderManager](ctx),
		providers:      make(map[string]adapter.Provider),
		outboundsCache: make(map[string][]adapter.Outbound),

		providerTags:    options.Providers,
		exclude:         options.Exclude.Build(),
		include:         options.Include.Build(),
		useAllProviders: options.UseAllProviders,
	}
	for i, tag := range outbound.tags {
		if slices.Contains(outbound.tags[:i], tag) {
			return nil, E.New("duplicate tag: ", tag)
		}
	}
	return outbound, nil
}

func (s *Selector) Network() []string {
	selected := s.selected.Load()
	if selected == nil {
		return []string{N.NetworkTCP, N.NetworkUDP}
	}
	return selected.Network()
}

func (s *Selector) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		if scope != nil {
			scope.Add(s.close)
		}
		return s.start()
	case adapter.StartStatePostStart:
		return s.postStart()
	}
	return nil
}

func (s *Selector) All() []string {
	s.stateAccess.RLock()
	defer s.stateAccess.RUnlock()
	return slices.Clone(s.tags)
}

func (s *Selector) Member(tag string) (adapter.Outbound, bool) {
	s.stateAccess.RLock()
	defer s.stateAccess.RUnlock()
	detour, loaded := s.outbounds[tag]
	return detour, loaded
}

func (s *Selector) Selected(network string) adapter.Outbound {
	return s.selected.Load()
}

func (s *Selector) SelectedTag(network string) string {
	s.stateAccess.RLock()
	defer s.stateAccess.RUnlock()
	return s.selectedTag
}

func (s *Selector) AttachConnection(closer io.Closer) func() {
	return s.interruptGroup.Add(closer, true)
}

func (s *Selector) References() ([]string, []string) {
	s.stateAccess.RLock()
	defer s.stateAccess.RUnlock()
	if s.selectedTag != "" {
		return []string{s.selectedTag}, nil
	}
	if len(s.tags) > 0 {
		return []string{s.tags[0]}, nil
	}
	return nil, nil
}

func (s *Selector) SelectOutbound(tag string) bool {
	s.providerAccess.Lock()
	s.stateAccess.Lock()
	detour, loaded := s.outbounds[tag]
	if !loaded {
		s.stateAccess.Unlock()
		s.providerAccess.Unlock()
		return false
	}
	s.selectionReady = true
	previousTag := s.selectedTag
	s.selectedTag = tag
	previous := s.selected.Swap(detour)
	s.stateAccess.Unlock()
	if previous == detour && previousTag == tag {
		s.providerAccess.Unlock()
		return true
	}
	if s.Tag() != "" {
		cacheFile := service.FromContext[adapter.CacheFile](s.ctx)
		if cacheFile != nil {
			err := cacheFile.StoreSelected(s.Tag(), tag)
			if err != nil {
				s.logger.Error("store selected: ", err)
			}
		}
	}
	s.providerAccess.Unlock()
	if previous != detour {
		s.interruptGroup.Interrupt(s.interruptExternalConnections)
	}
	if s.history != nil {
		s.history.NotifyUpdated()
	}
	return true
}

func (s *Selector) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	conn, err := s.selected.Load().DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsProviderConnectionFromContext(ctx)), nil
}

func (s *Selector) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	conn, err := s.selected.Load().ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	return s.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsProviderConnectionFromContext(ctx)), nil
}

func RealTag(detour adapter.Outbound, network string) string {
	for {
		group, isGroup := detour.(adapter.OutboundGroup)
		if !isGroup {
			return detour.Tag()
		}
		detour = group.Selected(network)
		if detour == nil {
			return ""
		}
	}
}

func RealOutbound(detour adapter.Outbound, network string) adapter.Outbound {
	for detour != nil {
		group, isGroup := detour.(adapter.OutboundGroup)
		if !isGroup {
			return detour
		}
		detour = group.Selected(network)
	}
	return nil
}
