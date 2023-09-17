package group

import (
	"context"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"

	"github.com/dlclark/regexp2/v2"
)

func RegisterURLTest(registry *outbound.Registry) {
	outbound.Register[option.URLTestOutboundOptions](registry, C.TypeURLTest, NewURLTest)
}

var (
	_ adapter.OutboundGroup           = (*URLTest)(nil)
	_ adapter.InterfaceUpdateListener = (*URLTest)(nil)
	_ adapter.Referrer                = (*URLTest)(nil)
)

type URLTest struct {
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	logger                       log.ContextLogger
	tags                         []string
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	group                        *URLTestGroup
	checkAccess                  sync.Mutex
	interruptExternalConnections bool
	providerAccess               sync.Mutex
	providerUpdateCheck          providerUpdateCheckScheduler

	provider       adapter.ProviderManager
	providers      map[string]adapter.Provider
	outboundsCache map[string][]adapter.Outbound

	providerTags    []string
	exclude         *regexp2.Regexp
	include         *regexp2.Regexp
	useAllProviders bool

	providerCallback *list.Element[adapter.ProviderUpdateCallback]
	closed           bool
}

func NewURLTest(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.URLTestOutboundOptions) (adapter.Outbound, error) {
	outbound := &URLTest{
		Adapter:                      outbound.NewAdapter(C.TypeURLTest, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		link:                         options.URL,
		interval:                     time.Duration(options.Interval),
		tolerance:                    options.Tolerance,
		idleTimeout:                  time.Duration(options.IdleTimeout),
		interruptExternalConnections: options.InterruptExistConnections,

		provider:       service.FromContext[adapter.ProviderManager](ctx),
		providers:      make(map[string]adapter.Provider),
		outboundsCache: make(map[string][]adapter.Outbound),

		providerTags:    options.Providers,
		exclude:         options.Exclude.Build(),
		include:         options.Include.Build(),
		useAllProviders: options.UseAllProviders,
	}
	return outbound, nil
}

func (s *URLTest) Start() error {
	s.providerAccess.Lock()
	defer s.providerAccess.Unlock()
	if s.useAllProviders {
		var providerTags []string
		for _, provider := range s.provider.Providers() {
			providerTags = append(providerTags, provider.Tag())
			s.providers[provider.Tag()] = provider
		}
		s.providerTags = providerTags
	} else {
		for i, tag := range s.providerTags {
			provider, loaded := s.provider.Get(tag)
			if !loaded {
				return E.New("outbound provider ", i, " not found: ", tag)
			}
			s.providers[tag] = provider
		}
	}
	if len(s.tags)+len(s.providerTags) == 0 {
		return E.New("missing outbound and provider tags")
	}
	tags, outbounds, cache, err := collectProviderOutbounds("", s.Dependencies(), s.outbound, s.providers, s.providerTags, s.outboundsCache, s.exclude, s.include)
	if err != nil {
		return err
	}
	s.outboundsCache = cache
	group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, tags, outbounds, s.link, s.interval, s.tolerance, s.idleTimeout, s.interruptExternalConnections)
	if err != nil {
		return err
	}
	s.group = group
	if len(s.providerTags) > 0 || s.useAllProviders {
		s.providerCallback = s.provider.RegisterCallback(s.onProviderUpdated)
	}
	return nil
}

func (s *URLTest) PostStart() error {
	s.group.PostStart()
	return nil
}

func (s *URLTest) Close() error {
	s.providerAccess.Lock()
	s.closed = true
	if s.providerCallback != nil {
		s.provider.UnregisterCallback(s.providerCallback)
		s.providerCallback = nil
	}
	s.providerAccess.Unlock()
	return common.Close(
		common.PtrOrNil(s.group),
	)
}

func (s *URLTest) All() []string {
	if s.group == nil {
		return slices.Clone(s.tags)
	}
	tags, _ := s.group.loadMembers()
	return slices.Clone(tags)
}

func (s *URLTest) Member(tag string) (adapter.Outbound, bool) {
	if s.group == nil {
		return nil, false
	}
	tags, outbounds := s.group.loadMembers()
	index := slices.Index(tags, tag)
	if index == -1 {
		return nil, false
	}
	return outbounds[index], true
}

func (s *URLTest) Selected(network string) adapter.Outbound {
	var outbound adapter.Outbound
	if network == N.NetworkUDP {
		outbound = s.group.selectedOutboundUDP.Load()
	} else {
		outbound = s.group.selectedOutboundTCP.Load()
	}
	if outbound == nil {
		outbound, _ = s.group.Select(network)
	}
	return outbound
}

func (s *URLTest) SelectedTag(network string) string {
	selected := s.Selected(network)
	tags, outbounds := s.group.loadMembers()
	return findOutboundTag(tags, outbounds, selected)
}

func (s *URLTest) AttachConnection(closer io.Closer) func() {
	s.group.Touch()
	return s.group.interruptGroup.Add(closer, true)
}

func (s *URLTest) References() []string {
	group := s.group
	if group == nil {
		return nil
	}
	tags, outbounds := group.loadMembers()
	var references []string
	selectedOutboundTCP := group.selectedOutboundTCP.Load()
	selectedOutboundUDP := group.selectedOutboundUDP.Load()
	if selectedOutboundTCP != nil {
		references = append(references, findOutboundTag(tags, outbounds, selectedOutboundTCP))
	}
	if selectedOutboundUDP != nil && selectedOutboundUDP != selectedOutboundTCP {
		references = append(references, findOutboundTag(tags, outbounds, selectedOutboundUDP))
	}
	return references
}

func (s *URLTest) URLTest(ctx context.Context) (map[string]uint16, error) {
	return s.group.URLTest(ctx)
}

func (s *URLTest) CheckOutbounds() {
	s.group.CheckOutbounds(s.ctx, true)
}

func (s *URLTest) PerformUpdateCheck() {
	s.group.performUpdateCheck()
}

func (s *URLTest) InterfaceUpdated(ctx context.Context) {
	group := s.group
	if group == nil {
		return
	}
	if group.pause.IsDevicePaused() || group.pause.IsNetworkPaused() {
		return
	}
	go func() {
		s.checkAccess.Lock()
		defer s.checkAccess.Unlock()
		if ctx.Err() != nil {
			return
		}
		group.CheckOutbounds(ctx, true)
	}()
}

func (s *URLTest) isGroupActive() bool {
	if !s.group.started.Load() {
		return false
	}
	return time.Since(s.group.lastActive.Load()) <= s.group.idleTimeout
}

func (s *URLTest) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
	var outbound adapter.Outbound
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		outbound = s.group.selectedOutboundTCP.Load()
	case N.NetworkUDP:
		outbound = s.group.selectedOutboundUDP.Load()
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if outbound == nil {
		outbound, _ = s.group.Select(network)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.DialContext(ctx, network, destination)
	if err == nil {
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsProviderConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.history.StoreURLTestHistoryForOutbound(outbound, nil)
	return nil, err
}

func (s *URLTest) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	s.group.Touch()
	outbound := s.group.selectedOutboundUDP.Load()
	if outbound == nil {
		outbound, _ = s.group.Select(N.NetworkUDP)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsProviderConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.history.StoreURLTestHistoryForOutbound(outbound, nil)
	return nil, err
}

func (s *URLTest) onProviderUpdated(tag string) error {
	s.providerAccess.Lock()
	if s.closed {
		s.providerAccess.Unlock()
		return nil
	}
	providerTags, relevant := refreshProvider(s.provider, s.providers, s.providerTags, s.useAllProviders, tag)
	if !relevant {
		s.providerAccess.Unlock()
		return nil
	}
	s.providerTags = providerTags
	tags, outbounds, outboundsCache, err := collectProviderOutbounds(
		tag,
		s.Dependencies(),
		s.outbound,
		s.providers,
		s.providerTags,
		s.outboundsCache,
		s.exclude,
		s.include,
	)
	if err != nil {
		s.providerAccess.Unlock()
		return E.Cause(err, s.Tag())
	}
	s.outboundsCache = outboundsCache
	s.group.replaceOutbounds(tags, outbounds)
	s.providerAccess.Unlock()
	if s.isGroupActive() {
		s.group.access.Lock()
		if s.group.ticker != nil {
			s.group.ticker.Reset(s.group.interval)
		}
		s.group.access.Unlock()
		s.providerUpdateCheck.Schedule(func() {
			// A check on the previous instances may still be in flight. Wait for
			// it, then replace its results instead of accepting its cached tags.
			_, _ = s.group.urlTestWait(s.ctx, true)
		})
	}
	return nil
}

type URLTestGroup struct {
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	pause                        pause.Manager
	pauseCallback                *list.Element[pause.Callback]
	logger                       log.Logger
	tags                         []string
	outbounds                    []adapter.Outbound
	outboundsAccess              sync.RWMutex
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	history                      *urltest.HistoryStorage
	checking                     sync.Mutex
	selectedOutboundTCP          common.TypedValue[adapter.Outbound]
	selectedOutboundUDP          common.TypedValue[adapter.Outbound]
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	access                       sync.Mutex
	updateAccess                 sync.Mutex
	ticker                       *time.Ticker
	close                        chan struct{}
	started                      atomic.Bool
	lastActive                   common.TypedValue[time.Time]
}

func NewURLTestGroup(ctx context.Context, outboundManager adapter.OutboundManager, logger log.Logger, tags []string, outbounds []adapter.Outbound, link string, interval time.Duration, tolerance uint16, idleTimeout time.Duration, interruptExternalConnections bool) (*URLTestGroup, error) {
	if interval == 0 {
		interval = C.DefaultURLTestInterval
	}
	if tolerance == 0 {
		tolerance = 50
	}
	if idleTimeout == 0 {
		idleTimeout = C.DefaultURLTestIdleTimeout
	}
	if interval > idleTimeout {
		return nil, E.New("interval must be less or equal than idle_timeout")
	}
	history := service.PtrFromContext[urltest.HistoryStorage](ctx)
	if history == nil {
		return nil, E.New("missing URL test history storage")
	}
	group := &URLTestGroup{
		ctx:                          ctx,
		outbound:                     outboundManager,
		logger:                       logger,
		link:                         link,
		interval:                     interval,
		tolerance:                    tolerance,
		idleTimeout:                  idleTimeout,
		history:                      history,
		close:                        make(chan struct{}),
		pause:                        service.FromContext[pause.Manager](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: interruptExternalConnections,
	}
	group.storeOutbounds(tags, outbounds)
	return group, nil
}

func (g *URLTestGroup) PostStart() {
	g.access.Lock()
	defer g.access.Unlock()
	g.started.Store(true)
	g.lastActive.Store(time.Now())
	go g.CheckOutbounds(g.ctx, false)
}

func (g *URLTestGroup) Touch() {
	if !g.started.Load() {
		return
	}
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker != nil {
		g.lastActive.Store(time.Now())
		return
	}
	ticker := time.NewTicker(g.interval)
	g.ticker = ticker
	g.pauseCallback = pause.RegisterTicker(g.pause, ticker, g.interval, nil)
	go g.loopCheck(ticker, g.close)
}

func (g *URLTestGroup) Close() error {
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker == nil {
		return nil
	}
	g.ticker.Stop()
	g.ticker = nil
	g.pause.UnregisterCallback(g.pauseCallback)
	g.pauseCallback = nil
	close(g.close)
	return nil
}

func (g *URLTestGroup) Select(network string) (adapter.Outbound, bool) {
	var minDelay uint16
	var minOutbound adapter.Outbound
	switch network {
	case N.NetworkTCP:
		selectedOutbound := g.selectedOutboundTCP.Load()
		if selectedOutbound != nil {
			if history := g.history.LoadURLTestHistoryForOutbound(RealOutbound(selectedOutbound, N.NetworkTCP)); history != nil {
				minOutbound = selectedOutbound
				minDelay = history.Delay
			}
		}
	case N.NetworkUDP:
		selectedOutbound := g.selectedOutboundUDP.Load()
		if selectedOutbound != nil {
			if history := g.history.LoadURLTestHistoryForOutbound(RealOutbound(selectedOutbound, N.NetworkUDP)); history != nil {
				minOutbound = selectedOutbound
				minDelay = history.Delay
			}
		}
	}
	outbounds := g.loadOutbounds()
	for _, detour := range outbounds {
		if !common.Contains(detour.Network(), network) {
			continue
		}
		history := g.history.LoadURLTestHistoryForOutbound(RealOutbound(detour, network))
		if history == nil {
			continue
		}
		if minDelay == 0 || minDelay > history.Delay+g.tolerance {
			minDelay = history.Delay
			minOutbound = detour
		}
	}
	if minOutbound == nil {
		for _, detour := range outbounds {
			if !common.Contains(detour.Network(), network) {
				continue
			}
			return detour, false
		}
		return nil, false
	}
	return minOutbound, true
}

func (g *URLTestGroup) loopCheck(ticker *time.Ticker, closeChan <-chan struct{}) {
	if time.Since(g.lastActive.Load()) > g.interval {
		g.lastActive.Store(time.Now())
		g.CheckOutbounds(g.ctx, false)
	}
	for {
		select {
		case <-closeChan:
			return
		case <-ticker.C:
		}
		if time.Since(g.lastActive.Load()) > g.idleTimeout {
			g.access.Lock()
			if g.ticker == ticker {
				g.ticker.Stop()
				g.ticker = nil
				g.pause.UnregisterCallback(g.pauseCallback)
				g.pauseCallback = nil
			}
			g.access.Unlock()
			return
		}
		g.CheckOutbounds(g.ctx, false)
	}
}

func (g *URLTestGroup) CheckOutbounds(ctx context.Context, force bool) {
	_, _ = g.urlTest(ctx, force)
}

func (g *URLTestGroup) URLTest(ctx context.Context) (map[string]uint16, error) {
	return g.urlTest(ctx, true)
}

func (g *URLTestGroup) urlTest(ctx context.Context, force bool) (map[string]uint16, error) {
	if !g.checking.TryLock() {
		return make(map[string]uint16), nil
	}
	defer g.checking.Unlock()
	return g.urlTestLocked(ctx, force)
}

func (g *URLTestGroup) urlTestWait(ctx context.Context, force bool) (map[string]uint16, error) {
	g.checking.Lock()
	defer g.checking.Unlock()
	return g.urlTestLocked(ctx, force)
}

func (g *URLTestGroup) urlTestLocked(ctx context.Context, force bool) (map[string]uint16, error) {
	tags, outbounds := g.loadMembers()
	result := URLTestOutbounds(ctx, g.outbound, g.history, g.logger, tags, outbounds, g.link, g.interval, force)
	select {
	case <-ctx.Done():
	default:
		g.performUpdateCheck()
	}
	return result, nil
}

type urlTestResult struct {
	delay uint16
	err   error
}

type urlTestBatch struct {
	ctx      context.Context
	outbound adapter.OutboundManager
	history  *urltest.HistoryStorage
	logger   log.Logger
	batch    *batch.Batch[any]
	checked  map[adapter.Outbound]bool
	groups   []urlTestBatchGroup
	access   sync.Mutex
	result   map[string]uint16
}

type urlTestBatchGroup struct {
	tag   string
	group adapter.OutboundGroup
}

func URLTestOutbounds(ctx context.Context, outboundManager adapter.OutboundManager, history *urltest.HistoryStorage, logger log.Logger, tags []string, outbounds []adapter.Outbound, link string, interval time.Duration, force bool) map[string]uint16 {
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	testBatch := &urlTestBatch{
		ctx:      ctx,
		outbound: outboundManager,
		history:  history,
		logger:   logger,
		batch:    b,
		checked:  make(map[adapter.Outbound]bool),
		result:   make(map[string]uint16),
	}
	testBatch.test(tags, outbounds, link, interval, force)
	b.Wait()
	for _, it := range testBatch.groups {
		groupHistory := history.LoadURLTestHistoryForOutbound(RealOutbound(it.group, N.NetworkTCP))
		if groupHistory != nil {
			testBatch.result[it.tag] = groupHistory.Delay
		}
	}
	return testBatch.result
}

func (b *urlTestBatch) test(tags []string, outbounds []adapter.Outbound, link string, interval time.Duration, force bool) {
	for i, detour := range outbounds {
		tag := tags[i]
		if b.checked[detour] {
			continue
		}
		switch nested := detour.(type) {
		case *URLTest:
			b.checked[detour] = true
			b.groups = append(b.groups, urlTestBatchGroup{tag, nested})
			b.batch.Go(tag, func() (any, error) {
				nestedResult, _ := nested.group.urlTest(b.ctx, force)
				b.access.Lock()
				maps.Copy(b.result, nestedResult)
				b.access.Unlock()
				return nil, nil
			})
		case adapter.OutboundGroup:
			b.checked[detour] = true
			b.groups = append(b.groups, urlTestBatchGroup{tag, nested})
			var (
				memberTags []string
				members    []adapter.Outbound
			)
			for _, memberTag := range nested.All() {
				member, loaded := nested.Member(memberTag)
				if !loaded {
					continue
				}
				memberTags = append(memberTags, memberTag)
				members = append(members, member)
			}
			b.test(memberTags, members, link, interval, force)
		default:
			history := b.history.LoadURLTestHistoryForOutbound(detour)
			if !force && history != nil && time.Since(history.Time) < interval {
				continue
			}
			b.checked[detour] = true
			b.batch.Go(tag, func() (any, error) {
				testCtx, cancel := context.WithTimeout(b.ctx, C.TCPTimeout)
				defer cancel()
				testChan := make(chan urlTestResult, 1)
				go func() {
					delay, testErr := urltest.URLTest(testCtx, link, detour)
					testChan <- urlTestResult{delay, testErr}
				}()
				var testResult urlTestResult
				select {
				case testResult = <-testChan:
				case <-testCtx.Done():
					testResult.err = testCtx.Err()
				}
				if testResult.err != nil {
					if b.ctx.Err() != nil {
						return nil, nil
					}
					b.logger.Debug("outbound ", tag, " unavailable: ", testResult.err)
					b.history.StoreURLTestHistoryForOutbound(detour, nil)
				} else {
					b.logger.Debug("outbound ", tag, " available: ", testResult.delay, "ms")
					stored := b.history.StoreURLTestHistoryForOutbound(detour, &adapter.URLTestHistory{
						Time:  time.Now(),
						Delay: testResult.delay,
					})
					if !stored {
						return nil, nil
					}
					b.access.Lock()
					b.result[tag] = testResult.delay
					b.access.Unlock()
				}
				return nil, nil
			})
		}
	}
}

func (g *URLTestGroup) performUpdateCheck() {
	g.updateAccess.Lock()
	defer g.updateAccess.Unlock()
	var (
		updated  bool
		selected bool
	)
	selectedOutboundTCP := g.selectedOutboundTCP.Load()
	if outbound, exists := g.Select(N.NetworkTCP); outbound != nil && (selectedOutboundTCP == nil || (exists && outbound != selectedOutboundTCP)) {
		if selectedOutboundTCP != nil {
			updated = true
		}
		g.selectedOutboundTCP.Store(outbound)
		selected = true
	}
	selectedOutboundUDP := g.selectedOutboundUDP.Load()
	if outbound, exists := g.Select(N.NetworkUDP); outbound != nil && (selectedOutboundUDP == nil || (exists && outbound != selectedOutboundUDP)) {
		if selectedOutboundUDP != nil {
			updated = true
		}
		g.selectedOutboundUDP.Store(outbound)
		selected = true
	}
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	if selected {
		g.history.NotifyUpdated()
	}
}

func (g *URLTestGroup) loadOutbounds() []adapter.Outbound {
	g.outboundsAccess.RLock()
	defer g.outboundsAccess.RUnlock()
	return g.outbounds
}

func (g *URLTestGroup) loadMembers() ([]string, []adapter.Outbound) {
	g.outboundsAccess.RLock()
	defer g.outboundsAccess.RUnlock()
	return g.tags, g.outbounds
}

func (g *URLTestGroup) storeOutbounds(tags []string, outbounds []adapter.Outbound) {
	g.outboundsAccess.Lock()
	g.tags = tags
	g.outbounds = outbounds
	g.outboundsAccess.Unlock()
}

func (g *URLTestGroup) replaceOutbounds(tags []string, outbounds []adapter.Outbound) {
	g.updateAccess.Lock()
	selectedOutboundTCP := g.selectedOutboundTCP.Load()
	selectedOutboundUDP := g.selectedOutboundUDP.Load()
	g.storeOutbounds(tags, outbounds)
	if !containsOutbound(outbounds, selectedOutboundTCP) {
		g.selectedOutboundTCP.Store(nil)
	}
	if !containsOutbound(outbounds, selectedOutboundUDP) {
		g.selectedOutboundUDP.Store(nil)
	}
	if g.selectedOutboundTCP.Load() == nil {
		if outbound, _ := g.Select(N.NetworkTCP); outbound != nil {
			g.selectedOutboundTCP.Store(outbound)
		}
	}
	if g.selectedOutboundUDP.Load() == nil {
		if outbound, _ := g.Select(N.NetworkUDP); outbound != nil {
			g.selectedOutboundUDP.Store(outbound)
		}
	}
	updated := (selectedOutboundTCP != nil && g.selectedOutboundTCP.Load() != selectedOutboundTCP) ||
		(selectedOutboundUDP != nil && g.selectedOutboundUDP.Load() != selectedOutboundUDP)
	selected := g.selectedOutboundTCP.Load() != selectedOutboundTCP || g.selectedOutboundUDP.Load() != selectedOutboundUDP
	g.updateAccess.Unlock()
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	if selected && g.history != nil {
		g.history.NotifyUpdated()
	}
}

func containsOutbound(outbounds []adapter.Outbound, selected adapter.Outbound) bool {
	if selected == nil {
		return true
	}
	for _, outbound := range outbounds {
		if outbound == selected {
			return true
		}
	}
	return false
}
