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
)

func RegisterURLTest(registry *outbound.Registry) {
	outbound.Register[option.URLTestOutboundOptions](registry, C.TypeURLTest, NewURLTest)
}

var (
	_ adapter.OutboundGroup           = (*URLTest)(nil)
	_ adapter.URLTestGroup            = (*URLTest)(nil)
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
	fallback                     bool
	maxDelay                     time.Duration
	group                        *URLTestGroup
	checkAccess                  sync.Mutex
	interruptExternalConnections bool
}

func NewURLTest(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.URLTestOutboundOptions) (adapter.Outbound, error) {
	var (
		fallback bool
		maxDelay time.Duration
	)
	if options.Fallback != nil {
		fallback = options.Fallback.Enabled
		maxDelay = time.Duration(options.Fallback.MaxDelay)
	}
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
		fallback:                     fallback,
		maxDelay:                     maxDelay,
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if len(outbound.tags) == 0 {
		return nil, E.New("missing tags")
	}
	for i, tag := range outbound.tags {
		if slices.Contains(outbound.tags[:i], tag) {
			return nil, E.New("duplicate tag: ", tag)
		}
	}
	return outbound, nil
}

func (s *URLTest) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		outbounds := make([]adapter.Outbound, 0, len(s.tags))
		for i, tag := range s.tags {
			detour, loaded := s.outbound.Outbound(tag)
			if !loaded {
				return E.New("outbound ", i, " not found: ", tag)
			}
			outbounds = append(outbounds, detour)
		}
		group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, outbounds, s.link, s.interval, s.tolerance, s.idleTimeout, s.interruptExternalConnections)
		if err != nil {
			return err
		}
		err = group.configureFallback(s.fallback, s.maxDelay)
		if err != nil {
			return err
		}
		s.group = group
	case adapter.StartStateStarted:
		s.group.PostStart()
		scope.Add(s.group.Close)
	}
	return nil
}

func (s *URLTest) All() []string {
	return s.tags
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

func (s *URLTest) AttachConnection(closer io.Closer) func() {
	s.group.Touch()
	return s.group.interruptGroup.Add(closer, true)
}

func (s *URLTest) References() ([]string, []string) {
	group := s.group
	if group == nil {
		return nil, nil
	}
	var references []string
	selectedOutboundTCP := group.selectedOutboundTCP.Load()
	selectedOutboundUDP := group.selectedOutboundUDP.Load()
	if selectedOutboundTCP != nil {
		references = append(references, selectedOutboundTCP.Tag())
	}
	if selectedOutboundUDP != nil && selectedOutboundUDP != selectedOutboundTCP {
		references = append(references, selectedOutboundUDP.Tag())
	}
	return references, nil
}

func (s *URLTest) URLTest(ctx context.Context) (map[string]uint16, error) {
	return s.group.URLTest(ctx)
}

func (s *URLTest) CheckOutbounds() {
	s.group.CheckOutbounds(s.ctx, true)
}

func (s *URLTest) PerformUpdateCheck() {
	if s.group.performUpdateCheck() {
		s.group.history.NotifyUpdated()
	}
}

func (s *URLTest) InterfaceUpdated(ctx context.Context) {
	group := s.group
	if group == nil {
		return
	}
	group.resetFallbackProbeStates()
	if group.pause != nil && (group.pause.IsDevicePaused() || group.pause.IsNetworkPaused()) {
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

func (s *URLTest) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
	networkName := N.NetworkName(network)
	var outbound adapter.Outbound
	switch networkName {
	case N.NetworkTCP:
		outbound = s.group.selectedOutboundTCP.Load()
	case N.NetworkUDP:
		outbound = s.group.selectedOutboundUDP.Load()
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if outbound == nil {
		outbound, _ = s.group.Select(networkName)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	realTag := RealTag(outbound, networkName)
	conn, err := outbound.DialContext(ctx, network, destination)
	if err == nil {
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	if !E.IsClosedOrCanceled(err) {
		NotifyOutboundFailure(s.outbound, s.group.history, []adapter.Outbound{s}, realTag)
	}
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
	realTag := RealTag(outbound, N.NetworkUDP)
	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	if !E.IsClosedOrCanceled(err) {
		NotifyOutboundFailure(s.outbound, s.group.history, []adapter.Outbound{s}, realTag)
	}
	return nil, err
}

type URLTestGroup struct {
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	pause                        pause.Manager
	pauseCallback                *list.Element[pause.Callback]
	logger                       log.Logger
	outbounds                    []adapter.Outbound
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	history                      *urltest.HistoryStorage
	checking                     atomic.Bool
	selectedOutboundTCP          common.TypedValue[adapter.Outbound]
	selectedOutboundUDP          common.TypedValue[adapter.Outbound]
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	access                       sync.Mutex
	updateAccess                 sync.Mutex
	ticker                       *time.Ticker
	close                        chan struct{}
	started                      atomic.Bool
	closed                       atomic.Bool
	lastActive                   common.TypedValue[time.Time]
	urlTestFailover
}

func NewURLTestGroup(ctx context.Context, outboundManager adapter.OutboundManager, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, tolerance uint16, idleTimeout time.Duration, interruptExternalConnections bool) (*URLTestGroup, error) {
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
		outbounds:                    outbounds,
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
	group.initFailover()
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
	if g.closed.Load() {
		g.access.Unlock()
		return
	}
	if g.ticker != nil {
		g.lastActive.Store(time.Now())
		g.access.Unlock()
		g.onTouch()
		return
	}
	needImmediateCheck := time.Since(g.lastActive.Load()) > g.interval
	g.lastActive.Store(time.Now())
	ticker := time.NewTicker(g.interval)
	g.ticker = ticker
	if g.pause != nil {
		g.pauseCallback = pause.RegisterTicker(g.pause, ticker, g.interval, nil)
	}
	go g.loopCheck(ticker, g.close, needImmediateCheck)
	g.access.Unlock()
	g.onTouch()
}

func (g *URLTestGroup) Close() error {
	g.access.Lock()
	defer g.access.Unlock()
	if g.closed.Swap(true) {
		return nil
	}
	if g.ticker != nil {
		g.ticker.Stop()
		g.ticker = nil
		if g.pause != nil && g.pauseCallback != nil {
			g.pause.UnregisterCallback(g.pauseCallback)
		}
		g.pauseCallback = nil
	}
	g.stopFallbackProbeLocked()
	close(g.close)
	return nil
}

func (g *URLTestGroup) Select(network string) (adapter.Outbound, bool) {
	return g.selectOutbound(network, nil)
}

func (g *URLTestGroup) loopCheck(ticker *time.Ticker, closeChan <-chan struct{}, immediate bool) {
	if immediate {
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
				if g.pause != nil && g.pauseCallback != nil {
					g.pause.UnregisterCallback(g.pauseCallback)
				}
				g.pauseCallback = nil
			}
			g.stopFallbackProbeLocked()
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
	g.resetFallbackProbeStates()
	return g.urlTest(ctx, true)
}

func (g *URLTestGroup) urlTest(ctx context.Context, force bool) (map[string]uint16, error) {
	if g.checking.Swap(true) {
		return make(map[string]uint16), nil
	}
	defer g.checking.Store(false)
	result := urlTestOutboundsWithCallback(ctx, g.outbound, g.history, g.logger, g.outbounds, g.link, g.interval, force, g.urlTestFunc, func(tag string, delay uint16, err error, inFlight map[string]bool) {
		g.handleBatchResult(tag, delay, err, inFlight)
	})
	g.performUpdateCheck()
	g.history.NotifyUpdated()
	return result, nil
}

type urlTestResult struct {
	delay uint16
	err   error
}

type urlTestTask struct {
	tag    string
	nested *URLTest
	detour adapter.Outbound
}

type urlTestBatch struct {
	ctx          context.Context
	outbound     adapter.OutboundManager
	history      *urltest.HistoryStorage
	logger       log.Logger
	batch        *batch.Batch[any]
	checked      map[string]bool
	inFlight     map[string]bool
	tasks        []urlTestTask
	groups       []adapter.OutboundGroup
	access       sync.Mutex
	result       map[string]uint16
	urlTestFunc  func(ctx context.Context, link string, detour N.Dialer) (uint16, error)
	onLeafResult func(tag string, delay uint16, err error, inFlight map[string]bool)
}

func URLTestOutbounds(ctx context.Context, outboundManager adapter.OutboundManager, history *urltest.HistoryStorage, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, force bool) map[string]uint16 {
	result := urlTestOutboundsWithCallback(ctx, outboundManager, history, logger, outbounds, link, interval, force, urltest.URLTest, nil)
	history.NotifyUpdated()
	return result
}

func urlTestOutboundsWithCallback(ctx context.Context, outboundManager adapter.OutboundManager, history *urltest.HistoryStorage, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, force bool, testFunc func(ctx context.Context, link string, detour N.Dialer) (uint16, error), onLeafResult func(tag string, delay uint16, err error, inFlight map[string]bool)) map[string]uint16 {
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	if testFunc == nil {
		testFunc = urltest.URLTest
	}
	testBatch := &urlTestBatch{
		ctx:          ctx,
		outbound:     outboundManager,
		history:      history,
		logger:       logger,
		batch:        b,
		checked:      make(map[string]bool),
		inFlight:     make(map[string]bool),
		result:       make(map[string]uint16),
		urlTestFunc:  testFunc,
		onLeafResult: onLeafResult,
	}
	testBatch.collect(outbounds, link, interval, force)
	testBatch.run(link, force)
	b.Wait()
	for _, outboundGroup := range testBatch.groups {
		groupHistory := history.LoadURLTestHistory(RealTag(outboundGroup, N.NetworkTCP))
		if groupHistory != nil {
			testBatch.result[outboundGroup.Tag()] = groupHistory.Delay
		}
	}
	return testBatch.result
}

func (b *urlTestBatch) collect(outbounds []adapter.Outbound, link string, interval time.Duration, force bool) {
	for _, detour := range outbounds {
		tag := detour.Tag()
		if b.checked[tag] {
			continue
		}
		switch nested := detour.(type) {
		case *URLTest:
			b.checked[tag] = true
			b.inFlight[tag] = true
			b.groups = append(b.groups, nested)
			b.tasks = append(b.tasks, urlTestTask{
				tag:    tag,
				nested: nested,
			})
		case adapter.OutboundGroup:
			b.checked[tag] = true
			b.groups = append(b.groups, nested)
			if b.outbound != nil {
				b.collect(common.FilterNotNil(common.Map(nested.All(), func(it string) adapter.Outbound {
					member, _ := b.outbound.Outbound(it)
					return member
				})), link, interval, force)
			}
		default:
			history := b.history.LoadURLTestHistory(tag)
			if !force && history != nil && time.Since(history.Time) < interval {
				continue
			}
			b.checked[tag] = true
			b.inFlight[tag] = true
			b.tasks = append(b.tasks, urlTestTask{
				tag:    tag,
				detour: detour,
			})
		}
	}
}

func (b *urlTestBatch) run(link string, force bool) {
	for _, task := range b.tasks {
		if task.nested != nil {
			nested := task.nested
			tag := task.tag
			b.batch.Go(tag, func() (any, error) {
				nestedResult, _ := nested.group.urlTest(b.ctx, force)
				b.access.Lock()
				maps.Copy(b.result, nestedResult)
				delete(b.inFlight, tag)
				b.access.Unlock()
				return nil, nil
			})
			continue
		}
		detour := task.detour
		tag := task.tag
		b.batch.Go(tag, func() (any, error) {
			testCtx, cancel := context.WithTimeout(b.ctx, C.TCPTimeout)
			defer cancel()
			testChan := make(chan urlTestResult, 1)
			go func() {
				delay, testErr := b.urlTestFunc(testCtx, link, detour)
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
					b.access.Lock()
					delete(b.inFlight, tag)
					b.access.Unlock()
					return nil, nil
				}
				b.logger.Debug("outbound ", tag, " unavailable: ", testResult.err)
				b.history.UpdateURLTestHistory(nil, []string{tag})
				b.access.Lock()
				delete(b.inFlight, tag)
				inFlightSnapshot := maps.Clone(b.inFlight)
				b.access.Unlock()
				if b.onLeafResult != nil {
					b.onLeafResult(tag, 0, testResult.err, inFlightSnapshot)
				}
			} else {
				b.logger.Debug("outbound ", tag, " available: ", testResult.delay, "ms")
				b.history.UpdateURLTestHistory(map[string]*adapter.URLTestHistory{
					tag: {
						Time:  time.Now(),
						Delay: testResult.delay,
					},
				}, nil)
				b.access.Lock()
				delete(b.inFlight, tag)
				b.result[tag] = testResult.delay
				inFlightSnapshot := maps.Clone(b.inFlight)
				b.access.Unlock()
				if b.onLeafResult != nil {
					b.onLeafResult(tag, testResult.delay, nil, inFlightSnapshot)
				}
			}
			return nil, nil
		})
	}
}

func (g *URLTestGroup) performUpdateCheck() bool {
	selected, _ := g.performUpdateCheckWithFailed(nil, nil)
	return selected
}
