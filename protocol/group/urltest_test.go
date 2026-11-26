package group

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"

	"github.com/stretchr/testify/require"
)

type stubOutbound struct {
	outbound.Adapter
	dialFunc   func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
	listenFunc func(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error)
}

func newStubOutbound(tag string, networks ...string) *stubOutbound {
	if len(networks) == 0 {
		networks = []string{N.NetworkTCP, N.NetworkUDP}
	}
	return &stubOutbound{
		Adapter: outbound.NewAdapter("stub", tag, networks, nil),
	}
}

func (s *stubOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if s.dialFunc != nil {
		return s.dialFunc(ctx, network, destination)
	}
	c1, c2 := net.Pipe()
	_ = c2.Close()
	return c1, nil
}

func (s *stubOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if s.listenFunc != nil {
		return s.listenFunc(ctx, destination)
	}
	return nil, errors.New("packet unsupported")
}

type stubOutboundManager struct {
	adapter.OutboundManager
	mu        sync.RWMutex
	outbounds []adapter.Outbound
	byTag     map[string]adapter.Outbound
}

func newStubOutboundManager(outbounds ...adapter.Outbound) *stubOutboundManager {
	m := &stubOutboundManager{
		byTag: make(map[string]adapter.Outbound),
	}
	for _, ob := range outbounds {
		m.Add(ob)
	}
	return m
}

func (m *stubOutboundManager) Add(ob adapter.Outbound) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outbounds = append(m.outbounds, ob)
	m.byTag[ob.Tag()] = ob
}

func (m *stubOutboundManager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	return nil
}

func (m *stubOutboundManager) Outbounds() []adapter.Outbound {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]adapter.Outbound, len(m.outbounds))
	copy(result, m.outbounds)
	return result
}

func (m *stubOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ob, ok := m.byTag[tag]
	return ob, ok
}

func (m *stubOutboundManager) Default() adapter.Outbound {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.outbounds) == 0 {
		return nil
	}
	return m.outbounds[0]
}

func (m *stubOutboundManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	return nil
}

func newTestContext(t *testing.T, manager *stubOutboundManager, history *urltest.HistoryStorage) context.Context {
	t.Helper()
	ctx := service.ContextWithDefaultRegistry(context.Background())
	ctx = pause.WithDefaultManager(ctx)
	service.MustRegister[adapter.OutboundManager](ctx, manager)
	service.MustRegisterPtr(ctx, history)
	return ctx
}

func newStartedURLTest(t *testing.T, ctx context.Context, tag string, opts option.URLTestOutboundOptions) *URLTest {
	t.Helper()
	raw, err := NewURLTest(ctx, nil, log.NewNOPFactory().Logger(), tag, opts)
	require.NoError(t, err)
	u := raw.(*URLTest)
	require.NoError(t, u.Start(adapter.StartStateStart, nil))
	u.group.started.Store(true)
	u.group.lastActive.Store(time.Now())
	t.Cleanup(func() {
		_ = u.group.Close()
	})
	return u
}

func storeDelay(history *urltest.HistoryStorage, tag string, delay uint16) {
	history.StoreURLTestHistory(tag, &adapter.URLTestHistory{
		Time:  time.Now(),
		Delay: delay,
	})
}

func TestURLTestNormalSelectionAndTolerance(t *testing.T) {
	a := newStubOutbound("a")
	b := newStubOutbound("b")
	c := newStubOutbound("c")
	manager := newStubOutboundManager(a, b, c)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "auto", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b", "c"},
	})
	require.False(t, u.Fallback())
	require.Equal(t, uint16(50), u.group.tolerance)

	storeDelay(history, "a", 120)
	storeDelay(history, "b", 100)
	storeDelay(history, "c", 160)
	u.PerformUpdateCheck()
	require.Equal(t, "b", u.Selected(N.NetworkTCP).Tag())

	storeDelay(history, "a", 70)
	u.PerformUpdateCheck()
	require.Equal(t, "b", u.Selected(N.NetworkTCP).Tag())

	storeDelay(history, "a", 40)
	u.PerformUpdateCheck()
	require.Equal(t, "a", u.Selected(N.NetworkTCP).Tag())

	history.DeleteURLTestHistory("a")
	storeDelay(history, "b", 200)
	storeDelay(history, "c", 160)
	u.PerformUpdateCheck()
	require.Equal(t, "c", u.Selected(N.NetworkTCP).Tag())
}

func TestURLTestImmediateSwitchOnDialFailure(t *testing.T) {
	dialErr := errors.New("dial refused")
	a := newStubOutbound("a")
	a.dialFunc = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return nil, dialErr
	}
	b := newStubOutbound("b")
	b.dialFunc = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return nil, dialErr
	}
	c := newStubOutbound("c")
	manager := newStubOutboundManager(a, b, c)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "auto", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b", "c"},
	})
	manager.Add(u)

	storeDelay(history, "a", 50)
	storeDelay(history, "b", 120)
	storeDelay(history, "c", 80)
	u.PerformUpdateCheck()
	require.Equal(t, "a", u.Selected(N.NetworkTCP).Tag())

	_, err := u.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:443"))
	require.ErrorIs(t, err, dialErr)
	require.Nil(t, history.LoadURLTestHistory("a"))
	require.Equal(t, "c", u.Selected(N.NetworkTCP).Tag())

	history.DeleteURLTestHistory("b")
	history.DeleteURLTestHistory("c")
	u.group.selectedOutboundTCP.Store(a)
	u.group.selectedOutboundUDP.Store(a)

	var bgChecks atomic.Int32
	u.group.urlTestFunc = func(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
		bgChecks.Add(1)
		return 0, dialErr
	}

	_, err = u.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:443"))
	require.ErrorIs(t, err, dialErr)
	require.Equal(t, "b", u.Selected(N.NetworkTCP).Tag())

	_, err = u.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:443"))
	require.ErrorIs(t, err, dialErr)
	require.Equal(t, "c", u.Selected(N.NetworkTCP).Tag())

	require.Eventually(t, func() bool {
		return bgChecks.Load() > 0
	}, time.Second, 10*time.Millisecond)

	a.dialFunc = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return nil, context.Canceled
	}
	storeDelay(history, "a", 30)
	u.PerformUpdateCheck()
	require.Equal(t, "a", u.Selected(N.NetworkTCP).Tag())
	_, err = u.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:443"))
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, history.LoadURLTestHistory("a"))
	require.Equal(t, "a", u.Selected(N.NetworkTCP).Tag())
}

func TestURLTestBatchImmediateSwitchWithoutWaitingSlowNode(t *testing.T) {
	a := newStubOutbound("a")
	b := newStubOutbound("b")
	c := newStubOutbound("c")
	manager := newStubOutboundManager(a, b, c)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "auto", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b", "c"},
	})
	storeDelay(history, "a", 50)
	storeDelay(history, "b", 100)
	u.PerformUpdateCheck()
	require.Equal(t, "a", u.Selected(N.NetworkTCP).Tag())

	releaseSlow := make(chan struct{})
	u.group.urlTestFunc = func(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
		ob := detour.(adapter.Outbound)
		switch ob.Tag() {
		case "a":
			return 0, errors.New("dead")
		case "b":
			return 90, nil
		default:
			<-releaseSlow
			return 200, nil
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = u.URLTest(ctx)
	}()

	require.Eventually(t, func() bool {
		return u.Selected(N.NetworkTCP).Tag() == "b"
	}, time.Second, 10*time.Millisecond)

	select {
	case <-done:
		t.Fatal("batch should still be waiting for slow node c")
	default:
	}

	close(releaseSlow)
	<-done
	require.Equal(t, "b", u.Selected(N.NetworkTCP).Tag())
}

func TestURLTestNestedDialFailurePreservesNewInnerSelectionHistory(t *testing.T) {
	a := newStubOutbound("a")
	a.dialFunc = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("a failed")
	}
	b := newStubOutbound("b")
	c := newStubOutbound("c")
	manager := newStubOutboundManager(a, b, c)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	inner := newStartedURLTest(t, ctx, "inner", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b"},
	})
	manager.Add(inner)

	outer := newStartedURLTest(t, ctx, "outer", option.URLTestOutboundOptions{
		Outbounds: []string{"inner", "c"},
	})
	manager.Add(outer)

	storeDelay(history, "a", 50)
	storeDelay(history, "b", 80)
	storeDelay(history, "c", 200)
	inner.PerformUpdateCheck()
	outer.PerformUpdateCheck()
	require.Equal(t, "a", inner.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "inner", outer.Selected(N.NetworkTCP).Tag())

	_, err := outer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:443"))
	require.Error(t, err)
	require.Nil(t, history.LoadURLTestHistory("a"))
	require.NotNil(t, history.LoadURLTestHistory("b"))
	require.Equal(t, "b", inner.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "inner", outer.Selected(N.NetworkTCP).Tag())
}

func TestURLTestCrossGroupSharedOutboundFailure(t *testing.T) {
	a := newStubOutbound("a")
	a.dialFunc = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("a down")
	}
	b := newStubOutbound("b")
	manager := newStubOutboundManager(a, b)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	g1 := newStartedURLTest(t, ctx, "g1", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b"},
	})
	g2 := newStartedURLTest(t, ctx, "g2", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b"},
		Fallback:  &option.URLTestFallbackOptions{Enabled: true},
	})
	manager.Add(g1)
	manager.Add(g2)

	storeDelay(history, "a", 50)
	storeDelay(history, "b", 120)
	g1.PerformUpdateCheck()
	g2.PerformUpdateCheck()
	require.Equal(t, "a", g1.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "a", g2.Selected(N.NetworkTCP).Tag())

	_, err := g1.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("1.1.1.1:443"))
	require.Error(t, err)
	require.Equal(t, "b", g1.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "b", g2.Selected(N.NetworkTCP).Tag())
}

func TestFallbackOrderAndMaxDelaySemantics(t *testing.T) {
	a := newStubOutbound("a", N.NetworkTCP)
	b := newStubOutbound("b", N.NetworkTCP, N.NetworkUDP)
	c := newStubOutbound("c", N.NetworkTCP, N.NetworkUDP)
	manager := newStubOutboundManager(a, b, c)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	noLimit := newStartedURLTest(t, ctx, "fb-nolimit", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b", "c"},
		Fallback:  &option.URLTestFallbackOptions{Enabled: true},
	})
	require.True(t, noLimit.Fallback())
	require.Equal(t, uint16(50), noLimit.group.tolerance)
	require.Equal(t, time.Duration(0), noLimit.group.maxDelay)
	require.Equal(t, "a", noLimit.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "b", noLimit.Selected(N.NetworkUDP).Tag())

	storeDelay(history, "a", 350)
	storeDelay(history, "b", 80)
	storeDelay(history, "c", 40)
	noLimit.PerformUpdateCheck()
	require.Equal(t, "a", noLimit.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "b", noLimit.Selected(N.NetworkUDP).Tag())

	history.DeleteURLTestHistory("a")
	noLimit.ReportFailure("a")
	require.Equal(t, "b", noLimit.Selected(N.NetworkTCP).Tag())

	history.DeleteURLTestHistory("b")
	noLimit.ReportFailure("b")
	require.Equal(t, "c", noLimit.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "c", noLimit.Selected(N.NetworkUDP).Tag())

	storeDelay(history, "a", 300)
	noLimit.PerformUpdateCheck()
	require.Equal(t, "a", noLimit.Selected(N.NetworkTCP).Tag())
	require.Equal(t, "c", noLimit.Selected(N.NetworkUDP).Tag())

	withLimit := newStartedURLTest(t, ctx, "fb-limit", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b", "c"},
		Fallback: &option.URLTestFallbackOptions{
			Enabled:  true,
			MaxDelay: badoption.Duration(200 * time.Millisecond),
		},
	})
	storeDelay(history, "a", 300)
	storeDelay(history, "b", 150)
	storeDelay(history, "c", 50)
	withLimit.PerformUpdateCheck()
	require.Equal(t, "b", withLimit.Selected(N.NetworkTCP).Tag())

	storeDelay(history, "a", 300)
	storeDelay(history, "b", 250)
	storeDelay(history, "c", 400)
	withLimit.PerformUpdateCheck()
	require.Equal(t, "b", withLimit.Selected(N.NetworkTCP).Tag())

	storeDelay(history, "a", 300)
	storeDelay(history, "b", 300)
	storeDelay(history, "c", 350)
	withLimit.PerformUpdateCheck()
	require.Equal(t, "a", withLimit.Selected(N.NetworkTCP).Tag())

	invalidRaw, err := NewURLTest(ctx, nil, log.NewNOPFactory().Logger(), "fb-invalid", option.URLTestOutboundOptions{
		Outbounds: []string{"a"},
		Fallback: &option.URLTestFallbackOptions{
			Enabled:  true,
			MaxDelay: badoption.Duration(-100 * time.Millisecond),
		},
	})
	require.NoError(t, err)
	require.Error(t, invalidRaw.(*URLTest).Start(adapter.StartStateStart, nil))
}

func TestFallbackMidBatchAntiFlappingAndImmediateRecovery(t *testing.T) {
	a := newStubOutbound("a")
	b := newStubOutbound("b")
	c := newStubOutbound("c")
	manager := newStubOutboundManager(a, b, c)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "fb", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b", "c"},
		Fallback:  &option.URLTestFallbackOptions{Enabled: true},
	})
	u.group.selectedOutboundTCP.Store(a)
	u.group.selectedOutboundUDP.Store(a)

	bDone := make(chan struct{})
	releaseA := make(chan struct{})
	u.group.urlTestFunc = func(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
		ob := detour.(adapter.Outbound)
		switch ob.Tag() {
		case "a":
			<-releaseA
			return 120, nil
		case "b":
			defer close(bDone)
			return 40, nil
		default:
			<-releaseA
			return 60, nil
		}
	}

	batchDone := make(chan struct{})
	go func() {
		defer close(batchDone)
		_, _ = u.URLTest(ctx)
	}()

	<-bDone
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, "a", u.Selected(N.NetworkTCP).Tag())
	close(releaseA)
	<-batchDone
	require.Equal(t, "a", u.Selected(N.NetworkTCP).Tag())

	history.DeleteURLTestHistory("a")
	u.ReportFailure("a")
	require.Equal(t, "b", u.Selected(N.NetworkTCP).Tag())

	releaseSlowC := make(chan struct{})
	u.group.urlTestFunc = func(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
		ob := detour.(adapter.Outbound)
		switch ob.Tag() {
		case "a":
			return 80, nil
		case "b":
			return 50, nil
		default:
			<-releaseSlowC
			return 60, nil
		}
	}

	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		_, _ = u.URLTest(ctx)
	}()

	require.Eventually(t, func() bool {
		return u.Selected(N.NetworkTCP).Tag() == "a"
	}, time.Second, 10*time.Millisecond)

	close(releaseSlowC)
	<-recoveryDone
}

func TestFallbackFastRecoveryProbeLazyAndLifecycle(t *testing.T) {
	a := newStubOutbound("a")
	b := newStubOutbound("b")
	manager := newStubOutboundManager(a, b)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "fb", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b"},
		Fallback:  &option.URLTestFallbackOptions{Enabled: true},
	})
	u.group.fallbackProbeInterval = 20 * time.Millisecond
	u.group.fallbackProbeTimeout = 100 * time.Millisecond

	var aAvailable atomic.Bool
	var probeCountA atomic.Int32
	u.group.urlTestFunc = func(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
		ob := detour.(adapter.Outbound)
		if ob.Tag() == "a" {
			probeCountA.Add(1)
			if aAvailable.Load() {
				return 60, nil
			}
		}
		return 0, errors.New("unavailable")
	}

	storeDelay(history, "b", 100)
	u.PerformUpdateCheck()
	require.Equal(t, "b", u.Selected(N.NetworkTCP).Tag())

	time.Sleep(80 * time.Millisecond)
	require.Equal(t, int32(0), probeCountA.Load())

	u.group.Touch()
	require.Eventually(t, func() bool {
		return probeCountA.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	aAvailable.Store(true)
	require.Eventually(t, func() bool {
		return u.Selected(N.NetworkTCP).Tag() == "a"
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		u.group.access.Lock()
		stopped := u.group.fallbackTicker == nil
		u.group.access.Unlock()
		return stopped
	}, time.Second, 10*time.Millisecond)
}

func TestFallbackDoesNotFastProbeMaxDelayExceededNode(t *testing.T) {
	a := newStubOutbound("a")
	b := newStubOutbound("b")
	manager := newStubOutboundManager(a, b)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "fb", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b"},
		Fallback: &option.URLTestFallbackOptions{
			Enabled:  true,
			MaxDelay: badoption.Duration(200 * time.Millisecond),
		},
	})
	u.group.fallbackProbeInterval = 20 * time.Millisecond

	var probeCount atomic.Int32
	u.group.urlTestFunc = func(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
		probeCount.Add(1)
		return 300, nil
	}

	storeDelay(history, "a", 300)
	storeDelay(history, "b", 100)
	u.PerformUpdateCheck()
	u.group.Touch()
	require.Equal(t, "b", u.Selected(N.NetworkTCP).Tag())

	time.Sleep(80 * time.Millisecond)
	require.Equal(t, int32(0), probeCount.Load())
}

func TestFallbackAllUnavailableProbesAllAndDeduplicates(t *testing.T) {
	a := newStubOutbound("a")
	b := newStubOutbound("b")
	manager := newStubOutboundManager(a, b)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "fb", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b"},
		Fallback:  &option.URLTestFallbackOptions{Enabled: true},
	})
	u.group.fallbackProbeInterval = 15 * time.Millisecond
	u.group.fallbackProbeTimeout = 500 * time.Millisecond

	var (
		aInFlight    atomic.Int32
		aMaxInFlight atomic.Int32
		bProbes      atomic.Int32
		bAvailable   atomic.Bool
	)
	releaseA := make(chan struct{})
	u.group.urlTestFunc = func(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
		ob := detour.(adapter.Outbound)
		if ob.Tag() == "a" {
			cur := aInFlight.Add(1)
			for {
				prev := aMaxInFlight.Load()
				if cur <= prev || aMaxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			defer aInFlight.Add(-1)
			select {
			case <-releaseA:
			case <-ctx.Done():
			}
			return 0, errors.New("a still down")
		}
		bProbes.Add(1)
		if bAvailable.Load() {
			return 70, nil
		}
		return 0, errors.New("b down")
	}

	u.group.selectedOutboundTCP.Store(a)
	u.group.selectedOutboundUDP.Store(a)
	u.group.Touch()

	require.Eventually(t, func() bool {
		return bProbes.Load() >= 3 && aInFlight.Load() == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), aMaxInFlight.Load())

	bAvailable.Store(true)
	require.Eventually(t, func() bool {
		return u.Selected(N.NetworkTCP).Tag() == "b"
	}, time.Second, 10*time.Millisecond)

	close(releaseA)
}

func TestFallbackPersistentFailureAndFlapBackoff(t *testing.T) {
	a := newStubOutbound("a")
	b := newStubOutbound("b")
	manager := newStubOutboundManager(a, b)
	history := urltest.NewHistoryStorage()
	ctx := newTestContext(t, manager, history)

	u := newStartedURLTest(t, ctx, "fb", option.URLTestOutboundOptions{
		Outbounds: []string{"a", "b"},
		Fallback:  &option.URLTestFallbackOptions{Enabled: true},
	})
	u.group.fallbackProbeInterval = 10 * time.Millisecond
	u.group.fallbackFailureBurstLimit = 5
	u.group.fallbackFailureMaxBackoff = 160 * time.Millisecond
	u.group.fallbackFlapWindow = 200 * time.Millisecond
	u.group.fallbackFlapMaxBackoff = 80 * time.Millisecond

	for range 5 {
		u.group.recordProbeFailure("a")
		u.group.probeAccess.Lock()
		next := u.group.fallbackStates["a"].nextProbe
		u.group.probeAccess.Unlock()
		require.LessOrEqual(t, time.Until(next), 15*time.Millisecond)
	}

	u.group.recordProbeFailure("a")
	u.group.probeAccess.Lock()
	afterBurst := time.Until(u.group.fallbackStates["a"].nextProbe)
	u.group.probeAccess.Unlock()
	require.Greater(t, afterBurst, 10*time.Millisecond)

	for range 6 {
		u.group.recordProbeFailure("a")
	}
	u.group.probeAccess.Lock()
	capped := time.Until(u.group.fallbackStates["a"].nextProbe)
	u.group.probeAccess.Unlock()
	require.LessOrEqual(t, capped, 170*time.Millisecond)
	require.Greater(t, capped, 80*time.Millisecond)

	u.group.recordProbeRecovery("a")
	u.group.recordFlapFailure("a")
	u.group.probeAccess.Lock()
	flap1 := u.group.fallbackStates["a"].flapBackoff
	u.group.probeAccess.Unlock()
	require.Equal(t, 20*time.Millisecond, flap1)

	u.group.recordFlapFailure("a")
	u.group.probeAccess.Lock()
	flap2 := u.group.fallbackStates["a"].flapBackoff
	u.group.probeAccess.Unlock()
	require.Equal(t, 40*time.Millisecond, flap2)

	u.group.resetFallbackProbeStates()
	u.group.probeAccess.Lock()
	require.Empty(t, u.group.fallbackStates)
	u.group.probeAccess.Unlock()
}
