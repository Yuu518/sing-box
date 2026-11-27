package provider

import (
	"context"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func (a *Adapter) startHealthCheck() {
	a.healthCheckAccess.Lock()
	defer a.healthCheckAccess.Unlock()
	if a.ticker != nil {
		return
	}
	a.ticker = time.NewTicker(a.interval)
	if a.pause != nil {
		a.pauseCallback = pause.RegisterTicker(a.pause, a.ticker, a.interval, nil)
	}
	a.done = make(chan struct{})
	go a.loopCheck(a.ticker, a.done)
}

func (a *Adapter) loopCheck(ticker *time.Ticker, done chan struct{}) {
	a.healthcheck(a.ctx, a.link, a.timeout)
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			a.healthcheck(a.ctx, a.link, a.timeout)
		}
	}
}

func (a *Adapter) HealthCheck(ctx context.Context) (map[string]uint16, error) {
	return a.HealthCheckWithOptions(ctx, "", 0)
}

func (a *Adapter) HealthCheckWithOptions(ctx context.Context, link string, timeout time.Duration) (map[string]uint16, error) {
	a.healthCheckAccess.Lock()
	if a.ticker != nil {
		select {
		case <-a.done:
		default:
			a.ticker.Reset(a.interval)
		}
	}
	a.healthCheckAccess.Unlock()
	if link == "" {
		link = a.link
	}
	if timeout <= 0 {
		timeout = a.timeout
	}
	return a.healthcheck(ctx, link, timeout)
}

func (a *Adapter) healthcheck(ctx context.Context, link string, timeout time.Duration) (map[string]uint16, error) {
	result := make(map[string]uint16)
	if a.history == nil {
		return result, E.New("missing URL test history storage")
	}
	if a.checking.Swap(true) {
		return result, nil
	}
	defer a.checking.Store(false)
	outbounds := a.Outbounds()
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	var (
		resultAccess sync.Mutex
		storedMap    = make(map[adapter.Outbound]*adapter.URLTestHistory, len(outbounds))
		deleted      []adapter.Outbound
	)
	for _, detour := range outbounds {
		tag := detour.Tag()
		b.Go(tag, func() (any, error) {
			testCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			t, err := urltest.URLTest(testCtx, link, detour)
			resultAccess.Lock()
			if err != nil {
				a.logger.Debug("outbound ", tag, " unavailable: ", err)
				deleted = append(deleted, detour)
			} else {
				a.logger.Debug("outbound ", tag, " available: ", t, "ms")
				storedMap[detour] = &adapter.URLTestHistory{
					Time:  time.Now(),
					Delay: t,
				}
			}
			resultAccess.Unlock()
			return nil, nil
		})
	}
	b.Wait()
	storedOutbounds := a.history.UpdateURLTestHistoryForOutbounds(storedMap, deleted)
	for detour, history := range storedMap {
		if storedOutbounds[detour] {
			result[detour.Tag()] = history.Delay
		}
	}
	a.history.NotifyUpdated()
	if outboundManager := service.FromContext[adapter.OutboundManager](a.ctx); outboundManager != nil {
		for _, ob := range outboundManager.Outbounds() {
			if urlTestGroup, isURLTestGroup := ob.(adapter.URLTestGroup); isURLTestGroup {
				urlTestGroup.PerformUpdateCheck()
			}
		}
	}
	return result, nil
}
