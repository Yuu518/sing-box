package group

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	U "github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

type measuredProviderOutbound struct {
	providerUpdateTestOutbound
	calls atomic.Int32
}

type directProviderTestOutbound struct {
	providerUpdateTestOutbound
	net.Dialer
}

func (o *directProviderTestOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return o.Dialer.DialContext(ctx, network, destination.String())
}

func TestProviderReplacementRetestedAfterOldCheckCompletes(t *testing.T) {
	requestStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	defer releaseRequest()
	previous := &directProviderTestOutbound{providerUpdateTestOutbound: providerUpdateTestOutbound{tag: "node"}}
	replacement := &measuredProviderOutbound{providerUpdateTestOutbound: providerUpdateTestOutbound{tag: "node"}}
	history := U.NewHistoryStorage()
	manager := &providerUpdateTestOutboundManager{outbounds: map[string]adapter.Outbound{"node": replacement}}
	g := &URLTestGroup{ctx: context.Background(), outbound: manager, history: history, logger: log.NewNOPFactory().NewLogger("test"), link: server.URL, interval: time.Hour, idleTimeout: time.Hour, interruptGroup: interrupt.NewGroup()}
	g.storeOutbounds([]adapter.Outbound{previous})
	g.selectedOutboundTCP.Store(previous)
	g.started.Store(true)
	g.lastActive.Store(time.Now())
	oldCheckDone := make(chan struct{})
	go func() { _, _ = g.URLTest(context.Background()); close(oldCheckDone) }()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("old URL test did not start")
	}
	u := &URLTest{ctx: context.Background(), outbound: manager, group: g, providers: map[string]adapter.Provider{"p": &providerUpdateTestProvider{tag: "p", outbounds: []adapter.Outbound{replacement}}}, providerTags: []string{"p"}, outboundsCache: make(map[string][]adapter.Outbound)}
	require.NoError(t, u.onProviderUpdated("p"))
	releaseRequest()
	select {
	case <-oldCheckDone:
	case <-time.After(time.Second):
		t.Fatal("old URL test did not finish")
	}
	require.Eventually(t, func() bool {
		u.providerUpdateCheck.access.Lock()
		defer u.providerUpdateCheck.access.Unlock()
		return !u.providerUpdateCheck.running
	}, time.Second, time.Millisecond)
	require.Positive(t, replacement.calls.Load())
	require.Nil(t, history.LoadURLTestHistory("node"))
}

func (o *measuredProviderOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.calls.Add(1)
	return nil, errors.New("replacement server is unavailable")
}

func TestProviderReplacementIsRetested(t *testing.T) {
	previous := &providerUpdateTestOutbound{tag: "node"}
	replacement := &measuredProviderOutbound{providerUpdateTestOutbound: providerUpdateTestOutbound{tag: "node"}}
	history := U.NewHistoryStorage()
	history.StoreURLTestHistory("node", &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
	manager := &providerUpdateTestOutboundManager{outbounds: map[string]adapter.Outbound{"node": replacement}}
	g := &URLTestGroup{ctx: context.Background(), outbound: manager, history: history, logger: log.NewNOPFactory().NewLogger("test"), interval: time.Hour, idleTimeout: time.Hour, interruptGroup: interrupt.NewGroup()}
	g.storeOutbounds([]adapter.Outbound{previous})
	g.selectedOutboundTCP.Store(previous)
	g.started.Store(true)
	g.lastActive.Store(time.Now())
	u := &URLTest{ctx: context.Background(), outbound: manager, group: g, providers: map[string]adapter.Provider{"p": &providerUpdateTestProvider{tag: "p", outbounds: []adapter.Outbound{replacement}}}, providerTags: []string{"p"}, outboundsCache: make(map[string][]adapter.Outbound)}
	require.NoError(t, u.onProviderUpdated("p"))
	require.Eventually(t, func() bool {
		u.providerUpdateCheck.access.Lock()
		defer u.providerUpdateCheck.access.Unlock()
		return !u.providerUpdateCheck.running
	}, time.Second, time.Millisecond)
	require.Positive(t, replacement.calls.Load(), "new server must not reuse old server's tag history")
	require.Nil(t, history.LoadURLTestHistory("node"))
}
