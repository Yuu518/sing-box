package clashapi

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type delayTestOutbound struct {
	outbound.Adapter
	N.Dialer
	started chan struct{}
	release chan struct{}
}

func (o *delayTestOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	close(o.started)
	select {
	case <-o.release:
	case <-ctx.Done():
	}
	return nil, errors.New("old node failed")
}

type delayTestManager struct {
	adapter.OutboundManager
	member adapter.Outbound
}

func (m *delayTestManager) Outbound(string) (adapter.Outbound, bool) { return m.member, true }
func (m *delayTestManager) Outbounds() []adapter.Outbound            { return nil }

func TestDelayRequestCannotDeleteReplacementHistory(t *testing.T) {
	previous := &delayTestOutbound{Adapter: outbound.NewAdapter("test", "node", []string{N.NetworkTCP}, nil), started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(previous.release) }) }
	defer release()
	history := urltest.NewHistoryStorage()
	history.SetCurrentOutbound(previous)
	server := &Server{outbound: &delayTestManager{member: previous}, urlTestHistory: history}
	request := httptest.NewRequest("GET", "/proxies/node/delay?timeout=5000", nil)
	request = request.WithContext(context.WithValue(request.Context(), CtxKeyProxy, previous))
	done := make(chan struct{})
	go func() { getProxyDelay(server)(httptest.NewRecorder(), request); close(done) }()
	select {
	case <-previous.started:
	case <-time.After(time.Second):
		t.Fatal("delay request did not start")
	}
	replacement := &delayTestOutbound{Adapter: outbound.NewAdapter("test", "node", []string{N.NetworkTCP}, nil)}
	history.SetCurrentOutbound(replacement)
	current := &adapter.URLTestHistory{Time: time.Now(), Delay: 15}
	history.StoreURLTestHistoryForOutbound(replacement, current)
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delay request did not finish")
	}
	require.Same(t, current, history.LoadURLTestHistory("node"))
}
