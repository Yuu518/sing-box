package provider

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	U "github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type healthTestOutbound struct {
	outbound.Adapter
	N.Dialer
	server string
}

func (o *healthTestOutbound) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, o.server)
}

func (o *healthTestOutbound) Close() error { return nil }

func TestOldProviderHealthCheckCannotRestoreReplacementHistory(t *testing.T) {
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
	logger := log.NewNOPFactory()
	registry := outbound.NewRegistry()
	outbound.Register[option.HTTPOutboundOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options option.HTTPOutboundOptions) (adapter.Outbound, error) {
		return &healthTestOutbound{Adapter: outbound.NewAdapter("test", tag, []string{N.NetworkTCP}, nil), server: fmt.Sprintf("%s:%d", options.Server, options.ServerPort)}, nil
	})
	endpoints := endpoint.NewManager(logger.NewLogger("test"), endpoint.NewRegistry())
	manager := outbound.NewManager(logger.NewLogger("test"), registry, endpoints, "")
	p := newOwnershipTestProvider("p", manager, endpoints)
	defer p.Close()
	p.history = U.NewHistoryStorage()
	p.link = server.URL
	p.timeout = 5 * time.Second
	address := server.Listener.Addr().(*net.TCPAddr)
	old := []option.Outbound{{Type: "test", Tag: "node", Options: &option.HTTPOutboundOptions{ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: uint16(address.Port)}}}}
	p.UpdateOutbounds(nil, old)
	done := make(chan struct{})
	go func() { _, _ = p.HealthCheck(context.Background()); close(done) }()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("provider check did not start")
	}
	next := []option.Outbound{{Type: "test", Tag: "node", Options: &option.HTTPOutboundOptions{ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: 1}}}}
	p.UpdateOutbounds(old, next)
	group.URLTestOutbounds(context.Background(), manager, p.history, logger.NewLogger("test"), p.Outbounds(), server.URL, time.Hour, true)
	releaseRequest()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("provider check did not finish")
	}
	require.Nil(t, p.history.LoadURLTestHistory("node"))
}
