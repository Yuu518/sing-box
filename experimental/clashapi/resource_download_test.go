package clashapi

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type resourceDownloadOutbound struct {
	outbound.Adapter
	marked chan bool
}

func (o *resourceDownloadOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	o.marked <- interrupt.IsResourceDownloadFromContext(ctx)
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func (*resourceDownloadOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

type resourceDownloadManager struct {
	adapter.OutboundManager
	outbound adapter.Outbound
}

func (m *resourceDownloadManager) Default() adapter.Outbound { return m.outbound }

func (m *resourceDownloadManager) Outbound(tag string) (adapter.Outbound, bool) {
	return m.outbound, tag == m.outbound.Tag()
}

func TestExternalUIResourceDownloadContext(t *testing.T) {
	for _, detour := range []string{"", "download"} {
		for _, cancellation := range []string{"cancel", "deadline"} {
			t.Run(detour+"/"+cancellation, func(t *testing.T) {
				requested := make(chan struct{}, 1)
				httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requested <- struct{}{}
					<-r.Context().Done()
				}))
				defer httpServer.Close()
				timeout := 5 * time.Second
				expectedError := context.Canceled
				if cancellation == "deadline" {
					timeout = time.Second
					expectedError = context.DeadlineExceeded
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				if cancellation == "cancel" {
					go func() {
						select {
						case <-requested:
							cancel()
						case <-ctx.Done():
						}
					}()
				}
				downloadOutbound := &resourceDownloadOutbound{
					Adapter: outbound.NewAdapter("test", "download", []string{N.NetworkTCP}, nil),
					marked:  make(chan bool, 1),
				}
				server := &Server{
					ctx:                      ctx,
					logger:                   log.NewNOPFactory().NewLogger("test"),
					outbound:                 &resourceDownloadManager{outbound: downloadOutbound},
					externalUIDownloadURL:    httpServer.URL,
					externalUIDownloadDetour: detour,
				}
				require.ErrorIs(t, server.downloadExternalUI(), expectedError)
				select {
				case marked := <-downloadOutbound.marked:
					require.True(t, marked)
				default:
					t.Fatal("external UI download did not dial the outbound")
				}
			})
		}
	}
}
