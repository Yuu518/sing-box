package urltest

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type testDialer func(context.Context) (net.Conn, error)

func (d testDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d(ctx)
}

func (d testDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

func TestURLTestDelay(t *testing.T) {
	for _, test := range []struct {
		name        string
		closeHeader bool
		responses   []time.Duration
		delay       uint16
	}{
		{"keep-alive", false, []time.Duration{300 * time.Millisecond, 25 * time.Millisecond}, 25},
		{"connection close", true, []time.Duration{25 * time.Millisecond}, 25},
		{"silent close", false, []time.Duration{25 * time.Millisecond}, 25},
		{"sub-millisecond", false, []time.Duration{time.Millisecond, time.Microsecond}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, server := net.Pipe()
				defer client.Close()
				go func() {
					defer server.Close()
					reader := bufio.NewReader(server)
					for _, wait := range test.responses {
						request, err := http.ReadRequest(reader)
						if err != nil {
							t.Error(err)
							return
						}
						if request.Method != http.MethodHead {
							t.Errorf("unexpected method %s", request.Method)
						}
						time.Sleep(wait)
						response := "HTTP/1.1 204 No Content\r\n"
						if test.closeHeader {
							response += "Connection: close\r\n"
						}
						if _, err = io.WriteString(server, response+"\r\n"); err != nil {
							t.Error(err)
							return
						}
					}
				}()
				dials := 0
				dialer := testDialer(func(ctx context.Context) (net.Conn, error) {
					dials++
					time.Sleep(time.Second)
					return client, nil
				})
				delay, err := URLTest(t.Context(), "http://test.invalid/", dialer)
				require.NoError(t, err)
				require.Equal(t, test.delay, delay)
				require.Equal(t, 1, dials)
			})
		})
	}
}

type testCertificateStore struct {
	adapter.CertificateStore
	roots *x509.CertPool
}

func (s testCertificateStore) Pool() *x509.CertPool {
	return s.roots
}

func TestURLTestTLSWithoutKeepAlive(t *testing.T) {
	const handshakeDelay = 100 * time.Millisecond
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			time.Sleep(handshakeDelay)
			return nil, nil
		},
	}
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	ctx := service.ContextWith[adapter.CertificateStore](t.Context(), testCertificateStore{roots: roots})
	start := time.Now()
	delay, err := URLTest(ctx, server.URL, N.SystemDialer)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start)-time.Duration(delay)*time.Millisecond, handshakeDelay-time.Millisecond)
}

func TestURLTestDialDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		dialer := testDialer(func(ctx context.Context) (net.Conn, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) != time.Second {
				t.Error("dial did not inherit the request deadline")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		})
		_, err := URLTest(ctx, "http://test.invalid/", dialer)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestURLTestCancelDuringMeasurement(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 2 {
			cancel()
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	delay, err := URLTest(ctx, server.URL, N.SystemDialer)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, delay)
}
