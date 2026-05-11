package httpclient

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/coder/websocket"
)

type fakeResponseBody struct {
	bytes.Buffer
	closed bool
}

func (b *fakeResponseBody) Close() error {
	b.closed = true
	return nil
}

type readOnlyResponseBody struct {
	reader io.Reader
	closed bool
}

func (b *readOnlyResponseBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *readOnlyResponseBody) Close() error {
	b.closed = true
	return nil
}

type fakeInnerTransport struct {
	body   io.ReadCloser
	closed bool
}

func (t *fakeInnerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusSwitchingProtocols, Body: t.body}, nil
}

func (t *fakeInnerTransport) CloseIdleConnections() {}

func (t *fakeInnerTransport) Close() error {
	t.closed = true
	return nil
}

func newTestManagedTransport(body io.ReadCloser) (*ManagedTransport, *fakeInnerTransport) {
	inner := &fakeInnerTransport{body: body}
	transport := &ManagedTransport{
		factory: func() (innerTransport, error) {
			return inner, nil
		},
	}
	transport.epoch.Store(&transportEpoch{transport: inner})
	return transport, inner
}

func TestManagedTransportKeepsWritableBody(t *testing.T) {
	body := &fakeResponseBody{}
	transport, _ := newTestManagedTransport(body)

	request, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	writableBody, isWritable := response.Body.(io.ReadWriteCloser)
	if !isWritable {
		t.Fatalf("response body is not a io.ReadWriteCloser: %T", response.Body)
	}
	if _, err = writableBody.Write([]byte("upgraded")); err != nil {
		t.Fatal(err)
	}
	if body.String() != "upgraded" {
		t.Fatalf("write did not reach the underlying body, got %q", body.String())
	}
	if err = writableBody.Close(); err != nil {
		t.Fatal(err)
	}
	if !body.closed {
		t.Fatal("close did not reach the underlying body")
	}
}

type testDialer struct {
	dialer net.Dialer
}

func (d *testDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, destination.String())
}

func (d *testDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket(N.NetworkUDP, ":0")
}

func TestManagedTransportWebSocketUpgrade(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		messageType, content, err := conn.Read(request.Context())
		if err != nil {
			return
		}
		_ = conn.Write(request.Context(), messageType, content)
	}))
	defer server.Close()

	transport := &ManagedTransport{
		factory: func() (innerTransport, error) {
			return newHTTP2FallbackTransport(&testDialer{}, nil, option.HTTP2Options{})
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, server.URL, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if err = conn.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	_, content, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "ping" {
		t.Fatalf("unexpected echo %q", content)
	}
}

func TestManagedTransportReleasesEpochOnBodyClose(t *testing.T) {
	body := &readOnlyResponseBody{reader: bytes.NewReader([]byte("content"))}
	transport, inner := newTestManagedTransport(body)

	request, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, isWritable := response.Body.(io.Writer); isWritable {
		t.Fatal("read only body must not be reported as writable")
	}
	content, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "content" {
		t.Fatalf("unexpected body content %q", content)
	}

	transport.CloseIdleConnections()
	if inner.closed {
		t.Fatal("transport closed while the response body is still open")
	}
	if err = response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if !inner.closed {
		t.Fatal("transport not closed after the response body was released")
	}
}
