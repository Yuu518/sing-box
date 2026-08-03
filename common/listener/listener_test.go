package listener

import (
	"context"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"
)

func TestListenOptionsUnixJSON(t *testing.T) {
	var options option.ListenOptions
	err := json.Unmarshal([]byte(`{"listen_port":1080,"listen_unix":"/tmp/sing-box.sock"}`), &options)
	if err != nil {
		t.Fatal(err)
	}
	if options.ListenPort != 1080 || options.ListenUnix != "/tmp/sing-box.sock" {
		t.Fatalf("unexpected listen options: %+v", options)
	}
}

func TestUnixListenerAlongsideTCP(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UNIX sockets are unavailable on Windows")
	}
	socketFile, err := os.CreateTemp("/tmp", "sing-box-listener-")
	if err != nil {
		t.Fatal(err)
	}
	socketPath := socketFile.Name()
	_ = socketFile.Close()
	_ = os.Remove(socketPath)
	t.Cleanup(func() { _ = os.Remove(socketPath) })
	accepted := make(chan string, 2)
	l := New(Options{
		Context: context.Background(),
		Logger:  log.NewNOPFactory().Logger(),
		Network: []string{N.NetworkTCP},
		Listen: option.ListenOptions{
			Listen:     common.Ptr(badoption.Addr(netip.AddrFrom4([4]byte{127, 0, 0, 1}))),
			ListenUnix: socketPath,
		},
		ConnectionHandler: connectionHandlerFunc(func(_ context.Context, conn net.Conn, _ adapter.InboundContext, _ N.CloseHandlerFunc) {
			accepted <- conn.LocalAddr().Network()
			_ = conn.Close()
		}),
	})
	if err = l.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if l.TCPListener() == nil || l.UnixListener() == nil {
		t.Fatal("expected both TCP and UNIX listeners")
	}
	tcpConn, err := net.Dial("tcp", l.TCPListener().Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = tcpConn.Close()
	unixConn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = unixConn.Close()

	want := map[string]bool{"tcp": true, "unix": true}
	for range 2 {
		select {
		case network := <-accepted:
			delete(want, network)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for accepted connection")
		}
	}
	if len(want) != 0 {
		t.Fatalf("listeners did not accept all connections: %v", want)
	}
}

func TestUnixListenerRejectedForUDPOnlyInbound(t *testing.T) {
	l := New(Options{
		Context: context.Background(),
		Logger:  log.NewNOPFactory().Logger(),
		Network: []string{N.NetworkUDP},
		Listen: option.ListenOptions{
			ListenUnix: filepath.Join(t.TempDir(), "inbound.sock"),
		},
	})
	if err := l.Start(); err == nil {
		_ = l.Close()
		t.Fatal("expected UNIX listener on UDP-only inbound to fail")
	}
}

type connectionHandlerFunc func(context.Context, net.Conn, adapter.InboundContext, N.CloseHandlerFunc)

func (f connectionHandlerFunc) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	f(ctx, conn, metadata, onClose)
}
