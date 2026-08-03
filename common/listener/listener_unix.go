package listener

import (
	"net"
	"os"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
)

// listenUnix listens on a unix stream socket instead of a TCP port.
//
// Address-family specific options (bind_interface, routing_mark, reuse_addr,
// tcp_fast_open, tcp_multi_path, TCP keep-alive and TProxy) are meaningless on
// AF_UNIX and are not applied.
func (l *Listener) listenUnix() (net.Listener, error) {
	err := removeStaleUnixSocket(l.listenOptions.ListenUnix)
	if err != nil {
		return nil, err
	}
	var listenConfig net.ListenConfig
	unixListener, err := ListenNetworkNamespace[net.Listener](l.ctx, l.listenOptions.NetNs, func() (net.Listener, error) {
		return listenConfig.Listen(l.ctx, "unix", l.listenOptions.ListenUnix)
	})
	if err != nil {
		return nil, err
	}
	l.logger.Info("unix server started at ", unixListener.Addr())
	l.tcpListener = unixListener
	return unixListener, nil
}

// removeStaleUnixSocket removes the socket file left behind by a previous
// unclean shutdown, so that listening on the same path succeeds again.
func removeStaleUnixSocket(path string) error {
	if strings.HasPrefix(path, "@") {
		// Linux abstract socket, not backed by a file.
		return nil
	}
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if stat.Mode()&os.ModeSocket == 0 {
		return E.New("listen_unix path exists and is not a socket: ", path)
	}
	return os.Remove(path)
}
