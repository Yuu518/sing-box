package dialer

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func DialConcurrentNetwork(ctx context.Context, dialer N.Dialer, logger logger.ContextLogger, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if N.NetworkName(network) != N.NetworkTCP || len(destinationAddresses) == 1 {
		if parallelDialer, isParallel := dialer.(ParallelNetworkDialer); isParallel {
			return parallelDialer.DialParallelNetwork(ctx, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
		}
		return N.DialSerial(ctx, dialer, network, destination, destinationAddresses)
	}
	if concurrentDialer, isConcurrent := dialer.(ConcurrentNetworkDialer); isConcurrent {
		return concurrentDialer.DialConcurrentNetwork(ctx, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}

	if logger != nil {
		logger.DebugContext(ctx, "concurrent dial ", network, " ", destination, " with [", strings.Join(common.Map(destinationAddresses, netip.Addr.String), " "), "]")
	}
	type dialResult struct {
		net.Conn
		error
		address netip.Addr
	}
	dialCtx, dialCancel := context.WithCancel(ctx)
	defer dialCancel()
	returned := make(chan struct{})
	defer close(returned)
	results := make(chan dialResult)
	startRacer := func(address netip.Addr) {
		var (
			conn net.Conn
			err  error
		)
		if parallelDialer, isParallel := dialer.(ParallelNetworkDialer); isParallel {
			conn, err = parallelDialer.DialParallelNetwork(dialCtx, network, destination, []netip.Addr{address}, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
		} else {
			conn, err = dialer.DialContext(dialCtx, network, M.SocksaddrFrom(address, destination.Port))
		}
		select {
		case results <- dialResult{Conn: conn, error: err, address: address}:
		case <-returned:
			if conn != nil {
				conn.Close()
			}
		}
	}
	for _, address := range destinationAddresses {
		go startRacer(address)
	}
	var errors []error
	for range destinationAddresses {
		result := <-results
		if result.error == nil {
			if logger != nil {
				logger.InfoContext(ctx, "concurrent dial ", network, " ", destination, " connected ", M.SocksaddrFrom(result.address, destination.Port))
			}
			dialCancel()
			return result.Conn, nil
		}
		errors = append(errors, result.error)
	}
	return nil, E.Errors(errors...)
}
