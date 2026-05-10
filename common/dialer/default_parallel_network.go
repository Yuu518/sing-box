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

func DialSerialNetwork(ctx context.Context, dialer N.Dialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if parallelDialer, isParallel := dialer.(ParallelNetworkDialer); isParallel {
		return parallelDialer.DialParallelNetwork(ctx, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	var errors []error
	if parallelDialer, isParallel := dialer.(ParallelInterfaceDialer); isParallel {
		for _, address := range destinationAddresses {
			conn, err := parallelDialer.DialParallelInterface(ctx, network, M.SocksaddrFrom(address, destination.Port), strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
			if err == nil {
				return conn, nil
			}
			errors = append(errors, err)
		}
	} else {
		for _, address := range destinationAddresses {
			conn, err := dialer.DialContext(ctx, network, M.SocksaddrFrom(address, destination.Port))
			if err == nil {
				return conn, nil
			}
			errors = append(errors, err)
		}
	}
	return nil, E.Errors(errors...)
}

func DialParallelNetwork(ctx context.Context, dialer ParallelInterfaceDialer, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, preferIPv6 bool, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	return N.DialParallel(ctx, &networkStrategyDialer{
		ParallelInterfaceDialer: dialer,
		strategy:                strategy,
		interfaceType:           interfaceType,
		fallbackInterfaceType:   fallbackInterfaceType,
		fallbackDelay:           fallbackDelay,
	}, network, destination, destinationAddresses, preferIPv6, fallbackDelay)
}

type networkStrategyDialer struct {
	ParallelInterfaceDialer
	strategy              *C.NetworkStrategy
	interfaceType         []C.InterfaceType
	fallbackInterfaceType []C.InterfaceType
	fallbackDelay         time.Duration
}

func (d *networkStrategyDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d.DialParallelInterface(ctx, network, destination, d.strategy, d.interfaceType, d.fallbackInterfaceType, d.fallbackDelay)
}

func DialConcurrentNetwork(ctx context.Context, dialer N.Dialer, logger logger.ContextLogger, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	if N.NetworkName(network) != N.NetworkTCP {
		return DialSerialNetwork(ctx, dialer, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if len(destinationAddresses) == 1 {
		return DialSerialNetwork(ctx, dialer, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
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
		destinationAddress := M.SocksaddrFrom(address, destination.Port)
		if parallelDialer, isParallel := dialer.(ParallelInterfaceDialer); isParallel {
			conn, err = parallelDialer.DialParallelInterface(dialCtx, network, destinationAddress, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
		} else {
			conn, err = dialer.DialContext(dialCtx, network, destinationAddress)
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

func ListenSerialNetworkPacket(ctx context.Context, dialer N.Dialer, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.PacketConn, netip.Addr, error) {
	if len(destinationAddresses) == 0 {
		if !destination.IsIP() {
			panic("invalid usage")
		}
		destinationAddresses = []netip.Addr{destination.Addr}
	}
	if parallelDialer, isParallel := dialer.(ParallelNetworkDialer); isParallel {
		return parallelDialer.ListenSerialNetworkPacket(ctx, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	var errors []error
	if parallelDialer, isParallel := dialer.(ParallelInterfaceDialer); isParallel {
		for _, address := range destinationAddresses {
			conn, err := parallelDialer.ListenSerialInterfacePacket(ctx, M.SocksaddrFrom(address, destination.Port), strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
			if err == nil {
				return conn, address, nil
			}
			errors = append(errors, err)
		}
	} else {
		for _, address := range destinationAddresses {
			conn, err := dialer.ListenPacket(ctx, M.SocksaddrFrom(address, destination.Port))
			if err == nil {
				return conn, address, nil
			}
			errors = append(errors, err)
		}
	}
	return nil, netip.Addr{}, E.Errors(errors...)
}
