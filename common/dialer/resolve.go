package dialer

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

var (
	_ ResolveDialer               = (*resolveDialer)(nil)
	_ ResolveDialer               = (*resolveParallelNetworkDialer)(nil)
	_ ParallelNetworkDialer       = (*resolveParallelNetworkDialer)(nil)
	_ PacketDialerWithDestination = (*resolveParallelNetworkDialer)(nil)
)

type ResolveDialer interface {
	N.Dialer
	QueryOptions() adapter.DNSQueryOptions
}

type resolveDialer struct {
	transport     adapter.DNSTransportManager
	router        adapter.DNSRouter
	dialer        N.Dialer
	logger        log.ContextLogger
	parallel      bool
	concurrent    bool
	server        string
	initOnce      sync.Once
	initErr       error
	queryOptions  adapter.DNSQueryOptions
	fallbackDelay time.Duration
}

func NewResolveDialer(ctx context.Context, dialer N.Dialer, parallel bool, server string, queryOptions adapter.DNSQueryOptions, fallbackDelay time.Duration) ResolveDialer {
	var concurrent bool
	networkManager := service.FromContext[adapter.NetworkManager](ctx)
	if networkManager != nil {
		concurrent = networkManager.DefaultOptions().ConcurrentDial
	}
	var dialerLogger log.ContextLogger
	logFactory := service.FromContext[log.Factory](ctx)
	if logFactory != nil {
		dialerLogger = logFactory.NewLogger("dialer")
	}
	if parallelDialer, isParallel := dialer.(ParallelNetworkDialer); isParallel {
		return &resolveParallelNetworkDialer{
			resolveDialer{
				transport:     service.FromContext[adapter.DNSTransportManager](ctx),
				router:        service.FromContext[adapter.DNSRouter](ctx),
				dialer:        dialer,
				logger:        dialerLogger,
				parallel:      parallel,
				concurrent:    concurrent,
				server:        server,
				queryOptions:  queryOptions,
				fallbackDelay: fallbackDelay,
			},
			parallelDialer,
		}
	}
	return &resolveDialer{
		transport:     service.FromContext[adapter.DNSTransportManager](ctx),
		router:        service.FromContext[adapter.DNSRouter](ctx),
		dialer:        dialer,
		logger:        dialerLogger,
		parallel:      parallel,
		concurrent:    concurrent,
		server:        server,
		queryOptions:  queryOptions,
		fallbackDelay: fallbackDelay,
	}
}

type resolveParallelNetworkDialer struct {
	resolveDialer
	dialer ParallelNetworkDialer
}

func (d *resolveDialer) initialize() error {
	d.initOnce.Do(d.initServer)
	return d.initErr
}

func (d *resolveDialer) initServer() {
	if d.server == "" {
		return
	}
	transport, loaded := d.transport.Transport(d.server)
	if !loaded {
		d.initErr = E.New("domain resolver not found: " + d.server)
		return
	}
	d.queryOptions.Transport = transport
}

func (d *resolveDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	err := d.initialize()
	if err != nil {
		return nil, err
	}
	if !destination.IsDomain() {
		return d.dialer.DialContext(ctx, network, destination)
	}
	lookupCtx := log.ContextWithOverrideLevel(ctx, log.LevelDebug)
	addresses, err := d.router.Lookup(lookupCtx, destination.Fqdn, d.queryOptions)
	if err != nil {
		return nil, err
	}
	if d.concurrent && N.NetworkName(network) == N.NetworkTCP {
		return d.dialConcurrentNetwork(ctx, network, destination, addresses, nil, nil, nil, d.fallbackDelay)
	}
	if d.parallel {
		return N.DialParallel(ctx, d.dialer, network, destination, addresses, addresses[0].Is6(), d.fallbackDelay)
	} else {
		return N.DialSerial(ctx, d.dialer, network, destination, addresses)
	}
}

func (d *resolveDialer) dialConcurrentNetwork(ctx context.Context, network string, destination M.Socksaddr, addresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	var domainStrategy C.DomainStrategy
	if d.router != nil {
		domainStrategy = d.router.ResolveLookupStrategy(d.queryOptions)
	}
	if domainStrategy != C.DomainStrategyPreferIPv4 && domainStrategy != C.DomainStrategyPreferIPv6 {
		return DialConcurrentNetwork(ctx, d.dialer, d.logger, network, destination, addresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	var primaryAddresses, fallbackAddresses []netip.Addr
	for _, address := range addresses {
		isIPv6 := address.Is6() && !address.Is4In6()
		if isIPv6 == (domainStrategy == C.DomainStrategyPreferIPv6) {
			primaryAddresses = append(primaryAddresses, address)
		} else {
			fallbackAddresses = append(fallbackAddresses, address)
		}
	}
	if len(primaryAddresses) == 0 || len(fallbackAddresses) == 0 {
		return DialConcurrentNetwork(ctx, d.dialer, d.logger, network, destination, addresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	conn, err := DialConcurrentNetwork(ctx, d.dialer, d.logger, network, destination, primaryAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	if err == nil {
		return conn, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	conn, fallbackErr := DialConcurrentNetwork(ctx, d.dialer, d.logger, network, destination, fallbackAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	if fallbackErr != nil {
		return nil, E.Errors(err, fallbackErr)
	}
	return conn, nil
}

func (d *resolveDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	conn, destinationAddress, err := d.ListenPacketWithDestination(ctx, destination)
	if err != nil {
		return nil, err
	}
	if !destinationAddress.IsValid() {
		return conn, nil
	}
	return bufio.NewNATPacketConn(bufio.NewPacketConn(conn), M.SocksaddrFrom(destinationAddress, destination.Port), destination), nil
}

func (d *resolveDialer) ListenPacketWithDestination(ctx context.Context, destination M.Socksaddr) (net.PacketConn, netip.Addr, error) {
	err := d.initialize()
	if err != nil {
		return nil, netip.Addr{}, err
	}
	if !destination.IsDomain() {
		conn, err := d.dialer.ListenPacket(ctx, destination)
		return conn, netip.Addr{}, err
	}
	lookupCtx := log.ContextWithOverrideLevel(ctx, log.LevelDebug)
	addresses, err := d.router.Lookup(lookupCtx, destination.Fqdn, d.queryOptions)
	if err != nil {
		return nil, netip.Addr{}, err
	}
	return N.ListenSerial(ctx, d.dialer, destination, addresses)
}

func (d *resolveDialer) QueryOptions() adapter.DNSQueryOptions {
	return d.queryOptions
}

func (d *resolveDialer) Upstream() any {
	return d.dialer
}

func (d *resolveParallelNetworkDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if destination.IsDomain() {
		return d.DialParallelNetwork(ctx, network, destination, nil, nil, nil, nil, 0)
	}
	err := d.initialize()
	if err != nil {
		return nil, err
	}
	return d.dialer.DialContext(ctx, network, destination)
}

func (d *resolveParallelNetworkDialer) DialParallelNetwork(ctx context.Context, network string, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.Conn, error) {
	err := d.initialize()
	if err != nil {
		return nil, err
	}
	if len(destinationAddresses) == 0 && destination.IsDomain() {
		lookupCtx := log.ContextWithOverrideLevel(ctx, log.LevelDebug)
		destinationAddresses, err = d.router.Lookup(lookupCtx, destination.Fqdn, d.queryOptions)
		if err != nil {
			return nil, err
		}
	}
	if fallbackDelay == 0 {
		fallbackDelay = d.fallbackDelay
	}
	if d.concurrent && N.NetworkName(network) == N.NetworkTCP && len(destinationAddresses) > 1 {
		return d.dialConcurrentNetwork(ctx, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	if d.parallel || len(destinationAddresses) <= 1 {
		return d.dialer.DialParallelNetwork(ctx, network, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
	}
	var errors []error
	for _, address := range destinationAddresses {
		conn, err := d.dialer.DialParallelNetwork(ctx, network, destination, []netip.Addr{address}, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
		if err == nil {
			return conn, nil
		}
		errors = append(errors, err)
	}
	return nil, E.Errors(errors...)
}

func (d *resolveParallelNetworkDialer) ListenSerialNetworkPacket(ctx context.Context, destination M.Socksaddr, destinationAddresses []netip.Addr, strategy *C.NetworkStrategy, interfaceType []C.InterfaceType, fallbackInterfaceType []C.InterfaceType, fallbackDelay time.Duration) (net.PacketConn, netip.Addr, error) {
	err := d.initialize()
	if err != nil {
		return nil, netip.Addr{}, err
	}
	if len(destinationAddresses) == 0 && destination.IsDomain() {
		ctx = log.ContextWithOverrideLevel(ctx, log.LevelDebug)
		destinationAddresses, err = d.router.Lookup(ctx, destination.Fqdn, d.queryOptions)
		if err != nil {
			return nil, netip.Addr{}, err
		}
	}
	if fallbackDelay == 0 {
		fallbackDelay = d.fallbackDelay
	}
	return d.dialer.ListenSerialNetworkPacket(ctx, destination, destinationAddresses, strategy, interfaceType, fallbackInterfaceType, fallbackDelay)
}
