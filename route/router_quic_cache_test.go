package route

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/expiringmap"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/sniff"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	R "github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

func TestRouterQUICSniffCacheCloseWithoutParent(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Minute, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	metadata := quicCloseMetadata()
	onClose := router.wrapQUICSniffIdleCache(metadata, nil)
	require.NotNil(t, onClose)
	onClose(nil)
	entry, loaded := router.lookupQUICSniff(metadata.Source, metadata.Destination)
	require.True(t, loaded)
	require.Equal(t, quicSniffEntry{sniffHost: metadata.SniffHost, client: metadata.Client}, entry)
}

func TestRouterQUICSniffCacheInterrupt(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		withParent  bool
		closedCache bool
	}{
		{name: "with parent", withParent: true},
		{name: "without parent"},
		{name: "closed cache", withParent: true, closedCache: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Minute, 16)}
			t.Cleanup(router.quicSniffCache.Close)
			metadata := quicCloseMetadata()
			outer := &quicCloseGroup{group: interrupt.NewGroup()}
			inner := &quicCloseGroup{group: interrupt.NewGroup()}
			conn := &quicClosePacketConn{}
			parentCalls := 0
			var parent N.CloseHandlerFunc
			if testCase.withParent {
				parent = func(err error) {
					parentCalls++
					require.NoError(t, err)
					require.Equal(t, 1, outer.removals)
					require.Equal(t, 1, inner.removals)
					_, loaded := router.lookupQUICSniff(metadata.Source, metadata.Destination)
					require.Equal(t, !testCase.closedCache, loaded)
				}
			}
			conn.onClose = registerInterrupt([]adapter.Outbound{outer, inner}, conn, router.wrapQUICSniffIdleCache(metadata, parent))
			if testCase.closedCache {
				router.quicSniffCache.Close()
			}
			outer.group.Interrupt(true)
			inner.group.Interrupt(true)
			outer.group.Interrupt(true)
			require.Equal(t, 1, conn.closes)
			require.Equal(t, 1, outer.removals)
			require.Equal(t, 1, inner.removals)
			if testCase.withParent {
				require.Equal(t, 1, parentCalls)
			}
			entry, loaded := router.lookupQUICSniff(metadata.Source, metadata.Destination)
			require.Equal(t, !testCase.closedCache, loaded)
			if loaded {
				require.Equal(t, quicSniffEntry{sniffHost: metadata.SniffHost, client: metadata.Client}, entry)
			}
		})
	}
}

func TestRouterQUICSniffCacheHandshakeResponseFailure(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Minute, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	metadata := quicCloseMetadata()
	group := &quicCloseGroup{group: interrupt.NewGroup()}
	handshakeErr := errors.New("write handshake response")
	conn := &quicClosePacketConn{handshakeErr: handshakeErr}
	remote := &quicCloseRemotePacketConn{}
	parentCalls := 0
	onClose := registerInterrupt([]adapter.Outbound{group}, conn, router.wrapQUICSniffIdleCache(metadata, func(err error) {
		parentCalls++
		require.ErrorIs(t, err, handshakeErr)
		require.Equal(t, 1, conn.closes)
		require.Equal(t, 1, remote.closes)
		require.Equal(t, 1, group.removals)
		entry, loaded := router.lookupQUICSniff(metadata.Source, metadata.Destination)
		require.True(t, loaded)
		require.Equal(t, quicSniffEntry{sniffHost: metadata.SniffHost, client: metadata.Client}, entry)
	}))
	manager := NewConnectionManager(context.Background(), logger.NOP())
	manager.NewPacketConnection(context.Background(), &quicCloseDialer{remote: remote}, conn, metadata, onClose)
	require.Equal(t, 1, parentCalls)
	group.group.Interrupt(true)
	require.Equal(t, 1, conn.closes)
	manager.NewPacketConnection(context.Background(), &quicCloseDialer{remote: &quicCloseRemotePacketConn{}}, &quicClosePacketConn{handshakeErr: handshakeErr}, metadata, nil)
}

func quicCloseMetadata() adapter.InboundContext {
	return adapter.InboundContext{
		Protocol:    C.ProtocolQUIC,
		Source:      M.ParseSocksaddr("127.0.0.1:10000"),
		Destination: M.ParseSocksaddr("1.1.1.1:443"),
		SniffHost:   "example.com",
		Client:      C.ClientChromium,
	}
}

type quicCloseGroup struct {
	adapter.OutboundGroup
	group    *interrupt.Group
	removals int
}

func (g *quicCloseGroup) AttachConnection(conn io.Closer) func() {
	remove := g.group.Add(conn, true)
	return func() {
		g.removals++
		remove()
	}
}

type quicClosePacketConn struct {
	N.PacketConn
	closes       int
	onClose      N.CloseHandlerFunc
	handshakeErr error
}

func (c *quicClosePacketConn) Close() error {
	c.closes++
	if c.onClose != nil {
		c.onClose(nil)
	}
	return nil
}

func (c *quicClosePacketConn) HandshakeSuccess() error {
	return c.handshakeErr
}

type quicCloseRemotePacketConn struct {
	net.PacketConn
	closes int
}

func (c *quicCloseRemotePacketConn) Close() error {
	c.closes++
	return nil
}

type quicCloseDialer struct {
	N.Dialer
	remote net.PacketConn
}

func (d *quicCloseDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return d.remote, nil
}

func TestRouterQUICSniffCacheRefreshAndExpiry(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](200*time.Millisecond, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	source := M.ParseSocksaddr("127.0.0.1:10000")
	destination := M.ParseSocksaddr("1.1.1.1:443")
	router.cacheQUICSniff(source, destination, quicSniffEntry{sniffHost: "old.example", client: C.ClientFirefox})
	time.Sleep(120 * time.Millisecond)
	router.cacheQUICSniff(source, destination, quicSniffEntry{sniffHost: "new.example", client: C.ClientChromium})
	time.Sleep(120 * time.Millisecond)
	entry, loaded := router.lookupQUICSniff(source, destination)
	require.True(t, loaded)
	require.Equal(t, quicSniffEntry{sniffHost: "new.example", client: C.ClientChromium}, entry)
	require.Eventually(t, func() bool {
		return router.quicSniffCache.Len() == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestRouterQUICSniffCacheLookupRefreshesTTL(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](200*time.Millisecond, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	source := M.ParseSocksaddr("127.0.0.1:10000")
	destination := M.ParseSocksaddr("1.1.1.1:443")
	expected := quicSniffEntry{sniffHost: "example.com", client: C.ClientChromium}
	router.cacheQUICSniff(source, destination, expected)
	time.Sleep(120 * time.Millisecond)
	entry, loaded := router.lookupQUICSniff(source, destination)
	require.True(t, loaded)
	require.Equal(t, expected, entry)
	time.Sleep(120 * time.Millisecond)
	entry, loaded = router.lookupQUICSniff(source, destination)
	require.True(t, loaded)
	require.Equal(t, expected, entry)
	require.Eventually(t, func() bool {
		return router.quicSniffCache.Len() == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestRouterQUICSniffCacheRefreshesOnCloseAfterOriginalExpiry(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](20*time.Millisecond, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	source := M.ParseSocksaddr("127.0.0.1:10000")
	destination := M.ParseSocksaddr("1.1.1.1:443")
	expected := quicSniffEntry{sniffHost: "example.com", client: C.ClientChromium}
	router.cacheQUICSniff(source, destination, expected)
	require.Eventually(t, func() bool {
		return router.quicSniffCache.Len() == 0
	}, time.Second, 10*time.Millisecond)
	closed := false
	onClose := router.wrapQUICSniffIdleCache(adapter.InboundContext{
		Protocol:    C.ProtocolQUIC,
		Source:      source,
		Destination: destination,
		SniffHost:   expected.sniffHost,
		Client:      expected.client,
	}, func(error) {
		closed = true
	})
	onClose(nil)
	require.True(t, closed)
	entry, loaded := router.lookupQUICSniff(source, destination)
	require.True(t, loaded)
	require.Equal(t, expected, entry)
}

func TestRouterQUICSniffCacheCloseKeepsNewerOrECHEntry(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Second, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	source := M.ParseSocksaddr("127.0.0.1:10000")
	destination := M.ParseSocksaddr("1.1.1.1:443")
	router.cacheQUICSniff(source, destination, quicSniffEntry{sniffHost: "old.example", client: C.ClientChromium})
	echMetadata := adapter.InboundContext{
		Protocol:    C.ProtocolQUIC,
		Source:      source,
		Destination: destination,
		Client:      C.ClientChromium,
	}
	router.recordQUICSniff(&echMetadata)
	onClose := router.wrapQUICSniffIdleCache(adapter.InboundContext{
		Protocol:    C.ProtocolQUIC,
		Source:      source,
		Destination: destination,
		SniffHost:   "old.example",
		Client:      C.ClientChromium,
	}, func(error) {})
	onClose(nil)
	entry, loaded := router.lookupQUICSniff(source, destination)
	require.True(t, loaded)
	require.Equal(t, quicSniffEntry{sniffHost: "", client: C.ClientChromium}, entry)
}

func TestRouterQUICSniffCacheCloseUsesRouteOriginalDestination(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Second, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	source := M.ParseSocksaddr("127.0.0.1:10000")
	originDestination := M.ParseSocksaddr("1.1.1.1:443")
	onClose := router.wrapQUICSniffIdleCache(adapter.InboundContext{
		Protocol:                 C.ProtocolQUIC,
		Source:                   source,
		Destination:              M.ParseSocksaddr("192.0.2.1:443"),
		RouteOriginalDestination: originDestination,
		SniffHost:                "example.com",
		Client:                   C.ClientSafari,
	}, func(error) {})
	onClose(nil)
	entry, loaded := router.lookupQUICSniff(source, originDestination)
	require.True(t, loaded)
	require.Equal(t, quicSniffEntry{sniffHost: "example.com", client: C.ClientSafari}, entry)
}

func TestRouterQUICSniffCacheUnmapsIPv4MappedKeys(t *testing.T) {
	router := &Router{quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Minute, 16)}
	t.Cleanup(router.quicSniffCache.Close)
	mappedSource := M.SocksaddrFrom(netip.AddrFrom16(netip.MustParseAddr("127.0.0.1").As16()), 10000)
	mappedDest := M.SocksaddrFrom(netip.AddrFrom16(netip.MustParseAddr("1.1.1.1").As16()), 443)
	plainSource := M.ParseSocksaddr("127.0.0.1:10000")
	plainDest := M.ParseSocksaddr("1.1.1.1:443")
	expected := quicSniffEntry{sniffHost: "example.com", client: C.ClientChromium}
	router.cacheQUICSniff(mappedSource, mappedDest, expected)
	entry, loaded := router.lookupQUICSniff(plainSource, plainDest)
	require.True(t, loaded)
	require.Equal(t, expected, entry)
}

func TestRouterQUICSniffCacheResetNetworkAndNilSafety(t *testing.T) {
	nilRouter := &Router{logger: logger.NOP()}
	metadata := quicCloseMetadata()
	nilRouter.recordQUICSniff(&metadata)
	require.False(t, nilRouter.restoreQUICSniff(context.Background(), &metadata, nil, quicShortPacket()))
	nilRouter.refreshQUICSniff(metadata.Source, metadata.Destination, quicSniffEntry{sniffHost: "example.com"})

	router := &Router{
		logger:            logger.NOP(),
		httpClientManager: &stubHTTPClientManager{},
		dns:               &stubResetDNSRouter{},
		quicSniffCache:    expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Minute, 16),
	}
	t.Cleanup(router.quicSniffCache.Close)
	router.cacheQUICSniff(metadata.Source, metadata.Destination, quicSniffEntry{sniffHost: "example.com", client: C.ClientChromium})
	require.Equal(t, 1, router.quicSniffCache.Len())
	router.ResetNetwork()
	require.Zero(t, router.quicSniffCache.Len())
}

func TestActionSniffQUICShortHeaderRequiresCacheAndEnabledQUIC(t *testing.T) {
	router := &Router{
		logger:         logger.NOP(),
		quicSniffCache: expiringmap.NewWithLimit[quicSniffCacheKey, quicSniffEntry](time.Minute, 16),
	}
	t.Cleanup(router.quicSniffCache.Close)

	defaultAction, err := R.NewRuleAction(logger.NOP(), option.RuleAction{
		Action: C.RuleActionTypeSniff,
	})
	require.NoError(t, err)
	dnsOnlyAction, err := R.NewRuleAction(logger.NOP(), option.RuleAction{
		Action:       C.RuleActionTypeSniff,
		SniffOptions: option.RouteActionSniff{Sniffer: []string{C.ProtocolDNS}},
	})
	require.NoError(t, err)

	source := M.ParseSocksaddr("127.0.0.1:10000")
	destination := M.ParseSocksaddr("1.1.1.1:443")

	newBuffered := func(data []byte) []*N.PacketBuffer {
		buffer := buf.NewPacket()
		buffer.Write(data)
		return []*N.PacketBuffer{{Buffer: buffer, Destination: destination}}
	}

	utpSYN := make([]byte, 20)
	utpSYN[0] = 0x41
	utpMeta := adapter.InboundContext{Source: source, Destination: destination}
	utpBuffers := newBuffered(utpSYN)
	_, _, err = router.actionSniff(context.Background(), &utpMeta, defaultAction.(*R.RuleActionSniff), nil, &quicClosePacketConn{}, nil, utpBuffers)
	N.ReleaseMultiPacketBuffer(utpBuffers)
	require.NoError(t, err)
	require.Equal(t, C.ProtocolBitTorrent, utpMeta.Protocol)

	uncachedMeta := adapter.InboundContext{Source: source, Destination: destination}
	uncachedBuffers := newBuffered(quicShortPacket())
	_, _, err = router.actionSniff(context.Background(), &uncachedMeta, defaultAction.(*R.RuleActionSniff), nil, &quicClosePacketConn{}, nil, uncachedBuffers)
	N.ReleaseMultiPacketBuffer(uncachedBuffers)
	require.NoError(t, err)
	require.Empty(t, uncachedMeta.Protocol)
	require.Empty(t, uncachedMeta.SniffHost)
	require.Error(t, uncachedMeta.SniffError)

	router.cacheQUICSniff(source, destination, quicSniffEntry{sniffHost: "example.com", client: C.ClientChromium})

	multiRuleMeta := adapter.InboundContext{Source: source, Destination: destination}
	multiBuffers := newBuffered(quicShortPacket())
	_, _, err = router.actionSniff(context.Background(), &multiRuleMeta, dnsOnlyAction.(*R.RuleActionSniff), nil, &quicClosePacketConn{}, nil, multiBuffers)
	require.NoError(t, err)
	require.Empty(t, multiRuleMeta.Protocol)
	require.Error(t, multiRuleMeta.SniffError)

	_, _, err = router.actionSniff(context.Background(), &multiRuleMeta, defaultAction.(*R.RuleActionSniff), nil, &quicClosePacketConn{}, nil, multiBuffers)
	N.ReleaseMultiPacketBuffer(multiBuffers)
	require.NoError(t, err)
	require.Equal(t, C.ProtocolQUIC, multiRuleMeta.Protocol)
	require.Equal(t, "example.com", multiRuleMeta.SniffHost)
	require.Equal(t, C.ClientChromium, multiRuleMeta.Client)
	require.NoError(t, multiRuleMeta.SniffError)

	var sniffMeta adapter.InboundContext
	require.Error(t, sniff.PeekPacket(context.Background(), &sniffMeta, quicShortPacket(), defaultPacketSniffers...))
	require.Empty(t, sniffMeta.Protocol)
}

type stubHTTPClientManager struct{ adapter.HTTPClientManager }

func (*stubHTTPClientManager) ResetNetwork() {}

type stubResetDNSRouter struct{ adapter.DNSRouter }

func (*stubResetDNSRouter) ResetNetwork() {}
