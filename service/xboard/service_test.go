package xboard

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/experimental/cachefile"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	singBufio "github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestBuildInboundOptionsVLESS(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeVLESS,
		Tag:  "vless-in",
		Options: &option.VLESSInboundOptions{
			ListenOptions: option.ListenOptions{
				ListenPort: 8443,
			},
		},
	}
	result, err := buildInboundOptions(template, "", "", nil, nil, map[string]any{
		"protocol":    "vless",
		"server_port": float64(443),
		"flow":        "xtls-rprx-vision",
	}, []xboardUser{
		{ID: 1001, UUID: "00000000-0000-0000-0000-000000000001"},
	})
	require.NoError(t, err)
	require.Equal(t, C.TypeVLESS, result.Type)
	options := result.Options.(*option.VLESSInboundOptions)
	require.Equal(t, uint16(443), options.ListenPort)
	require.Equal(t, []option.VLESSUser{{
		Name: "1001",
		UUID: "00000000-0000-0000-0000-000000000001",
		Flow: "xtls-rprx-vision",
	}}, options.Users)
}

func TestTrafficTrackerAddTrafficRestoresFailedReport(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.addTraffic("in", map[string][2]int64{
		"1001": {123, 456},
	})

	firstRead := tracker.readTraffic("in")
	require.Equal(t, map[string][2]int64{
		"1001": {123, 456},
	}, firstRead)
	require.Empty(t, tracker.readTraffic("in"))

	tracker.addTraffic("in", firstRead)
	secondRead := tracker.readTraffic("in")
	require.Equal(t, map[string][2]int64{
		"1001": {123, 456},
	}, secondRead)
}

func TestTotalOnlineCountsActiveConnections(t *testing.T) {
	require.Equal(t, int64(5), totalOnline(map[int]int{
		1001: 2,
		1002: 3,
		1003: 0,
	}))
}

func TestTrafficTrackerTracksTotalsForRealtimeStatus(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	client, server := net.Pipe()
	defer client.Close()
	tracked := tracker.RoutedConnection(t.Context(), server, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("::ffff:192.0.2.1"), 12345),
	}, nil, nil)
	defer tracked.Close()

	writeErr := make(chan error, 1)
	go func() {
		_, err := client.Write([]byte("hello"))
		writeErr <- err
	}()
	buffer := make([]byte, 5)
	_, err := tracked.Read(buffer)
	require.NoError(t, err)
	require.NoError(t, <-writeErr)

	readErr := make(chan error, 1)
	go func() {
		buffer := make([]byte, 6)
		_, err := client.Read(buffer)
		readErr <- err
	}()
	_, err = tracked.Write([]byte("world!"))
	require.NoError(t, err)
	require.NoError(t, <-readErr)

	uploadTotal, downloadTotal := tracker.totalTraffic("in")
	require.Equal(t, int64(5), uploadTotal)
	require.Equal(t, int64(6), downloadTotal)
	require.Equal(t, int64(1), tracker.totalConnections("in"))
	require.Equal(t, int64(1), totalOnline(tracker.readOnline("in")))
	require.Equal(t, map[int][]string{
		1001: {"192.0.2.1"},
	}, tracker.readAlive("in", false))
	require.Equal(t, map[int][]string{
		1001: {"192.0.2.1"},
	}, tracker.readAlive("in", true))
	require.Empty(t, tracker.readAlive("in", false))
}

func TestTrafficTrackerRateSnapshotIsSharedByReportAndWebSocket(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	client, server := net.Pipe()
	defer client.Close()
	tracked := tracker.RoutedConnection(t.Context(), server, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}, nil, nil)
	defer tracked.Close()

	writeErr := make(chan error, 1)
	go func() {
		_, err := client.Write([]byte("hello"))
		writeErr <- err
	}()
	buffer := make([]byte, 5)
	_, err := tracked.Read(buffer)
	require.NoError(t, err)
	require.NoError(t, <-writeErr)

	tracker.access.Lock()
	tracker.rates["in"].lastTime = time.Now().Add(-2 * time.Second)
	tracker.access.Unlock()

	uploadSpeed, downloadSpeed := tracker.readTrafficRate("in")
	require.Positive(t, uploadSpeed)
	require.Zero(t, downloadSpeed)

	uploadSpeedAgain, downloadSpeedAgain := tracker.readTrafficRate("in")
	require.Equal(t, uploadSpeed, uploadSpeedAgain)
	require.Equal(t, downloadSpeed, downloadSpeedAgain)
}

func TestTrafficTrackerClosesRemovedUserConnections(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	client, server := net.Pipe()
	defer client.Close()
	tracked := tracker.RoutedConnection(t.Context(), server, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}, nil, nil)

	tracker.closeRemovedUsers("in", []xboardUser{{ID: 1002}})

	buffer := make([]byte, 1)
	_, err := tracked.Read(buffer)
	require.Error(t, err)
	require.Empty(t, tracker.active["in"]["1001"])
	require.Equal(t, int64(0), totalOnline(tracker.readOnline("in")))
}

func TestTrafficTrackerDeviceLimitCountsUniqueSourceIPs(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, DeviceLimit: 1}})

	client1, server1 := net.Pipe()
	defer client1.Close()
	tracked1 := tracker.RoutedConnection(t.Context(), server1, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}, nil, nil)
	defer tracked1.Close()

	client2, server2 := net.Pipe()
	defer client2.Close()
	tracked2 := tracker.RoutedConnection(t.Context(), server2, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 23456),
	}, nil, nil)
	defer tracked2.Close()

	require.Equal(t, 1, activeDeviceCount(tracker, "in", "1001"))
	require.Equal(t, int64(2), totalOnline(tracker.readOnline("in")))

	client3, server3 := net.Pipe()
	defer client3.Close()
	denied := tracker.RoutedConnection(t.Context(), server3, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("198.51.100.1"), 34567),
	}, nil, nil)
	buffer := make([]byte, 1)
	_, err := denied.Read(buffer)
	require.Error(t, err)
	require.Equal(t, 1, activeDeviceCount(tracker, "in", "1001"))
	require.Equal(t, int64(2), totalOnline(tracker.readOnline("in")))

	require.NoError(t, tracked1.Close())
	require.Equal(t, 1, activeDeviceCount(tracker, "in", "1001"))
	require.NoError(t, tracked2.Close())
	require.Equal(t, 0, activeDeviceCount(tracker, "in", "1001"))

	client4, server4 := net.Pipe()
	defer client4.Close()
	tracked4 := tracker.RoutedConnection(t.Context(), server4, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("198.51.100.1"), 45678),
	}, nil, nil)
	defer tracked4.Close()
	require.Equal(t, 1, activeDeviceCount(tracker, "in", "1001"))
}

func TestTrafficTrackerDeviceLimitZeroAllowsMultipleSourceIPs(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.updateUserLimits("in", []xboardUser{{ID: 1001}})

	client1, server1 := net.Pipe()
	defer client1.Close()
	tracked1 := tracker.RoutedConnection(t.Context(), server1, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}, nil, nil)
	defer tracked1.Close()

	client2, server2 := net.Pipe()
	defer client2.Close()
	tracked2 := tracker.RoutedConnection(t.Context(), server2, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("198.51.100.1"), 23456),
	}, nil, nil)
	defer tracked2.Close()

	require.Equal(t, 2, activeDeviceCount(tracker, "in", "1001"))
}

func TestTrafficTrackerDeviceLimitReloadAppliesToNewConnections(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, DeviceLimit: 2}})

	client1, server1 := net.Pipe()
	defer client1.Close()
	tracked1 := tracker.RoutedConnection(t.Context(), server1, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}, nil, nil)
	defer tracked1.Close()

	client2, server2 := net.Pipe()
	defer client2.Close()
	tracked2 := tracker.RoutedConnection(t.Context(), server2, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("198.51.100.1"), 23456),
	}, nil, nil)
	defer tracked2.Close()

	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, DeviceLimit: 1}})

	client3, server3 := net.Pipe()
	defer client3.Close()
	denied := tracker.RoutedConnection(t.Context(), server3, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("203.0.113.1"), 34567),
	}, nil, nil)
	buffer := make([]byte, 1)
	_, err := denied.Read(buffer)
	require.Error(t, err)
	require.Equal(t, 2, activeDeviceCount(tracker, "in", "1001"))
}

func TestTrafficTrackerSpeedLimitStateUpdates(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, SpeedLimit: 1}})

	limit := trackerUserLimit(tracker, "in", "1001")
	require.NotNil(t, limit)
	require.Equal(t, int64(125000), limit.speedBytes.Load())
	require.NotNil(t, limit.uploadLimiter.Load())
	require.NotNil(t, limit.downloadLimiter.Load())
	firstUploadLimiter := limit.uploadLimiter.Load()

	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, SpeedLimit: 2}})
	require.Equal(t, int64(250000), limit.speedBytes.Load())
	require.NotSame(t, firstUploadLimiter, limit.uploadLimiter.Load())

	tracker.updateUserLimits("in", []xboardUser{{ID: 1001}})
	require.Zero(t, limit.speedBytes.Load())
	require.Nil(t, limit.uploadLimiter.Load())
	require.Nil(t, limit.downloadLimiter.Load())
}

func TestTrafficTrackerPacketConnectionUsesSharedSpeedLimit(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, SpeedLimit: 1}})

	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	tracked := tracker.RoutedPacketConnection(t.Context(), singBufio.NewPacketConn(packetConn), adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}, nil, nil)
	defer tracked.Close()

	require.IsType(t, &trackedUserPacketConn{}, tracked)
	require.Same(t, trackerUserLimit(tracker, "in", "1001"), tracked.(*trackedUserPacketConn).PacketConn.(*limitedUserPacketConn).limit)
}

type headroomConn struct {
	net.Conn
}

func (c *headroomConn) FrontHeadroom() int { return 4 }

type headroomPacketConn struct {
	N.PacketConn
}

func (c *headroomPacketConn) FrontHeadroom() int { return 42 }

func TestTrafficTrackerSpeedLimitKeepsHeadroomChain(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, SpeedLimit: 1}})
	metadata := adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}

	client, server := net.Pipe()
	defer client.Close()
	tracked := tracker.RoutedConnection(t.Context(), &headroomConn{Conn: server}, metadata, nil, nil)
	defer tracked.Close()
	require.Equal(t, 4, N.CalculateFrontHeadroom(tracked))

	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer packetConn.Close()
	trackedPacket := tracker.RoutedPacketConnection(t.Context(), &headroomPacketConn{PacketConn: singBufio.NewPacketConn(packetConn)}, metadata, nil, nil)
	defer trackedPacket.Close()
	require.Equal(t, 42, N.CalculateFrontHeadroom(trackedPacket))
}

func TestTrafficTrackerSpeedLimitSurvivesCopyUnwrap(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.updateUserLimits("in", []xboardUser{{ID: 1001, SpeedLimit: 1}})

	client, server := net.Pipe()
	defer client.Close()
	tracked := tracker.RoutedConnection(t.Context(), server, adapter.InboundContext{
		Inbound: "in",
		User:    "1001",
		Source:  M.SocksaddrFrom(netip.MustParseAddr("192.0.2.1"), 12345),
	}, nil, nil)
	defer tracked.Close()

	reader, _ := N.UnwrapCountReader(tracked, nil)
	limitedReader, isLimited := reader.(*limitedUserConn)
	require.True(t, isLimited)
	require.Same(t, trackerUserLimit(tracker, "in", "1001"), limitedReader.limit)

	writer, _ := N.UnwrapCountWriter(tracked, nil)
	limitedWriter, isLimited := writer.(*limitedUserConn)
	require.True(t, isLimited)
	require.Same(t, trackerUserLimit(tracker, "in", "1001"), limitedWriter.limit)
}

func activeDeviceCount(tracker *trafficTracker, inbound string, user string) int {
	tracker.access.Lock()
	defer tracker.access.Unlock()
	return len(tracker.devices[inbound][user])
}

func trackerUserLimit(tracker *trafficTracker, inbound string, user string) *userLimit {
	tracker.access.Lock()
	defer tracker.access.Unlock()
	return tracker.limits[inbound][user]
}

func TestBuildNodeReportUsesXboardNodeStatusAndMetricsShape(t *testing.T) {
	service := &Service{
		ctx:       context.Background(),
		tracker:   newTrafficTracker(),
		startTime: time.Now().Add(-time.Minute),
	}
	service.apiSuccess.Store(3)
	service.apiFailure.Store(1)
	managedNode := &node{
		options: option.XboardNodeOptions{
			Inbound: "in",
			Panel:   "panel",
			NodeID:  1,
		},
		panel: &panel{websocket: true},
		users: []xboardUser{
			{ID: 1001},
			{ID: 1002},
		},
	}
	managedNode.wsConnected.Store(true)
	service.tracker.register("in", "panel", 1)
	report := service.buildNodeReport(managedNode, nil, nil)

	status := report["status"].(nodeStatus)
	require.NotNil(t, status.Mem)
	metrics := report["metrics"].(map[string]any)
	require.Contains(t, metrics, "uptime")
	require.Contains(t, metrics, "goroutines")
	require.Contains(t, metrics, "active_connections")
	require.Contains(t, metrics, "total_connections")
	require.Equal(t, 0, metrics["active_users"])
	require.Equal(t, 2, metrics["total_users"])
	require.Contains(t, metrics, "inbound_speed")
	require.Contains(t, metrics, "outbound_speed")
	require.Equal(t, false, metrics["kernel_status"])
	require.Equal(t, map[string]any{"success": uint64(3), "failure": uint64(1)}, metrics["api"])
	require.Equal(t, map[string]any{"enabled": true, "connected": true}, metrics["ws"])

	wsStatus := service.buildWebSocketStatus(managedNode)
	require.NotContains(t, wsStatus, "status")
	require.NotContains(t, wsStatus, "traffic")
	require.Contains(t, wsStatus, "uptime")
	require.Contains(t, wsStatus, "inbound_speed")
	require.Contains(t, wsStatus, "outbound_speed")
}

func TestHandshakeUsesPost(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method = request.Method
		_, _ = writer.Write([]byte(`{"websocket":{"enabled":true,"ws_url":"ws://127.0.0.1/ws"}}`))
	}))
	defer server.Close()

	service := &Service{}
	managedNode := &node{
		options: option.XboardNodeOptions{NodeID: 1},
		panel: &panel{
			options: option.XboardPanelOptions{
				URL:   server.URL,
				Token: "token",
			},
			client: server.Client(),
		},
	}
	wsURL, err := service.handshake(t.Context(), managedNode)
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, method)
	require.Equal(t, "ws://127.0.0.1/ws", wsURL)
}

// The node_type enum published in the JSON schema must match what the service
// can actually build an inbound for.
func TestNodeTypeEnumMatchesSupportedInbounds(t *testing.T) {
	field, found := reflect.TypeFor[option.XboardNodeOptions]().FieldByName("NodeType")
	require.True(t, found)
	enumTag := field.Tag.Get("enum")
	require.NotEmpty(t, enumTag)
	for enumValue := range strings.SplitSeq(enumTag, ",") {
		t.Run(enumValue, func(t *testing.T) {
			_, err := defaultInboundOptions(nodeType(enumValue), "", "")
			require.NoError(t, err)
		})
	}
}

type countingRoundTripper struct {
	transport http.RoundTripper
	requests  atomic.Int64
}

func (t *countingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	t.requests.Add(1)
	return t.transport.RoundTrip(request)
}

// The WebSocket handshake must go through the panel client, so it shares its
// resolver, detour and TLS settings instead of using http.DefaultClient.
func TestRunWebSocketDialsWithPanelClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v2/server/handshake":
			_, _ = writer.Write([]byte(`{"websocket":{"enabled":true,"ws_url":"ws://` + request.Host + `/ws"}}`))
		case "/ws":
			conn, err := websocket.Accept(writer, request, nil)
			if err != nil {
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "")
			if err = conn.Write(request.Context(), websocket.MessageText, []byte(`{"event":"auth.success"}`)); err != nil {
				return
			}
			<-request.Context().Done()
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	transport := &countingRoundTripper{transport: server.Client().Transport}
	service := newTestService(&stubInboundManager{})
	managedNode := &node{
		options: option.XboardNodeOptions{Inbound: "in", Panel: "panel", NodeID: 1},
		panel: &panel{
			options: option.XboardPanelOptions{
				URL:   server.URL,
				Token: "token",
			},
			client: &http.Client{Transport: transport},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.runWebSocket(ctx, managedNode)
	}()

	require.Eventually(t, managedNode.wsConnected.Load, 5*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 2, transport.requests.Load(), "handshake and websocket dial must both use the panel client")
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runWebSocket did not return after cancel")
	}
}

func TestBuildInboundOptionsRejectsUnsupported(t *testing.T) {
	template := option.Inbound{
		Type:    C.TypeTun,
		Tag:     "tun-in",
		Options: &option.TunInboundOptions{},
	}
	_, err := buildInboundOptions(template, "", "", nil, nil, map[string]any{}, nil)
	require.Error(t, err)
}

func TestBuildInboundOptionsRejectsMissingType(t *testing.T) {
	template := option.Inbound{
		Tag: "xboard/main/1",
	}
	_, err := buildInboundOptions(template, "", "", nil, nil, map[string]any{}, nil)
	require.ErrorContains(t, err, "missing xboard inbound type")
}

func TestBuildInboundOptionsListenUnix(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeTrojan,
		Tag:  "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "/run/sing-box/trojan.sock", nil, nil, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
	}, []xboardUser{
		{ID: 1001, UUID: "00000000-0000-0000-0000-000000000001"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.Equal(t, "/run/sing-box/trojan.sock", options.ListenUnix)
	// server_port is advertised to clients only, it must not reach the listener.
	require.Zero(t, options.ListenPort)
	require.Nil(t, options.Listen)
	require.NoError(t, options.CheckListenUnix())
}

func TestBuildInboundOptionsListenUnixVMess(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeVMess,
		Tag:  "vmess-in",
	}
	result, err := buildInboundOptions(template, "", "/run/sing-box/vmess.sock", nil, nil, map[string]any{
		"protocol":    "vmess",
		"server_port": float64(443),
	}, []xboardUser{
		{ID: 1001, UUID: "00000000-0000-0000-0000-000000000001"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.VMessInboundOptions)
	require.Equal(t, "/run/sing-box/vmess.sock", options.ListenUnix)
	require.Zero(t, options.ListenPort)
	require.NoError(t, options.CheckListenUnix())
}

func TestDefaultInboundOptionsRejectsListenUnixForUnsupportedType(t *testing.T) {
	for _, inboundType := range []string{C.TypeShadowsocks, C.TypeVLESS, C.TypeHysteria2, C.TypeTUIC, C.TypeNaive} {
		_, err := defaultInboundOptions(inboundType, "", "/run/sing-box/node.sock")
		require.ErrorContains(t, err, "listen_unix is not supported by xboard inbound type", inboundType)
	}
}

func TestBuildInboundOptionsServiceManagedShadowsocks(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeShadowsocks,
		Tag:  "ss-in",
	}
	result, err := buildInboundOptions(template, "127.0.0.1", "", nil, nil, map[string]any{
		"protocol":    "shadowsocks",
		"server_port": float64(10001),
		"cipher":      "2022-blake3-aes-128-gcm",
		"server_key":  "1234567890abcdef",
	}, []xboardUser{
		{ID: 1001, UUID: "00000000-0000-0000-0000-000000000001"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.ShadowsocksInboundOptions)
	require.Equal(t, "2022-blake3-aes-128-gcm", options.Method)
	require.Equal(t, "MTIzNDU2Nzg5MGFiY2RlZg==", options.Password)
	require.Equal(t, uint16(10001), options.ListenPort)
	require.Equal(t, []option.ShadowsocksUser{{
		Name:     "1001",
		Password: "MDAwMDAwMDAtMDAwMC0wMA==",
	}}, options.Users)
}

func TestBuildInboundOptionsServiceManagedTrojanTLS(t *testing.T) {
	template := option.Inbound{
		Tag: "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "", nil, nil, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
		"tls":         float64(1),
		"tls_settings": map[string]any{
			"server_name":    "example.com",
			"allow_insecure": true,
		},
		"cert_config": map[string]any{
			"cert_path": "/etc/ssl/fullchain.pem",
			"key_path":  "/etc/ssl/private.key",
		},
	}, []xboardUser{
		{ID: 1002, UUID: "trojan-password"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.Equal(t, uint16(443), options.ListenPort)
	require.NotNil(t, options.TLS)
	require.True(t, options.TLS.Enabled)
	require.Equal(t, "example.com", options.TLS.ServerName)
	require.True(t, options.TLS.Insecure)
	require.Equal(t, "/etc/ssl/fullchain.pem", options.TLS.CertificatePath)
	require.Equal(t, "/etc/ssl/private.key", options.TLS.KeyPath)
	require.Equal(t, []option.TrojanUser{{
		Name:     "1002",
		Password: "trojan-password",
	}}, options.Users)
}

func TestBuildInboundOptionsServiceManagedTrojanManualTLS(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeTrojan,
		Tag:  "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "", &option.InboundTLSOptions{
		Enabled:         true,
		ServerName:      "manual.example.com",
		CertificatePath: "/manual/fullchain.pem",
		KeyPath:         "/manual/private.key",
	}, nil, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
	}, []xboardUser{
		{ID: 1002, UUID: "trojan-password"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.NotNil(t, options.TLS)
	require.True(t, options.TLS.Enabled)
	require.Equal(t, "manual.example.com", options.TLS.ServerName)
	require.Equal(t, "/manual/fullchain.pem", options.TLS.CertificatePath)
	require.Equal(t, "/manual/private.key", options.TLS.KeyPath)
	require.Equal(t, []option.TrojanUser{{
		Name:     "1002",
		Password: "trojan-password",
	}}, options.Users)
}

func TestBuildInboundOptionsServiceManagedTrojanManualTLSKeepsCertificateProvider(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeTrojan,
		Tag:  "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "", &option.InboundTLSOptions{
		Enabled:    true,
		ServerName: "manual.example.com",
		ALPN:       []string{"h2", "http/1.1"},
		MinVersion: "1.2",
		CertificateProvider: &option.CertificateProviderOptions{
			Tag: "my-cert",
		},
	}, nil, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
		"tls":         float64(1),
		"tls_settings": map[string]any{
			"server_name":    "panel.example.com",
			"allow_insecure": true,
		},
		"cert_config": map[string]any{
			"cert_path": "/etc/ssl/fullchain.pem",
			"key_path":  "/etc/ssl/private.key",
		},
	}, []xboardUser{
		{ID: 1002, UUID: "trojan-password"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.NotNil(t, options.TLS)
	require.True(t, options.TLS.Enabled)
	require.Equal(t, "manual.example.com", options.TLS.ServerName)
	require.Equal(t, []string{"h2", "http/1.1"}, []string(options.TLS.ALPN))
	require.Equal(t, "1.2", options.TLS.MinVersion)
	require.NotNil(t, options.TLS.CertificateProvider)
	require.Equal(t, "my-cert", options.TLS.CertificateProvider.Tag)
	require.Empty(t, options.TLS.CertificatePath)
	require.Empty(t, options.TLS.KeyPath)
	require.False(t, options.TLS.Insecure)
}

func TestBuildInboundOptionsServiceManagedTrojanManualTransport(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeTrojan,
		Tag:  "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "", nil, &option.V2RayTransportOptions{
		Type: C.V2RayTransportTypeWebsocket,
		WebsocketOptions: option.V2RayWebsocketOptions{
			Path: "/flare/mqtt",
		},
	}, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
	}, []xboardUser{
		{ID: 1002, UUID: "trojan-password"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.NotNil(t, options.Transport)
	require.Equal(t, C.V2RayTransportTypeWebsocket, options.Transport.Type)
	require.Equal(t, "/flare/mqtt", options.Transport.WebsocketOptions.Path)
}

func TestBuildInboundOptionsServiceManagedTrojanXboardTransport(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeTrojan,
		Tag:  "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "", nil, nil, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
		"network":     "ws",
		"networkSettings": map[string]any{
			"path": "/flare/mqtt",
		},
	}, []xboardUser{
		{ID: 1002, UUID: "trojan-password"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.NotNil(t, options.Transport)
	require.Equal(t, C.V2RayTransportTypeWebsocket, options.Transport.Type)
	require.Equal(t, "/flare/mqtt", options.Transport.WebsocketOptions.Path)
}

func TestBuildInboundOptionsServiceManagedTrojanXboardHTTPTransport(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeTrojan,
		Tag:  "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "", nil, nil, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
		"network":     "h2",
		"networkSettings": map[string]any{
			"path": "/h2",
			"host": []any{"example.com"},
		},
	}, []xboardUser{
		{ID: 1002, UUID: "trojan-password"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.NotNil(t, options.Transport)
	require.Equal(t, C.V2RayTransportTypeHTTP, options.Transport.Type)
	require.Equal(t, "/h2", options.Transport.HTTPOptions.Path)
	require.Equal(t, []string{"example.com"}, []string(options.Transport.HTTPOptions.Host))
}

func TestBuildInboundOptionsServiceManagedTrojanXboardQUICTransport(t *testing.T) {
	template := option.Inbound{
		Type: C.TypeTrojan,
		Tag:  "trojan-in",
	}
	result, err := buildInboundOptions(template, "", "", nil, nil, map[string]any{
		"protocol":    "trojan",
		"server_port": float64(443),
		"network":     "quic",
	}, []xboardUser{
		{ID: 1002, UUID: "trojan-password"},
	})
	require.NoError(t, err)
	options := result.Options.(*option.TrojanInboundOptions)
	require.NotNil(t, options.Transport)
	require.Equal(t, C.V2RayTransportTypeQUIC, options.Transport.Type)
}

type stubInboundManager struct {
	exists    map[string]bool
	removed   []string
	creates   int
	createErr error
}

func (m *stubInboundManager) Start(adapter.StartStage) error { return nil }

func (m *stubInboundManager) Close() error { return nil }

func (m *stubInboundManager) Inbounds() []adapter.Inbound { return nil }

func (m *stubInboundManager) Get(tag string) (adapter.Inbound, bool) {
	return nil, m.exists[tag]
}

func (m *stubInboundManager) Remove(tag string) error {
	if !m.exists[tag] {
		return os.ErrInvalid
	}
	delete(m.exists, tag)
	m.removed = append(m.removed, tag)
	return nil
}

func (m *stubInboundManager) Create(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ string, _ any) error {
	m.creates++
	if m.createErr != nil {
		return m.createErr
	}
	if m.exists == nil {
		m.exists = make(map[string]bool)
	}
	m.exists[tag] = true
	return nil
}

func newTestService(manager adapter.InboundManager) *Service {
	return &Service{
		ctx:            context.Background(),
		logger:         log.NewNOPFactory().Logger(),
		inboundManager: manager,
		tracker:        newTrafficTracker(),
	}
}

func newTestCacheFile(t *testing.T) adapter.CacheFile {
	t.Helper()
	cacheFile := cachefile.New(context.Background(), log.NewNOPFactory().Logger(), option.CacheFileOptions{
		Enabled: true,
		Path:    filepath.Join(t.TempDir(), "cache.db"),
	})
	require.NoError(t, cacheFile.Start(adapter.StartStateInitialize))
	t.Cleanup(func() {
		require.NoError(t, cacheFile.Close())
	})
	return cacheFile
}

func newCachingTestService(t *testing.T, manager adapter.InboundManager, cacheFile adapter.CacheFile) *Service {
	t.Helper()
	service := newTestService(manager)
	service.cacheFile = cacheFile
	service.cacheMaxAge = defaultCacheMaxAge
	return service
}

func testCachedNodeOptions() option.XboardNodeOptions {
	return option.XboardNodeOptions{Inbound: "in", Panel: "panel", NodeID: 1}
}

func testCachedNode() *node {
	return &node{
		options:    testCachedNodeOptions(),
		template:   option.Inbound{Type: C.TypeTrojan, Tag: "in"},
		config:     map[string]any{"protocol": "trojan", "server_port": float64(443)},
		users:      []xboardUser{{ID: 1001, UUID: "trojan-password"}},
		configETag: `"config-v1"`,
		usersETag:  `"users-v1"`,
	}
}

func TestRestoreNodeFromCache(t *testing.T) {
	cacheFile := newTestCacheFile(t)
	source := testCachedNode()
	newCachingTestService(t, &stubInboundManager{}, cacheFile).saveNodeCache(source, true)

	manager := &stubInboundManager{}
	service := newCachingTestService(t, manager, cacheFile)
	restored := &node{options: testCachedNodeOptions(), template: option.Inbound{Type: C.TypeTrojan, Tag: "in"}}
	require.True(t, service.restoreNode(restored))

	// the inbound must be listening without any panel round trip
	require.Equal(t, 1, manager.creates)
	require.True(t, manager.exists["in"])
	require.Equal(t, source.config, restored.config)
	require.Equal(t, source.users, restored.users)
	// ETags come back too, so the background sync can be answered with 304
	require.Equal(t, source.configETag, restored.configETag)
	require.Equal(t, source.usersETag, restored.usersETag)
}

func TestRestoreNodeWithoutCacheFile(t *testing.T) {
	manager := &stubInboundManager{}
	service := newTestService(manager)
	require.False(t, service.restoreNode(testCachedNode()))
	require.Zero(t, manager.creates)
}

func TestRestoreNodeIgnoresExpiredCache(t *testing.T) {
	cacheFile := newTestCacheFile(t)
	content, err := json.Marshal(cachedNode{
		Panel:  "panel",
		NodeID: 1,
		Users:  []xboardUser{{ID: 1001, UUID: "trojan-password"}},
	})
	require.NoError(t, err)
	require.NoError(t, cacheFile.SaveXboardNode("in", &adapter.SavedBinary{
		Content:     content,
		LastUpdated: time.Now().Add(-defaultCacheMaxAge - time.Hour),
	}))

	manager := &stubInboundManager{}
	service := newCachingTestService(t, manager, cacheFile)
	managedNode := &node{options: testCachedNodeOptions(), template: option.Inbound{Type: C.TypeTrojan, Tag: "in"}}
	require.False(t, service.restoreNode(managedNode))
	require.Zero(t, manager.creates)
	require.Empty(t, managedNode.configETag)
}

func TestRestoreNodeRollsBackWhenInboundFails(t *testing.T) {
	cacheFile := newTestCacheFile(t)
	newCachingTestService(t, &stubInboundManager{}, cacheFile).saveNodeCache(testCachedNode(), true)

	manager := &stubInboundManager{createErr: os.ErrPermission}
	service := newCachingTestService(t, manager, cacheFile)
	managedNode := &node{options: testCachedNodeOptions(), template: option.Inbound{Type: C.TypeTrojan, Tag: "in"}}
	require.False(t, service.restoreNode(managedNode))

	// leftover ETags would make the fallback fetch take the 304 branch and skip
	// a node that never came up
	require.Empty(t, managedNode.configETag)
	require.Empty(t, managedNode.usersETag)
	require.Nil(t, managedNode.config)
	require.Nil(t, managedNode.users)
	require.True(t, managedNode.cacheSavedAt.IsZero())
}

func TestDecodeCachedNode(t *testing.T) {
	valid := cachedNode{
		Panel:      "panel",
		NodeID:     1,
		ConfigETag: `"config-v1"`,
		UsersETag:  `"users-v1"`,
		Config:     map[string]any{"protocol": "trojan"},
		Users:      []xboardUser{{ID: 1001, UUID: "trojan-password"}},
	}
	content, err := json.Marshal(valid)
	require.NoError(t, err)
	emptyUsers, err := json.Marshal(cachedNode{Panel: "panel", NodeID: 1})
	require.NoError(t, err)
	saved := func(content []byte, age time.Duration) *adapter.SavedBinary {
		return &adapter.SavedBinary{Content: content, LastUpdated: time.Now().Add(-age)}
	}

	cached, loaded := decodeCachedNode(saved(content, time.Minute), defaultCacheMaxAge, "panel", 1)
	require.True(t, loaded)
	require.Equal(t, valid.Users, cached.Users)
	require.Equal(t, valid.ConfigETag, cached.ConfigETag)
	require.Equal(t, valid.UsersETag, cached.UsersETag)

	for _, testCase := range []struct {
		name   string
		saved  *adapter.SavedBinary
		maxAge time.Duration
		panel  string
		nodeID int
	}{
		{"missing", nil, defaultCacheMaxAge, "panel", 1},
		{"expired", saved(content, defaultCacheMaxAge+time.Hour), defaultCacheMaxAge, "panel", 1},
		{"other panel", saved(content, time.Minute), defaultCacheMaxAge, "other", 1},
		{"other node", saved(content, time.Minute), defaultCacheMaxAge, "panel", 2},
		{"no users", saved(emptyUsers, time.Minute), defaultCacheMaxAge, "panel", 1},
		{"corrupted", saved([]byte("{invalid"), time.Minute), defaultCacheMaxAge, "panel", 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, loaded := decodeCachedNode(testCase.saved, testCase.maxAge, testCase.panel, testCase.nodeID)
			require.False(t, loaded)
		})
	}

	// a non-positive max age disables expiry entirely
	_, loaded = decodeCachedNode(saved(content, 10*365*24*time.Hour), 0, "panel", 1)
	require.True(t, loaded)
}

func TestSaveNodeCacheThrottlesUnchangedSnapshots(t *testing.T) {
	cacheFile := newTestCacheFile(t)
	service := newCachingTestService(t, &stubInboundManager{}, cacheFile)
	managedNode := testCachedNode()

	service.saveNodeCache(managedNode, true)
	firstSaved := managedNode.cacheSavedAt
	require.False(t, firstSaved.IsZero())
	require.NotNil(t, cacheFile.LoadXboardNode("in"))

	service.saveNodeCache(managedNode, false)
	require.Equal(t, firstSaved, managedNode.cacheSavedAt)

	// an unchanged snapshot still refreshes its timestamp once it gets old, so a
	// panel answering 304 for hours does not let the cache age out
	stale := time.Now().Add(-cacheTouchInterval - time.Minute)
	managedNode.cacheSavedAt = stale
	service.saveNodeCache(managedNode, false)
	require.True(t, managedNode.cacheSavedAt.After(stale))
}

func TestSaveNodeCacheInvalidatesWhenPanelDropsAllUsers(t *testing.T) {
	cacheFile := newTestCacheFile(t)
	service := newCachingTestService(t, &stubInboundManager{}, cacheFile)
	managedNode := testCachedNode()
	service.saveNodeCache(managedNode, true)
	require.NotNil(t, cacheFile.LoadXboardNode("in"))

	// the panel dropped every user, which removes the inbound: the stale snapshot
	// must be overwritten, or the node would come back to life on the next restart
	managedNode.users = nil
	service.saveNodeCache(managedNode, true)
	_, loaded := decodeCachedNode(cacheFile.LoadXboardNode("in"), defaultCacheMaxAge, "panel", 1)
	require.False(t, loaded)

	manager := &stubInboundManager{}
	restored := &node{options: testCachedNodeOptions(), template: option.Inbound{Type: C.TypeTrojan, Tag: "in"}}
	require.False(t, newCachingTestService(t, manager, cacheFile).restoreNode(restored))
	require.Zero(t, manager.creates)
}

func TestTrafficTrackerRestoreAliveAfterFailedReport(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	tracker.addAlive("in", "1001", "192.0.2.1")

	snapshot := tracker.readAlive("in", true)
	require.Equal(t, map[int][]string{1001: {"192.0.2.1"}}, snapshot)
	require.Empty(t, tracker.readAlive("in", false))

	// an IP recorded between snapshot and restore must survive the merge
	tracker.addAlive("in", "1001", "192.0.2.2")
	tracker.restoreAlive("in", snapshot)
	restored := tracker.readAlive("in", false)
	require.ElementsMatch(t, []string{"192.0.2.1", "192.0.2.2"}, restored[1001])
}

func TestReloadNodeWithoutUsersRemovesInbound(t *testing.T) {
	manager := &stubInboundManager{exists: map[string]bool{"in": true}}
	service := newTestService(manager)
	managedNode := &node{
		options:  option.XboardNodeOptions{Inbound: "in", Panel: "panel", NodeID: 1},
		template: option.Inbound{Type: C.TypeNaive, Tag: "in"},
		config:   map[string]any{"server_port": float64(443)},
	}
	require.NoError(t, service.reloadNode(context.Background(), managedNode))
	require.Equal(t, []string{"in"}, manager.removed)
	require.Zero(t, manager.creates)
	require.False(t, manager.exists["in"])

	require.NoError(t, service.reloadNodeUsers(context.Background(), managedNode))
	require.Zero(t, manager.creates)

	managedNode.users = []xboardUser{{ID: 1001, UUID: "uuid"}}
	require.NoError(t, service.reloadNodeUsers(context.Background(), managedNode))
	require.Equal(t, 1, manager.creates)
	require.True(t, manager.exists["in"])
}

func TestReloadNodeRemovesExistingInboundBeforeCreate(t *testing.T) {
	manager := &stubInboundManager{exists: map[string]bool{"in": true}}
	service := newTestService(manager)
	managedNode := &node{
		options:  option.XboardNodeOptions{Inbound: "in", Panel: "panel", NodeID: 1},
		template: option.Inbound{Type: C.TypeTrojan, Tag: "in"},
		config:   map[string]any{"server_port": float64(443)},
		users:    []xboardUser{{ID: 1001, UUID: "trojan-password"}},
	}
	require.NoError(t, service.reloadNode(context.Background(), managedNode))
	require.Equal(t, []string{"in"}, manager.removed)
	require.Equal(t, 1, manager.creates)
	require.True(t, manager.exists["in"])
}

func TestSyncNodeRetriesReloadAfterFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var etag, body string
		switch request.URL.Path {
		case "/api/v2/server/config":
			etag = `"config-v1"`
			body = `{"protocol":"trojan","server_port":443}`
		case "/api/v2/server/user":
			etag = `"users-v1"`
			body = `{"users":[{"id":1001,"uuid":"trojan-password"}]}`
		default:
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if request.Header.Get("If-None-Match") == etag {
			writer.WriteHeader(http.StatusNotModified)
			return
		}
		writer.Header().Set("ETag", etag)
		_, _ = writer.Write([]byte(body))
	}))
	defer server.Close()

	manager := &stubInboundManager{createErr: errors.New("address already in use")}
	service := newTestService(manager)
	managedNode := &node{
		options:  option.XboardNodeOptions{Inbound: "in", Panel: "panel", NodeID: 1},
		template: option.Inbound{Type: C.TypeTrojan, Tag: "in"},
		panel: &panel{
			options: option.XboardPanelOptions{
				URL:   server.URL,
				Token: "token",
			},
			client: server.Client(),
		},
	}

	require.Error(t, service.syncNode(context.Background(), managedNode, true))
	require.Equal(t, 1, manager.creates)
	require.True(t, managedNode.pendingReload)
	require.Equal(t, `"config-v1"`, managedNode.configETag)
	require.Equal(t, `"users-v1"`, managedNode.usersETag)

	// both endpoints answer 304 now, but the failed reload must be retried
	manager.createErr = nil
	require.NoError(t, service.syncNode(context.Background(), managedNode, false))
	require.Equal(t, 2, manager.creates)
	require.False(t, managedNode.pendingReload)

	require.NoError(t, service.syncNode(context.Background(), managedNode, false))
	require.Equal(t, 2, manager.creates)
}

func TestSyncNodeSkipsReloadWhenContentUnchangedWithoutETag(t *testing.T) {
	configBody := `{"protocol":"trojan","server_port":443}`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v2/server/config":
			_, _ = writer.Write([]byte(configBody))
		case "/api/v2/server/user":
			_, _ = writer.Write([]byte(`{"users":[{"id":1001,"uuid":"trojan-password"}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	manager := &stubInboundManager{}
	service := newTestService(manager)
	managedNode := &node{
		options:  option.XboardNodeOptions{Inbound: "in", Panel: "panel", NodeID: 1},
		template: option.Inbound{Type: C.TypeTrojan, Tag: "in"},
		panel: &panel{
			options: option.XboardPanelOptions{
				URL:   server.URL,
				Token: "token",
			},
			client: server.Client(),
		},
	}

	require.NoError(t, service.syncNode(context.Background(), managedNode, true))
	require.Equal(t, 1, manager.creates)

	require.NoError(t, service.syncNode(context.Background(), managedNode, false))
	require.NoError(t, service.syncNode(context.Background(), managedNode, false))
	require.Equal(t, 1, manager.creates)

	configBody = `{"protocol":"trojan","server_port":8443}`
	require.NoError(t, service.syncNode(context.Background(), managedNode, false))
	require.Equal(t, 2, manager.creates)
}
