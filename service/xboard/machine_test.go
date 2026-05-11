package xboard

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type lockedInboundManager struct {
	access  sync.Mutex
	exists  map[string]string
	removed []string
}

func (m *lockedInboundManager) Get(tag string) (adapter.Inbound, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	_, loaded := m.exists[tag]
	return nil, loaded
}

func (m *lockedInboundManager) Remove(tag string) error {
	m.access.Lock()
	defer m.access.Unlock()
	if _, loaded := m.exists[tag]; !loaded {
		return os.ErrInvalid
	}
	delete(m.exists, tag)
	m.removed = append(m.removed, tag)
	return nil
}

func (m *lockedInboundManager) Create(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, inboundType string, _ any) error {
	m.access.Lock()
	defer m.access.Unlock()
	if m.exists == nil {
		m.exists = make(map[string]string)
	}
	m.exists[tag] = inboundType
	return nil
}

func (m *lockedInboundManager) tags() []string {
	m.access.Lock()
	defer m.access.Unlock()
	tags := make([]string, 0, len(m.exists))
	for tag := range m.exists {
		tags = append(tags, tag)
	}
	slices.Sort(tags)
	return tags
}

type machinePanelServer struct {
	access   sync.Mutex
	server   *httptest.Server
	nodes    []machineNode
	queries  []url.Values
	bodies   map[string][]map[string]any
	nodeType map[string]string
}

func newMachinePanelServer(t *testing.T) *machinePanelServer {
	t.Helper()
	panelServer := &machinePanelServer{
		bodies: make(map[string][]map[string]any),
		nodeType: map[string]string{
			"1": "trojan",
			"2": "vmess",
		},
	}
	panelServer.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		panelServer.access.Lock()
		defer panelServer.access.Unlock()
		content, _ := io.ReadAll(request.Body)
		var body map[string]any
		if len(content) > 0 {
			_ = json.Unmarshal(content, &body)
			panelServer.bodies[request.URL.Path] = append(panelServer.bodies[request.URL.Path], body)
		}
		query := request.URL.Query()
		switch request.URL.Path {
		case machineNodesPath:
			_ = json.NewEncoder(writer).Encode(map[string]any{"nodes": panelServer.nodes})
		case machineStatusPath:
			_, _ = writer.Write([]byte(`{"data":true}`))
		case "/api/v2/server/config":
			panelServer.queries = append(panelServer.queries, query)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"protocol":    panelServer.nodeType[query.Get("node_id")],
				"listen_ip":   "::",
				"server_port": 443,
				"tls":         0,
			})
		case "/api/v2/server/user":
			panelServer.queries = append(panelServer.queries, query)
			_, _ = writer.Write([]byte(`{"users":[{"id":1001,"uuid":"00000000-0000-0000-0000-000000000001"}]}`))
		case handshakePath:
			panelServer.queries = append(panelServer.queries, query)
			_, _ = writer.Write([]byte(`{"websocket":{"enabled":true,"ws_url":"ws://` + request.Host + `/ws"}}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(panelServer.server.Close)
	return panelServer
}

func (p *machinePanelServer) setNodes(nodes ...machineNode) {
	p.access.Lock()
	p.nodes = append([]machineNode{}, nodes...)
	p.access.Unlock()
}

func newMachineTestService(t *testing.T, manager inboundController, serverURL string, client *http.Client) (*Service, *panel) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service := newTestService(manager)
	service.ctx = ctx
	managedPanel := &panel{
		options: option.XboardPanelOptions{
			Tag:          "main",
			URL:          serverURL,
			Token:        "machine-token",
			MachineID:    7,
			PollInterval: badoption.Duration(time.Hour),
			PushInterval: badoption.Duration(time.Hour),
		},
		client:    client,
		nodesWake: make(chan struct{}, 1),
		machineID: 7,
		overrides: make(map[int]option.XboardNodeOptions),
		skipped:   make(map[int]string),
	}
	service.panels = map[string]*panel{"main": managedPanel}
	return service, managedPanel
}

func TestXboardURLMachineMode(t *testing.T) {
	managedNode := &node{
		options: option.XboardNodeOptions{NodeID: 12, NodeType: "trojan"},
		panel: &panel{
			options:   option.XboardPanelOptions{URL: "https://panel.example", Token: "machine-token"},
			machineID: 7,
		},
	}
	requestURL, err := xboardURL(managedNode, "/api/v2/server/config")
	require.NoError(t, err)
	parsedURL, err := url.Parse(requestURL)
	require.NoError(t, err)
	query := parsedURL.Query()
	require.Equal(t, "7", query.Get("machine_id"))
	require.Equal(t, "machine-token", query.Get("token"))
	require.Equal(t, "12", query.Get("node_id"))
	require.False(t, query.Has("node_type"))
}

func TestApplyMachineNodesAddsAndRemovesNodes(t *testing.T) {
	panelServer := newMachinePanelServer(t)
	manager := &lockedInboundManager{}
	service, managedPanel := newMachineTestService(t, manager, panelServer.server.URL, panelServer.server.Client())
	service.cacheFile = newTestCacheFile(t)
	service.cacheMaxAge = defaultCacheMaxAge
	managedPanel.overrides[2] = option.XboardNodeOptions{Inbound: "vmess-in", Panel: "main", NodeID: 2}
	managedPanel.overrides[9] = option.XboardNodeOptions{Inbound: "unused-in", Panel: "main", NodeID: 9}

	panelServer.setNodes(
		machineNode{ID: 1, Type: "trojan", Name: "a"},
		machineNode{ID: 2, Type: "vmess", Name: "b"},
		machineNode{ID: 3, Type: "snell", Name: "c"},
	)
	service.refreshMachineNodes(managedPanel, false)
	require.Equal(t, []string{"vmess-in", "xboard/main/1"}, manager.tags())
	require.Len(t, service.listNodes(managedPanel), 2)
	require.Equal(t, "snell", managedPanel.skipped[3])
	require.NotNil(t, service.cacheFile.LoadXboardNode("xboard/main/1"))

	panelServer.access.Lock()
	for _, query := range panelServer.queries {
		require.Equal(t, "7", query.Get("machine_id"))
		require.Equal(t, "machine-token", query.Get("token"))
		require.NotEmpty(t, query.Get("node_id"))
	}
	nodesBody := panelServer.bodies[machineNodesPath][0]
	panelServer.access.Unlock()
	require.EqualValues(t, 7, nodesBody["machine_id"])
	require.Equal(t, "machine-token", nodesBody["token"])

	cachedNodes, loaded := service.loadMachineCache(managedPanel)
	require.True(t, loaded)
	require.Len(t, cachedNodes, 3)

	removedNode := service.findNode(managedPanel, 1)
	require.NotNil(t, removedNode)
	service.tracker.addTraffic("xboard/main/1", map[string][2]int64{"1001": {1, 2}})
	panelServer.setNodes(machineNode{ID: 2, Type: "vmess", Name: "b"})
	service.refreshMachineNodes(managedPanel, false)
	require.Equal(t, []string{"vmess-in"}, manager.tags())
	require.Nil(t, service.findNode(managedPanel, 1))
	require.True(t, removedNode.closed.Load())
	require.Error(t, removedNode.ctx.Err())
	require.Nil(t, service.cacheFile.LoadXboardNode("xboard/main/1"))
	require.Empty(t, service.tracker.readTraffic("xboard/main/1"))
	require.Empty(t, managedPanel.skipped)
	require.NoError(t, service.syncNode(context.Background(), removedNode, true))
	require.Equal(t, []string{"vmess-in"}, manager.tags())

	panelServer.setNodes()
	service.refreshMachineNodes(managedPanel, false)
	require.Empty(t, manager.tags())
	require.Empty(t, service.listNodes(managedPanel))
}

func TestApplyMachineNodesRecreatesNodeOnTypeChange(t *testing.T) {
	panelServer := newMachinePanelServer(t)
	manager := &lockedInboundManager{}
	service, managedPanel := newMachineTestService(t, manager, panelServer.server.URL, panelServer.server.Client())

	service.applyMachineNodes(managedPanel, []machineNode{{ID: 1, Type: "trojan"}}, false)
	firstNode := service.findNode(managedPanel, 1)
	require.NotNil(t, firstNode)

	panelServer.access.Lock()
	panelServer.nodeType["1"] = "vmess"
	panelServer.access.Unlock()
	service.applyMachineNodes(managedPanel, []machineNode{{ID: 1, Type: "vmess"}}, false)
	secondNode := service.findNode(managedPanel, 1)
	require.NotNil(t, secondNode)
	require.NotSame(t, firstNode, secondNode)
	require.True(t, firstNode.closed.Load())
	manager.access.Lock()
	require.Equal(t, C.TypeVMess, manager.exists["xboard/main/1"])
	manager.access.Unlock()
}

func TestStartMachineRestoresNodesFromCache(t *testing.T) {
	panelServer := newMachinePanelServer(t)
	cacheFile := newTestCacheFile(t)
	manager := &lockedInboundManager{}
	service, managedPanel := newMachineTestService(t, manager, panelServer.server.URL, panelServer.server.Client())
	service.cacheFile = cacheFile
	service.cacheMaxAge = defaultCacheMaxAge
	panelServer.setNodes(machineNode{ID: 1, Type: "trojan"})
	service.refreshMachineNodes(managedPanel, false)
	require.Equal(t, []string{"xboard/main/1"}, manager.tags())

	panelServer.server.Close()
	offlineManager := &lockedInboundManager{}
	offlineService, offlinePanel := newMachineTestService(t, offlineManager, panelServer.server.URL, panelServer.server.Client())
	offlineService.cacheFile = cacheFile
	offlineService.cacheMaxAge = defaultCacheMaxAge
	offlinePanel.websocket = false
	offlineService.startMachine(offlinePanel)
	require.Equal(t, []string{"xboard/main/1"}, offlineManager.tags())

	_, loaded := offlineService.loadMachineCache(&panel{options: offlinePanel.options, machineID: 8})
	require.False(t, loaded)
}

func TestReportMachineStatus(t *testing.T) {
	panelServer := newMachinePanelServer(t)
	service, managedPanel := newMachineTestService(t, &lockedInboundManager{}, panelServer.server.URL, panelServer.server.Client())
	counters := [][2]uint64{{1000, 2000}, {5000, 4000}}
	managedPanel.network.read = func() (uint64, uint64, error) {
		counter := counters[0]
		if len(counters) > 1 {
			counters = counters[1:]
		}
		return counter[0], counter[1], nil
	}

	_, _, loaded := managedPanel.network.sample()
	require.False(t, loaded)
	managedPanel.network.last = time.Now().Add(-2 * time.Second)
	require.NoError(t, service.reportMachineStatus(context.Background(), managedPanel))

	panelServer.access.Lock()
	body := panelServer.bodies[machineStatusPath][0]
	panelServer.access.Unlock()
	require.EqualValues(t, 7, body["machine_id"])
	require.Equal(t, "machine-token", body["token"])
	require.Contains(t, body, "cpu")
	for _, key := range []string{"mem", "swap", "disk"} {
		usage, isUsage := body[key].(map[string]any)
		require.True(t, isUsage, key)
		require.Contains(t, usage, "total")
		require.Contains(t, usage, "used")
	}
	network, isNetwork := body["net"].(map[string]any)
	require.True(t, isNetwork)
	inboundSpeed := network["in_speed"].(float64)
	outboundSpeed := network["out_speed"].(float64)
	require.Greater(t, inboundSpeed, float64(0))
	require.LessOrEqual(t, inboundSpeed, float64(2000))
	require.InDelta(t, 2, inboundSpeed/outboundSpeed, 0.001)
}

func TestNetworkSamplerSkipsCounterReset(t *testing.T) {
	counters := [][2]uint64{{1000, 1000}, {10, 10}, {20, 30}}
	sampler := &networkSampler{read: func() (uint64, uint64, error) {
		counter := counters[0]
		counters = counters[1:]
		return counter[0], counter[1], nil
	}}
	_, _, loaded := sampler.sample()
	require.False(t, loaded)
	sampler.last = time.Now().Add(-time.Second)
	_, _, loaded = sampler.sample()
	require.False(t, loaded)
	sampler.last = time.Now().Add(-time.Second)
	_, _, loaded = sampler.sample()
	require.True(t, loaded)
}

func TestRunMachineWebSocket(t *testing.T) {
	manager := &lockedInboundManager{}
	statusCh := make(chan map[string]any, 4)
	var (
		access  sync.Mutex
		wsQuery url.Values
	)
	panelServer := newMachinePanelServer(t)
	panelServer.setNodes(machineNode{ID: 1, Type: "trojan"})
	mux := http.NewServeMux()
	mux.Handle("/", panelServer.server.Config.Handler)
	mux.HandleFunc("/ws", func(writer http.ResponseWriter, request *http.Request) {
		access.Lock()
		wsQuery = request.URL.Query()
		access.Unlock()
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		ctx := request.Context()
		for _, message := range []string{
			`{"event":"auth.success","data":{"machine_id":7,"node_ids":[]}}`,
			`{"event":"sync.nodes","data":{"nodes":[{"id":2,"type":"vmess","name":"obsolete"}]}}`,
		} {
			if err = conn.Write(ctx, websocket.MessageText, []byte(message)); err != nil {
				return
			}
		}
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var message struct {
				Event string         `json:"event"`
				Data  map[string]any `json:"data"`
			}
			if json.Unmarshal(payload, &message) == nil && message.Event == "node.status" {
				select {
				case statusCh <- message.Data:
				default:
				}
			}
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	service, managedPanel := newMachineTestService(t, manager, server.URL, server.Client())
	ctx, cancel := context.WithCancel(service.ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go service.loopMachineNodes(managedPanel)
	go func() {
		errCh <- service.runMachineWebSocket(ctx, managedPanel)
	}()

	require.Eventually(t, managedPanel.wsConnected.Load, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return slices.Equal(manager.tags(), []string{"xboard/main/1"})
	}, 5*time.Second, 10*time.Millisecond)
	access.Lock()
	require.Equal(t, "7", wsQuery.Get("machine_id"))
	require.Equal(t, "notify", wsQuery.Get("nodes_mode"))
	require.Equal(t, "machine-token", wsQuery.Get("token"))
	require.False(t, wsQuery.Has("node_id"))
	access.Unlock()

	managedNode := service.findNode(managedPanel, 1)
	require.NotNil(t, managedNode)
	require.True(t, service.buildNodeMetrics(managedNode, nil)["ws"].(map[string]any)["connected"].(bool))

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runMachineWebSocket did not return after cancel")
	}
}

func TestHandleMachineWebSocketMessageDispatchesByNodeID(t *testing.T) {
	panelServer := newMachinePanelServer(t)
	service, managedPanel := newMachineTestService(t, &lockedInboundManager{}, panelServer.server.URL, panelServer.server.Client())
	service.applyMachineNodes(managedPanel, []machineNode{{ID: 1, Type: "trojan"}, {ID: 2, Type: "vmess"}}, false)

	countFor := func(nodeID string) int {
		panelServer.access.Lock()
		defer panelServer.access.Unlock()
		count := 0
		for _, query := range panelServer.queries {
			if query.Get("node_id") == nodeID {
				count++
			}
		}
		return count
	}
	firstBefore, secondBefore := countFor("1"), countFor("2")
	require.NoError(t, service.handleMachineWebSocketMessage(managedPanel, []byte(`{"event":"sync.users","data":{"node_id":2,"users":[]}}`), nil))
	require.Eventually(t, func() bool {
		return countFor("2") > secondBefore
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, firstBefore, countFor("1"))

	require.NoError(t, service.handleMachineWebSocketMessage(managedPanel, []byte(`{"event":"sync.config","data":{"node_id":99}}`), nil))
	require.NoError(t, service.handleMachineWebSocketMessage(managedPanel, []byte(`{"event":"sync.config","data":{}}`), nil))
	require.ErrorContains(t, service.handleMachineWebSocketMessage(managedPanel, []byte(`{"event":"error","data":{"message":"invalid machine credentials"}}`), nil), "invalid machine credentials")

	ponged := false
	require.NoError(t, service.handleMachineWebSocketMessage(managedPanel, []byte(`{"event":"ping"}`), func() error {
		ponged = true
		return nil
	}))
	require.True(t, ponged)
}

func machineTLSOptions(t *testing.T, inboundType string, config map[string]any, machineTLS *option.InboundTLSOptions, nodeTLS *option.InboundTLSOptions) (*option.InboundTLSOptions, []string) {
	t.Helper()
	result, warnings, err := buildManagedInboundOptions(option.Inbound{Type: inboundType, Tag: "in"}, inboundBuildOptions{
		tls:        nodeTLS,
		machine:    true,
		machineTLS: machineTLS,
	}, config, []xboardUser{{ID: 1001, UUID: "00000000-0000-0000-0000-000000000001"}})
	require.NoError(t, err)
	tlsOptions, loaded := result.Options.(option.InboundTLSOptionsWrapper)
	if !loaded {
		return nil, warnings
	}
	return tlsOptions.TakeInboundTLSOptions(), warnings
}

func TestMachineTLSUsesPanelCertificateAndServerName(t *testing.T) {
	machineTLS := &option.InboundTLSOptions{
		CertificatePath: "/etc/ssl/fullchain.pem",
		KeyPath:         "/etc/ssl/private.key",
		MinVersion:      "1.3",
	}
	tlsOptions, warnings := machineTLSOptions(t, C.TypeTrojan, map[string]any{
		"protocol":     "trojan",
		"tls":          float64(1),
		"server_name":  "node.example.com",
		"tls_settings": map[string]any{"server_name": "node.example.com", "allow_insecure": false},
	}, machineTLS, nil)
	require.Empty(t, warnings)
	require.NotNil(t, tlsOptions)
	require.True(t, tlsOptions.Enabled)
	require.Equal(t, "node.example.com", tlsOptions.ServerName)
	require.Equal(t, "/etc/ssl/fullchain.pem", tlsOptions.CertificatePath)
	require.Equal(t, "/etc/ssl/private.key", tlsOptions.KeyPath)
	require.Equal(t, "1.3", tlsOptions.MinVersion)
	require.Empty(t, machineTLS.ServerName)
	require.False(t, machineTLS.Enabled)
}

func TestMachineTLSFollowsPanelMode(t *testing.T) {
	machineTLS := &option.InboundTLSOptions{CertificatePath: "/etc/ssl/fullchain.pem", KeyPath: "/etc/ssl/private.key"}
	for _, inboundType := range []string{C.TypeVMess, C.TypeVLESS, C.TypeTrojan, C.TypeNaive, C.TypeHTTP} {
		tlsOptions, _ := machineTLSOptions(t, inboundType, map[string]any{
			"tls":          float64(0),
			"tls_settings": map[string]any{"server_name": "node.example.com"},
		}, machineTLS, &option.InboundTLSOptions{ServerName: "override.example.com"})
		require.Nil(t, tlsOptions, inboundType)
	}
	for _, inboundType := range []string{C.TypeHysteria2, C.TypeTUIC, C.TypeAnyTLS} {
		tlsOptions, _ := machineTLSOptions(t, inboundType, map[string]any{
			"server_name":  "node.example.com",
			"tls_settings": map[string]any{"server_name": "node.example.com"},
		}, machineTLS, nil)
		require.NotNil(t, tlsOptions, inboundType)
		require.True(t, tlsOptions.Enabled, inboundType)
		require.Equal(t, "/etc/ssl/fullchain.pem", tlsOptions.CertificatePath, inboundType)
		require.Equal(t, "node.example.com", tlsOptions.ServerName, inboundType)
	}
	tlsOptions, _ := machineTLSOptions(t, C.TypeShadowsocks, map[string]any{"cipher": "aes-128-gcm"}, machineTLS, nil)
	require.Nil(t, tlsOptions)
}

func TestMachineTLSReality(t *testing.T) {
	machineTLS := &option.InboundTLSOptions{
		CertificatePath: "/etc/ssl/fullchain.pem",
		KeyPath:         "/etc/ssl/private.key",
		MinVersion:      "1.3",
	}
	for _, inboundType := range []string{C.TypeVLESS, C.TypeTrojan} {
		tlsOptions, warnings := machineTLSOptions(t, inboundType, map[string]any{
			"tls":         float64(2),
			"server_name": "tls.example.com",
			"tls_settings": map[string]any{
				"server_name": "www.example.org",
				"server_port": "8443",
				"public_key":  "public",
				"private_key": "private",
				"short_id":    "0123abcd",
			},
			"cert_config": map[string]any{"cert_mode": "dns"},
		}, machineTLS, nil)
		require.Empty(t, warnings, inboundType)
		require.NotNil(t, tlsOptions, inboundType)
		require.True(t, tlsOptions.Enabled)
		require.Equal(t, "www.example.org", tlsOptions.ServerName)
		require.Empty(t, tlsOptions.CertificatePath)
		require.Empty(t, tlsOptions.KeyPath)
		require.Equal(t, "1.3", tlsOptions.MinVersion)
		require.NotNil(t, tlsOptions.Reality)
		require.True(t, tlsOptions.Reality.Enabled)
		require.Equal(t, "www.example.org", tlsOptions.Reality.Handshake.Server)
		require.Equal(t, uint16(8443), tlsOptions.Reality.Handshake.ServerPort)
		require.Equal(t, "private", tlsOptions.Reality.PrivateKey)
		require.Equal(t, []string{"0123abcd"}, []string(tlsOptions.Reality.ShortID))
	}
	tlsOptions, _ := machineTLSOptions(t, C.TypeVLESS, map[string]any{
		"tls":          float64(2),
		"tls_settings": map[string]any{"server_name": "www.example.org", "private_key": "private", "short_id": ""},
	}, nil, nil)
	require.Equal(t, uint16(443), tlsOptions.Reality.Handshake.ServerPort)
	require.Nil(t, machineTLS.Reality)
}

func TestMachineTLSCertConfig(t *testing.T) {
	machineTLS := &option.InboundTLSOptions{CertificatePath: "/etc/ssl/fullchain.pem", KeyPath: "/etc/ssl/private.key"}
	config := func(certConfig map[string]any) map[string]any {
		return map[string]any{
			"tls":          float64(1),
			"tls_settings": map[string]any{"server_name": "node.example.com"},
			"cert_config":  certConfig,
		}
	}
	for _, certMode := range []string{"content", "file"} {
		tlsOptions, warnings := machineTLSOptions(t, C.TypeVLESS, config(map[string]any{
			"cert_mode":    certMode,
			"cert_content": "-----BEGIN CERTIFICATE-----\r\nAAAA\r\n-----END CERTIFICATE-----\r\n",
			"key_content":  "-----BEGIN PRIVATE KEY-----\nBBBB\n-----END PRIVATE KEY-----",
		}), machineTLS, nil)
		require.Empty(t, warnings, certMode)
		require.Equal(t, []string{"-----BEGIN CERTIFICATE-----", "AAAA", "-----END CERTIFICATE-----"}, []string(tlsOptions.Certificate))
		require.Equal(t, []string{"-----BEGIN PRIVATE KEY-----", "BBBB", "-----END PRIVATE KEY-----"}, []string(tlsOptions.Key))
		require.Empty(t, tlsOptions.CertificatePath)
		require.Empty(t, tlsOptions.KeyPath)
		require.Equal(t, "node.example.com", tlsOptions.ServerName)
	}
	for _, certMode := range []string{"dns", "http", "self"} {
		tlsOptions, warnings := machineTLSOptions(t, C.TypeVLESS, config(map[string]any{
			"cert_mode": certMode,
			"domain":    "node.example.com",
		}), machineTLS, nil)
		require.Len(t, warnings, 1, certMode)
		require.Contains(t, warnings[0], certMode)
		require.Equal(t, "/etc/ssl/fullchain.pem", tlsOptions.CertificatePath)
		require.Empty(t, tlsOptions.Certificate)
	}
	tlsOptions, warnings := machineTLSOptions(t, C.TypeVLESS, config(map[string]any{"cert_mode": "content", "cert_content": "AAAA"}), machineTLS, nil)
	require.Len(t, warnings, 1)
	require.Equal(t, "/etc/ssl/fullchain.pem", tlsOptions.CertificatePath)
	require.Equal(t, "/etc/ssl/fullchain.pem", machineTLS.CertificatePath)
}

func TestMachineTLSNodeOverrideMergesFields(t *testing.T) {
	machineTLS := &option.InboundTLSOptions{
		CertificatePath: "/etc/ssl/fullchain.pem",
		KeyPath:         "/etc/ssl/private.key",
		MinVersion:      "1.2",
		ALPN:            []string{"h2"},
	}
	config := map[string]any{
		"tls":          float64(1),
		"tls_settings": map[string]any{"server_name": "node.example.com"},
		"cert_config": map[string]any{
			"cert_mode":    "content",
			"cert_content": "CERT",
			"key_content":  "KEY",
		},
	}
	tlsOptions, _ := machineTLSOptions(t, C.TypeVLESS, config, machineTLS, &option.InboundTLSOptions{
		CertificatePath: "/etc/ssl/other.pem",
		KeyPath:         "/etc/ssl/other.key",
	})
	require.Equal(t, "/etc/ssl/other.pem", tlsOptions.CertificatePath)
	require.Equal(t, "/etc/ssl/other.key", tlsOptions.KeyPath)
	require.Empty(t, tlsOptions.Certificate)
	require.Empty(t, tlsOptions.Key)
	require.Equal(t, "node.example.com", tlsOptions.ServerName)
	require.Equal(t, "1.2", tlsOptions.MinVersion)
	require.Equal(t, []string{"h2"}, []string(tlsOptions.ALPN))

	tlsOptions, _ = machineTLSOptions(t, C.TypeVLESS, config, machineTLS, &option.InboundTLSOptions{
		ServerName: "override.example.com",
		MinVersion: "1.3",
	})
	require.Equal(t, "override.example.com", tlsOptions.ServerName)
	require.Equal(t, "1.3", tlsOptions.MinVersion)
	require.Equal(t, []string{"CERT"}, []string(tlsOptions.Certificate))
	require.Empty(t, tlsOptions.CertificatePath)
	require.Equal(t, "1.2", machineTLS.MinVersion)
}

func TestSingleNodeModeKeepsManualTLSReplacement(t *testing.T) {
	result, err := buildInboundOptions(option.Inbound{Type: C.TypeTrojan, Tag: "in"}, "", "", &option.InboundTLSOptions{
		Enabled:         true,
		CertificatePath: "/manual/fullchain.pem",
	}, nil, map[string]any{
		"tls":          float64(1),
		"server_name":  "node.example.com",
		"tls_settings": map[string]any{"server_name": "node.example.com"},
	}, []xboardUser{{ID: 1001, UUID: "password"}})
	require.NoError(t, err)
	tlsOptions := result.Options.(*option.TrojanInboundOptions).TLS
	require.Equal(t, "/manual/fullchain.pem", tlsOptions.CertificatePath)
	require.Empty(t, tlsOptions.ServerName)
}

func TestTrafficTrackerUnregisterClosesConnections(t *testing.T) {
	tracker := newTrafficTracker()
	tracker.register("in", "panel", 1)
	closer := &countingCloser{}
	tracker.addActive("in", "1001", closer)
	tracker.addTraffic("in", map[string][2]int64{"1001": {10, 20}})
	tracker.unregister("in")
	require.Equal(t, 1, closer.closed)
	require.Empty(t, tracker.readTraffic("in"))
	traffic, _, _, _, allowed := tracker.prepareUser("in", "1001", "192.0.2.1")
	require.True(t, allowed)
	require.Nil(t, traffic)
	tracker.removeActive("in", "1001", closer)
	tracker.releaseDevice("in", "1001", "192.0.2.1")
}

type countingCloser struct {
	closed int
}

func (c *countingCloser) Close() error {
	c.closed++
	return nil
}

func TestMachineListRefreshCoalescesWhileHTTPIsInFlight(t *testing.T) {
	responses := make(chan chan string, 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := make(chan string, 1)
		responses <- response
		select {
		case body := <-response:
			_, _ = w.Write([]byte(body))
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	s, p := newMachineTestService(t, &lockedInboundManager{}, server.URL, server.Client())
	ctx, cancel := context.WithCancel(s.ctx)
	s.ctx = ctx
	defer cancel()
	p.options.PollInterval = 0
	p.options.PushInterval = 0
	go s.loopMachineNodes(p)
	wake := func() {
		require.NoError(t, s.handleMachineWebSocketMessage(p, []byte(`{"event":"sync.nodes","data":{}}`), nil))
	}
	next := func() chan string {
		select {
		case response := <-responses:
			return response
		case <-time.After(3 * time.Second):
			t.Fatal("machine list was not fetched")
			return nil
		}
	}
	wake()
	first := next()
	for range 20 {
		wake()
	}
	require.Len(t, p.nodesWake, 1)
	require.Empty(t, responses)
	first <- `{"nodes":[],"base_config":{"pull_interval":65,"push_interval":66}}`
	second := next()
	pull, _ := p.schedule.poll.current()
	require.Equal(t, 65*time.Second, pull)
	second <- `{"nodes":[],"sync":{"pull_interval":75,"push_interval":76}}`
	require.Eventually(t, func() bool {
		pull, _ := p.schedule.poll.current()
		push, _ := p.schedule.push.current()
		return pull == 75*time.Second && push == 76*time.Second
	}, 3*time.Second, time.Millisecond)
	require.Empty(t, responses)
	require.Empty(t, p.nodesWake)
}

func TestSyncIntervalReconfiguresRunningTimer(t *testing.T) {
	var interval syncInterval
	interval.set(time.Hour, 0)
	_, before := interval.current()
	interval.set(time.Hour, 7)
	_, after := interval.current()
	require.Equal(t, before, after)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ticks := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval.run(ctx, nil, func() {
			select {
			case ticks <- struct{}{}:
			default:
			}
		})
	}()
	interval.set(5*time.Millisecond, 0)
	select {
	case <-ticks:
	case <-time.After(3 * time.Second):
		t.Fatal("running timer did not adopt the new interval")
	}
	cancel()
	<-done
}

func TestMachineListDenialInvalidatesCachedMembership(t *testing.T) {
	upstream := newMachinePanelServer(t)
	upstream.setNodes(machineNode{ID: 1, Type: "trojan"})
	for _, denied := range []string{"legacy", "header"} {
		t.Run(denied, func(t *testing.T) {
			var denial atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == machineNodesPath {
					switch denial.Load() {
					case 1:
						http.Error(w, "upstream unavailable", http.StatusForbidden)
						return
					case 2:
						if denied == "header" {
							w.Header().Set("X-Xboard-Machine-State", "unavailable")
						}
						w.WriteHeader(http.StatusForbidden)
						if denied == "legacy" {
							_, _ = w.Write([]byte(`{"message":"Machine not found or disabled"}`))
						}
						return
					}
				}
				upstream.server.Config.Handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			manager := &lockedInboundManager{}
			s, panel := newMachineTestService(t, manager, server.URL, server.Client())
			s.cacheFile = newTestCacheFile(t)
			s.refreshMachineNodes(panel, false)
			require.Len(t, manager.tags(), 1)
			denial.Store(1)
			s.refreshMachineNodes(panel, false)
			require.Len(t, manager.tags(), 1)
			denial.Store(2)
			s.refreshMachineNodes(panel, false)
			require.Empty(t, manager.tags())
			cached, loaded := s.loadMachineCache(panel)
			require.True(t, loaded)
			require.Empty(t, cached)
		})
	}
}
