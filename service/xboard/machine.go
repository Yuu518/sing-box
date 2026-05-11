package xboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"

	psnet "github.com/shirou/gopsutil/v4/net"
)

const (
	machineNodesPath  = "/api/v2/server/machine/nodes"
	machineStatusPath = "/api/v2/server/machine/status"
)

var errMachineUnavailable = errors.New("xboard machine not found or disabled")

var supportedNodeTypes = []string{
	C.TypeShadowsocks,
	C.TypeVMess,
	C.TypeVLESS,
	C.TypeTrojan,
	C.TypeHysteria,
	C.TypeHysteria2,
	C.TypeTUIC,
	C.TypeAnyTLS,
	C.TypeSOCKS,
	C.TypeHTTP,
	C.TypeNaive,
}

type machineNode struct {
	ID   int    `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
}

type cachedMachine struct {
	Panel     string        `json:"panel"`
	MachineID int           `json:"machine_id"`
	Nodes     []machineNode `json:"nodes"`
}

func (p *panel) logFields() string {
	return F.ToString("panel=", p.options.Tag, " machine_id=", p.machineID)
}

func machineCacheKey(panelTag string) string {
	return F.ToString(C.TypeXboard, "/", panelTag, "/machine")
}

func panelURL(managedPanel *panel, path string, credentials bool) (string, error) {
	baseURL, err := url.Parse(managedPanel.options.URL)
	if err != nil {
		return "", err
	}
	relative, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	requestURL := baseURL.ResolveReference(relative)
	if credentials {
		query := requestURL.Query()
		query.Set("token", managedPanel.options.Token)
		query.Set("machine_id", F.ToString(managedPanel.machineID))
		requestURL.RawQuery = query.Encode()
	}
	return requestURL.String(), nil
}

func (s *Service) machineRequest(ctx context.Context, managedPanel *panel, path string, body map[string]any, destination any) error {
	requestURL, err := panelURL(managedPanel, path, false)
	if err != nil {
		return err
	}
	if body == nil {
		body = make(map[string]any)
	}
	body["machine_id"] = managedPanel.machineID
	body["token"] = managedPanel.options.Token
	_, _, err = doRequest(ctx, managedPanel.client, http.MethodPost, requestURL, path, "", body, destination)
	return err
}

func (s *Service) fetchMachineNodes(ctx context.Context, managedPanel *panel) ([]machineNode, error) {
	var response struct {
		Nodes []machineNode `json:"nodes"`
		Sync  *syncPolicy   `json:"sync"`
		Base  syncPolicy    `json:"base_config"`
	}
	err := s.machineRequest(ctx, managedPanel, machineNodesPath, map[string]any{"config_format": 2}, &response)
	if err != nil {
		return nil, err
	}
	if err := validateMachineNodes(response.Nodes); err != nil {
		return nil, err
	}
	policy := response.Base
	if response.Sync != nil {
		policy = *response.Sync
	}
	managedPanel.schedule.configure(managedPanel.options, policy)
	return response.Nodes, nil
}

func validateMachineNodes(nodes []machineNode) error {
	if nodes == nil {
		return E.New("missing xboard nodes array")
	}
	seen := make(map[int]bool, len(nodes))
	for _, node := range nodes {
		if node.ID <= 0 || node.Type == "" || seen[node.ID] {
			return E.New("invalid xboard machine node: ", node.ID)
		}
		seen[node.ID] = true
	}
	return nil
}

func (s *Service) startMachine(managedPanel *panel) {
	managedPanel.schedule.configure(managedPanel.options, syncPolicy{})
	s.logger.Info("fetch machine nodes: ", managedPanel.logFields())
	s.refreshMachineNodes(managedPanel, true)
	go s.loopMachineNodes(managedPanel)
	go s.loopMachineStatus(managedPanel)
	if managedPanel.websocket {
		go s.loopWebSocketSession(s.ctx, managedPanel.logFields(), func() error {
			return s.runMachineWebSocket(s.ctx, managedPanel)
		})
	}
}

func (s *Service) loopMachineNodes(managedPanel *panel) {
	managedPanel.schedule.poll.run(s.ctx, managedPanel.nodesWake, func() {
		s.refreshMachineNodes(managedPanel, false)
	})
}

func (s *Service) refreshMachineNodes(managedPanel *panel, restoreCache bool) {
	nodes, err := s.fetchMachineNodes(s.ctx, managedPanel)
	if err != nil {
		if s.ctx.Err() != nil {
			return
		}
		s.recordAPIFailure()
		s.logger.Error(E.Cause(err, "fetch machine nodes ", managedPanel.logFields()))
		if errors.Is(err, errMachineUnavailable) {
			s.applyMachineNodes(managedPanel, []machineNode{}, true)
		} else if restoreCache {
			if cached, loaded := s.loadMachineCache(managedPanel); loaded {
				s.logger.Info("restored machine nodes from cache: ", managedPanel.logFields())
				s.applyMachineNodes(managedPanel, cached, false)
			}
		}
		return
	}
	s.recordAPISuccess()
	s.applyMachineNodes(managedPanel, nodes, true)
}

func (s *Service) applyMachineNodes(managedPanel *panel, nodes []machineNode, save bool) {
	managedPanel.nodesAccess.Lock()
	defer managedPanel.nodesAccess.Unlock()
	if s.ctx.Err() != nil {
		return
	}
	wanted := make(map[int]machineNode, len(nodes))
	for _, machineNode := range nodes {
		if machineNode.ID > 0 {
			wanted[machineNode.ID] = machineNode
		}
	}
	existing := make(map[int]bool)
	changed := false
	for _, managedNode := range s.listNodes(managedPanel) {
		nodeID := managedNode.options.NodeID
		wantedNode, loaded := wanted[nodeID]
		if loaded && wantedNode.Type == managedNode.options.NodeType {
			existing[nodeID] = true
			continue
		}
		s.removeNode(managedNode)
		changed = true
		if _, overridden := managedPanel.overrides[nodeID]; overridden && !loaded {
			s.logger.Warn("xboard node override is no longer used: ", managedNode.logFields())
		}
	}
	for nodeID := range managedPanel.skipped {
		if _, loaded := wanted[nodeID]; !loaded {
			delete(managedPanel.skipped, nodeID)
		}
	}
	for _, machineNode := range nodes {
		if machineNode.ID <= 0 || existing[machineNode.ID] {
			continue
		}
		if !slices.Contains(supportedNodeTypes, machineNode.Type) {
			if managedPanel.skipped[machineNode.ID] != machineNode.Type {
				managedPanel.skipped[machineNode.ID] = machineNode.Type
				s.logger.Warn("skipped unsupported node type: ", managedPanel.logFields(), " node_id=", machineNode.ID, " type=", machineNode.Type)
			}
			continue
		}
		delete(managedPanel.skipped, machineNode.ID)
		existing[machineNode.ID] = true
		managedNode, err := s.addMachineNode(managedPanel, machineNode)
		if err != nil {
			s.logger.Error(E.Cause(err, "add node ", managedPanel.logFields(), " node_id=", machineNode.ID))
			continue
		}
		changed = true
		s.startNode(managedNode)
	}
	if !managedPanel.nodesLoaded {
		managedPanel.nodesLoaded = true
		for nodeID, nodeOptions := range managedPanel.overrides {
			if _, loaded := wanted[nodeID]; !loaded {
				s.logger.Warn("xboard node override is not used: panel=", nodeOptions.Panel, " node_id=", nodeID, " inbound=", nodeOptions.Inbound)
			}
		}
		changed = true
	}
	if changed {
		s.logger.Info("machine nodes updated: ", managedPanel.logFields(), " nodes=", len(s.listNodes(managedPanel)))
	}
	if save {
		s.saveMachineCache(managedPanel, nodes)
	}
}

func (s *Service) addMachineNode(managedPanel *panel, machineNode machineNode) (*node, error) {
	nodeOptions, overridden := managedPanel.overrides[machineNode.ID]
	if !overridden {
		nodeOptions = option.XboardNodeOptions{
			Panel:  managedPanel.options.Tag,
			NodeID: machineNode.ID,
		}
	}
	if nodeOptions.Inbound == "" {
		nodeOptions.Inbound = defaultInboundTag(managedPanel.options.Tag, machineNode.ID)
	}
	nodeOptions.NodeType = machineNode.Type
	managedNode := s.newNode(managedPanel, nodeOptions)
	s.access.Lock()
	defer s.access.Unlock()
	if common.Any(s.nodes, func(otherNode *node) bool {
		return otherNode.options.Inbound == nodeOptions.Inbound
	}) {
		managedNode.cancel()
		return nil, E.New("duplicate xboard inbound mapping: ", nodeOptions.Inbound)
	}
	s.nodes = append(s.nodes, managedNode)
	return managedNode, nil
}

func (s *Service) removeNode(managedNode *node) {
	managedNode.closed.Store(true)
	if managedNode.cancel != nil {
		managedNode.cancel()
	}
	managedNode.syncAccess.Lock()
	defer managedNode.syncAccess.Unlock()
	inboundTag := managedNode.options.Inbound
	err := s.inboundManager.Remove(inboundTag)
	if err != nil && !errors.Is(err, os.ErrInvalid) {
		s.logger.Error(E.Cause(err, "remove inbound ", managedNode.logFields()))
	}
	s.tracker.closeRemovedUsers(inboundTag, nil)
	managedNode.reportAccess.Lock()
	if s.journal != nil {
		if _, stageErr := s.stageTraffic(managedNode); stageErr != nil {
			s.logger.Error(E.Cause(stageErr, "save removed node traffic ", managedNode.logFields()))
		}
	}
	s.tracker.unregister(inboundTag)
	managedNode.reportAccess.Unlock()
	s.access.Lock()
	managedNode.listenUnix = ""
	s.nodes = common.Filter(s.nodes, func(otherNode *node) bool {
		return otherNode != managedNode
	})
	s.access.Unlock()
	if s.cacheFile != nil {
		err = s.cacheFile.DeleteXboardNode(inboundTag)
		if err != nil {
			s.logger.Error(E.Cause(err, "delete node cache ", managedNode.logFields()))
		}
	}
	s.logger.Info("removed node: ", managedNode.logFields())
}

func (s *Service) findNode(managedPanel *panel, nodeID int) *node {
	s.access.Lock()
	defer s.access.Unlock()
	for _, managedNode := range s.nodes {
		if managedNode.panel == managedPanel && managedNode.options.NodeID == nodeID {
			return managedNode
		}
	}
	return nil
}

func (s *Service) loadMachineCache(managedPanel *panel) ([]machineNode, bool) {
	if s.cacheFile == nil {
		return nil, false
	}
	saved := s.cacheFile.LoadXboardNode(machineCacheKey(managedPanel.options.Tag))
	if saved == nil {
		return nil, false
	}
	if s.cacheMaxAge > 0 && time.Since(saved.LastUpdated) > s.cacheMaxAge {
		return nil, false
	}
	var cached cachedMachine
	err := json.Unmarshal(saved.Content, &cached)
	if err != nil {
		return nil, false
	}
	if cached.Panel != managedPanel.options.Tag || cached.MachineID != managedPanel.machineID {
		return nil, false
	}
	return cached.Nodes, true
}

func (s *Service) saveMachineCache(managedPanel *panel, nodes []machineNode) {
	if s.cacheFile == nil {
		return
	}
	content, err := json.Marshal(cachedMachine{
		Panel:     managedPanel.options.Tag,
		MachineID: managedPanel.machineID,
		Nodes:     nodes,
	})
	if err != nil {
		s.logger.Error(E.Cause(err, "encode machine cache ", managedPanel.logFields()))
		return
	}
	err = s.cacheFile.SaveXboardNode(machineCacheKey(managedPanel.options.Tag), &adapter.SavedBinary{
		Content:     content,
		LastUpdated: time.Now(),
	})
	if err != nil {
		s.logger.Error(E.Cause(err, "save machine cache ", managedPanel.logFields()))
	}
}

func (s *Service) buildNodeInbound(managedNode *node, config map[string]any, users []xboardUser) (option.Inbound, error) {
	build := inboundBuildOptions{
		listen:     managedNode.options.Listen,
		listenUnix: managedNode.options.ListenUnix,
		tls:        managedNode.options.TLS,
		transport:  managedNode.options.Transport,
	}
	if managedNode.panel != nil && managedNode.panel.machineID > 0 {
		build.machine = true
		build.machineTLS = managedNode.panel.tls
	}
	inboundOptions, warnings, err := buildManagedInboundOptions(managedNode.template, build, config, users)
	warning := strings.Join(warnings, "; ")
	s.access.Lock()
	changed := warning != managedNode.tlsWarning
	managedNode.tlsWarning = warning
	s.access.Unlock()
	if changed && warning != "" {
		s.logger.Warn(warning, ": ", managedNode.logFields())
	}
	return inboundOptions, err
}

func (s *Service) loopMachineStatus(managedPanel *panel) {
	managedPanel.network.sample()
	managedPanel.schedule.push.run(s.ctx, nil, func() {
		if err := s.reportMachineStatus(s.ctx, managedPanel); err != nil {
			if s.ctx.Err() == nil {
				s.recordAPIFailure()
				s.logger.Error(E.Cause(err, "report machine status ", managedPanel.logFields()))
			}
		} else {
			s.recordAPISuccess()
		}
	})
}

func (s *Service) reportMachineStatus(ctx context.Context, managedPanel *panel) error {
	return s.machineRequest(ctx, managedPanel, machineStatusPath, s.buildMachineStatus(managedPanel), nil)
}

func (s *Service) buildMachineStatus(managedPanel *panel) map[string]any {
	status := s.status.sample()
	body := map[string]any{
		"cpu":  status.CPU,
		"mem":  status.Mem,
		"swap": status.Swap,
		"disk": status.Disk,
	}
	inboundSpeed, outboundSpeed, loaded := managedPanel.network.sample()
	if loaded {
		body["net"] = map[string]any{
			"in_speed":  inboundSpeed,
			"out_speed": outboundSpeed,
		}
	}
	return body
}

type networkSampler struct {
	access   sync.Mutex
	read     func() (uint64, uint64, error)
	last     time.Time
	received uint64
	sent     uint64
}

func (n *networkSampler) sample() (float64, float64, bool) {
	n.access.Lock()
	defer n.access.Unlock()
	read := n.read
	if read == nil {
		read = readNetworkCounters
	}
	received, sent, err := read()
	if err != nil {
		return 0, 0, false
	}
	now := time.Now()
	lastTime, lastReceived, lastSent := n.last, n.received, n.sent
	n.last, n.received, n.sent = now, received, sent
	if lastTime.IsZero() {
		return 0, 0, false
	}
	seconds := now.Sub(lastTime).Seconds()
	if seconds <= 0 || received < lastReceived || sent < lastSent {
		return 0, 0, false
	}
	return float64(received-lastReceived) / seconds, float64(sent-lastSent) / seconds, true
}

func readNetworkCounters() (uint64, uint64, error) {
	counters, err := psnet.IOCounters(true)
	if err != nil {
		return 0, 0, err
	}
	var received, sent uint64
	for _, counter := range counters {
		name := strings.ToLower(counter.Name)
		if name == "lo" || strings.HasPrefix(name, "lo0") || strings.HasPrefix(name, "loopback") {
			continue
		}
		received += counter.BytesRecv
		sent += counter.BytesSent
	}
	return received, sent, nil
}

func (s *Service) runMachineWebSocket(ctx context.Context, managedPanel *panel) error {
	requestURL, err := panelURL(managedPanel, handshakePath, true)
	if err != nil {
		return err
	}
	wsURL, err := requestHandshake(ctx, managedPanel.client, requestURL)
	if err != nil {
		return err
	}
	if wsURL == "" {
		return errWebSocketDisabled
	}
	query := make(url.Values)
	query.Set("token", managedPanel.options.Token)
	query.Set("nodes_mode", "notify")
	query.Set("machine_id", F.ToString(managedPanel.machineID))
	return s.serveWebSocket(ctx, webSocketSession{
		client:    managedPanel.client,
		url:       wsURL,
		query:     query,
		connected: &managedPanel.wsConnected,
		name:      managedPanel.logFields(),
		handle: func(payload []byte, pong func() error) error {
			return s.handleMachineWebSocketMessage(managedPanel, payload, pong)
		},
		status: func() []map[string]any {
			return common.Map(s.listNodes(managedPanel), func(managedNode *node) map[string]any {
				status := s.buildWebSocketStatus(managedNode)
				status["node_id"] = managedNode.options.NodeID
				return status
			})
		},
	})
}

func (s *Service) handleMachineWebSocketMessage(managedPanel *panel, payload []byte, pong func() error) error {
	var message struct {
		Event string         `json:"event"`
		Data  map[string]any `json:"data"`
	}
	err := json.Unmarshal(payload, &message)
	if err != nil {
		return nil
	}
	switch message.Event {
	case "error":
		if messageText, loaded := stringValue(message.Data, "message"); loaded && messageText != "" {
			return E.New("websocket auth failed: ", messageText)
		}
		return E.New("websocket auth failed")
	case "ping":
		if pong != nil {
			return pong()
		}
	case "auth.success", "sync.nodes":
		select {
		case managedPanel.nodesWake <- struct{}{}:
		default:
		}
	case "sync.config", "sync.users", "sync.user.delta":
		nodeID, loaded := intValue(message.Data, "node_id")
		if !loaded {
			return nil
		}
		managedNode := s.findNode(managedPanel, nodeID)
		if managedNode != nil {
			s.scheduleNodeSync(s.nodeContext(managedNode), managedNode, message.Event)
		}
	}
	return nil
}

func (t *trafficTracker) unregister(inbound string) {
	var closeList []io.Closer
	t.access.Lock()
	for _, activeByConn := range t.active[inbound] {
		for conn := range activeByConn {
			closeList = append(closeList, conn)
		}
	}
	delete(t.nodes, inbound)
	delete(t.traffic, inbound)
	delete(t.online, inbound)
	delete(t.departed, inbound)
	delete(t.connectionTotals, inbound)
	delete(t.rates, inbound)
	delete(t.active, inbound)
	delete(t.limits, inbound)
	delete(t.devices, inbound)
	t.access.Unlock()
	for _, conn := range closeList {
		_ = conn.Close()
	}
}
