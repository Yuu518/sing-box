package xboard

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxService "github.com/sagernet/sing-box/adapter/service"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/coder/websocket"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"golang.org/x/time/rate"
)

const (
	defaultInterval = time.Minute
	// Cached nodes older than this are ignored on startup, so a box that has been
	// down for a long time does not serve a stale user list.
	defaultCacheMaxAge = 24 * time.Hour
	// Unchanged snapshots only refresh their timestamp this often, to avoid
	// rewriting the whole snapshot on every poll tick.
	cacheTouchInterval = 10 * time.Minute
)

var errWebSocketDisabled = E.New("websocket disabled")

func RegisterService(registry *boxService.Registry) {
	boxService.Register[option.XboardServiceOptions](registry, C.TypeXboard, NewService)
}

type Service struct {
	boxService.Adapter
	ctx            context.Context
	cancel         context.CancelFunc
	logger         log.ContextLogger
	logFactory     log.Factory
	router         adapter.Router
	inboundManager adapter.InboundManager
	cacheFile      adapter.CacheFile
	cacheMaxAge    time.Duration
	panels         map[string]*panel
	nodes          []*node
	tracker        *trafficTracker
	startTime      time.Time
	status         statusSampler
	access         sync.Mutex
	apiSuccess     atomic.Uint64
	apiFailure     atomic.Uint64
}

type panel struct {
	options      option.XboardPanelOptions
	client       *http.Client
	pollInterval time.Duration
	pushInterval time.Duration
	websocket    bool
}

type node struct {
	options       option.XboardNodeOptions
	panel         *panel
	template      option.Inbound
	configETag    string
	usersETag     string
	config        map[string]any
	users         []xboardUser
	pendingReload bool
	cacheSavedAt  time.Time
	syncAccess    sync.Mutex

	wsConnected atomic.Bool
}

type xboardUser struct {
	ID          int    `json:"id"`
	UUID        string `json:"uuid"`
	SpeedLimit  int    `json:"speed_limit"`
	DeviceLimit int    `json:"device_limit"`
}

type userResponse struct {
	Users []xboardUser `json:"users"`
}

// cachedNode is the snapshot persisted per node, so the inbound can be rebuilt
// locally on startup instead of waiting for the panel.
type cachedNode struct {
	Panel      string         `json:"panel"`
	NodeID     int            `json:"node_id"`
	ConfigETag string         `json:"config_etag,omitempty"`
	UsersETag  string         `json:"users_etag,omitempty"`
	Config     map[string]any `json:"config"`
	Users      []xboardUser   `json:"users"`
}

func NewService(ctx context.Context, logger log.ContextLogger, tag string, options option.XboardServiceOptions) (adapter.Service, error) {
	ctx, cancel := context.WithCancel(ctx)
	inboundManager := service.FromContext[adapter.InboundManager](ctx)
	router := service.FromContext[adapter.Router](ctx)
	logFactory := service.FromContext[log.Factory](ctx)
	if inboundManager == nil || router == nil {
		cancel()
		return nil, E.New("missing xboard dependencies")
	}
	if len(options.Panels) == 0 {
		cancel()
		return nil, E.New("missing xboard panels")
	}
	if len(options.Nodes) == 0 {
		cancel()
		return nil, E.New("missing xboard nodes")
	}
	cacheMaxAge := time.Duration(options.CacheMaxAge)
	if cacheMaxAge == 0 {
		cacheMaxAge = defaultCacheMaxAge
	}
	s := &Service{
		Adapter:        boxService.NewAdapter(C.TypeXboard, tag),
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		logFactory:     logFactory,
		router:         router,
		inboundManager: inboundManager,
		cacheMaxAge:    cacheMaxAge,
		panels:         make(map[string]*panel),
		tracker:        newTrafficTracker(),
		startTime:      time.Now(),
	}
	httpClientManager := service.FromContext[adapter.HTTPClientManager](ctx)
	for _, panelOptions := range options.Panels {
		if _, exists := s.panels[panelOptions.Tag]; exists {
			cancel()
			return nil, E.New("duplicate xboard panel tag: ", panelOptions.Tag)
		}
		transport := http.DefaultTransport
		if panelOptions.HTTPClient != nil {
			if httpClientManager == nil {
				cancel()
				return nil, E.New("missing http client manager")
			}
			resolvedTransport, err := httpClientManager.ResolveTransport(ctx, logger, *panelOptions.HTTPClient)
			if err != nil {
				cancel()
				return nil, E.Cause(err, "create xboard http client[", panelOptions.Tag, "]")
			}
			transport = resolvedTransport
		} else if httpClientManager != nil {
			if defaultTransport := httpClientManager.DefaultTransport(); defaultTransport != nil {
				transport = defaultTransport
			}
		}
		pollInterval := time.Duration(panelOptions.PollInterval)
		if pollInterval == 0 {
			pollInterval = defaultInterval
		}
		pushInterval := time.Duration(panelOptions.PushInterval)
		if pushInterval == 0 {
			pushInterval = defaultInterval
		}
		websocketEnabled := true
		if panelOptions.WebSocket != nil {
			websocketEnabled = *panelOptions.WebSocket
		}
		s.panels[panelOptions.Tag] = &panel{
			options:      panelOptions,
			client:       &http.Client{Transport: transport},
			pollInterval: pollInterval,
			pushInterval: pushInterval,
			websocket:    websocketEnabled,
		}
	}
	seenInbound := make(map[string]bool)
	seenListenUnix := make(map[string]bool)
	for _, nodeOptions := range options.Nodes {
		if nodeOptions.Inbound == "" {
			nodeOptions.Inbound = defaultInboundTag(nodeOptions.Panel, nodeOptions.NodeID)
		}
		nodePanel, loaded := s.panels[nodeOptions.Panel]
		if !loaded {
			cancel()
			return nil, E.New("xboard panel not found: ", nodeOptions.Panel)
		}
		if seenInbound[nodeOptions.Inbound] {
			cancel()
			return nil, E.New("duplicate xboard inbound mapping: ", nodeOptions.Inbound)
		}
		seenInbound[nodeOptions.Inbound] = true
		if nodeOptions.ListenUnix != "" {
			if nodeOptions.Listen != "" {
				cancel()
				return nil, E.New("xboard node listen conflicts with listen_unix: ", nodeOptions.Inbound)
			}
			if seenListenUnix[nodeOptions.ListenUnix] {
				cancel()
				return nil, E.New("duplicate xboard listen_unix: ", nodeOptions.ListenUnix)
			}
			seenListenUnix[nodeOptions.ListenUnix] = true
		}
		template := option.Inbound{
			Type: nodeType(nodeOptions.NodeType),
			Tag:  nodeOptions.Inbound,
		}
		s.nodes = append(s.nodes, &node{
			options:  nodeOptions,
			panel:    nodePanel,
			template: template,
		})
	}
	router.AppendTracker(s.tracker)
	return s, nil
}

func (s *Service) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	// The cache file is registered after services are created, so it can only be
	// resolved from here, not from NewService.
	s.cacheFile = service.FromContext[adapter.CacheFile](s.ctx)
	s.logger.Info("starting xboard service: nodes=", len(s.nodes))
	for _, managedNode := range s.nodes {
		if s.restoreNode(managedNode) {
			s.logger.Info("restored node from cache: ", managedNode.logFields())
			go s.syncNodeBackground(managedNode)
		} else {
			s.logger.Info("sync node: ", managedNode.logFields())
			err := s.syncNode(s.ctx, managedNode, true)
			if err != nil {
				err = E.Cause(err, "sync node ", managedNode.logFields())
				s.recordAPIFailure()
				s.logger.Error(err)
			} else {
				s.recordAPISuccess()
			}
		}
		go s.loopNode(managedNode)
		go s.loopReport(managedNode)
		if managedNode.panel.websocket {
			go s.loopWebSocket(managedNode)
		}
	}
	return nil
}

func defaultInboundTag(panel string, nodeID int) string {
	return F.ToString(C.TypeXboard, "/", panel, "/", nodeID)
}

func (n *node) logFields() string {
	return F.ToString("panel=", n.options.Panel, " node_id=", n.options.NodeID, " inbound=", n.options.Inbound)
}

func (s *Service) inboundLogger(inboundType string, tag string) log.ContextLogger {
	if s.logFactory == nil {
		return s.logger
	}
	return s.logFactory.NewLogger(F.ToString("inbound/", inboundType, "[", tag, "]"))
}

func (s *Service) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (s *Service) loopNode(managedNode *node) {
	ticker := time.NewTicker(managedNode.panel.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.syncNode(s.ctx, managedNode, false); err != nil {
				s.recordAPIFailure()
				s.logger.Error(E.Cause(err, "poll node ", managedNode.logFields()))
			} else {
				s.recordAPISuccess()
			}
		}
	}
}

func (s *Service) loopReport(managedNode *node) {
	ticker := time.NewTicker(managedNode.panel.pushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.reportNode(s.ctx, managedNode); err != nil {
				s.recordAPIFailure()
				s.logger.Error(E.Cause(err, "report node ", managedNode.logFields()))
			} else {
				s.recordAPISuccess()
			}
		}
	}
}

// syncNodeBackground refreshes a node restored from cache. It is not forced, so
// an unchanged panel answers 304 and the cached inbound is left untouched.
func (s *Service) syncNodeBackground(managedNode *node) {
	err := s.syncNode(s.ctx, managedNode, false)
	if err != nil {
		s.recordAPIFailure()
		s.logger.Error(E.Cause(err, "sync node ", managedNode.logFields()))
	} else {
		s.recordAPISuccess()
	}
}

// restoreNode brings a node up from the local cache without touching the
// network. It reports whether the inbound is now running.
func (s *Service) restoreNode(managedNode *node) bool {
	if s.cacheFile == nil {
		return false
	}
	saved := s.cacheFile.LoadXboardNode(managedNode.options.Inbound)
	cached, loaded := decodeCachedNode(saved, s.cacheMaxAge, managedNode.options.Panel, managedNode.options.NodeID)
	if !loaded {
		return false
	}
	managedNode.syncAccess.Lock()
	defer managedNode.syncAccess.Unlock()
	s.access.Lock()
	managedNode.config = cached.Config
	managedNode.users = cached.Users
	managedNode.configETag = cached.ConfigETag
	managedNode.usersETag = cached.UsersETag
	managedNode.cacheSavedAt = saved.LastUpdated
	s.access.Unlock()
	err := s.reloadNode(s.ctx, managedNode)
	if err == nil {
		return true
	}
	// Roll back, otherwise the leftover ETags would make the blocking fetch that
	// follows take the 304 branch and skip a node that never came up.
	s.access.Lock()
	managedNode.config = nil
	managedNode.users = nil
	managedNode.configETag = ""
	managedNode.usersETag = ""
	managedNode.cacheSavedAt = time.Time{}
	s.access.Unlock()
	s.logger.Warn(E.Cause(err, "restore node from cache ", managedNode.logFields()))
	return false
}

func decodeCachedNode(saved *adapter.SavedBinary, maxAge time.Duration, panel string, nodeID int) (*cachedNode, bool) {
	if saved == nil {
		return nil, false
	}
	if maxAge > 0 && time.Since(saved.LastUpdated) > maxAge {
		return nil, false
	}
	var cached cachedNode
	err := json.Unmarshal(saved.Content, &cached)
	if err != nil {
		return nil, false
	}
	// The cache is keyed by inbound tag, which may have been pointed at a
	// different panel node since it was written.
	if cached.Panel != panel || cached.NodeID != nodeID {
		return nil, false
	}
	// An empty user list means "remove the inbound" in reloadNode, so there is
	// nothing to restore.
	if len(cached.Users) == 0 {
		return nil, false
	}
	return &cached, true
}

// saveNodeCache persists the current snapshot. When nothing changed it only
// refreshes the timestamp, so a panel answering 304 for hours does not let the
// snapshot age out of cacheMaxAge.
func (s *Service) saveNodeCache(managedNode *node, changed bool) {
	if s.cacheFile == nil {
		return
	}
	now := time.Now()
	s.access.Lock()
	if !changed && now.Sub(managedNode.cacheSavedAt) < cacheTouchInterval {
		s.access.Unlock()
		return
	}
	cached := cachedNode{
		Panel:      managedNode.options.Panel,
		NodeID:     managedNode.options.NodeID,
		ConfigETag: managedNode.configETag,
		UsersETag:  managedNode.usersETag,
		Config:     cloneMap(managedNode.config),
		Users:      slices.Clone(managedNode.users),
	}
	s.access.Unlock()
	content, err := json.Marshal(cached)
	if err != nil {
		s.logger.Error(E.Cause(err, "encode node cache ", managedNode.logFields()))
		return
	}
	err = s.cacheFile.SaveXboardNode(managedNode.options.Inbound, &adapter.SavedBinary{
		Content:     content,
		LastUpdated: now,
	})
	if err != nil {
		s.logger.Error(E.Cause(err, "save node cache ", managedNode.logFields()))
		return
	}
	s.access.Lock()
	managedNode.cacheSavedAt = now
	s.access.Unlock()
}

func (s *Service) recordAPISuccess() {
	s.apiSuccess.Add(1)
}

func (s *Service) recordAPIFailure() {
	s.apiFailure.Add(1)
}

func (s *Service) syncNode(ctx context.Context, managedNode *node, force bool) error {
	managedNode.syncAccess.Lock()
	defer managedNode.syncAccess.Unlock()
	config, configETag, configChanged, err := s.fetchConfig(ctx, managedNode)
	if err != nil {
		return err
	}
	users, usersETag, usersChanged, err := s.fetchUsers(ctx, managedNode)
	if err != nil {
		return err
	}
	s.access.Lock()
	if configChanged {
		if reflect.DeepEqual(managedNode.config, config) {
			configChanged = false
		} else {
			managedNode.config = config
		}
		managedNode.configETag = configETag
	}
	if usersChanged {
		if reflect.DeepEqual(managedNode.users, users) {
			usersChanged = false
		} else {
			managedNode.users = users
		}
		managedNode.usersETag = usersETag
	}
	pendingReload := managedNode.pendingReload
	s.access.Unlock()
	switch {
	case force || configChanged || pendingReload:
		err = s.reloadNode(ctx, managedNode)
	case usersChanged:
		err = s.reloadNodeUsers(ctx, managedNode)
	default:
		s.saveNodeCache(managedNode, false)
		return nil
	}
	s.setPendingReload(managedNode, err != nil)
	if err != nil {
		return err
	}
	s.saveNodeCache(managedNode, configChanged || usersChanged)
	return nil
}

func (s *Service) setPendingReload(managedNode *node, pending bool) {
	s.access.Lock()
	managedNode.pendingReload = pending
	s.access.Unlock()
}

func (s *Service) reloadNode(ctx context.Context, managedNode *node) error {
	s.access.Lock()
	config := cloneMap(managedNode.config)
	users := slices.Clone(managedNode.users)
	s.access.Unlock()
	if len(users) == 0 {
		err := s.inboundManager.Remove(managedNode.options.Inbound)
		if err != nil && !errors.Is(err, os.ErrInvalid) {
			return E.Cause(err, "remove inbound without users")
		}
		s.tracker.closeRemovedUsers(managedNode.options.Inbound, nil)
		s.logger.Info("removed inbound without users: ", managedNode.logFields())
		return nil
	}
	inboundOptions, err := buildInboundOptions(managedNode.template, managedNode.options.Listen, managedNode.options.ListenUnix, managedNode.options.TLS, managedNode.options.Transport, config, users)
	if err != nil {
		return err
	}
	err = s.inboundManager.Remove(managedNode.options.Inbound)
	if err != nil && !errors.Is(err, os.ErrInvalid) {
		return E.Cause(err, "remove existing inbound")
	}
	err = s.inboundManager.Create(
		ctx,
		s.router,
		s.inboundLogger(inboundOptions.Type, managedNode.options.Inbound),
		managedNode.options.Inbound,
		inboundOptions.Type,
		inboundOptions.Options,
	)
	if err != nil {
		return err
	}
	s.tracker.register(managedNode.options.Inbound, managedNode.options.Panel, managedNode.options.NodeID)
	s.tracker.updateUserLimits(managedNode.options.Inbound, users)
	s.tracker.closeRemovedUsers(managedNode.options.Inbound, users)
	s.logger.Info("reloaded inbound: ", managedNode.logFields(), " type=", inboundOptions.Type, " users=", len(users))
	return nil
}

func (s *Service) reloadNodeUsers(ctx context.Context, managedNode *node) error {
	s.access.Lock()
	config := cloneMap(managedNode.config)
	users := slices.Clone(managedNode.users)
	s.access.Unlock()
	if len(users) == 0 {
		return s.reloadNode(ctx, managedNode)
	}
	inboundOptions, err := buildInboundOptions(managedNode.template, managedNode.options.Listen, managedNode.options.ListenUnix, managedNode.options.TLS, managedNode.options.Transport, config, users)
	if err != nil {
		return err
	}
	inbound, loaded := s.inboundManager.Get(managedNode.options.Inbound)
	if loaded {
		if userUpdater, isUserUpdater := inbound.(interface {
			UpdateUsers(options any) error
		}); isUserUpdater {
			err = userUpdater.UpdateUsers(inboundOptions.Options)
			if err != nil {
				return err
			}
			s.tracker.register(managedNode.options.Inbound, managedNode.options.Panel, managedNode.options.NodeID)
			s.tracker.updateUserLimits(managedNode.options.Inbound, users)
			s.tracker.closeRemovedUsers(managedNode.options.Inbound, users)
			s.logger.Info("reloaded users: ", managedNode.logFields(), " type=", inbound.Type(), " users=", len(users))
			return nil
		}
		if managedSSMServer, isManagedSSMServer := inbound.(adapter.ManagedSSMServer); isManagedSSMServer {
			if shadowsocksOptions, isShadowsocksOptions := inboundOptions.Options.(*option.ShadowsocksInboundOptions); isShadowsocksOptions {
				err = managedSSMServer.UpdateUsers(common.Map(shadowsocksOptions.Users, func(user option.ShadowsocksUser) string {
					return user.Name
				}), common.Map(shadowsocksOptions.Users, func(user option.ShadowsocksUser) string {
					return user.Password
				}))
				if err != nil {
					return err
				}
				s.tracker.register(managedNode.options.Inbound, managedNode.options.Panel, managedNode.options.NodeID)
				s.tracker.updateUserLimits(managedNode.options.Inbound, users)
				s.tracker.closeRemovedUsers(managedNode.options.Inbound, users)
				s.logger.Info("reloaded users: ", managedNode.logFields(), " type=", inbound.Type(), " users=", len(users))
				return nil
			}
		}
	}
	return s.reloadNode(ctx, managedNode)
}

func (s *Service) fetchConfig(ctx context.Context, managedNode *node) (map[string]any, string, bool, error) {
	var response map[string]any
	etag, changed, err := s.doJSON(ctx, managedNode, http.MethodGet, "/api/v2/server/config", managedNode.configETag, nil, &response)
	if err != nil || !changed {
		return nil, etag, changed, err
	}
	if baseConfig, loaded := response["base_config"].(map[string]any); loaded {
		delete(response, "base_config")
		maps.Copy(response, baseConfig)
	}
	return response, etag, true, nil
}

func (s *Service) fetchUsers(ctx context.Context, managedNode *node) ([]xboardUser, string, bool, error) {
	var response userResponse
	etag, changed, err := s.doJSON(ctx, managedNode, http.MethodGet, "/api/v2/server/user", managedNode.usersETag, nil, &response)
	if err != nil || !changed {
		return nil, etag, changed, err
	}
	return response.Users, etag, true, nil
}

func (s *Service) doJSON(ctx context.Context, managedNode *node, method string, path string, etag string, body any, destination any) (string, bool, error) {
	requestURL, err := xboardURL(managedNode, path)
	if err != nil {
		return "", false, err
	}
	var bodyReader *bytes.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return "", false, err
		}
		bodyReader = bytes.NewReader(bodyBytes)
	} else {
		bodyReader = bytes.NewReader(nil)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
	if err != nil {
		return "", false, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	response, err := managedNode.panel.client.Do(request)
	if err != nil {
		return "", false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified {
		return etag, false, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return "", false, E.New(method, " ", path, " unexpected status: ", response.Status, ": ", string(responseBody))
	}
	if destination != nil {
		err = json.NewDecoder(response.Body).Decode(destination)
		if err != nil {
			return "", false, E.Cause(err, method, " ", path, " decode response")
		}
	}
	return response.Header.Get("ETag"), true, nil
}

func (s *Service) reportNode(ctx context.Context, managedNode *node) error {
	traffic := s.tracker.readTraffic(managedNode.options.Inbound)
	alive := s.tracker.readAlive(managedNode.options.Inbound, true)
	body := s.buildNodeReport(managedNode, traffic, alive)
	_, _, err := s.doJSON(ctx, managedNode, http.MethodPost, "/api/v2/server/report", "", body, nil)
	if err != nil {
		s.tracker.addTraffic(managedNode.options.Inbound, traffic)
		s.tracker.restoreAlive(managedNode.options.Inbound, alive)
		return err
	}
	var upload, download int64
	for _, value := range traffic {
		if len(value) == 2 {
			upload += value[0]
			download += value[1]
		}
	}
	s.logger.Info("reported traffic: ", managedNode.logFields(), " users=", len(traffic), " upload=", upload, " download=", download)
	return nil
}

func (s *Service) buildNodeReport(managedNode *node, traffic map[string][2]int64, alive map[int][]string) map[string]any {
	online := s.tracker.readOnline(managedNode.options.Inbound)
	status := s.status.sample()
	return map[string]any{
		"traffic": traffic,
		"online":  online,
		"alive":   alive,
		"status":  status,
		"metrics": s.buildNodeMetrics(managedNode, online),
	}
}

func (s *Service) buildNodeMetrics(managedNode *node, online map[int]int) map[string]any {
	uploadSpeed, downloadSpeed := s.tracker.readTrafficRate(managedNode.options.Inbound)
	s.access.Lock()
	totalUsers := len(managedNode.users)
	s.access.Unlock()
	return map[string]any{
		"uptime":             int64(time.Since(s.startTime).Seconds()),
		"goroutines":         runtime.NumGoroutine(),
		"active_connections": totalOnline(online),
		"total_connections":  s.tracker.totalConnections(managedNode.options.Inbound),
		"active_users":       len(online),
		"total_users":        totalUsers,
		"inbound_speed":      downloadSpeed,
		"outbound_speed":     uploadSpeed,
		"api": map[string]any{
			"success": s.apiSuccess.Load(),
			"failure": s.apiFailure.Load(),
		},
		"ws": map[string]any{
			"enabled":   managedNode.panel.websocket,
			"connected": managedNode.wsConnected.Load(),
		},
		"kernel_status": s.kernelStatus(managedNode),
	}
}

func (s *Service) buildWebSocketStatus(managedNode *node) map[string]any {
	return s.buildNodeMetrics(managedNode, s.tracker.readOnline(managedNode.options.Inbound))
}

func (s *Service) kernelStatus(managedNode *node) bool {
	if s.ctx.Err() != nil {
		return false
	}
	if s.inboundManager == nil {
		return false
	}
	_, loaded := s.inboundManager.Get(managedNode.options.Inbound)
	return loaded
}

type statusUsage struct {
	Total uint64 `json:"total"`
	Used  uint64 `json:"used"`
}

type nodeStatus struct {
	CPU  float64     `json:"cpu"`
	Mem  statusUsage `json:"mem"`
	Swap statusUsage `json:"swap"`
	Disk statusUsage `json:"disk"`
}

const statusSampleInterval = 10 * time.Second

type statusSampler struct {
	access sync.Mutex
	last   time.Time
	status nodeStatus
}

func (s *statusSampler) sample() nodeStatus {
	s.access.Lock()
	defer s.access.Unlock()
	now := time.Now()
	if !s.last.IsZero() && now.Sub(s.last) < statusSampleInterval {
		return s.status
	}
	s.status = runtimeStatus()
	s.last = now
	return s.status
}

func runtimeStatus() nodeStatus {
	var status nodeStatus
	if cpuPercent, err := cpu.Percent(0, false); err == nil && len(cpuPercent) > 0 {
		status.CPU = cpuPercent[0]
	}
	if memoryStat, err := mem.VirtualMemory(); err == nil {
		status.Mem = statusUsage{
			Total: memoryStat.Total,
			Used:  memoryStat.Used,
		}
	}
	if swapStat, err := mem.SwapMemory(); err == nil {
		status.Swap = statusUsage{
			Total: swapStat.Total,
			Used:  swapStat.Used,
		}
	}
	if diskStat, err := disk.Usage(diskStatusPath()); err == nil {
		status.Disk = statusUsage{
			Total: diskStat.Total,
			Used:  diskStat.Used,
		}
	}
	return status
}

func diskStatusPath() string {
	workingDir, err := os.Getwd()
	if err != nil {
		return string(filepath.Separator)
	}
	volume := filepath.VolumeName(workingDir)
	if volume == "" {
		return string(filepath.Separator)
	}
	return volume + string(filepath.Separator)
}

func totalOnline(online map[int]int) int64 {
	var total int64
	for _, value := range online {
		if value > 0 {
			total += int64(value)
		}
	}
	return total
}

func xboardURL(managedNode *node, path string) (string, error) {
	baseURL, err := url.Parse(managedNode.panel.options.URL)
	if err != nil {
		return "", err
	}
	relative, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	requestURL := baseURL.ResolveReference(relative)
	query := requestURL.Query()
	query.Set("token", managedNode.panel.options.Token)
	query.Set("node_id", F.ToString(managedNode.options.NodeID))
	if managedNode.options.NodeType != "" {
		query.Set("node_type", managedNode.options.NodeType)
	}
	requestURL.RawQuery = query.Encode()
	return requestURL.String(), nil
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	maps.Copy(output, input)
	return output
}

func buildInboundOptions(template option.Inbound, listen string, listenUnix string, manualTLS *option.InboundTLSOptions, manualTransport *option.V2RayTransportOptions, config map[string]any, users []xboardUser) (option.Inbound, error) {
	inboundOptions := template
	finalType := inboundOptions.Type
	if protocol, loaded := stringValue(config, "protocol"); loaded && protocol != "" {
		if protocol == "hysteria" {
			if version, _ := intValue(config, "version"); version == 2 {
				finalType = C.TypeHysteria2
			} else {
				finalType = C.TypeHysteria
			}
		} else {
			finalType = nodeType(protocol)
		}
	}
	if finalType == "" {
		return inboundOptions, E.New("missing xboard inbound type")
	}
	inboundOptions.Type = finalType
	if inboundOptions.Options != nil {
		if template.Type != "" && template.Type != finalType {
			return inboundOptions, E.New("xboard inbound type mismatch: template=", template.Type, " panel=", finalType)
		}
		inboundOptions.Options = cloneOptions(template.Options)
	} else {
		options, err := defaultInboundOptions(inboundOptions.Type, listen, listenUnix)
		if err != nil {
			return inboundOptions, err
		}
		inboundOptions.Options = options
	}
	// server_port is the port the panel advertises to clients, which has no
	// relation to the local socket when listening on a UNIX socket.
	if listenUnix == "" {
		if port, loaded := intValue(config, "server_port"); loaded && port > 0 && port <= 65535 {
			setUint16Field(inboundOptions.Options, "ListenPort", uint16(port))
		}
	}
	applyTLSConfig(inboundOptions.Options, config)
	applyManualTLSConfig(inboundOptions.Options, manualTLS)
	applyTransportConfig(inboundOptions.Options, manualTransport, config)
	return inboundOptions, applyUsersAndConfig(inboundOptions.Type, inboundOptions.Options, config, users)
}

func nodeType(protocol string) string {
	if protocol == "hysteria" {
		return C.TypeHysteria
	}
	return protocol
}

func defaultInboundOptions(inboundType string, listenString string, listenUnix string) (any, error) {
	var listenOptions option.ListenOptions
	if listenUnix != "" {
		switch inboundType {
		case C.TypeTrojan, C.TypeVMess:
		default:
			return nil, E.New("listen_unix is not supported by xboard inbound type: ", inboundType)
		}
		listenOptions.ListenUnix = listenUnix
	} else {
		listen := netip.IPv6Unspecified()
		if listenString != "" {
			listenAddr, err := netip.ParseAddr(listenString)
			if err != nil {
				return nil, E.Cause(err, "parse xboard listen")
			}
			listen = listenAddr
		}
		listenOptions.Listen = (*badoption.Addr)(&listen)
	}
	switch inboundType {
	case C.TypeShadowsocks:
		return &option.ShadowsocksInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeVMess:
		return &option.VMessInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeVLESS:
		return &option.VLESSInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeTrojan:
		return &option.TrojanInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeHysteria:
		return &option.HysteriaInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeHysteria2:
		return &option.Hysteria2InboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeTUIC:
		return &option.TUICInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeAnyTLS:
		return &option.AnyTLSInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeSOCKS:
		return &option.SocksInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeHTTP:
		return &option.HTTPMixedInboundOptions{ListenOptions: listenOptions}, nil
	case C.TypeNaive:
		return &option.NaiveInboundOptions{ListenOptions: listenOptions}, nil
	default:
		return nil, E.New("unsupported xboard inbound type: ", inboundType)
	}
}

func cloneOptions(options any) any {
	value := reflect.ValueOf(options)
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Ptr {
		cloned := reflect.New(value.Elem().Type())
		cloned.Elem().Set(value.Elem())
		return cloned.Interface()
	}
	cloned := reflect.New(value.Type())
	cloned.Elem().Set(value)
	return cloned.Interface()
}

func applyUsersAndConfig(inboundType string, options any, config map[string]any, users []xboardUser) error {
	switch inboundType {
	case C.TypeShadowsocks:
		typedOptions := options.(*option.ShadowsocksInboundOptions)
		if method, loaded := stringValue(config, "cipher"); loaded && method != "" {
			typedOptions.Method = method
		}
		if serverKey, loaded := stringValue(config, "server_key"); loaded && serverKey != "" {
			typedOptions.Password = serverKey
		}
		serverKeySize, userKeySize, is2022 := shadowsocks2022KeySize(typedOptions.Method)
		if is2022 {
			typedOptions.Password = base64Key(typedOptions.Password, serverKeySize)
		}
		typedOptions.Users = common.Map(users, func(user xboardUser) option.ShadowsocksUser {
			password := user.UUID
			if is2022 {
				password = base64Key(password, userKeySize)
			}
			return option.ShadowsocksUser{Name: F.ToString(user.ID), Password: password}
		})
	case C.TypeVMess:
		typedOptions := options.(*option.VMessInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) option.VMessUser {
			return option.VMessUser{Name: F.ToString(user.ID), UUID: user.UUID}
		})
	case C.TypeVLESS:
		typedOptions := options.(*option.VLESSInboundOptions)
		flow, _ := stringValue(config, "flow")
		typedOptions.Users = common.Map(users, func(user xboardUser) option.VLESSUser {
			return option.VLESSUser{Name: F.ToString(user.ID), UUID: user.UUID, Flow: flow}
		})
	case C.TypeTrojan:
		typedOptions := options.(*option.TrojanInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) option.TrojanUser {
			return option.TrojanUser{Name: F.ToString(user.ID), Password: user.UUID}
		})
	case C.TypeHysteria:
		typedOptions := options.(*option.HysteriaInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) option.HysteriaUser {
			return option.HysteriaUser{Name: F.ToString(user.ID), AuthString: user.UUID}
		})
	case C.TypeHysteria2:
		typedOptions := options.(*option.Hysteria2InboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) option.Hysteria2User {
			return option.Hysteria2User{Name: F.ToString(user.ID), Password: user.UUID}
		})
	case C.TypeTUIC:
		typedOptions := options.(*option.TUICInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) option.TUICUser {
			return option.TUICUser{Name: F.ToString(user.ID), UUID: user.UUID, Password: user.UUID}
		})
	case C.TypeAnyTLS:
		typedOptions := options.(*option.AnyTLSInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) option.AnyTLSUser {
			return option.AnyTLSUser{Name: F.ToString(user.ID), Password: user.UUID}
		})
	case C.TypeSOCKS:
		typedOptions := options.(*option.SocksInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) auth.User {
			return auth.User{Username: F.ToString(user.ID), Password: user.UUID}
		})
	case C.TypeHTTP:
		typedOptions := options.(*option.HTTPMixedInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) auth.User {
			return auth.User{Username: F.ToString(user.ID), Password: user.UUID}
		})
	case C.TypeNaive:
		typedOptions := options.(*option.NaiveInboundOptions)
		typedOptions.Users = common.Map(users, func(user xboardUser) auth.User {
			return auth.User{Username: F.ToString(user.ID), Password: user.UUID}
		})
	default:
		return E.New("unsupported xboard inbound type: ", inboundType)
	}
	return nil
}

func shadowsocks2022KeySize(method string) (int, int, bool) {
	switch method {
	case "2022-blake3-aes-128-gcm":
		return 16, 16, true
	case "2022-blake3-aes-256-gcm", "2022-blake3-chacha20-poly1305":
		return 32, 32, true
	default:
		return 0, 0, false
	}
}

func base64Key(value string, size int) string {
	if size <= 0 || value == "" {
		return value
	}
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil && len(decoded) == size {
		return value
	}
	if len(value) > size {
		value = value[:size]
	}
	return base64.StdEncoding.EncodeToString([]byte(value))
}

func applyTLSConfig(options any, config map[string]any) {
	tlsOptions, loaded := options.(option.InboundTLSOptionsWrapper)
	if !loaded {
		return
	}
	tlsValue := tlsOptions.TakeInboundTLSOptions()
	if tlsValue == nil {
		tlsValue = new(option.InboundTLSOptions)
	}
	enabled := false
	if tlsMode, loaded := intValue(config, "tls"); loaded && tlsMode > 0 {
		enabled = true
	}
	tlsSettings, hasTLSSettings := mapValue(config, "tls_settings")
	if !hasTLSSettings {
		tlsSettings, hasTLSSettings = mapValue(config, "tls")
	}
	if serverName, loaded := stringValue(config, "server_name"); loaded && serverName != "" {
		tlsValue.ServerName = serverName
		enabled = true
	}
	if hasTLSSettings {
		if serverName, loaded := stringValue(tlsSettings, "server_name"); loaded && serverName != "" {
			tlsValue.ServerName = serverName
		}
		if insecure, loaded := boolValue(tlsSettings, "allow_insecure"); loaded {
			tlsValue.Insecure = insecure
		}
		enabled = true
	}
	if certConfig, loaded := mapValue(config, "cert_config"); loaded {
		if certificatePath, loaded := firstStringValue(certConfig, "certificate_path", "cert_path", "cert_file", "fullchain_path", "fullchain"); loaded {
			tlsValue.CertificatePath = certificatePath
			enabled = true
		}
		if keyPath, loaded := firstStringValue(certConfig, "key_path", "private_key_path", "key_file", "private_key_file"); loaded {
			tlsValue.KeyPath = keyPath
			enabled = true
		}
	}
	if enabled {
		tlsValue.Enabled = true
		tlsOptions.ReplaceInboundTLSOptions(tlsValue)
	}
}

func applyManualTLSConfig(options any, manualTLS *option.InboundTLSOptions) {
	if manualTLS == nil {
		return
	}
	tlsOptions, loaded := options.(option.InboundTLSOptionsWrapper)
	if !loaded {
		return
	}
	tlsValue := *manualTLS
	tlsOptions.ReplaceInboundTLSOptions(&tlsValue)
}

func applyTransportConfig(options any, manualTransport *option.V2RayTransportOptions, config map[string]any) {
	if manualTransport != nil {
		setTransport(options, manualTransport)
		return
	}
	transport := transportFromConfig(config)
	if transport != nil {
		setTransport(options, transport)
	}
}

func setTransport(options any, transport *option.V2RayTransportOptions) {
	value := reflect.Indirect(reflect.ValueOf(options))
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return
	}
	field := value.FieldByName("Transport")
	if field.IsValid() && field.CanSet() && field.Kind() == reflect.Ptr && field.Type() == reflect.TypeFor[*option.V2RayTransportOptions]() {
		transportValue := *transport
		field.Set(reflect.ValueOf(&transportValue))
	}
}

func transportFromConfig(config map[string]any) *option.V2RayTransportOptions {
	network, loaded := stringValue(config, "network")
	if !loaded || network == "" {
		return nil
	}
	networkSettings, _ := mapValue(config, "networkSettings")
	if networkSettings == nil {
		networkSettings, _ = mapValue(config, "network_settings")
	}
	switch network {
	case "tcp":
		header, _ := mapValue(networkSettings, "header")
		if headerType, _ := stringValue(header, "type"); headerType != "http" {
			return nil
		}
		transport := &option.V2RayTransportOptions{Type: C.V2RayTransportTypeHTTP}
		request, _ := mapValue(header, "request")
		if paths, loaded := stringListValue(request, "path"); loaded && len(paths) > 0 {
			transport.HTTPOptions.Path = paths[0]
		}
		if headers, loaded := mapValue(request, "headers"); loaded {
			if hosts, loaded := stringListValue(headers, "Host"); loaded {
				transport.HTTPOptions.Host = hosts
			}
		}
		return transport
	case "ws", "websocket":
		transport := &option.V2RayTransportOptions{Type: C.V2RayTransportTypeWebsocket}
		if path, loaded := stringValue(networkSettings, "path"); loaded {
			transport.WebsocketOptions.Path = path
		}
		if headers, loaded := mapValue(networkSettings, "headers"); loaded {
			if host, loaded := stringValue(headers, "Host"); loaded && host != "" {
				transport.WebsocketOptions.Headers = badoption.HTTPHeader{"Host": badoption.Listable[string]{host}}
			}
		}
		return transport
	case "h2", "http":
		transport := &option.V2RayTransportOptions{Type: C.V2RayTransportTypeHTTP}
		if path, loaded := stringValue(networkSettings, "path"); loaded {
			transport.HTTPOptions.Path = path
		}
		if hosts, loaded := stringListValue(networkSettings, "host"); loaded {
			transport.HTTPOptions.Host = hosts
		}
		return transport
	case "quic":
		return &option.V2RayTransportOptions{Type: C.V2RayTransportTypeQUIC}
	case "grpc":
		transport := &option.V2RayTransportOptions{Type: C.V2RayTransportTypeGRPC}
		if serviceName, loaded := stringValue(networkSettings, "serviceName"); loaded {
			transport.GRPCOptions.ServiceName = serviceName
		}
		return transport
	case "httpupgrade":
		transport := &option.V2RayTransportOptions{Type: C.V2RayTransportTypeHTTPUpgrade}
		if path, loaded := stringValue(networkSettings, "path"); loaded {
			transport.HTTPUpgradeOptions.Path = path
		}
		if host, loaded := stringValue(networkSettings, "host"); loaded {
			transport.HTTPUpgradeOptions.Host = host
		}
		return transport
	default:
		return nil
	}
}

func setUint16Field(options any, name string, value uint16) {
	field := reflect.Indirect(reflect.ValueOf(options)).FieldByName(name)
	if field.IsValid() && field.CanSet() && field.Kind() == reflect.Uint16 {
		field.SetUint(uint64(value))
	}
}

func stringValue(config map[string]any, key string) (string, bool) {
	value, loaded := config[key]
	if !loaded || value == nil {
		return "", false
	}
	switch typedValue := value.(type) {
	case string:
		return typedValue, true
	default:
		return F.ToString(typedValue), true
	}
}

func firstStringValue(config map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		if value, loaded := stringValue(config, key); loaded && value != "" {
			return value, true
		}
	}
	return "", false
}

func stringListValue(config map[string]any, key string) ([]string, bool) {
	value, loaded := config[key]
	if !loaded || value == nil {
		return nil, false
	}
	switch typedValue := value.(type) {
	case []string:
		return typedValue, true
	case []any:
		values := make([]string, 0, len(typedValue))
		for _, value := range typedValue {
			if value != nil {
				values = append(values, F.ToString(value))
			}
		}
		return values, true
	case string:
		return []string{typedValue}, true
	default:
		return []string{F.ToString(typedValue)}, true
	}
}

func mapValue(config map[string]any, key string) (map[string]any, bool) {
	value, loaded := config[key]
	if !loaded || value == nil {
		return nil, false
	}
	switch typedValue := value.(type) {
	case map[string]any:
		return typedValue, true
	case map[string]string:
		output := make(map[string]any, len(typedValue))
		for key, value := range typedValue {
			output[key] = value
		}
		return output, true
	default:
		return nil, false
	}
}

func boolValue(config map[string]any, key string) (bool, bool) {
	value, loaded := config[key]
	if !loaded || value == nil {
		return false, false
	}
	switch typedValue := value.(type) {
	case bool:
		return typedValue, true
	case string:
		boolValue, err := strconv.ParseBool(typedValue)
		return boolValue, err == nil
	default:
		intValue, loaded := intValue(config, key)
		return intValue != 0, loaded
	}
}

func intValue(config map[string]any, key string) (int, bool) {
	value, loaded := config[key]
	if !loaded || value == nil {
		return 0, false
	}
	switch typedValue := value.(type) {
	case int:
		return typedValue, true
	case float64:
		return int(typedValue), true
	case json.Number:
		intValue, err := typedValue.Int64()
		return int(intValue), err == nil
	case string:
		intValue, err := strconv.Atoi(typedValue)
		return intValue, err == nil
	default:
		return 0, false
	}
}

func (s *Service) loopWebSocket(managedNode *node) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		err := s.runWebSocket(s.ctx, managedNode)
		if errors.Is(err, errWebSocketDisabled) {
			s.logger.Info("websocket disabled, using polling: ", managedNode.logFields())
			return
		}
		if err != nil && !E.IsClosedOrCanceled(err) {
			s.logger.Warn("websocket disconnected: ", managedNode.logFields(), ": ", err)
		}
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (s *Service) runWebSocket(ctx context.Context, managedNode *node) error {
	wsURL, err := s.handshake(ctx, managedNode)
	if err != nil {
		return err
	}
	if wsURL == "" {
		return errWebSocketDisabled
	}
	parsedURL, err := url.Parse(wsURL)
	if err != nil {
		return err
	}
	query := parsedURL.Query()
	query.Set("token", managedNode.panel.options.Token)
	query.Set("node_id", F.ToString(managedNode.options.NodeID))
	parsedURL.RawQuery = query.Encode()
	conn, _, err := websocket.Dial(ctx, parsedURL.String(), &websocket.DialOptions{
		HTTPClient: managedNode.panel.client,
	})
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	_, firstPayload, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if err = s.handleWebSocketMessage(ctx, managedNode, firstPayload, func() error {
		return wsWriteJSON(ctx, conn, map[string]any{"event": "pong"})
	}); err != nil {
		return err
	}
	managedNode.wsConnected.Store(true)
	defer managedNode.wsConnected.Store(false)
	s.logger.Info("websocket connected: ", managedNode.logFields())
	messageCh := make(chan []byte, 16)
	errCh := make(chan error, 1)
	go func() {
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case messageCh <- payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case payload := <-messageCh:
			err = s.handleWebSocketMessage(ctx, managedNode, payload, func() error {
				return wsWriteJSON(ctx, conn, map[string]any{"event": "pong"})
			})
			if err != nil {
				return err
			}
		case <-statusTicker.C:
			err = wsWriteJSON(ctx, conn, map[string]any{
				"event":     "node.status",
				"data":      s.buildWebSocketStatus(managedNode),
				"timestamp": time.Now().Unix(),
			})
			if err != nil {
				return err
			}
		}
	}
}

func (s *Service) handleWebSocketMessage(ctx context.Context, managedNode *node, payload []byte, pong func() error) error {
	var message struct {
		Event string         `json:"event"`
		Data  map[string]any `json:"data"`
	}
	err := json.Unmarshal(payload, &message)
	if err != nil {
		return nil
	}
	switch message.Event {
	case "auth.success":
		return nil
	case "error":
		if messageText, loaded := stringValue(message.Data, "message"); loaded && messageText != "" {
			return E.New("websocket auth failed: ", messageText)
		}
		return E.New("websocket auth failed")
	case "ping":
		if pong != nil {
			return pong()
		}
	case "sync.config", "sync.users", "sync.user.delta":
		go s.syncNodeFromWebSocket(ctx, managedNode, message.Event)
	}
	return nil
}

func (s *Service) syncNodeFromWebSocket(ctx context.Context, managedNode *node, event string) {
	err := s.syncNode(ctx, managedNode, false)
	if err != nil {
		s.recordAPIFailure()
		s.logger.Warn("websocket ", event, " pull sync failed: ", managedNode.logFields(), ": ", err)
		return
	}
	s.recordAPISuccess()
}

func (s *Service) handshake(ctx context.Context, managedNode *node) (string, error) {
	var response struct {
		WebSocket struct {
			Enabled bool   `json:"enabled"`
			URL     string `json:"ws_url"`
		} `json:"websocket"`
	}
	_, _, err := s.doJSON(ctx, managedNode, http.MethodPost, "/api/v2/server/handshake", "", nil, &response)
	if err != nil {
		return "", err
	}
	if !response.WebSocket.Enabled {
		return "", nil
	}
	return response.WebSocket.URL, nil
}

func wsWriteJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	content, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, content)
}

var _ adapter.ConnectionTracker = (*trafficTracker)(nil)

type trafficTracker struct {
	access           sync.Mutex
	nodes            map[string]nodeIdentity
	traffic          map[string]map[string]*userTraffic
	online           map[string]map[string]*atomic.Int64
	alive            map[string]map[int]map[string]struct{}
	connectionTotals map[string]*atomic.Int64
	rates            map[string]*trafficRate
	active           map[string]map[string]map[io.Closer]struct{}
	limits           map[string]map[string]*userLimit
	devices          map[string]map[string]map[string]int
}

type nodeIdentity struct {
	panel  string
	nodeID int
}

type userTraffic struct {
	upload        atomic.Int64
	download      atomic.Int64
	uploadTotal   atomic.Int64
	downloadTotal atomic.Int64
}

type trafficRate struct {
	lastTime          time.Time
	lastUploadTotal   int64
	lastDownloadTotal int64
	uploadSpeed       int64
	downloadSpeed     int64
}

type userLimit struct {
	speedBytes      atomic.Int64
	deviceLimit     int
	uploadLimiter   atomic.Pointer[rate.Limiter]
	downloadLimiter atomic.Pointer[rate.Limiter]
}

type userDeviceLease struct {
	tracker *trafficTracker
	inbound string
	user    string
	ip      string
	once    sync.Once
}

func newTrafficTracker() *trafficTracker {
	return &trafficTracker{
		nodes:            make(map[string]nodeIdentity),
		traffic:          make(map[string]map[string]*userTraffic),
		online:           make(map[string]map[string]*atomic.Int64),
		alive:            make(map[string]map[int]map[string]struct{}),
		connectionTotals: make(map[string]*atomic.Int64),
		rates:            make(map[string]*trafficRate),
		active:           make(map[string]map[string]map[io.Closer]struct{}),
		limits:           make(map[string]map[string]*userLimit),
		devices:          make(map[string]map[string]map[string]int),
	}
}

func (t *trafficTracker) register(inbound string, panel string, nodeID int) {
	t.access.Lock()
	t.nodes[inbound] = nodeIdentity{panel: panel, nodeID: nodeID}
	if t.traffic[inbound] == nil {
		t.traffic[inbound] = make(map[string]*userTraffic)
	}
	if t.online[inbound] == nil {
		t.online[inbound] = make(map[string]*atomic.Int64)
	}
	if t.alive[inbound] == nil {
		t.alive[inbound] = make(map[int]map[string]struct{})
	}
	if t.connectionTotals[inbound] == nil {
		t.connectionTotals[inbound] = new(atomic.Int64)
	}
	if t.rates[inbound] == nil {
		t.rates[inbound] = new(trafficRate)
	}
	if t.active[inbound] == nil {
		t.active[inbound] = make(map[string]map[io.Closer]struct{})
	}
	if t.limits[inbound] == nil {
		t.limits[inbound] = make(map[string]*userLimit)
	}
	if t.devices[inbound] == nil {
		t.devices[inbound] = make(map[string]map[string]int)
	}
	t.access.Unlock()
}

func (t *trafficTracker) updateUserLimits(inbound string, users []xboardUser) {
	t.access.Lock()
	defer t.access.Unlock()
	if _, loaded := t.nodes[inbound]; !loaded {
		return
	}
	currentLimits := t.limits[inbound]
	if currentLimits == nil {
		currentLimits = make(map[string]*userLimit)
	}
	nextLimits := make(map[string]*userLimit, len(users))
	for _, user := range users {
		userName := F.ToString(user.ID)
		limit := currentLimits[userName]
		if limit == nil {
			limit = new(userLimit)
		}
		limit.deviceLimit = user.DeviceLimit
		limit.setSpeed(speedLimitBytes(user.SpeedLimit))
		nextLimits[userName] = limit
	}
	t.limits[inbound] = nextLimits
	if t.devices[inbound] == nil {
		t.devices[inbound] = make(map[string]map[string]int)
	}
}

func (t *trafficTracker) RoutedConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, _ adapter.Rule, _ adapter.Outbound) net.Conn {
	sourceIP := sourceIPString(metadata)
	traffic, online, limit, deviceLease, allowed := t.prepareUser(metadata.Inbound, metadata.User, sourceIP)
	if !allowed {
		_ = conn.Close()
		return conn
	}
	if traffic == nil {
		return conn
	}
	online.Add(1)
	t.addConnection(metadata.Inbound)
	t.addAlive(metadata.Inbound, metadata.User, sourceIP)
	var tracked net.Conn = bufio.NewInt64CounterConn(conn, []*atomic.Int64{&traffic.upload, &traffic.uploadTotal}, []*atomic.Int64{&traffic.download, &traffic.downloadTotal})
	if limit != nil {
		tracked = &limitedUserConn{Conn: tracked, limit: limit, ctx: ctx}
	}
	trackedConn := &trackedUserConn{
		Conn:        tracked,
		online:      online,
		tracker:     t,
		inbound:     metadata.Inbound,
		user:        metadata.User,
		deviceLease: deviceLease,
	}
	t.addActive(metadata.Inbound, metadata.User, trackedConn)
	return trackedConn
}

func (t *trafficTracker) RoutedPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, _ adapter.Rule, _ adapter.Outbound) N.PacketConn {
	sourceIP := sourceIPString(metadata)
	traffic, online, limit, deviceLease, allowed := t.prepareUser(metadata.Inbound, metadata.User, sourceIP)
	if !allowed {
		_ = conn.Close()
		return conn
	}
	if traffic == nil {
		return conn
	}
	online.Add(1)
	t.addConnection(metadata.Inbound)
	t.addAlive(metadata.Inbound, metadata.User, sourceIP)
	var tracked N.PacketConn = bufio.NewInt64CounterPacketConn(conn, []*atomic.Int64{&traffic.upload, &traffic.uploadTotal}, nil, []*atomic.Int64{&traffic.download, &traffic.downloadTotal}, nil)
	if limit != nil {
		tracked = &limitedUserPacketConn{PacketConn: tracked, limit: limit, ctx: ctx}
	}
	trackedConn := &trackedUserPacketConn{
		PacketConn:  tracked,
		online:      online,
		tracker:     t,
		inbound:     metadata.Inbound,
		user:        metadata.User,
		deviceLease: deviceLease,
	}
	t.addActive(metadata.Inbound, metadata.User, trackedConn)
	return trackedConn
}

func (t *trafficTracker) RoutedFlow(ctx context.Context, metadata adapter.InboundContext, _ adapter.Rule, _ adapter.Outbound) tun.FlowTracker {
	sourceIP := sourceIPString(metadata)
	traffic, online, _, deviceLease, allowed := t.prepareUser(metadata.Inbound, metadata.User, sourceIP)
	if !allowed {
		return &rejectedUserFlow{}
	}
	if traffic == nil {
		return nil
	}
	online.Add(1)
	t.addConnection(metadata.Inbound)
	t.addAlive(metadata.Inbound, metadata.User, sourceIP)
	flow := &trackedUserFlow{
		traffic:     traffic,
		online:      online,
		tracker:     t,
		inbound:     metadata.Inbound,
		user:        metadata.User,
		deviceLease: deviceLease,
	}
	t.addActive(metadata.Inbound, metadata.User, flow)
	return flow
}

var _ tun.FlowTracker = (*rejectedUserFlow)(nil)

type rejectedUserFlow struct{}

func (f *rejectedUserFlow) AttachFlow(handle tun.FlowHandle) {
	handle.CloseFlow()
}

func (f *rejectedUserFlow) CountForward(n int) {
}

func (f *rejectedUserFlow) CountReverse(n int) {
}

func (f *rejectedUserFlow) FlowEstablished() {
}

func (f *rejectedUserFlow) CloseFlow(reason tun.FlowCloseReason) {
}

var _ tun.FlowTracker = (*trackedUserFlow)(nil)

type trackedUserFlow struct {
	traffic     *userTraffic
	online      *atomic.Int64
	tracker     *trafficTracker
	inbound     string
	user        string
	deviceLease *userDeviceLease
	handle      tun.FlowHandle
	once        sync.Once
}

func (f *trackedUserFlow) AttachFlow(handle tun.FlowHandle) {
	f.handle = handle
}

func (f *trackedUserFlow) CountForward(n int) {
	f.traffic.upload.Add(int64(n))
	f.traffic.uploadTotal.Add(int64(n))
}

func (f *trackedUserFlow) CountReverse(n int) {
	f.traffic.download.Add(int64(n))
	f.traffic.downloadTotal.Add(int64(n))
}

func (f *trackedUserFlow) FlowEstablished() {
}

func (f *trackedUserFlow) CloseFlow(reason tun.FlowCloseReason) {
	f.cleanup()
}

func (f *trackedUserFlow) Close() error {
	if handle := f.handle; handle != nil {
		handle.CloseFlow()
	} else {
		f.cleanup()
	}
	return nil
}

func (f *trackedUserFlow) cleanup() {
	f.once.Do(func() {
		f.online.Add(-1)
		f.tracker.removeActive(f.inbound, f.user, f)
		if f.deviceLease != nil {
			f.deviceLease.release()
		}
	})
}

func (t *trafficTracker) prepareUser(inbound string, user string, ip string) (*userTraffic, *atomic.Int64, *userLimit, *userDeviceLease, bool) {
	if inbound == "" || user == "" {
		return nil, nil, nil, nil, true
	}
	t.access.Lock()
	defer t.access.Unlock()
	if _, loaded := t.nodes[inbound]; !loaded {
		return nil, nil, nil, nil, true
	}
	trafficByUser := t.traffic[inbound]
	traffic, loaded := trafficByUser[user]
	if !loaded {
		traffic = &userTraffic{}
		trafficByUser[user] = traffic
	}
	onlineByUser := t.online[inbound]
	userOnline, loaded := onlineByUser[user]
	if !loaded {
		userOnline = new(atomic.Int64)
		onlineByUser[user] = userOnline
	}
	var limit *userLimit
	if limitsByUser := t.limits[inbound]; limitsByUser != nil {
		limit = limitsByUser[user]
	}
	if ip == "" {
		return traffic, userOnline, limit, nil, true
	}
	devicesByUser := t.devices[inbound]
	if devicesByUser == nil {
		devicesByUser = make(map[string]map[string]int)
		t.devices[inbound] = devicesByUser
	}
	userDevices := devicesByUser[user]
	if userDevices == nil {
		userDevices = make(map[string]int)
		devicesByUser[user] = userDevices
	}
	if userDevices[ip] == 0 && limit != nil && limit.deviceLimit > 0 && len(userDevices) >= limit.deviceLimit {
		return traffic, userOnline, limit, nil, false
	}
	userDevices[ip]++
	return traffic, userOnline, limit, &userDeviceLease{tracker: t, inbound: inbound, user: user, ip: ip}, true
}

func (t *trafficTracker) addActive(inbound string, user string, conn io.Closer) {
	t.access.Lock()
	defer t.access.Unlock()
	if _, loaded := t.nodes[inbound]; !loaded {
		return
	}
	activeByUser := t.active[inbound]
	if activeByUser == nil {
		activeByUser = make(map[string]map[io.Closer]struct{})
		t.active[inbound] = activeByUser
	}
	activeByConn := activeByUser[user]
	if activeByConn == nil {
		activeByConn = make(map[io.Closer]struct{})
		activeByUser[user] = activeByConn
	}
	activeByConn[conn] = struct{}{}
}

func (t *trafficTracker) removeActive(inbound string, user string, conn io.Closer) {
	t.access.Lock()
	defer t.access.Unlock()
	activeByUser := t.active[inbound]
	if activeByUser == nil {
		return
	}
	activeByConn := activeByUser[user]
	if activeByConn == nil {
		return
	}
	delete(activeByConn, conn)
	if len(activeByConn) == 0 {
		delete(activeByUser, user)
	}
}

func (t *trafficTracker) closeRemovedUsers(inbound string, users []xboardUser) {
	allowedUsers := make(map[string]bool, len(users))
	for _, user := range users {
		allowedUsers[F.ToString(user.ID)] = true
	}
	var closeList []io.Closer
	t.access.Lock()
	for user, activeByConn := range t.active[inbound] {
		if allowedUsers[user] {
			continue
		}
		for conn := range activeByConn {
			closeList = append(closeList, conn)
		}
	}
	t.access.Unlock()
	for _, conn := range closeList {
		_ = conn.Close()
	}
}

func (t *trafficTracker) addConnection(inbound string) {
	t.access.Lock()
	counter := t.connectionTotals[inbound]
	t.access.Unlock()
	if counter != nil {
		counter.Add(1)
	}
}

func (t *trafficTracker) addAlive(inbound string, user string, ip string) {
	if inbound == "" || user == "" || ip == "" || ip == "<nil>" {
		return
	}
	uid, err := strconv.Atoi(user)
	if err != nil {
		return
	}
	ip = normalizeUserIP(ip)
	if ip == "" {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	if _, loaded := t.nodes[inbound]; !loaded {
		return
	}
	aliveByUser := t.alive[inbound]
	if aliveByUser == nil {
		aliveByUser = make(map[int]map[string]struct{})
		t.alive[inbound] = aliveByUser
	}
	ips := aliveByUser[uid]
	if ips == nil {
		ips = make(map[string]struct{})
		aliveByUser[uid] = ips
	}
	ips[ip] = struct{}{}
}

func (t *trafficTracker) releaseDevice(inbound string, user string, ip string) {
	if inbound == "" || user == "" || ip == "" {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	devicesByUser := t.devices[inbound]
	if devicesByUser == nil {
		return
	}
	userDevices := devicesByUser[user]
	if userDevices == nil {
		return
	}
	if userDevices[ip] <= 1 {
		delete(userDevices, ip)
	} else {
		userDevices[ip]--
	}
	if len(userDevices) == 0 {
		delete(devicesByUser, user)
	}
}

func (l *userLimit) setSpeed(speedBytes int64) {
	l.speedBytes.Store(speedBytes)
	if speedBytes <= 0 {
		l.uploadLimiter.Store(nil)
		l.downloadLimiter.Store(nil)
		return
	}
	burst := max(int(speedBytes), 1)
	l.uploadLimiter.Store(rate.NewLimiter(rate.Limit(speedBytes), burst))
	l.downloadLimiter.Store(rate.NewLimiter(rate.Limit(speedBytes), burst))
}

func (l *userLimit) waitUpload(ctx context.Context, n int) error {
	return waitRateLimit(ctx, l.uploadLimiter.Load(), n)
}

func (l *userLimit) waitDownload(ctx context.Context, n int) error {
	return waitRateLimit(ctx, l.downloadLimiter.Load(), n)
}

func waitRateLimit(ctx context.Context, limiter *rate.Limiter, n int) error {
	if limiter == nil || n <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for n > 0 {
		chunk := min(max(limiter.Burst(), 1), n)
		if err := limiter.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

func speedLimitBytes(speedLimit int) int64 {
	if speedLimit <= 0 {
		return 0
	}
	return int64(speedLimit) * 1000 * 1000 / 8
}

func sourceIPString(metadata adapter.InboundContext) string {
	if !metadata.Source.Addr.IsValid() {
		return ""
	}
	return normalizeUserIP(metadata.Source.Addr.String())
}

func normalizeUserIP(ip string) string {
	if ip == "" || ip == "<nil>" {
		return ""
	}
	ip = strings.TrimPrefix(ip, "::ffff:")
	if addr, err := netip.ParseAddr(ip); err == nil {
		return addr.Unmap().String()
	}
	return ip
}

func (t *trafficTracker) readTraffic(inbound string) map[string][2]int64 {
	t.access.Lock()
	defer t.access.Unlock()
	result := make(map[string][2]int64)
	for user, traffic := range t.traffic[inbound] {
		upload := traffic.upload.Swap(0)
		download := traffic.download.Swap(0)
		if upload > 0 || download > 0 {
			result[user] = [2]int64{upload, download}
		}
	}
	return result
}

func (t *trafficTracker) addTraffic(inbound string, traffic map[string][2]int64) {
	if len(traffic) == 0 {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	if _, loaded := t.nodes[inbound]; !loaded {
		return
	}
	trafficByUser := t.traffic[inbound]
	for user, value := range traffic {
		if value[0] == 0 && value[1] == 0 {
			continue
		}
		trackedTraffic, loaded := trafficByUser[user]
		if !loaded {
			trackedTraffic = &userTraffic{}
			trafficByUser[user] = trackedTraffic
		}
		trackedTraffic.upload.Add(value[0])
		trackedTraffic.download.Add(value[1])
	}
}

func (t *trafficTracker) totalTraffic(inbound string) (int64, int64) {
	t.access.Lock()
	defer t.access.Unlock()
	return t.totalTrafficLocked(inbound)
}

func (t *trafficTracker) totalTrafficLocked(inbound string) (int64, int64) {
	var uploadTotal, downloadTotal int64
	for _, traffic := range t.traffic[inbound] {
		uploadTotal += traffic.uploadTotal.Load()
		downloadTotal += traffic.downloadTotal.Load()
	}
	return uploadTotal, downloadTotal
}

func (t *trafficTracker) readTrafficRate(inbound string) (int64, int64) {
	t.access.Lock()
	defer t.access.Unlock()
	rate := t.rates[inbound]
	if rate == nil {
		rate = new(trafficRate)
		t.rates[inbound] = rate
	}
	uploadTotal, downloadTotal := t.totalTrafficLocked(inbound)
	now := time.Now()
	if rate.lastTime.IsZero() {
		rate.lastTime = now
		rate.lastUploadTotal = uploadTotal
		rate.lastDownloadTotal = downloadTotal
		return rate.uploadSpeed, rate.downloadSpeed
	}
	elapsed := now.Sub(rate.lastTime)
	if elapsed < time.Second {
		return rate.uploadSpeed, rate.downloadSpeed
	}
	seconds := elapsed.Seconds()
	uploadSpeed := max(int64(float64(uploadTotal-rate.lastUploadTotal)/seconds), 0)
	downloadSpeed := max(int64(float64(downloadTotal-rate.lastDownloadTotal)/seconds), 0)
	rate.lastTime = now
	rate.lastUploadTotal = uploadTotal
	rate.lastDownloadTotal = downloadTotal
	rate.uploadSpeed = uploadSpeed
	rate.downloadSpeed = downloadSpeed
	return uploadSpeed, downloadSpeed
}

func (t *trafficTracker) totalConnections(inbound string) int64 {
	t.access.Lock()
	counter := t.connectionTotals[inbound]
	t.access.Unlock()
	if counter == nil {
		return 0
	}
	return counter.Load()
}

func (t *trafficTracker) readOnline(inbound string) map[int]int {
	t.access.Lock()
	defer t.access.Unlock()
	result := make(map[int]int)
	for user, online := range t.online[inbound] {
		if value := online.Load(); value > 0 {
			uid, err := strconv.Atoi(user)
			if err == nil {
				result[uid] = int(value)
			}
		}
	}
	return result
}

func (t *trafficTracker) readAlive(inbound string, reset bool) map[int][]string {
	t.access.Lock()
	defer t.access.Unlock()
	aliveByUser := t.alive[inbound]
	result := make(map[int][]string, len(aliveByUser))
	for uid, ips := range aliveByUser {
		for ip := range ips {
			result[uid] = append(result[uid], ip)
		}
	}
	if reset {
		t.alive[inbound] = make(map[int]map[string]struct{})
	}
	return result
}

func (t *trafficTracker) restoreAlive(inbound string, alive map[int][]string) {
	if len(alive) == 0 {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	if _, loaded := t.nodes[inbound]; !loaded {
		return
	}
	aliveByUser := t.alive[inbound]
	if aliveByUser == nil {
		aliveByUser = make(map[int]map[string]struct{})
		t.alive[inbound] = aliveByUser
	}
	for uid, ips := range alive {
		userIPs := aliveByUser[uid]
		if userIPs == nil {
			userIPs = make(map[string]struct{}, len(ips))
			aliveByUser[uid] = userIPs
		}
		for _, ip := range ips {
			userIPs[ip] = struct{}{}
		}
	}
}

type trackedUserConn struct {
	net.Conn
	online      *atomic.Int64
	tracker     *trafficTracker
	inbound     string
	user        string
	deviceLease *userDeviceLease
	once        sync.Once
}

func (c *trackedUserConn) Close() error {
	c.once.Do(func() {
		c.online.Add(-1)
		c.tracker.removeActive(c.inbound, c.user, c)
		if c.deviceLease != nil {
			c.deviceLease.release()
		}
	})
	return c.Conn.Close()
}

func (c *trackedUserConn) Upstream() any {
	return c.Conn
}

func (c *trackedUserConn) ReaderReplaceable() bool {
	return true
}

func (c *trackedUserConn) WriterReplaceable() bool {
	return true
}

var _ N.PacketConn = (*trackedUserPacketConn)(nil)

type trackedUserPacketConn struct {
	N.PacketConn
	online      *atomic.Int64
	tracker     *trafficTracker
	inbound     string
	user        string
	deviceLease *userDeviceLease
	once        sync.Once
}

func (c *trackedUserPacketConn) Close() error {
	c.once.Do(func() {
		c.online.Add(-1)
		c.tracker.removeActive(c.inbound, c.user, c)
		if c.deviceLease != nil {
			c.deviceLease.release()
		}
	})
	return c.PacketConn.Close()
}

func (c *trackedUserPacketConn) Upstream() any {
	return c.PacketConn
}

func (c *trackedUserPacketConn) ReaderReplaceable() bool {
	return true
}

func (c *trackedUserPacketConn) WriterReplaceable() bool {
	return true
}

func (l *userDeviceLease) release() {
	l.once.Do(func() {
		l.tracker.releaseDevice(l.inbound, l.user, l.ip)
	})
}

type limitedUserConn struct {
	net.Conn
	limit *userLimit
	ctx   context.Context
}

func (c *limitedUserConn) Read(p []byte) (n int, err error) {
	n, err = c.Conn.Read(p)
	if n > 0 {
		if limitErr := c.limit.waitUpload(c.ctx, n); limitErr != nil && err == nil {
			err = limitErr
		}
	}
	return
}

func (c *limitedUserConn) ReadBuffer(buffer *buf.Buffer) error {
	extendedReader, loaded := c.Conn.(N.ExtendedReader)
	if !loaded {
		extendedReader = bufio.NewExtendedReader(c.Conn)
	}
	err := extendedReader.ReadBuffer(buffer)
	if buffer.Len() > 0 {
		if limitErr := c.limit.waitUpload(c.ctx, buffer.Len()); limitErr != nil && err == nil {
			err = limitErr
		}
	}
	return err
}

func (c *limitedUserConn) Write(p []byte) (n int, err error) {
	if err = c.limit.waitDownload(c.ctx, len(p)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

func (c *limitedUserConn) WriteBuffer(buffer *buf.Buffer) error {
	if err := c.limit.waitDownload(c.ctx, buffer.Len()); err != nil {
		buffer.Release()
		return err
	}
	extendedWriter, loaded := c.Conn.(N.ExtendedWriter)
	if !loaded {
		return bufio.NewExtendedWriter(c.Conn).WriteBuffer(buffer)
	}
	return extendedWriter.WriteBuffer(buffer)
}

func (c *limitedUserConn) Upstream() any {
	return c.Conn
}

var _ N.PacketConn = (*limitedUserPacketConn)(nil)

type limitedUserPacketConn struct {
	N.PacketConn
	limit *userLimit
	ctx   context.Context
}

func (c *limitedUserPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	destination, err = c.PacketConn.ReadPacket(buffer)
	if buffer.Len() > 0 {
		if limitErr := c.limit.waitUpload(c.ctx, buffer.Len()); limitErr != nil && err == nil {
			err = limitErr
		}
	}
	return
}

func (c *limitedUserPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	if err := c.limit.waitDownload(c.ctx, buffer.Len()); err != nil {
		buffer.Release()
		return err
	}
	return c.PacketConn.WritePacket(buffer, destination)
}

func (c *limitedUserPacketConn) Upstream() any {
	return c.PacketConn
}
