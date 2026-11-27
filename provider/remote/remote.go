package remote

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/provider"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/outboundfilter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/experimental/deprecated"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/provider/parser"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"
	"github.com/sagernet/sing/service/pause"

	"github.com/dlclark/regexp2/v2"
)

func RegisterProvider(registry *provider.Registry) {
	provider.Register[option.ProviderRemoteOptions](registry, C.ProviderTypeRemote, NewProvider)
}

var (
	_ adapter.Provider                       = (*Provider)(nil)
	_ adapter.ProviderUpdater                = (*Provider)(nil)
	_ adapter.ProviderSubscriptionInfo       = (*Provider)(nil)
	_ adapter.ProviderHealthCheckWithOptions = (*Provider)(nil)
)

const (
	defaultRemoteFetchTimeout = 30 * time.Second
	maxSubscriptionSize       = 32 * 1024 * 1024
)

type Provider struct {
	provider.Adapter
	ctx                 context.Context
	cancel              context.CancelFunc
	logger              log.ContextLogger
	cacheFile           adapter.CacheFile
	httpClient          *http.Client
	contentHash         []byte
	access              sync.RWMutex
	lastEtag            string
	rawContent          string
	hasSubscriptionInfo bool
	lastUpdated         time.Time
	subscriptionInfo    adapter.SubscriptionInfo
	tickerAccess        sync.Mutex
	pause               pause.Manager
	pauseCallback       *list.Element[pause.Callback]
	ticker              *time.Ticker
	closed              bool
	loopWG              sync.WaitGroup
	updating            atomic.Bool
	fetchTimeout        time.Duration

	httpClientOptions *option.HTTPClientOptions
	downloadDetour    string
	url               string
	urlHash           [sha256.Size]byte
	path              string
	initialPath       string
	userAgent         string
	updateInterval    time.Duration
	exclude           *regexp2.Regexp
	include           *regexp2.Regexp

	overrideDialer *option.OverrideDialerOptions
	overrideTLS    *option.OverrideTLSOptions
	overrideAnyTLS *option.OverrideAnyTLSOptions
}

func NewProvider(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, options option.ProviderRemoteOptions) (adapter.Provider, error) {
	if options.URL == "" {
		return nil, E.New("provider URL is required")
	}
	if options.Path != "" && options.InitialPath != "" {
		return nil, E.New("provider path and initial_path are mutually exclusive")
	}
	var (
		path        string
		initialPath string
	)
	if options.Path != "" {
		path = filemanager.BasePath(ctx, options.Path)
		path, _ = filepath.Abs(path)
	}
	if options.InitialPath != "" {
		initialPath = filemanager.BasePath(ctx, options.InitialPath)
		initialPath, _ = filepath.Abs(initialPath)
	}
	if path != "" {
		info, err := filemanager.Stat(ctx, path)
		if err == nil && info.IsDir() {
			return nil, E.New("provider path is a directory: ", path)
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, E.Cause(err, "check provider path")
		}
	}
	if initialPath != "" {
		info, err := filemanager.Stat(ctx, initialPath)
		if err == nil && info.IsDir() {
			return nil, E.New("provider initial_path is a directory: ", initialPath)
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, E.Cause(err, "check provider initial_path")
		}
	}
	updateInterval := time.Duration(options.UpdateInterval)
	if updateInterval <= 0 {
		updateInterval = 24 * time.Hour
	}
	if updateInterval < time.Hour {
		updateInterval = time.Hour
	}
	if options.UserAgent != "" && options.HTTPClient != nil && !options.HTTPClient.IsEmpty() {
		return nil, E.New("user_agent conflicts with http_client: configure User-Agent via http_client.headers instead")
	}
	userAgent := options.UserAgent
	if userAgent == "" {
		userAgent = "sing-box " + C.Version
	}
	ctx, cancel := context.WithCancel(ctx)
	logger := logFactory.NewLogger(F.ToString("provider/remote[", tag, "]"))
	url := options.URL
	return &Provider{
		Adapter:           provider.NewAdapter(ctx, router, logFactory, logger, tag, C.ProviderTypeRemote, options.HealthCheck),
		ctx:               ctx,
		cancel:            cancel,
		logger:            logger,
		pause:             service.FromContext[pause.Manager](ctx),
		httpClientOptions: options.HTTPClient,
		url:               url,
		urlHash:           sha256.Sum256([]byte(url)),
		path:              path,
		initialPath:       initialPath,
		userAgent:         userAgent,
		updateInterval:    updateInterval,
		exclude:           options.Exclude.Build(),
		include:           options.Include.Build(),
		overrideDialer:    options.OverrideDialer,
		overrideTLS:       options.OverrideTLS,
		overrideAnyTLS:    options.OverrideAnyTLS,
		//nolint:staticcheck
		downloadDetour: options.DownloadDetour,
	}, nil
}

func (s *Provider) StartContext(ctx context.Context, startContext *adapter.HTTPStartContext) error {
	s.cacheFile = service.FromContext[adapter.CacheFile](s.ctx)
	if s.initialPath != "" && s.cacheFile == nil {
		return E.New("provider initial_path requires cache_file")
	}
	loadedFromCache, err := s.loadCacheFile()
	if err != nil {
		s.logger.Warn(E.Cause(err, "restore cached outbound provider, will refetch"))
	}
	var loadedFromInitialPath bool
	if !loadedFromCache && s.initialPath != "" {
		err = s.loadInitialPath()
		if err != nil {
			s.logger.Warn(E.Cause(err, "load initial outbound provider from ", s.initialPath))
		} else {
			loadedFromInitialPath = true
		}
	}
	transport, err := s.resolveTransport()
	if err != nil {
		return E.Cause(err, "create provider http client")
	}
	startContext.Register(transport)
	s.httpClient = &http.Client{Transport: transport}
	if !loadedFromCache && !loadedFromInitialPath {
		ctx = interrupt.ContextWithIsProviderConnection(ctx)
		err = s.fetch(ctx, true)
		if err != nil {
			return E.Cause(err, "initial outbound provider: ", s.Tag())
		}
	}
	return nil
}

func (s *Provider) PostStart() error {
	s.tickerAccess.Lock()
	if s.closed || s.ticker != nil {
		s.tickerAccess.Unlock()
		return nil
	}
	ticker := time.NewTicker(s.updateInterval)
	s.ticker = ticker
	if s.pause != nil {
		s.pauseCallback = pause.RegisterTicker(s.pause, ticker, s.updateInterval, nil)
	}
	s.loopWG.Add(1)
	s.tickerAccess.Unlock()
	go s.loopUpdate(ticker)
	return nil
}

func (s *Provider) Update() error {
	ctx := interrupt.ContextWithIsProviderConnection(s.ctx)
	err := s.fetch(ctx, false)
	if err != nil {
		return err
	}
	s.resetTicker(s.updateInterval)
	return nil
}

func (s *Provider) UpdatedAt() time.Time {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.lastUpdated
}

func (s *Provider) SubscriptionInfo() (adapter.SubscriptionInfo, bool) {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.subscriptionInfo, s.hasSubscriptionInfo
}

func (s *Provider) Close() error {
	s.tickerAccess.Lock()
	s.closed = true
	if s.ticker != nil {
		s.ticker.Stop()
		if s.pause != nil && s.pauseCallback != nil {
			s.pause.UnregisterCallback(s.pauseCallback)
			s.pauseCallback = nil
		}
	}
	s.tickerAccess.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	s.loopWG.Wait()
	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}
	return common.Close(&s.Adapter)
}

func (s *Provider) resetTicker(duration time.Duration) {
	s.tickerAccess.Lock()
	defer s.tickerAccess.Unlock()
	if !s.closed && s.ticker != nil {
		s.ticker.Reset(duration)
	}
}

func (s *Provider) resolveTransport() (adapter.HTTPTransport, error) {
	httpClientManager := service.FromContext[adapter.HTTPClientManager](s.ctx)
	if s.httpClientOptions != nil && !s.httpClientOptions.IsEmpty() {
		if s.downloadDetour != "" {
			return nil, E.New("http_client is conflict with deprecated download_detour field")
		}
		return httpClientManager.ResolveTransport(s.ctx, s.logger, *s.httpClientOptions)
	}
	if s.downloadDetour != "" {
		deprecated.Report(s.ctx, deprecated.OptionLegacyProviderDownloadDetour)
		return httpClientManager.ResolveTransport(s.ctx, s.logger, option.HTTPClientOptions{
			DialerOptions: option.DialerOptions{
				Detour: s.downloadDetour,
			},
			DisableEmptyDirectCheck: true,
		})
	}
	defaultTransport := httpClientManager.DefaultTransport()
	if defaultTransport == nil {
		return nil, E.New("default http client transport is not initialized")
	}
	return defaultTransport, nil
}

func (s *Provider) updateOnce() {
	ctx := interrupt.ContextWithIsProviderConnection(s.ctx)
	if err := s.fetch(ctx, false); err != nil {
		s.logger.Error("update outbound provider: ", err)
	}
}

func (s *Provider) fetch(ctx context.Context, isStart bool) error {
	if s.updating.Swap(true) {
		return E.New("provider is updating")
	}
	defer s.updating.Store(false)
	timeout := s.fetchTimeout
	if timeout <= 0 {
		timeout = defaultRemoteFetchTimeout
	}
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s.logger.Debug("updating outbound provider ", s.Tag(), " from URL: ", s.url)
	request, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, s.url, nil)
	if err != nil {
		return err
	}
	s.access.RLock()
	lastEtag := s.lastEtag
	s.access.RUnlock()
	if lastEtag != "" {
		request.Header.Set("If-None-Match", lastEtag)
	}
	request.Header.Set("User-Agent", s.userAgent)
	if !isStart {
		defer s.httpClient.CloseIdleConnections()
	}
	response, err := s.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	info, hasInfo := parseInfo(response.Header.Get("subscription-userinfo"))
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotModified:
		s.access.Lock()
		if s.rawContent == "" {
			s.access.Unlock()
			return E.New("provider returned not modified without cached content")
		}
		rewriteFile := false
		if hasInfo && (!s.hasSubscriptionInfo || s.subscriptionInfo != info) {
			s.subscriptionInfo, s.hasSubscriptionInfo = info, true
			rewriteFile = true
		}
		if etag := response.Header.Get("Etag"); etag != "" {
			s.lastEtag = etag
		}
		s.lastUpdated = time.Now()
		s.access.Unlock()
		if err := s.storeCache(rewriteFile); err != nil {
			return err
		}
		s.logger.Info("update outbound provider ", s.Tag(), ": not modified")
		return nil
	default:
		return E.New("unexpected status: ", response.Status)
	}
	contentRaw, err := io.ReadAll(io.LimitReader(response.Body, maxSubscriptionSize+1))
	if err != nil {
		return err
	}
	if len(contentRaw) > maxSubscriptionSize {
		return E.New("subscription size exceeds limit (", maxSubscriptionSize, " bytes)")
	}
	content, _ := parser.DecodeBase64URLSafe(string(contentRaw))
	if !hasInfo {
		firstLine, others := getFirstLine(content)
		if info, hasInfo = parseCommentInfo(firstLine); hasInfo {
			content, _ = parser.DecodeBase64URLSafe(others)
		}
	}
	if err := s.updateProviderFromContent(content); err != nil {
		return err
	}
	s.access.Lock()
	s.rawContent = content
	s.subscriptionInfo, s.hasSubscriptionInfo = info, hasInfo
	s.lastEtag = response.Header.Get("Etag")
	s.lastUpdated = time.Now()
	s.access.Unlock()
	if err := s.storeCache(true); err != nil {
		return err
	}
	s.logger.Info("updated outbound provider ", s.Tag())
	return nil
}

func (s *Provider) loopUpdate(ticker *time.Ticker) {
	defer s.loopWG.Done()
	s.tickerAccess.Lock()
	ticker.Stop()
	select {
	case <-ticker.C:
	default:
	}
	s.tickerAccess.Unlock()
	if remaining := time.Until(s.UpdatedAt().Add(s.updateInterval)); remaining > 0 {
		s.resetTicker(remaining)
	} else {
		if s.ctx.Err() != nil {
			return
		}
		s.updateOnce()
		s.resetTicker(s.updateInterval)
	}
	for {
		runtime.GC()
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.ctx.Err() != nil {
				return
			}
			s.updateOnce()
			s.resetTicker(s.updateInterval)
		}
	}
}

func (s *Provider) updateProviderFromContent(content string) error {
	outboundOpts, endpointOpts, err := parser.ParseRemoteSubscription(s.ctx, content, s.overrideDialer, s.overrideTLS, s.overrideAnyTLS)
	if err != nil {
		return err
	}
	filteredOutbounds := outboundOpts[:0]
	for _, it := range outboundOpts {
		matched, err := outboundfilter.Match(it.Tag, s.exclude, s.include)
		if err != nil {
			return err
		}
		if matched {
			filteredOutbounds = append(filteredOutbounds, it)
		}
	}
	filteredEndpoints := endpointOpts[:0]
	for _, it := range endpointOpts {
		matched, err := outboundfilter.Match(it.Tag, s.exclude, s.include)
		if err != nil {
			return err
		}
		if matched {
			filteredEndpoints = append(filteredEndpoints, it)
		}
	}
	s.Adapter.Update(filteredOutbounds, filteredEndpoints)
	return nil
}
