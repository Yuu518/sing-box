package local

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/sagernet/fswatch"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/provider"
	"github.com/sagernet/sing-box/common/outboundfilter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/provider/parser"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/service/filemanager"

	"github.com/dlclark/regexp2/v2"
)

func RegisterLocal(registry *provider.Registry) {
	provider.Register[option.ProviderLocalOptions](registry, C.ProviderTypeLocal, NewLocal)
}

func RegisterInline(registry *provider.Registry) {
	provider.Register[option.ProviderInlineOptions](registry, C.ProviderTypeInline, NewInline)
}

var (
	_ adapter.Provider        = (*Provider)(nil)
	_ adapter.ProviderUpdater = (*Provider)(nil)
)

type Provider struct {
	provider.Adapter
	ctx          context.Context
	logger       log.ContextLogger
	path         string
	reloadAccess sync.Mutex
	access       sync.RWMutex
	lastUpdated  time.Time
	watcher      *fswatch.Watcher
	exclude      *regexp2.Regexp
	include      *regexp2.Regexp

	overrideDialer *option.OverrideDialerOptions
	overrideTLS    *option.OverrideTLSOptions
	overrideAnyTLS *option.OverrideAnyTLSOptions
}

func NewInline(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, options option.ProviderInlineOptions) (adapter.Provider, error) {
	for i, outbound := range options.Outbounds {
		switch outbound.Type {
		case C.TypeSelector, C.TypeURLTest:
			return nil, E.New("unsupported outbound type in inline provider[", i, "]: ", outbound.Type)
		}
	}
	for i, endpoint := range options.Endpoints {
		switch endpoint.Type {
		case C.TypeOpenVPNServer, C.TypeMASQUEServer:
			return nil, E.New("unsupported endpoint type in inline provider[", i, "]: ", endpoint.Type)
		}
	}
	logger := logFactory.NewLogger(F.ToString("provider/inline[", tag, "]"))
	p := &Provider{
		Adapter: provider.NewAdapter(ctx, router, logFactory, logger, tag, C.ProviderTypeInline, options.HealthCheck),
		ctx:     ctx,
		logger:  logger,
	}
	p.Adapter.Update(options.Outbounds, options.Endpoints)
	return p, nil
}

func NewLocal(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, options option.ProviderLocalOptions) (adapter.Provider, error) {
	if options.Path == "" {
		return nil, E.New("provider path is required")
	}
	filePath, err := filepath.Abs(filemanager.BasePath(ctx, options.Path))
	if err != nil {
		return nil, E.Cause(err, "resolve provider path")
	}
	logger := logFactory.NewLogger(F.ToString("provider/local[", tag, "]"))
	p := &Provider{
		Adapter:        provider.NewAdapter(ctx, router, logFactory, logger, tag, C.ProviderTypeLocal, options.HealthCheck),
		ctx:            ctx,
		logger:         logger,
		path:           filePath,
		exclude:        options.Exclude.Build(),
		include:        options.Include.Build(),
		overrideDialer: options.OverrideDialer,
		overrideTLS:    options.OverrideTLS,
		overrideAnyTLS: options.OverrideAnyTLS,
	}
	watcher, err := fswatch.NewWatcher(fswatch.Options{
		Path: []string{filePath},
		Callback: func(path string) {
			uErr := p.reloadFile(path)
			if uErr != nil {
				logger.Error(E.Cause(uErr, "reload provider ", tag))
			}
		},
	})
	if err != nil {
		return nil, err
	}
	p.watcher = watcher
	return p, nil
}

func (s *Provider) StartContext(ctx context.Context, startContext *adapter.HTTPStartContext) error {
	if s.path != "" {
		return s.reloadFile(s.path)
	}
	return nil
}

func (s *Provider) PostStart() error {
	if s.watcher != nil {
		if err := s.watcher.Start(); err != nil {
			s.logger.Error(E.Cause(err, "watch provider file"))
		}
	}
	return nil
}

func (s *Provider) Update() error {
	if s.path == "" {
		return nil
	}
	return s.reloadFile(s.path)
}

func (s *Provider) UpdatedAt() time.Time {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.lastUpdated
}

func (s *Provider) reloadFile(path string) error {
	s.reloadAccess.Lock()
	defer s.reloadAccess.Unlock()
	file, err := filemanager.Open(s.ctx, path)
	if err != nil {
		return err
	}
	contentRaw, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return err
	}
	fileInfo, err := file.Stat()
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	content, _ := parser.DecodeBase64URLSafe(string(contentRaw))
	outboundOpts, endpointOpts, err := parser.ParseSubscription(s.ctx, content, s.overrideDialer, s.overrideTLS, s.overrideAnyTLS)
	if err != nil {
		return err
	}
	filteredOutbounds := outboundOpts[:0]
	for _, it := range outboundOpts {
		matched, matchErr := outboundfilter.Match(it.Tag, s.exclude, s.include)
		if matchErr != nil {
			return matchErr
		}
		if matched {
			filteredOutbounds = append(filteredOutbounds, it)
		}
	}
	filteredEndpoints := endpointOpts[:0]
	for _, it := range endpointOpts {
		matched, matchErr := outboundfilter.Match(it.Tag, s.exclude, s.include)
		if matchErr != nil {
			return matchErr
		}
		if matched {
			filteredEndpoints = append(filteredEndpoints, it)
		}
	}
	s.Adapter.Update(filteredOutbounds, filteredEndpoints)
	s.access.Lock()
	s.lastUpdated = fileInfo.ModTime()
	s.access.Unlock()
	return nil
}

func (s *Provider) Close() error {
	return common.Close(common.PtrOrNil(s.watcher), &s.Adapter)
}
