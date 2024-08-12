package rule

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/hash"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/experimental/deprecated"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"
	"github.com/sagernet/sing/service/pause"
)

var _ adapter.RuleSet = (*RemoteRuleSet)(nil)

type RemoteRuleSet struct {
	abstractRuleSet
	cancel         context.CancelFunc
	outbound       adapter.OutboundManager
	url            string
	urlHash        [32]byte
	initialPath    string
	options        option.RemoteRuleSet
	updateInterval time.Duration
	httpClient     *http.Client
	hash           hash.HashType
	lastEtag       string
	cacheFile      adapter.CacheFile
	pauseManager   pause.Manager
	updateAccess   sync.Mutex
	cacheDirty     bool
}

func NewRemoteRuleSet(ctx context.Context, logger logger.ContextLogger, tag string, options option.RuleSet) (*RemoteRuleSet, error) {
	ctx, cancel := context.WithCancel(ctx)
	if options.Path != "" && options.RemoteOptions.InitialPath != "" {
		cancel()
		return nil, E.New("rule-set path and initial_path are mutually exclusive")
	}
	var path string
	if options.Path != "" {
		path = filemanager.BasePath(ctx, strings.ReplaceAll(options.Path, C.RuleSetTagPlaceholder, tag))
		path, _ = filepath.Abs(path)
	}
	var updateInterval time.Duration
	if options.RemoteOptions.UpdateInterval > 0 {
		updateInterval = time.Duration(options.RemoteOptions.UpdateInterval)
	} else {
		updateInterval = 24 * time.Hour
	}
	var initialPath string
	if options.RemoteOptions.InitialPath != "" {
		initialPath = filemanager.BasePath(ctx, strings.ReplaceAll(options.RemoteOptions.InitialPath, C.RuleSetTagPlaceholder, tag))
		initialPath, _ = filepath.Abs(initialPath)
	}
	url := strings.ReplaceAll(options.RemoteOptions.URL, C.RuleSetTagPlaceholder, tag)
	return &RemoteRuleSet{
		abstractRuleSet: abstractRuleSet{
			ctx:    ctx,
			logger: logger,
			tag:    tag,
			sType:  options.Type,
			path:   path,
			format: options.Format,
		},
		outbound:       service.FromContext[adapter.OutboundManager](ctx),
		url:            url,
		urlHash:        sha256.Sum256([]byte(url)),
		initialPath:    initialPath,
		cancel:         cancel,
		options:        options.RemoteOptions,
		updateInterval: updateInterval,
		pauseManager:   service.FromContext[pause.Manager](ctx),
	}, nil
}

func (s *RemoteRuleSet) StartContext(ctx context.Context, startContext *adapter.HTTPStartContext) error {
	s.cacheFile = service.FromContext[adapter.CacheFile](s.ctx)
	transport, err := s.resolveTransport()
	if err != nil {
		return E.Cause(err, "create rule-set http client")
	}
	startContext.Register(transport)
	s.httpClient = &http.Client{Transport: transport}
	if s.initialPath != "" && s.cacheFile == nil {
		return E.New("rule-set initial_path requires cache_file")
	}
	loadedFromCache, err := s.loadCacheFile()
	if err != nil {
		s.logger.Warn(E.Cause(err, "restore cached rule-set, will refetch"))
	}
	var loadedFromInitialPath bool
	if !loadedFromCache && s.initialPath != "" {
		var content []byte
		content, err = filemanager.ReadFile(s.ctx, s.initialPath)
		if err == nil {
			err = s.loadBytes(content, s)
		}
		if err != nil {
			s.logger.Warn(E.Cause(err, "load initial rule-set from ", s.initialPath))
		} else {
			loadedFromInitialPath = true
		}
	}
	if !loadedFromCache && !loadedFromInitialPath {
		err = s.fetch(ctx, true)
		if err != nil {
			return E.Cause(err, "initial rule-set: ", s.tag)
		}
	}
	return nil
}

func (s *RemoteRuleSet) update() {
	ctx := log.ContextWithNewID(s.ctx)
	err := s.Update(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "fetch rule-set ", s.tag, ": ", err)
	}
}

func (s *RemoteRuleSet) Update(ctx context.Context) error {
	if _, loaded := log.IDFromContext(ctx); !loaded {
		ctx = log.ContextWithNewID(ctx)
	}
	err := s.fetch(ctx, false)
	if err != nil {
		return err
	}
	s.Cleanup()
	return nil
}

func (s *RemoteRuleSet) fetch(ctx context.Context, isStart bool) error {
	if !s.updateAccess.TryLock() {
		return E.New("rule-set is updating")
	}
	defer s.updateAccess.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(s.ctx, cancel)
	defer stopCancel()
	defer cancel()
	if err := s.updateContextError(ctx); err != nil {
		return err
	}
	s.logger.DebugContext(ctx, "updating rule-set ", s.tag, " from URL: ", s.url)
	request, err := http.NewRequest("GET", s.url, nil)
	if err != nil {
		return err
	}
	if s.lastEtag != "" && !s.cacheDirty {
		request.Header.Set("If-None-Match", s.lastEtag)
	}
	if !isStart {
		defer s.httpClient.CloseIdleConnections()
	}
	response, err := s.httpClient.Do(request.WithContext(ctx))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotModified:
		if s.lastEtag == "" || s.cacheDirty {
			return E.New("rule-set returned not modified without a valid cache")
		}
		if err = s.updateContextError(ctx); err != nil {
			return err
		}
		lastUpdated := time.Now()
		if s.cacheFile != nil {
			s.cacheDirty = true
			savedRuleSet := s.cacheFile.LoadRuleSet(s.tag)
			if savedRuleSet == nil {
				return E.New("rule-set cache metadata is missing")
			}
			savedRuleSet.LastUpdated = lastUpdated
			savedRuleSet.URLHash = s.urlHash[:]
			if err = s.cacheFile.SaveRuleSet(s.tag, savedRuleSet); err != nil {
				return E.Cause(err, "save rule-set updated time")
			}
			s.cacheDirty = false
		}
		if err = s.updateContextError(ctx); err != nil {
			return err
		}
		s.setUpdatedTime(lastUpdated)
		s.logger.InfoContext(ctx, "update rule-set ", s.tag, ": not modified")
		return nil
	default:
		return E.New("unexpected status: ", response.Status)
	}
	content, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	rules, err := s.parseBytes(content)
	if err != nil {
		return err
	}
	eTagHeader := response.Header.Get("Etag")
	if err = s.updateContextError(ctx); err != nil {
		return err
	}
	lastUpdated := time.Now()
	contentHash := hash.MakeHash(content)
	// A partially persisted update must be retried without a conditional request.
	s.cacheDirty = true
	if s.path != "" {
		if err = s.saveCacheFile(ctx, content); err != nil {
			return E.Cause(err, "save rule-set cache file")
		}
	}
	if s.cacheFile != nil {
		savedRuleSet := &adapter.SavedBinary{
			LastUpdated: lastUpdated,
			LastEtag:    eTagHeader,
			URLHash:     s.urlHash[:],
		}
		if s.path != "" {
			savedRuleSet.Hash = contentHash
		} else {
			savedRuleSet.Content = content
		}
		if err = s.cacheFile.SaveRuleSet(s.tag, savedRuleSet); err != nil {
			return E.Cause(err, "save rule-set cache metadata")
		}
	}
	if err = s.updateContextError(ctx); err != nil {
		return err
	}
	s.hash = contentHash
	s.lastEtag = eTagHeader
	s.cacheDirty = false
	if err = s.applyRules(rules, s, lastUpdated); err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "updated rule-set ", s.tag)
	return nil
}

func (s *RemoteRuleSet) resolveTransport() (adapter.HTTPTransport, error) {
	httpClientManager := service.FromContext[adapter.HTTPClientManager](s.ctx)
	if s.options.HTTPClient != nil && !s.options.HTTPClient.IsEmpty() {
		if s.options.DownloadDetour != "" { //nolint:staticcheck
			return nil, E.New("http_client is conflict with deprecated download_detour field")
		}
		return httpClientManager.ResolveTransport(s.ctx, s.logger, *s.options.HTTPClient)
	}
	if s.options.DownloadDetour != "" { //nolint:staticcheck
		deprecated.Report(s.ctx, deprecated.OptionLegacyRuleSetDownloadDetour)
		return httpClientManager.ResolveTransport(s.ctx, s.logger, option.HTTPClientOptions{
			DialerOptions: option.DialerOptions{
				Detour: s.options.DownloadDetour, //nolint:staticcheck
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

func (s *RemoteRuleSet) updateContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.ctx.Err()
}

func (s *RemoteRuleSet) loadCacheFile() (bool, error) {
	var content []byte
	var lastUpdated time.Time
	var lastEtag string
	var savedSet *adapter.SavedBinary
	if s.cacheFile != nil {
		if savedSet = s.cacheFile.LoadRuleSet(s.tag); savedSet != nil {
			if len(savedSet.URLHash) > 0 && !bytes.Equal(savedSet.URLHash, s.urlHash[:]) {
				s.logger.Info("cached rule-set was downloaded from another URL, will refetch")
				return false, nil
			}
			s.hash = savedSet.Hash
		}
	}
	if s.path != "" {
		exists, err := pathExists(s.ctx, s.path)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
		file, err := filemanager.Open(s.ctx, s.path)
		if err != nil {
			return false, err
		}
		content, err = io.ReadAll(file)
		if err != nil {
			file.Close()
			return false, err
		}
		info, err := file.Stat()
		closeErr := file.Close()
		if err != nil {
			return false, err
		}
		if closeErr != nil {
			return false, closeErr
		}
		if savedSet != nil {
			if !s.hash.Equal(hash.MakeHash(content)) {
				return false, E.New("load rule-set cache file failed: validation failed")
			}
			lastUpdated = savedSet.LastUpdated
			lastEtag = savedSet.LastEtag
		} else {
			lastUpdated = info.ModTime()
		}
	} else if savedSet != nil && len(savedSet.Content) > 0 {
		content = savedSet.Content
		lastUpdated = savedSet.LastUpdated
		lastEtag = savedSet.LastEtag
	} else {
		return false, nil
	}
	rules, err := s.parseBytes(content)
	if err != nil {
		return false, err
	}
	if err = s.applyRules(rules, s, lastUpdated); err != nil {
		return false, err
	}
	s.lastEtag = lastEtag
	return true, nil
}

func pathExists(ctx context.Context, path string) (bool, error) {
	info, err := filemanager.Stat(ctx, path)
	if err == nil {
		if info.IsDir() {
			return false, E.New("rule_set path is a directory: ", path)
		}
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (s *RemoteRuleSet) saveCacheFile(ctx context.Context, content []byte) error {
	dir := filepath.Dir(s.path)
	err := filemanager.MkdirAll(s.ctx, dir, 0o755)
	if err != nil {
		return err
	}
	mode := os.FileMode(0o666)
	if info, statErr := filemanager.Stat(s.ctx, s.path); statErr == nil {
		mode = info.Mode().Perm()
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}
	tempPath := filepath.Join(dir, ".rule-set-"+rand.Text()+".tmp")
	file, err := filemanager.OpenFile(s.ctx, tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer filemanager.Remove(s.ctx, tempPath)
	_, err = file.Write(content)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = s.updateContextError(ctx); err != nil {
		return err
	}
	return filemanager.Rename(s.ctx, tempPath, s.path)
}

func (s *RemoteRuleSet) Close() error {
	s.cancel()
	s.updateAccess.Lock()
	defer s.updateAccess.Unlock()
	s.closeRules()
	return nil
}
