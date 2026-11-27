package remote

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/provider/parser"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service/filemanager"
)

// Older caches contain filtered, overridden options and cannot be re-evaluated
// faithfully after configuration changes. Refetch them once on migration.
const providerCachePrefix = "# sing-box provider cache v1\n"

func (s *Provider) storeCache(writeFile bool) error {
	if s.path == "" && s.cacheFile == nil {
		return nil
	}
	s.access.RLock()
	content := []byte(providerCachePrefix)
	if s.hasSubscriptionInfo {
		info := s.subscriptionInfo
		content = fmt.Appendf(content, "# upload=%d; download=%d; total=%d; expire=%d;\n", info.Upload, info.Download, info.Total, info.Expire)
	}
	content = append(content, s.rawContent...)
	saved := &adapter.SavedBinary{
		LastUpdated: s.lastUpdated,
		LastEtag:    s.lastEtag,
		URLHash:     s.urlHash[:],
		ContentHash: append([]byte(nil), s.contentHash...),
	}
	s.access.RUnlock()
	if s.path != "" {
		if writeFile || len(saved.ContentHash) != sha256.Size {
			newHash, err := s.saveCacheFile(content)
			if err != nil {
				return E.Cause(err, "save outbound provider cache file")
			}
			s.access.Lock()
			s.contentHash = append([]byte(nil), newHash...)
			s.access.Unlock()
			saved.ContentHash = newHash
		}
	} else {
		saved.Content = content
	}
	if s.cacheFile != nil {
		if err := s.cacheFile.SaveSubscription(s.Tag(), saved); err != nil {
			return E.Cause(err, "save outbound provider cache metadata")
		}
	}
	return nil
}

func (s *Provider) loadCacheFile() (bool, error) {
	var (
		content           []byte
		lastUpdated       time.Time
		lastEtag          string
		savedSubscription *adapter.SavedBinary
		savedHash         []byte
	)
	if s.cacheFile != nil {
		if savedSubscription = s.cacheFile.LoadSubscription(s.Tag()); savedSubscription != nil {
			if len(savedSubscription.URLHash) > 0 && !bytes.Equal(savedSubscription.URLHash, s.urlHash[:]) {
				s.logger.Info("cached outbound provider was downloaded from another URL, will refetch")
				return false, nil
			}
			savedHash = savedSubscription.ContentHash
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
		fileInfo, err := file.Stat()
		closeErr := file.Close()
		if err != nil {
			return false, err
		}
		if closeErr != nil {
			return false, closeErr
		}
		contentHash := sha256.Sum256(content)
		if savedSubscription != nil {
			if len(savedHash) != sha256.Size || !bytes.Equal(savedHash, contentHash[:]) {
				return false, E.New("load outbound provider cache file failed: validation failed")
			}
			lastUpdated = savedSubscription.LastUpdated
			lastEtag = savedSubscription.LastEtag
		} else {
			lastUpdated = fileInfo.ModTime()
		}
		savedHash = contentHash[:]
	} else if savedSubscription != nil && len(savedSubscription.Content) > 0 {
		content = savedSubscription.Content
		lastUpdated = savedSubscription.LastUpdated
		lastEtag = savedSubscription.LastEtag
	} else {
		return false, nil
	}
	if err := s.loadFromContent(content); err != nil {
		return false, err
	}
	s.access.Lock()
	s.contentHash = append([]byte(nil), savedHash...)
	s.lastUpdated, s.lastEtag = lastUpdated, lastEtag
	s.access.Unlock()
	return true, nil
}

func (s *Provider) loadInitialPath() error {
	contentRaw, err := filemanager.ReadFile(s.ctx, s.initialPath)
	if err != nil {
		return err
	}
	content := s.decodeContent(contentRaw)
	err = s.updateProviderFromContent(content)
	if err != nil {
		return err
	}
	s.access.Lock()
	s.rawContent = content
	s.access.Unlock()
	return nil
}

func (s *Provider) loadFromContent(contentRaw []byte) error {
	if !bytes.HasPrefix(contentRaw, []byte(providerCachePrefix)) {
		return E.New("legacy provider cache requires a fresh download")
	}
	content := s.decodeContent(contentRaw[len(providerCachePrefix):])
	if err := s.updateProviderFromContent(content); err != nil {
		return err
	}
	s.access.Lock()
	s.rawContent = content
	s.access.Unlock()
	return nil
}

func (s *Provider) decodeContent(contentRaw []byte) string {
	content, _ := parser.DecodeBase64URLSafe(string(contentRaw))
	firstLine, others := getFirstLine(content)
	if info, ok := parseCommentInfo(firstLine); ok {
		s.access.Lock()
		s.subscriptionInfo, s.hasSubscriptionInfo = info, true
		s.access.Unlock()
		content, _ = parser.DecodeBase64URLSafe(others)
	}
	return content
}

func pathExists(ctx context.Context, path string) (bool, error) {
	info, err := filemanager.Stat(ctx, path)
	if err == nil {
		if info.IsDir() {
			return false, E.New("provider path is a directory: ", path)
		}
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (s *Provider) saveCacheFile(content []byte) ([]byte, error) {
	dir := filepath.Dir(s.path)
	err := filemanager.MkdirAll(s.ctx, dir, 0o755)
	if err != nil {
		return nil, err
	}
	mode := os.FileMode(0o600)
	if info, statErr := filemanager.Stat(s.ctx, s.path); statErr == nil {
		mode = info.Mode().Perm()
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return nil, statErr
	}
	tempPath := filepath.Join(dir, ".provider-cache-"+rand.Text()+".tmp")
	file, err := filemanager.OpenFile(s.ctx, tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return nil, err
	}
	defer filemanager.Remove(s.ctx, tempPath)
	_, err = file.Write(content)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	err = filemanager.Rename(s.ctx, tempPath, s.path)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(content)
	return sum[:], nil
}
