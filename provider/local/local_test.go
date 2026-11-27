package local_test

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	providerLocal "github.com/sagernet/sing-box/provider/local"
	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

func TestProviderLocalBase64FilterAndUpdate(t *testing.T) {
	ctx := include.Context(context.Background())
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.txt")
	raw := "trojan://pw@192.0.2.1:443#keep-hk\ntrojan://pw@192.0.2.2:443#drop-us\n"
	require.NoError(t, os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString([]byte(raw))), 0o600))

	var includeReg, excludeReg option.Regexp
	require.NoError(t, json.Unmarshal([]byte(`"keep"`), &includeReg))
	require.NoError(t, json.Unmarshal([]byte(`"drop"`), &excludeReg))

	p, err := providerLocal.NewLocal(ctx, nil, log.NewNOPFactory(), "local-p", option.ProviderLocalOptions{
		Path:    path,
		Include: &includeReg,
		Exclude: &excludeReg,
	})
	require.NoError(t, err)
	defer func() { _ = p.(*providerLocal.Provider).Close() }()

	require.NoError(t, p.(*providerLocal.Provider).StartContext(ctx, nil))
	require.Len(t, p.Outbounds(), 1)
	_, found := p.Outbound("keep-hk")
	require.True(t, found)
	initialUpdatedAt := p.UpdatedAt()
	require.False(t, initialUpdatedAt.IsZero())

	require.NoError(t, os.WriteFile(path, []byte("not a valid subscription"), 0o600))
	futureTime := initialUpdatedAt.Add(time.Minute)
	require.NoError(t, os.Chtimes(path, futureTime, futureTime))
	updater := p.(adapter.ProviderUpdater)
	require.Error(t, updater.Update())
	require.Equal(t, initialUpdatedAt, p.UpdatedAt())
	require.Len(t, p.Outbounds(), 1)

	require.NoError(t, os.WriteFile(path, []byte("trojan://pw@192.0.2.3:443#keep-jp\n"), 0o600))
	require.NoError(t, updater.Update())
	require.Len(t, p.Outbounds(), 1)
	_, found = p.Outbound("keep-jp")
	require.True(t, found)
}
