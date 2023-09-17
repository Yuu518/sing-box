package provider_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestBoxProviderDefaultLifecycle(t *testing.T) {
	const nodes = `[{"type":"http","tag":"node-a","server":"127.0.0.1","server_port":8080},{"type":"http","tag":"node-b","server":"127.0.0.1","server_port":8081}]`
	for _, kind := range []string{"local", "remote", "inline"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(include.Context(context.Background()))
			defer cancel()
			var providerConfig string
			switch kind {
			case "local":
				path := filepath.Join(t.TempDir(), "subscription.json")
				require.NoError(t, os.WriteFile(path, []byte(`{"outbounds":`+nodes+`}`), 0o600))
				encodedPath, err := json.Marshal(path)
				require.NoError(t, err)
				providerConfig = fmt.Sprintf(`{"type":"local","tag":"subscription","path":%s}`, encodedPath)
			case "remote":
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = fmt.Fprint(w, `{"outbounds":`+nodes+`}`)
				}))
				defer server.Close()
				providerConfig = fmt.Sprintf(`{"type":"remote","tag":"subscription","url":%q}`, server.URL)
			case "inline":
				providerConfig = `{"type":"inline","tag":"subscription","outbounds":` + nodes + `}`
			}
			config := `{"log":{"disabled":true},"outbounds":[{"type":"selector","tag":"select","providers":["subscription"],"default":"node-b"}],"outbound_providers":[` + providerConfig + `],"route":{"final":"select"}}`
			var options option.Options
			require.NoError(t, json.UnmarshalContext(ctx, []byte(config), &options))
			instance, err := box.New(box.Options{Context: ctx, Options: options})
			require.NoError(t, err)
			defer func() { cancel(); _ = instance.Close() }()
			require.NoError(t, instance.Start())
			manager := service.FromContext[adapter.OutboundManager](ctx)
			selected, found := manager.Outbound("select")
			require.True(t, found)
			group := selected.(adapter.OutboundGroup)
			require.Equal(t, "node-b", group.Selected(N.NetworkTCP).Tag())
			require.Equal(t, []string{"node-a", "node-b"}, group.All())
		})
	}
}

func TestBoxProvidersKeepDuplicateNodeNames(t *testing.T) {
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	defer cancel()
	const node = `[{"type":"http","tag":"HK","server":"127.0.0.1","server_port":8080}]`
	config := `{"log":{"disabled":true},"outbounds":[{"type":"http","tag":"HK","server":"127.0.0.1","server_port":8081},{"type":"selector","tag":"select","outbounds":["HK"],"providers":["a","b"]}],"outbound_providers":[{"type":"inline","tag":"a","outbounds":` + node + `},{"type":"inline","tag":"b","outbounds":` + node + `}],"route":{"final":"select"}}`
	var options option.Options
	require.NoError(t, json.UnmarshalContext(ctx, []byte(config), &options))
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	require.NoError(t, err)
	defer func() { cancel(); _ = instance.Close() }()
	require.NoError(t, instance.Start())
	outboundManager := service.FromContext[adapter.OutboundManager](ctx)
	providerManager := service.FromContext[adapter.ProviderManager](ctx)
	static, found := outboundManager.Outbound("HK")
	require.True(t, found)
	first, _ := providerManager.Get("a")
	second, _ := providerManager.Get("b")
	firstNode, found := first.Outbound("HK")
	require.True(t, found)
	secondNode, found := second.Outbound("HK")
	require.True(t, found)
	require.Equal(t, "HK", firstNode.Tag())
	require.Equal(t, "HK", secondNode.Tag())
	selected, _ := outboundManager.Outbound("select")
	group := selected.(adapter.OutboundGroup)
	require.Equal(t, []string{"HK", "HK (1)", "HK (2)"}, group.All())
	for tag, expected := range map[string]adapter.Outbound{"HK": static, "HK (1)": firstNode, "HK (2)": secondNode} {
		member, loaded := group.Member(tag)
		require.True(t, loaded)
		require.Same(t, expected, member)
	}
	require.True(t, group.(interface{ SelectOutbound(string) bool }).SelectOutbound("HK (2)"))
	require.Same(t, secondNode, group.Selected(N.NetworkTCP))
	require.Equal(t, "HK (2)", group.SelectedTag(N.NetworkTCP))
}
