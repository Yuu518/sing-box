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
			require.Equal(t, "node-b", group.Now())
			require.Equal(t, []string{"node-a", "node-b"}, group.All())
		})
	}
}
