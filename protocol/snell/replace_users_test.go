package snell

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	snellprotocol "github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common/auth"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func parseQUICProxyUser(t *testing.T, inbound *Inbound, psk string) (string, bool) {
	t.Helper()
	serverConn := new(captureQUICProxyWritesConn)
	_, err := snellprotocol.NewQUICProxyPacketConn(serverConn, []byte(psk), []byte("surge-client"), M.ParseSocksaddr("example.com:443"), []byte{0xc0, 0, 0, 0, 1, 1})
	require.NoError(t, err)
	require.Len(t, serverConn.writes, 1)
	session, _, err := inbound.quicAuth.parser.ParseQUICProxyInit(serverConn.writes[0])
	if err != nil {
		return "", false
	}
	return auth.UserFromContext[string](session.Context(context.Background()))
}

func TestReplaceUsersSwapsCredentials(t *testing.T) {
	created, err := NewInbound(context.Background(), nil, log.NewNOPFactory().NewLogger("snell"), "snell-in", option.SnellInboundOptions{
		Version: 6,
		AbstractSnellInboundOptions: option.AbstractSnellInboundOptions{
			Users:                   []option.SnellUser{{Name: "alice", PSK: "alice-password"}},
			MultiUserAuthentication: "psk",
		},
	})
	require.NoError(t, err)
	inbound := created.(*Inbound)
	scope := adapter.NewScope(t.Context(), log.NewNOPFactory().Logger())
	t.Cleanup(func() { require.NoError(t, scope.Close()) })
	require.NoError(t, inbound.Start(adapter.StartStateInitialize, scope))

	user, loaded := parseQUICProxyUser(t, inbound, "alice-password")
	require.True(t, loaded)
	require.Equal(t, "alice", user)

	var managed adapter.UserManagedInbound = inbound
	require.NoError(t, managed.ReplaceUsers(&option.SnellInboundOptions{
		Version: 6,
		AbstractSnellInboundOptions: option.AbstractSnellInboundOptions{
			Users:                   []option.SnellUser{{Name: "bob", PSK: "bob-password"}, {Name: "bob-surge", PSK: "bob-surge-password"}},
			MultiUserAuthentication: "psk",
		},
	}))

	_, loaded = parseQUICProxyUser(t, inbound, "alice-password")
	require.False(t, loaded)
	user, loaded = parseQUICProxyUser(t, inbound, "bob-surge-password")
	require.True(t, loaded)
	require.Equal(t, "bob-surge", user)

	require.Error(t, managed.ReplaceUsers(&option.SnellInboundOptions{Version: 6}))
	require.Error(t, managed.ReplaceUsers(&option.VLESSInboundOptions{}))
}
