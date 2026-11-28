package sniff_test

import (
	"context"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/sniff"

	"github.com/stretchr/testify/require"
)

func TestSniffHTTP1(t *testing.T) {
	t.Parallel()
	pkt := "GET / HTTP/1.1\r\nHost: www.google.com\r\nAccept: */*\r\n\r\n"
	var metadata adapter.InboundContext
	err := sniff.HTTPHost(context.Background(), &metadata, strings.NewReader(pkt))
	require.NoError(t, err)
	require.Equal(t, metadata.SniffHost, "www.google.com")
}

func TestSniffHTTP1WithPort(t *testing.T) {
	t.Parallel()
	pkt := "GET / HTTP/1.1\r\nHost: www.gov.cn:8080\r\nAccept: */*\r\n\r\n"
	var metadata adapter.InboundContext
	err := sniff.HTTPHost(context.Background(), &metadata, strings.NewReader(pkt))
	require.NoError(t, err)
	require.Equal(t, metadata.SniffHost, "www.gov.cn")
}

func TestSniffHTTP1WithIPHost(t *testing.T) {
	t.Parallel()
	for _, host := range []string{"1.1.1.1", "1.1.1.1:8080", "[2001:db8::1]:443"} {
		pkt := "GET / HTTP/1.1\r\nHost: " + host + "\r\nAccept: */*\r\n\r\n"
		var metadata adapter.InboundContext
		err := sniff.HTTPHost(context.Background(), &metadata, strings.NewReader(pkt))
		require.NoError(t, err)
		require.Empty(t, metadata.SniffHost)
	}
}
