package vless

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestRejectUnixListener(t *testing.T) {
	_, err := NewInbound(context.Background(), nil, log.NewNOPFactory().Logger(), "", option.VLESSInboundOptions{
		ListenOptions: option.ListenOptions{
			ListenUnix: "/tmp/vless.sock",
		},
	})
	if err == nil || err.Error() != "`listen_unix` is not supported for VLESS inbounds" {
		t.Fatalf("unexpected error: %v", err)
	}
}
