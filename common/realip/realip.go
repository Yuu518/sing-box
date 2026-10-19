package realip

import (
	"context"
	"net/http"
	"strings"

	M "github.com/sagernet/sing/common/metadata"
	sHttp "github.com/sagernet/sing/protocol/http"
)

const DefaultUnixHeader = "CF-Connecting-IP"

type headerKey struct{}

func ContextWithHeader(ctx context.Context, header string) context.Context {
	if header == "" {
		return ctx
	}
	return context.WithValue(ctx, headerKey{}, header)
}

func HeaderFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	header, _ := ctx.Value(headerKey{}).(string)
	return header
}

func ParseHeaderValue(value string) (M.Socksaddr, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return M.Socksaddr{}, false
	}
	first := strings.TrimSpace(strings.Split(value, ",")[0])
	addr := M.ParseAddr(first)
	if !addr.IsValid() {
		addr = M.ParseSocksaddr(first).Addr
	}
	if !addr.IsValid() {
		return M.Socksaddr{}, false
	}
	return M.SocksaddrFrom(addr, 0), true
}

func SourceAddress(request *http.Request, header string) M.Socksaddr {
	if header != "" {
		if source, ok := ParseHeaderValue(request.Header.Get(header)); ok {
			remote := M.ParseSocksaddr(request.RemoteAddr)
			if remote.IsValid() {
				source.Port = remote.Port
			}
			return source.Unwrap()
		}
	}
	return sHttp.SourceAddress(request)
}
