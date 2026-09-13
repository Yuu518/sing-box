//go:build !with_quic

package daemon

import "context"

func contextWithQUICKeepSession(ctx context.Context) context.Context {
	return ctx
}
