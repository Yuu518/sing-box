package adapter

import "context"

type OutboundScope interface {
	Outbound(tag string) (Outbound, bool)
}

type outboundScopeKey struct{}

func ContextWithOutboundScope(ctx context.Context, scope OutboundScope) context.Context {
	return context.WithValue(ctx, outboundScopeKey{}, scope)
}

func OutboundScopeFromContext(ctx context.Context) OutboundScope {
	scope, _ := ctx.Value(outboundScopeKey{}).(OutboundScope)
	return scope
}
