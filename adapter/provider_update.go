package adapter

import "context"

type providerUpdateKey struct{}

type providerUpdate struct {
	expected Outbound
}

// ContextWithProviderUpdate makes replacement conditional on the provider's
// current member still being registered. A nil member requires an unused tag.
func ContextWithProviderUpdate(ctx context.Context, expected Outbound) context.Context {
	return context.WithValue(ctx, providerUpdateKey{}, providerUpdate{expected})
}

func ProviderUpdateFromContext(ctx context.Context) (Outbound, bool) {
	update, ok := ctx.Value(providerUpdateKey{}).(providerUpdate)
	return update.expected, ok
}

// ProviderMemberRemover prevents a stale provider snapshot from removing a
// replacement registered by another caller.
type ProviderMemberRemover interface {
	RemoveIfSame(member Outbound) error
}
