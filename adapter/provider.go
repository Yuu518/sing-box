package adapter

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
)

type Provider interface {
	Type() string
	Tag() string
	Outbounds() []Outbound
	Outbound(tag string) (Outbound, bool)
	UpdatedAt() time.Time
	HealthCheck(ctx context.Context) (map[string]uint16, error)
}

type ProviderUpdater interface {
	Update() error
}

type ProviderSubscriptionInfo interface {
	SubscriptionInfo() SubscriptionInfo
}

type ProviderRegistry interface {
	option.ProviderOptionsRegistry
	CreateProvider(ctx context.Context, router Router, logFactory log.Factory, tag string, providerType string, options any) (Provider, error)
}

type ProviderManager interface {
	Lifecycle
	Providers() []Provider
	Get(tag string) (Provider, bool)
	Remove(tag string) error
	Create(ctx context.Context, router Router, logFactory log.Factory, tag string, providerType string, options any) error
	RegisterCallback(callback ProviderUpdateCallback) *list.Element[ProviderUpdateCallback]
	UnregisterCallback(element *list.Element[ProviderUpdateCallback])
	UpdateGroups(tag string)
}

type SubscriptionInfo struct {
	Upload   int64
	Download int64
	Total    int64
	Expire   int64
}

type ProviderUpdateCallback = func(tag string) error

func ProviderOutbounds(ctx context.Context) []Outbound {
	manager := service.FromContext[ProviderManager](ctx)
	if manager == nil {
		return nil
	}
	var outbounds []Outbound
	for _, provider := range manager.Providers() {
		outbounds = append(outbounds, provider.Outbounds()...)
	}
	return outbounds
}
