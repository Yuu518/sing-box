package daemon

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (i *Instance) readProxyProviders() *ProxyProviderList {
	result := &ProxyProviderList{}
	if i == nil || i.providerManager == nil {
		return result
	}
	for _, provider := range i.providerManager.Providers() {
		item := &ProxyProvider{Tag: provider.Tag(), Type: provider.Type()}
		if updated := provider.UpdatedAt(); !updated.IsZero() {
			item.UpdatedAt = updated.UnixMilli()
		}
		_, item.Updatable = provider.(adapter.ProviderUpdater)
		for _, outbound := range provider.Outbounds() {
			item.Outbounds = append(item.Outbounds, i.outboundInfo(outbound))
		}
		if subscription, ok := provider.(adapter.ProviderSubscriptionInfo); ok {
			info := subscription.SubscriptionInfo()
			item.Subscription = &ProxyProviderSubscription{Upload: info.Upload, Download: info.Download, Total: info.Total, Expire: info.Expire}
		}
		result.Providers = append(result.Providers, item)
	}
	return result
}

func (s *StartedService) SubscribeProxyProviders(_ *emptypb.Empty, server grpc.ServerStreamingServer[ProxyProviderList]) error {
	updates, done, err := s.urlTestObserver.Subscribe()
	if err != nil {
		return err
	}
	defer s.urlTestObserver.UnSubscribe(updates)
	return s.followInstance(server.Context(), func(ctx context.Context, instance *Instance) error {
		// Provider metadata (including HTTP 304 quota/timestamp updates) has no
		// shared observer. Poll snapshots, sending only changes. Delay notifications
		// use the existing observer; followInstance handles stop/reload and cleanup.
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var previous *ProxyProviderList
		for {
			current := instance.readProxyProviders()
			if previous == nil || !proto.Equal(previous, current) {
				if err := server.Send(current); err != nil {
					return err
				}
				previous = current
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				return nil
			case <-ticker.C:
			case <-updates:
				// Coalesce large batches instead of serializing every node update.
				timer := time.NewTimer(urlTestPushMinInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
	})
}

func (s *StartedService) proxyProvider(tag string) (*Instance, adapter.Provider, error) {
	s.serviceAccess.RLock()
	defer s.serviceAccess.RUnlock()
	if s.serviceStatus.Status != ServiceStatus_STARTED || s.instance == nil {
		return nil, nil, status.Error(codes.FailedPrecondition, "service is not started")
	}
	if s.instance.providerManager != nil {
		if provider, ok := s.instance.providerManager.Get(tag); ok {
			return s.instance, provider, nil
		}
	}
	return nil, nil, status.Error(codes.NotFound, "proxy provider not found: "+tag)
}

func (s *StartedService) UpdateProxyProvider(ctx context.Context, request *ProxyProviderRequest) (*emptypb.Empty, error) {
	instance, provider, err := s.proxyProvider(request.Tag)
	if err != nil {
		return nil, err
	}
	updater, ok := provider.(adapter.ProviderUpdater)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "proxy provider is not updatable")
	}
	if err = ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	// ProviderUpdater owns its fetch context. Return its actual completion/error
	// rather than reporting an update as successful while it is still running.
	if err = updater.Update(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	instance.urlTestHistoryStorage.NotifyUpdated()
	return &emptypb.Empty{}, nil
}

func (s *StartedService) HealthCheckProxyProvider(ctx context.Context, request *ProxyProviderHealthCheckRequest) (*emptypb.Empty, error) {
	options, err := parseProbeOptions(request.Url, request.TimeoutMs, request.Ipv6Test)
	if err != nil {
		return nil, err
	}
	instance, provider, err := s.proxyProvider(request.Tag)
	if err != nil {
		return nil, err
	}
	probeCtx, cancel := instance.probeContext(ctx)
	defer cancel()
	if request.Url == "" && request.TimeoutMs == 0 && !request.Ipv6Test {
		_, err = provider.HealthCheck(probeCtx)
	} else {
		err = instance.probeOutbounds(probeCtx, provider.Outbounds(), options)
	}
	if err != nil {
		if probeCtx.Err() != nil {
			return nil, status.FromContextError(probeCtx.Err()).Err()
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}
