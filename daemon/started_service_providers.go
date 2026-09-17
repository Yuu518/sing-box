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

func (s *StartedService) apiInstance() (*Instance, error) {
	s.serviceAccess.RLock()
	defer s.serviceAccess.RUnlock()
	if s.serviceStatus.Status != ServiceStatus_STARTED || s.instance == nil {
		return nil, status.Error(codes.FailedPrecondition, "service is not started")
	}
	return s.instance, nil
}

func (s *StartedService) apiProvider(tag string) (*Instance, adapter.Provider, error) {
	if tag == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "missing provider tag")
	}
	instance, err := s.apiInstance()
	if err != nil {
		return nil, nil, err
	}
	if instance.providerManager != nil {
		if provider, loaded := instance.providerManager.Get(tag); loaded {
			return instance, provider, nil
		}
	}
	return nil, nil, status.Error(codes.NotFound, "provider not found: "+tag)
}

func (i *Instance) proxyProviders() *ProxyProviderList {
	result := &ProxyProviderList{}
	if i.providerManager == nil {
		return result
	}
	for _, provider := range i.providerManager.Providers() {
		_, updatable := provider.(adapter.ProviderUpdater)
		item := &ProxyProvider{Tag: provider.Tag(), Type: provider.Type(), Updatable: updatable}
		if updatedAt := provider.UpdatedAt(); !updatedAt.IsZero() {
			item.UpdatedAt = updatedAt.UnixMilli()
		}
		for _, outbound := range provider.Outbounds() {
			item.Outbounds = append(item.Outbounds, i.apiGroupItem(outbound))
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
	return s.followInstance(server.Context(), func(ctx context.Context, instance *Instance) error {
		if instance == nil {
			if err := server.Send(&ProxyProviderList{}); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}
		updates := make(chan struct{}, 1)
		if instance.providerManager != nil {
			for _, provider := range instance.providerManager.Providers() {
				element := provider.RegisterCallback(func(string) error {
					select {
					case updates <- struct{}{}:
					default:
					}
					return nil
				})
				defer provider.UnregisterCallback(element)
			}
		}
		historyUpdates, historyDone, err := s.urlTestObserver.Subscribe()
		if err != nil {
			return err
		}
		defer s.urlTestObserver.UnSubscribe(historyUpdates)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var previous *ProxyProviderList
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			current := instance.proxyProviders()
			if previous == nil || !proto.Equal(previous, current) {
				if err = server.Send(current); err != nil {
					return err
				}
				previous = current
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-historyDone:
				return nil
			case <-historyUpdates:
			case <-updates:
			case <-ticker.C:
			}
		}
	})
}

func (s *StartedService) UpdateProxyProvider(ctx context.Context, request *ProxyProviderRequest) (*emptypb.Empty, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	_, provider, err := s.apiProvider(request.GetTag())
	if err != nil {
		return nil, err
	}
	updater, ok := provider.(adapter.ProviderUpdater)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "provider does not support updates: "+provider.Tag())
	}
	if err = updater.Update(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	s.urlTestSubscriber.Emit(struct{}{})
	return &emptypb.Empty{}, nil
}

func (s *StartedService) HealthCheckProxyProvider(ctx context.Context, request *ProxyProviderHealthCheckRequest) (*emptypb.Empty, error) {
	options, err := parseAPIURLTestOptions(request.GetUrl(), request.GetTimeoutMs(), request.GetIpv6Test())
	if err != nil {
		return nil, err
	}
	instance, provider, err := s.apiProvider(request.GetTag())
	if err != nil {
		return nil, err
	}
	tester := instance.apiURLTest
	run, err := tester.prepare(provider.Outbounds())
	if err != nil {
		return nil, err
	}
	testCtx, cancel := context.WithCancel(tester.ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if ctx.Err() != nil {
		cancel()
	}
	run(testCtx, options)
	if err = testCtx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &emptypb.Empty{}, nil
}
