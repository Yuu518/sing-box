package daemon

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func ruleProviderUpdatable(ruleSet adapter.RuleSet) bool {
	return ruleSet.Type() == C.RuleSetTypeLocal || ruleSet.Type() == C.RuleSetTypeRemote
}

func (i *Instance) ruleProviders() *RuleProviderList {
	result := &RuleProviderList{}
	if i.router == nil {
		return result
	}
	for _, ruleSet := range i.router.RuleSets() {
		provider := &RuleProvider{
			Tag: ruleSet.Name(), Type: ruleSet.Type(), Format: ruleSet.Format(),
			RuleCount: ruleSet.RuleCount(), Updatable: ruleProviderUpdatable(ruleSet),
		}
		if updatedAt := ruleSet.UpdatedTime(); !updatedAt.IsZero() {
			provider.UpdatedAt = updatedAt.UnixMilli()
		}
		result.Providers = append(result.Providers, provider)
	}
	return result
}

func (s *StartedService) SubscribeRuleProviders(_ *emptypb.Empty, server grpc.ServerStreamingServer[RuleProviderList]) error {
	return s.followInstance(server.Context(), func(ctx context.Context, instance *Instance) error {
		if instance == nil {
			if err := server.Send(&RuleProviderList{}); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}
		updates := make(chan struct{}, 1)
		if instance.router != nil {
			for _, ruleSet := range instance.router.RuleSets() {
				element := ruleSet.RegisterCallback(func(adapter.RuleSet) {
					select {
					case updates <- struct{}{}:
					default:
					}
				})
				defer ruleSet.UnregisterCallback(element)
			}
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var previous *RuleProviderList
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			current := instance.ruleProviders()
			if previous == nil || !proto.Equal(previous, current) {
				if err := server.Send(current); err != nil {
					return err
				}
				previous = current
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-updates:
			case <-ticker.C:
			}
		}
	})
}

func (s *StartedService) UpdateRuleProvider(ctx context.Context, request *RuleProviderRequest) (*emptypb.Empty, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.GetTag() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing rule provider tag")
	}
	instance, err := s.apiInstance()
	if err != nil {
		return nil, err
	}
	var ruleSet adapter.RuleSet
	if instance.router != nil {
		ruleSet, _ = instance.router.RuleSet(request.GetTag())
	}
	if ruleSet == nil {
		return nil, status.Error(codes.NotFound, "rule provider not found: "+request.GetTag())
	}
	if !ruleProviderUpdatable(ruleSet) {
		return nil, status.Error(codes.FailedPrecondition, "rule provider does not support updates: "+request.GetTag())
	}
	updateCtx, cancel := context.WithCancel(instance.ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if deadline, exists := ctx.Deadline(); exists {
		var cancelDeadline context.CancelFunc
		updateCtx, cancelDeadline = context.WithDeadline(updateCtx, deadline)
		defer cancelDeadline()
	}
	if ctx.Err() != nil {
		cancel()
	}
	err = ruleSet.Update(updateCtx)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if updateCtx.Err() != nil {
		return nil, status.FromContextError(updateCtx.Err()).Err()
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &emptypb.Empty{}, nil
}
