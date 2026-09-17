package daemon

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (i *Instance) routeRules() *RuleList {
	result := &RuleList{}
	if i.router == nil {
		return result
	}
	for _, rule := range i.router.Rules() {
		item := &RouteRule{
			Type:      rule.Type(),
			Condition: rule.String(),
		}
		if action := rule.Action(); action != nil {
			item.Action = action.Type()
			item.ActionDescription = action.String()
		}
		result.Rules = append(result.Rules, item)
	}
	return result
}

func (s *StartedService) SubscribeRules(_ *emptypb.Empty, server grpc.ServerStreamingServer[RuleList]) error {
	return s.followInstance(server.Context(), func(ctx context.Context, instance *Instance) error {
		result := &RuleList{}
		if instance != nil {
			result = instance.routeRules()
		}
		if err := server.Send(result); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
}
