package daemon

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type routeRulesTestRouter struct {
	adapter.Router
	rules []adapter.Rule
}

func (r *routeRulesTestRouter) Rules() []adapter.Rule {
	return r.rules
}

func routeRulesTestInstance(t *testing.T, content string) *Instance {
	t.Helper()
	var options []option.Rule
	require.NoError(t, json.UnmarshalContext(context.Background(), []byte(content), &options))
	router := &routeRulesTestRouter{}
	for _, ruleOptions := range options {
		compiled, err := rule.NewRule(context.Background(), log.NewNOPFactory().NewLogger("router"), ruleOptions, true)
		require.NoError(t, err)
		router.rules = append(router.rules, compiled)
	}
	return &Instance{router: router}
}

func TestRouteRulesPreserveOrderAndActions(t *testing.T) {
	instance := routeRulesTestInstance(t, `[
		{"domain_suffix":"example.com","outbound":"proxy"},
		{"type":"logical","mode":"or","invert":true,"rules":[{"network":"tcp"},{"port":53}],"action":"reject"},
		{"action":"sniff"},
		{"protocol":"dns","action":"hijack-dns"}
	]`)
	rules := instance.routeRules().Rules
	require.Len(t, rules, 4)
	require.Equal(t, "default", rules[0].Type)
	require.Equal(t, "domain_suffix=example.com", rules[0].Condition)
	require.Equal(t, "route", rules[0].Action)
	require.Equal(t, "route(proxy)", rules[0].ActionDescription)
	require.Equal(t, "logical", rules[1].Type)
	require.Equal(t, "!(network=tcp || port=53)", rules[1].Condition)
	require.Equal(t, "reject", rules[1].Action)
	require.Empty(t, rules[2].Condition)
	require.Equal(t, "sniff", rules[2].Action)
	require.Equal(t, "hijack-dns", rules[3].Action)
	require.Empty(t, (&Instance{}).routeRules().Rules)
	withoutAction, err := rule.NewDefaultRule(context.Background(), log.NewNOPFactory().NewLogger("router"), option.DefaultRule{})
	require.NoError(t, err)
	withoutActionInstance := &Instance{router: &routeRulesTestRouter{rules: []adapter.Rule{withoutAction}}}
	require.Empty(t, withoutActionInstance.routeRules().Rules[0].Action)
}

func TestSubscribeRulesFollowsServiceLifecycle(t *testing.T) {
	service := NewStartedService(ServiceOptions{Context: context.Background()})
	t.Cleanup(service.Close)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterStartedServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///rules", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := NewStartedServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	version, err := client.GetVersion(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.True(t, version.RulesSupported)
	stream, err := client.SubscribeRules(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	initial, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, initial.Rules)
	setInstance := func(instance *Instance, state ServiceStatus_Type) {
		service.serviceAccess.Lock()
		defer service.serviceAccess.Unlock()
		service.instance = instance
		service.updateStatus(state)
	}
	for _, content := range []string{
		`[{"domain":"first.example","outbound":"proxy"}]`,
		`[{"domain":"second.example","action":"reject"}]`,
		`[]`,
	} {
		instance := routeRulesTestInstance(t, content)
		setInstance(instance, ServiceStatus_STARTED)
		snapshot, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.True(t, proto.Equal(instance.routeRules(), snapshot))
	}
	setInstance(nil, ServiceStatus_IDLE)
	stopped, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, stopped.Rules)
	cancel()
	_, err = stream.Recv()
	require.Equal(t, codes.Canceled, status.Code(err))
}
