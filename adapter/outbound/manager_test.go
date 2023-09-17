package outbound

import (
	"context"
	"fmt"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

type managerTestOutbound struct {
	Adapter
	N.Dialer
	closed bool
}

func (o *managerTestOutbound) Close() error { o.closed = true; return nil }

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	logger := log.NewNOPFactory().NewLogger("test")
	registry := NewRegistry()
	Register[option.DirectOutboundOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options option.DirectOutboundOptions) (adapter.Outbound, error) {
		var dependencies []string
		if options.Detour != "" {
			dependencies = []string{options.Detour}
		}
		return &managerTestOutbound{Adapter: NewAdapter("test", tag, []string{N.NetworkTCP}, dependencies)}, nil
	})
	manager := NewManager(logger, registry, endpoint.NewManager(logger, endpoint.NewRegistry()), "")
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func createTestMember(t *testing.T, manager *Manager, tag, detour string) *managerTestOutbound {
	t.Helper()
	require.NoError(t, manager.Create(context.Background(), nil, log.NewNOPFactory().NewLogger("test"), tag, "test", &option.DirectOutboundOptions{DialerOptions: option.DialerOptions{Detour: detour}}))
	member, _ := manager.Outbound(tag)
	return member.(*managerTestOutbound)
}

func TestDefaultDetourSeesReplacement(t *testing.T) {
	manager := newTestManager(t)
	previous := createTestMember(t, manager, "first", "")
	require.NoError(t, manager.Start(adapter.StartStateInitialize))
	detour := dialer.NewDefaultOutboundDetour(manager).(*dialer.DetourDialer)
	resolved, err := detour.Dialer()
	require.NoError(t, err)
	require.Same(t, previous, resolved)
	replacement := createTestMember(t, manager, "first", "")
	require.True(t, previous.closed)
	resolved, err = detour.Dialer()
	require.NoError(t, err)
	require.Same(t, replacement, resolved)
}

func TestRemovedExplicitDefaultUsesSurvivingOutbound(t *testing.T) {
	manager := newTestManager(t)
	manager.defaultTag = "node"
	node := createTestMember(t, manager, "node", "")
	survivor := createTestMember(t, manager, "survivor", "")
	require.NoError(t, manager.Start(adapter.StartStateInitialize))
	require.NoError(t, manager.RemoveIfSame(node))
	require.Same(t, survivor, manager.Default())
	replacement := createTestMember(t, manager, "node", "")
	require.Same(t, replacement, manager.Default())
}

func TestRemovedSoleDefaultRemainsSafeAndRecovers(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			manager := newTestManager(t)
			if explicit {
				manager.defaultTag = "node"
			}
			node := createTestMember(t, manager, "node", "")
			require.NoError(t, manager.Start(adapter.StartStateInitialize))
			require.NoError(t, manager.RemoveIfSame(node))
			unavailable := manager.Default()
			require.NotNil(t, unavailable)
			require.Contains(t, unavailable.Network(), N.NetworkTCP)
			_, err := unavailable.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("127.0.0.1:80"))
			require.ErrorContains(t, err, "default outbound is not available")
			replacement := createTestMember(t, manager, "renamed", "")
			require.Same(t, replacement, manager.Default())
			returned := createTestMember(t, manager, "node", "")
			if explicit {
				require.Same(t, returned, manager.Default())
			}
		})
	}
}

func TestProviderRemovalClosesReferencedMember(t *testing.T) {
	manager := newTestManager(t)
	member := createTestMember(t, manager, "node", "")
	createTestMember(t, manager, "dependent", "node")
	require.NoError(t, manager.Start(adapter.StartStateInitialize))
	require.ErrorContains(t, manager.Remove("node"), "is depended by")
	current, found := manager.Outbound("node")
	require.True(t, found)
	require.Same(t, member, current)
	require.False(t, member.closed)
	require.NoError(t, manager.RemoveIfSame(member))
	require.True(t, member.closed)
	_, found = manager.Outbound("node")
	require.False(t, found)
	replacement := createTestMember(t, manager, "node", "")
	require.ErrorContains(t, manager.Remove("node"), "is depended by")
	require.NoError(t, manager.Remove("dependent"))
	require.NoError(t, manager.Remove("node"))
	require.True(t, replacement.closed)
}

func TestReplacementRemovesOldDependency(t *testing.T) {
	manager := newTestManager(t)
	createTestMember(t, manager, "first", "")
	createTestMember(t, manager, "second", "")
	createTestMember(t, manager, "dependent", "first")
	createTestMember(t, manager, "dependent", "second")
	require.NoError(t, manager.Remove("first"))
	require.ErrorContains(t, manager.Remove("second"), "is depended by")
}

type stageTrackingOutbound struct {
	managerTestOutbound
	stages []adapter.StartStage
}

func (o *stageTrackingOutbound) Start(stage adapter.StartStage) error {
	o.stages = append(o.stages, stage)
	return nil
}

func TestImplicitDefaultReceivesInitializeStage(t *testing.T) {
	manager := newTestManager(t)
	fallback := &stageTrackingOutbound{managerTestOutbound: managerTestOutbound{
		Adapter: NewAdapter("test", "fallback", []string{N.NetworkTCP}, nil),
	}}
	manager.Initialize(func() (adapter.Outbound, error) { return fallback, nil })
	for _, stage := range adapter.ListStartStages {
		require.NoError(t, manager.Start(stage))
	}
	require.Same(t, fallback, manager.Default())
	require.Equal(t, adapter.ListStartStages, fallback.stages)
}

func TestExplicitDefaultCanLoadDuringProviderStartup(t *testing.T) {
	manager := newTestManager(t)
	manager.defaultTag = "node"
	require.NoError(t, manager.Start(adapter.StartStateInitialize))
	require.NoError(t, manager.StartAvailable())
	node := createTestMember(t, manager, "node", "")
	require.NoError(t, manager.StartRemaining())
	require.Same(t, node, manager.Default())
}

func TestMissingExplicitDefaultFailsAfterProviderStartup(t *testing.T) {
	for _, providers := range []bool{false, true} {
		t.Run(fmt.Sprint(providers), func(t *testing.T) {
			manager := newTestManager(t)
			manager.defaultTag = "missing"
			require.NoError(t, manager.Start(adapter.StartStateInitialize))
			if providers {
				require.NoError(t, manager.StartAvailable())
				require.ErrorContains(t, manager.StartRemaining(), "default outbound not found: missing")
			} else {
				require.ErrorContains(t, manager.Start(adapter.StartStateStart), "default outbound not found: missing")
			}
		})
	}
}
