package xboard

import (
	"context"
	"os"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"

	"github.com/stretchr/testify/require"
)

type stubInbound struct {
	tag    string
	stages []adapter.StartStage
	closed int
}

func (i *stubInbound) Type() string { return "stub" }

func (i *stubInbound) Tag() string { return i.tag }

func (i *stubInbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	i.stages = append(i.stages, stage)
	if stage == adapter.StartStateStart {
		scope.Add(func() error {
			i.closed++
			return nil
		})
	}
	return nil
}

type stubInboundRegistry struct {
	created []*stubInbound
}

func (r *stubInboundRegistry) OptionTypes() []string { return nil }

func (r *stubInboundRegistry) CreateOptions(string) (any, bool) { return nil, false }

func (r *stubInboundRegistry) Create(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ string, _ any) (adapter.Inbound, error) {
	inbound := &stubInbound{tag: tag}
	r.created = append(r.created, inbound)
	return inbound, nil
}

func TestManagedInboundsLifecycle(t *testing.T) {
	logger := log.NewNOPFactory().Logger()
	registry := &stubInboundRegistry{}
	inbounds := newManagedInbounds(registry)
	require.Error(t, inbounds.Create(context.Background(), nil, logger, "in", "stub", nil))
	require.Empty(t, registry.created)

	scope := adapter.NewScope(context.Background(), logger)
	require.NoError(t, inbounds.Start(adapter.StartStateInitialize, scope))
	require.NoError(t, inbounds.Start(adapter.StartStateStart, scope))
	require.NoError(t, inbounds.Create(context.Background(), nil, logger, "in", "stub", nil))
	require.Error(t, inbounds.Create(context.Background(), nil, logger, "in", "stub", nil))
	require.Len(t, registry.created, 1)
	first := registry.created[0]
	require.Equal(t, []adapter.StartStage{adapter.StartStateInitialize, adapter.StartStateStart}, first.stages)
	loaded, found := inbounds.Get("in")
	require.True(t, found)
	require.Same(t, first, loaded)

	require.NoError(t, inbounds.Start(adapter.StartStatePostStart, scope))
	require.NoError(t, inbounds.Start(adapter.StartStateStarted, scope))
	require.Equal(t, adapter.ListStartStages, first.stages)

	require.NoError(t, inbounds.Remove("in"))
	require.Equal(t, 1, first.closed)
	_, found = inbounds.Get("in")
	require.False(t, found)
	require.ErrorIs(t, inbounds.Remove("in"), os.ErrInvalid)

	require.NoError(t, inbounds.Create(context.Background(), nil, logger, "in", "stub", nil))
	second := registry.created[1]
	require.Equal(t, adapter.ListStartStages, second.stages)
	require.NoError(t, scope.Close())
	require.Equal(t, 1, second.closed)
	require.Equal(t, 1, first.closed)
}
