package parser

import (
	"testing"

	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

func TestOverrideDialerOptionPreservesSubscriptionDetour(t *testing.T) {
	options := overrideDialerOption(option.DialerOptions{
		Detour: "configured-outbound",
	}, nil)

	require.Equal(t, "configured-outbound", options.Detour)
}

func TestOverrideDialerOptionPreservesSubscriptionDetourOverOverride(t *testing.T) {
	detour := "override-outbound"
	options := overrideDialerOption(option.DialerOptions{
		Detour: "subscription-outbound",
	}, &option.OverrideDialerOptions{
		Detour: &detour,
	})

	require.Equal(t, "subscription-outbound", options.Detour)
}

func TestOverrideDialerOptionAppliesDetourWhenMissing(t *testing.T) {
	detour := "configured-outbound"
	options := overrideDialerOption(option.DialerOptions{}, &option.OverrideDialerOptions{
		Detour: &detour,
	})

	require.Equal(t, "configured-outbound", options.Detour)
}
