package parser

import (
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"

	"github.com/stretchr/testify/require"
)

func TestOverrideAnyTLSOptions(t *testing.T) {
	testCases := []struct {
		name                   string
		clientMetadata         string
		override               *option.OverrideAnyTLSOptions
		expectedClientMetadata string
	}{
		{
			name: "preserve unset",
		},
		{
			name:                   "preserve value",
			clientMetadata:         "original-client/1.0",
			expectedClientMetadata: "original-client/1.0",
		},
		{
			name:           "clear",
			clientMetadata: "original-client/1.0",
			override: &option.OverrideAnyTLSOptions{
				ClientMetadata: common.Ptr(""),
			},
		},
		{
			name: "replace",
			override: &option.OverrideAnyTLSOptions{
				ClientMetadata: common.Ptr("custom-client/1.0"),
			},
			expectedClientMetadata: "custom-client/1.0",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			outbounds := overrideOutbounds([]option.Outbound{{
				Type: C.TypeAnyTLS,
				Options: &option.AnyTLSOutboundOptions{
					ClientMetadata: testCase.clientMetadata,
				},
			}}, nil, nil, testCase.override)
			options := outbounds[0].Options.(*option.AnyTLSOutboundOptions)
			require.Equal(t, testCase.expectedClientMetadata, options.ClientMetadata)
		})
	}
}

func TestOverrideDialerDetour(t *testing.T) {
	for _, detour := range []string{"香港节点", "original/endpoint", "external-outbound", ""} {
		t.Run(detour, func(t *testing.T) {
			options := overrideDialerOption(option.DialerOptions{Detour: detour}, nil)
			require.Equal(t, detour, options.Detour)
			options = overrideDialerOption(options, &option.OverrideDialerOptions{
				BindInterface: common.Ptr("eth0"),
			})
			require.Equal(t, detour, options.Detour)
			for _, override := range []string{"forced-outbound", ""} {
				options = overrideDialerOption(option.DialerOptions{Detour: detour}, &option.OverrideDialerOptions{
					Detour: common.Ptr(override),
				})
				require.Equal(t, override, options.Detour)
			}
		})
	}
}
