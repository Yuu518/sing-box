package option

import (
	"context"
	"testing"

	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

func TestRouteOptionsConcurrentDialUnmarshalJSON(t *testing.T) {
	t.Parallel()

	var options Options
	err := json.UnmarshalContext(context.Background(), []byte(`{"route":{"concurrent_dial":true}}`), &options)
	require.NoError(t, err)
	require.NotNil(t, options.Route)
	require.True(t, options.Route.ConcurrentDial)
}

func TestRouteOptionsConcurrentDialDefaultFalse(t *testing.T) {
	t.Parallel()

	var options Options
	err := json.UnmarshalContext(context.Background(), []byte(`{"route":{}}`), &options)
	require.NoError(t, err)
	require.NotNil(t, options.Route)
	require.False(t, options.Route.ConcurrentDial)
}
