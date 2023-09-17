package outboundfilter

import (
	"strings"
	"testing"
	"time"

	"github.com/dlclark/regexp2/v2"
	"github.com/stretchr/testify/require"
)

func TestMatch(t *testing.T) {
	include := regexp2.MustCompile(`^(?!.*(?i:DMIT)).*🇺🇸`)
	exclude := regexp2.MustCompile(`(?<=US )blocked`)
	for _, test := range []struct {
		tag  string
		want bool
	}{
		{"🇺🇸 US premium", true},
		{"🇺🇸 US blocked", false},
		{"🇺🇸 dMiT", false},
		{"🇯🇵 JP premium", false},
	} {
		matched, err := Match(test.tag, exclude, include)
		require.NoError(t, err)
		require.Equal(t, test.want, matched, test.tag)
	}
	matched, err := Match("node", nil, nil)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = Match("node", regexp2.MustCompile(""), nil)
	require.NoError(t, err)
	require.False(t, matched)
	matched, err = Match("node", nil, regexp2.MustCompile(""))
	require.NoError(t, err)
	require.True(t, matched)
}

func TestMatchTimeout(t *testing.T) {
	expression := regexp2.MustCompile(`^(a+)+$`)
	expression.MatchTimeout = -time.Second
	tag := strings.Repeat("a", 100) + "!"
	for _, field := range []string{"include", "exclude"} {
		var include, exclude *regexp2.Regexp
		if field == "include" {
			include = expression
		} else {
			exclude = expression
		}
		matched, err := Match(tag, exclude, include)
		require.ErrorContains(t, err, "match "+field+" for node")
		require.ErrorContains(t, err, "timeout")
		require.False(t, matched)
	}
	matched, err := Match(tag, regexp2.MustCompile(""), expression)
	require.NoError(t, err)
	require.False(t, matched)
}
