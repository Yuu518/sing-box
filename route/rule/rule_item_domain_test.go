package rule

import (
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func TestDomainRuleItemsPriority(t *testing.T) {
	t.Parallel()
	domainItem, err := NewDomainItem([]string{"origin.example", "sniff.example", "reverse.example"}, nil)
	require.NoError(t, err)
	keywordItem := NewDomainKeywordItem([]string{"origin"})
	regexItem, err := NewDomainRegexItem([]string{"^sniff\\.example$"})
	require.NoError(t, err)

	originMetadata := &adapter.InboundContext{
		Destination: M.ParseSocksaddr("origin.example:443"),
		SniffHost:   "sniff.example",
		Domain:      "reverse.example",
	}
	require.True(t, domainItem.Match(originMetadata))
	require.True(t, keywordItem.Match(originMetadata))
	require.False(t, regexItem.Match(originMetadata))

	sniffMetadata := &adapter.InboundContext{
		Destination: M.ParseSocksaddr("1.1.1.1:443"),
		SniffHost:   "sniff.example",
		Domain:      "reverse.example",
	}
	require.True(t, domainItem.Match(sniffMetadata))
	require.False(t, keywordItem.Match(sniffMetadata))
	require.True(t, regexItem.Match(sniffMetadata))

	reverseMetadata := &adapter.InboundContext{
		Destination: M.ParseSocksaddr("1.1.1.1:443"),
		Domain:      "reverse.example",
	}
	require.True(t, domainItem.Match(reverseMetadata))
	require.False(t, keywordItem.Match(reverseMetadata))
	require.False(t, regexItem.Match(reverseMetadata))
}
