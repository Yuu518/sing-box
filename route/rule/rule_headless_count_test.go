package rule

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/ipset"
	"github.com/sagernet/sing-box/common/srs"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/domain"
	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
)

func TestHeadlessRuleEntryCount(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		options option.DefaultHeadlessRule
		count   uint64
	}{
		{
			name: "domains and suffixes",
			options: option.DefaultHeadlessRule{
				Domain:       []string{"a.example", "b.example", "c.example"},
				DomainSuffix: []string{"example.org", ".example.net"},
			},
			count: 5,
		},
		{
			name: "domain patterns",
			options: option.DefaultHeadlessRule{
				DomainKeyword: []string{"example", "test"},
				DomainRegex:   []string{`^a\.example$`, `^b\.example$`},
				AdGuardDomain: []string{"||example.org^", "||example.net^"},
			},
			count: 6,
		},
		{
			name: "IPv4 IPv6 and source addresses",
			options: option.DefaultHeadlessRule{
				SourceIPCIDR: []string{"192.0.2.1", "2001:db8::1"},
				IPCIDR:       []string{"198.51.100.1", "203.0.113.0/24", "2001:db8:1::/48"},
			},
			count: 5,
		},
		{
			name: "additional conditions do not collapse entries",
			options: option.DefaultHeadlessRule{
				Domain:       []string{"a.example", "b.example"},
				DomainSuffix: []string{"example.org"},
				IPCIDR:       []string{"192.0.2.0/24", "2001:db8::/32"},
				Port:         []uint16{443},
				Network:      []string{"tcp"},
				Invert:       true,
			},
			count: 5,
		},
		{
			name: "CIDRs are not expanded into addresses",
			options: option.DefaultHeadlessRule{
				IPCIDR: []string{"0.0.0.0/0", "::/0"},
			},
			count: 2,
		},
		{
			name: "other conditions retain their count",
			options: option.DefaultHeadlessRule{
				Port:      []uint16{80, 443},
				PortRange: []string{"1000:2000"},
			},
			count: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rule, err := NewDefaultHeadlessRule(t.Context(), test.options)
			require.NoError(t, err)
			require.Equal(t, test.count, rule.RuleCount())
		})
	}
}

func TestBinaryHeadlessRuleEntryCount(t *testing.T) {
	t.Parallel()
	for version := uint8(1); version <= C.RuleSetVersionCurrent; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			options := option.DefaultHeadlessRule{
				Domain:        []string{"a.example", "b.example"},
				DomainSuffix:  []string{"example.org", ".example.net"},
				DomainKeyword: []string{"keyword1", "keyword2"},
				DomainRegex:   []string{`^one$`, `^two$`},
				SourceIPCIDR:  []string{"192.0.2.1", "2001:db8::1"},
				// These adjacent CIDRs form one range, but still require two prefixes.
				IPCIDR: []string{"10.0.0.0/24", "10.0.1.0/25"},
				Port:   []uint16{443},
			}
			expected := uint64(12)
			if version >= C.RuleSetVersion2 {
				options.AdGuardDomain = []string{"||ad1.example^", "||ad2.example^"}
				expected += 2
			}
			var content bytes.Buffer
			require.NoError(t, srs.Write(&content, option.PlainRuleSet{Rules: []option.HeadlessRule{{
				Type: C.RuleTypeDefault, DefaultOptions: options,
			}}}, version))
			for _, recover := range []bool{false, true} {
				t.Run(fmt.Sprint("recover=", recover), func(t *testing.T) {
					parsed, err := srs.Read(bytes.NewReader(content.Bytes()), recover)
					require.NoError(t, err)
					rule, err := NewHeadlessRule(t.Context(), parsed.Options.Rules[0])
					require.NoError(t, err)
					require.Equal(t, expected, rule.RuleCount())
				})
			}
		})
	}
}

func TestHeadlessRuleEntryCountUsesAvailableEntries(t *testing.T) {
	t.Parallel()
	options := option.DefaultHeadlessRule{
		Domain:       []string{"a.example", "a.example"},
		DomainSuffix: []string{"example.org", "example.org"},
		IPCIDR:       []string{"10.0.0.0/25", "10.0.0.128/25", "2001:db8::/32", "2001:db8:1::/48"},
	}
	rule, err := NewDefaultHeadlessRule(t.Context(), options)
	require.NoError(t, err)
	require.Equal(t, uint64(8), rule.RuleCount())
	var content bytes.Buffer
	require.NoError(t, srs.Write(&content, option.PlainRuleSet{Rules: []option.HeadlessRule{{
		Type: C.RuleTypeDefault, DefaultOptions: options,
	}}}, C.RuleSetVersionCurrent))
	parsed, err := srs.Read(&content, false)
	require.NoError(t, err)
	rule, err = NewDefaultHeadlessRule(t.Context(), parsed.Options.Rules[0].DefaultOptions)
	require.NoError(t, err)
	require.Equal(t, uint64(4), rule.RuleCount())
}

func TestHeadlessRuleEntryCountMmapMatchers(t *testing.T) {
	t.Parallel()
	// Exercise the exported mmap representation without requiring OS mmap support.
	matcher, err := domain.NewMatcherFromMmap(domain.NewMatcher(
		[]string{"a.example", "b.example"}, []string{"example.org"}, true,
	).Mmap())
	require.NoError(t, err)
	adGuard, err := domain.NewAdGuardMatcherFromMmap(domain.NewAdGuardMatcher(
		[]string{"||ad1.example^", "||ad2.example^"},
	).Mmap())
	require.NoError(t, err)
	rule, err := NewDefaultHeadlessRule(t.Context(), option.DefaultHeadlessRule{
		DomainMatcher: matcher, AdGuardDomainMatcher: adGuard,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), rule.RuleCount())
}

func TestHeadlessRuleEntryCountEmptyIPSet(t *testing.T) {
	t.Parallel()
	emptySet, err := ipset.FromRanges(nil, nil, nil)
	require.NoError(t, err)
	rule, err := NewHeadlessRule(t.Context(), option.HeadlessRule{DefaultOptions: option.DefaultHeadlessRule{
		SourceIPSet: emptySet, IPSet: emptySet,
	}})
	require.NoError(t, err)
	require.Zero(t, rule.RuleCount())
}

func TestRuleSetEntryCountReload(t *testing.T) {
	t.Parallel()
	ruleSet := &LocalRuleSet{abstractRuleSet: abstractRuleSet{
		ctx: t.Context(), logger: logger.NOP(), format: C.RuleSetFormatSource,
	}}
	var callbackCount uint64
	ruleSet.RegisterCallback(func(updated adapter.RuleSet) { callbackCount = updated.RuleCount() })
	require.NoError(t, ruleSet.loadBytes([]byte(`{"version":4,"rules":[
		{"domain":["a.example","b.example"]},
		{"type":"logical","mode":"and","rules":[
			{"domain_suffix":["example.org","example.net"]},
			{"type":"logical","mode":"or","invert":true,"rules":[
				{"ip_cidr":["192.0.2.0/24","2001:db8::/32"]},
				{"network":"tcp"}
			]}
		]}
	]}`), ruleSet))
	require.Equal(t, uint64(7), ruleSet.RuleCount())
	require.Equal(t, uint64(7), callbackCount)
	ruleSet.Cleanup()
	require.Equal(t, uint64(7), ruleSet.RuleCount())
	require.Error(t, ruleSet.loadBytes([]byte(`{"version":4,"rules":[{"domain_regex":["["]}]}`), ruleSet))
	require.Equal(t, uint64(7), ruleSet.RuleCount())
	require.NoError(t, ruleSet.loadBytes([]byte(`{"version":4,"rules":[{"ip_cidr":["192.0.2.1","2001:db8::1"]}]}`), ruleSet))
	require.Equal(t, uint64(2), ruleSet.RuleCount())
	require.Equal(t, uint64(2), callbackCount)
	require.NoError(t, ruleSet.loadBytes([]byte(`{"version":4,"rules":[]}`), ruleSet))
	require.Zero(t, ruleSet.RuleCount())
}
