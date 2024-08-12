package ipset

import (
	"math"
	"math/rand/v2"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
	"go4.org/netipx"
)

func TestSetPrefixCount(t *testing.T) {
	t.Parallel()
	full6 := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	for _, test := range []struct {
		name    string
		ranges4 []Range4
		ranges6 []Range6
	}{
		{name: "empty"},
		{name: "full", ranges4: []Range4{{0, math.MaxUint32}}, ranges6: []Range6{{To: full6}}},
		{name: "single address", ranges4: []Range4{{10, 10}}},
		{name: "unaligned", ranges4: []Range4{{1, 254}}},
		{name: "adjacent", ranges4: []Range4{{0, 127}, {128, 255}}},
		{name: "adjacent at end", ranges4: []Range4{{0, math.MaxUint32 - 1}, {math.MaxUint32, math.MaxUint32}}},
		{name: "adjacent IPv6", ranges6: []Range6{{To: [16]byte{15: 0x7f}}, {From: [16]byte{15: 0x80}, To: full6}}},
		{name: "unaligned IPv6", ranges6: []Range6{{From: [16]byte{7: 1}, To: [16]byte{0: 1, 15: 5}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			set, err := FromRanges(test.ranges4, test.ranges6, nil)
			require.NoError(t, err)
			require.EqualValues(t, len(set.Prefixes()), set.PrefixCount())
		})
	}
}

func TestSetPrefixCountRandom(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		var builder netipx.IPSetBuilder
		for range 8 {
			from4 := random.Uint32()
			builder.AddRange(netipx.IPRangeFrom(addr4(from4), addr4(from4+random.Uint32N(1<<20))))
			var from6, to6 [16]byte
			for i := range from6 {
				from6[i] = byte(random.Uint32())
			}
			to6 = from6
			for i := 8 + random.IntN(8); i < 16; i++ {
				to6[i] = 0xff
			}
			builder.AddRange(netipx.IPRangeFrom(netip.AddrFrom16(from6), netip.AddrFrom16(to6)))
		}
		ipSet, err := builder.IPSet()
		require.NoError(t, err)
		set := FromIPSet(ipSet)
		require.EqualValues(t, len(ipSet.Prefixes()), set.PrefixCount())
	}
}

func addr4(value uint32) netip.Addr {
	return netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
}
