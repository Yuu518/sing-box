package ipset

import (
	"encoding/binary"
	"math"
	"math/bits"
	"net/netip"
	"slices"

	E "github.com/sagernet/sing/common/exceptions"

	"go4.org/netipx"
)

type Range4 struct {
	From uint32
	To   uint32
}

type Range6 struct {
	From [16]byte
	To   [16]byte
}

type Set struct {
	ranges4 []Range4
	ranges6 []Range6
	storage any
}

func FromIPSet(set *netipx.IPSet) *Set {
	result := &Set{}
	for _, ipRange := range set.Ranges() {
		from := ipRange.From()
		to := ipRange.To()
		if from.Is4() {
			result.ranges4 = append(result.ranges4, Range4{
				From: binary.BigEndian.Uint32(from.AsSlice()),
				To:   binary.BigEndian.Uint32(to.AsSlice()),
			})
		} else {
			result.ranges6 = append(result.ranges6, Range6{
				From: from.As16(),
				To:   to.As16(),
			})
		}
	}
	return result
}

func FromRanges(ranges4 []Range4, ranges6 []Range6, storage any) (*Set, error) {
	for i, ipRange := range ranges4 {
		if ipRange.From > ipRange.To {
			return nil, E.New("ipset: malformed range ", i)
		}
		if i > 0 && ranges4[i-1].To >= ipRange.From {
			return nil, E.New("ipset: unordered range ", i)
		}
	}
	for i, ipRange := range ranges6 {
		if compare16(ipRange.From, ipRange.To) > 0 {
			return nil, E.New("ipset: malformed range ", i)
		}
		if i > 0 && compare16(ranges6[i-1].To, ipRange.From) >= 0 {
			return nil, E.New("ipset: unordered range ", i)
		}
	}
	return &Set{
		ranges4: ranges4,
		ranges6: ranges6,
		storage: storage,
	}, nil
}

func (s *Set) Ranges4() []Range4 {
	return s.ranges4
}

func (s *Set) Ranges6() []Range6 {
	return s.ranges6
}

func (s *Set) Contains(addr netip.Addr) bool {
	if addr.Is4() {
		value := binary.BigEndian.Uint32(addr.AsSlice())
		index, found := slices.BinarySearchFunc(s.ranges4, value, func(ipRange Range4, target uint32) int {
			if ipRange.From > target {
				return 1
			}
			if ipRange.To < target {
				return -1
			}
			return 0
		})
		_ = index
		return found
	}
	value := addr.As16()
	_, found := slices.BinarySearchFunc(s.ranges6, value, func(ipRange Range6, target [16]byte) int {
		if compare16(ipRange.From, target) > 0 {
			return 1
		}
		if compare16(ipRange.To, target) < 0 {
			return -1
		}
		return 0
	})
	return found
}

func (s *Set) IPSet() *netipx.IPSet {
	var builder netipx.IPSetBuilder
	for _, ipRange := range s.ranges4 {
		var from, to [4]byte
		binary.BigEndian.PutUint32(from[:], ipRange.From)
		binary.BigEndian.PutUint32(to[:], ipRange.To)
		builder.AddRange(netipx.IPRangeFrom(netip.AddrFrom4(from), netip.AddrFrom4(to)))
	}
	for _, ipRange := range s.ranges6 {
		builder.AddRange(netipx.IPRangeFrom(netip.AddrFrom16(ipRange.From), netip.AddrFrom16(ipRange.To)))
	}
	set, err := builder.IPSet()
	if err != nil {
		panic(err)
	}
	return set
}

func (s *Set) Prefixes() []netip.Prefix {
	return s.IPSet().Prefixes()
}

func (s *Set) PrefixCount() uint64 {
	var count uint64
	for i := 0; i < len(s.ranges4); {
		from, to := s.ranges4[i].From, s.ranges4[i].To
		for i++; i < len(s.ranges4) && to != math.MaxUint32 && s.ranges4[i].From == to+1; i++ {
			to = s.ranges4[i].To
		}
		count += rangePrefixCount(uint128{lo: uint64(from)}, uint128{lo: uint64(to)}, 32)
	}
	for i := 0; i < len(s.ranges6); {
		from, to := uint128From16(s.ranges6[i].From), uint128From16(s.ranges6[i].To)
		for i++; i < len(s.ranges6); i++ {
			next, ok := to.addOne()
			if !ok || uint128From16(s.ranges6[i].From) != next {
				break
			}
			to = uint128From16(s.ranges6[i].To)
		}
		count += rangePrefixCount(from, to, 128)
	}
	return count
}

type uint128 struct {
	hi, lo uint64
}

func uint128From16(value [16]byte) uint128 {
	return uint128{
		hi: binary.BigEndian.Uint64(value[:8]),
		lo: binary.BigEndian.Uint64(value[8:]),
	}
}

func (u uint128) trailingZeros() int {
	if u.lo != 0 {
		return bits.TrailingZeros64(u.lo)
	}
	if u.hi != 0 {
		return 64 + bits.TrailingZeros64(u.hi)
	}
	return 128
}

func (u uint128) withLowBits(n int) uint128 {
	if n >= 64 {
		return uint128{hi: u.hi | (uint64(1)<<(n-64) - 1), lo: math.MaxUint64}
	}
	return uint128{hi: u.hi, lo: u.lo | (uint64(1)<<n - 1)}
}

func (u uint128) greater(other uint128) bool {
	return u.hi > other.hi || u.hi == other.hi && u.lo > other.lo
}

func (u uint128) addOne() (uint128, bool) {
	lo, carry := bits.Add64(u.lo, 1, 0)
	hi, carry := bits.Add64(u.hi, 0, carry)
	return uint128{hi: hi, lo: lo}, carry == 0
}

func rangePrefixCount(from, to uint128, maxBits int) uint64 {
	var count uint64
	for {
		size := min(from.trailingZeros(), maxBits)
		for from.withLowBits(size).greater(to) {
			size--
		}
		count++
		end := from.withLowBits(size)
		if end == to {
			return count
		}
		from, _ = end.addOne()
	}
}

func compare16(a [16]byte, b [16]byte) int {
	aHigh := binary.BigEndian.Uint64(a[:8])
	bHigh := binary.BigEndian.Uint64(b[:8])
	if aHigh != bHigh {
		if aHigh < bHigh {
			return -1
		}
		return 1
	}
	aLow := binary.BigEndian.Uint64(a[8:])
	bLow := binary.BigEndian.Uint64(b[8:])
	if aLow != bLow {
		if aLow < bLow {
			return -1
		}
		return 1
	}
	return 0
}
