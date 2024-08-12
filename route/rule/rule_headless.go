package rule

import (
	"context"
	"math/bits"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

func NewHeadlessRule(ctx context.Context, options option.HeadlessRule) (adapter.HeadlessRule, error) {
	switch options.Type {
	case "", C.RuleTypeDefault:
		if !options.DefaultOptions.IsValid() {
			return nil, E.New("missing conditions")
		}
		return NewDefaultHeadlessRule(ctx, options.DefaultOptions)
	case C.RuleTypeLogical:
		if !options.LogicalOptions.IsValid() {
			return nil, E.New("missing conditions")
		}
		return NewLogicalHeadlessRule(ctx, options.LogicalOptions)
	default:
		return nil, E.New("unknown rule type: ", options.Type)
	}
}

var _ adapter.HeadlessRule = (*DefaultHeadlessRule)(nil)

type DefaultHeadlessRule struct {
	abstractDefaultRule
	ruleCount uint64
}

func NewDefaultHeadlessRule(ctx context.Context, options option.DefaultHeadlessRule) (*DefaultHeadlessRule, error) {
	networkManager := service.FromContext[adapter.NetworkManager](ctx)
	rule := &DefaultHeadlessRule{
		abstractDefaultRule: abstractDefaultRule{
			invert: options.Invert,
		},
	}
	if len(options.QueryType) > 0 {
		item := NewQueryTypeItem(options.QueryType)
		rule.items = append(rule.items, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.Network) > 0 {
		item := NewNetworkItem(options.Network)
		rule.items = append(rule.items, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.Domain) > 0 || len(options.DomainSuffix) > 0 {
		item, err := NewDomainItem(options.Domain, options.DomainSuffix)
		if err != nil {
			return nil, err
		}
		rule.destinationAddressItems = append(rule.destinationAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	} else if options.DomainMatcher != nil {
		item := NewRawDomainItem(options.DomainMatcher)
		rule.destinationAddressItems = append(rule.destinationAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.DomainKeyword) > 0 {
		item := NewDomainKeywordItem(options.DomainKeyword)
		rule.destinationAddressItems = append(rule.destinationAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.DomainRegex) > 0 {
		item, err := NewDomainRegexItem(options.DomainRegex)
		if err != nil {
			return nil, E.Cause(err, "domain_regex")
		}
		rule.destinationAddressItems = append(rule.destinationAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.SourceIPCIDR) > 0 {
		item, err := NewIPCIDRItem(true, options.SourceIPCIDR)
		if err != nil {
			return nil, E.Cause(err, "source_ip_cidr")
		}
		rule.sourceAddressItems = append(rule.sourceAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	} else if options.SourceIPSet != nil {
		item := NewRawIPCIDRItem(true, options.SourceIPSet)
		rule.sourceAddressItems = append(rule.sourceAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.IPCIDR) > 0 {
		item, err := NewIPCIDRItem(false, options.IPCIDR)
		if err != nil {
			return nil, E.Cause(err, "ipcidr")
		}
		rule.destinationIPCIDRItems = append(rule.destinationIPCIDRItems, item)
		rule.allItems = append(rule.allItems, item)
	} else if options.IPSet != nil {
		item := NewRawIPCIDRItem(false, options.IPSet)
		rule.destinationIPCIDRItems = append(rule.destinationIPCIDRItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.SourcePort) > 0 {
		item := NewPortItem(true, options.SourcePort)
		rule.sourcePortItems = append(rule.sourcePortItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.SourcePortRange) > 0 {
		item, err := NewPortRangeItem(true, options.SourcePortRange)
		if err != nil {
			return nil, E.Cause(err, "source_port_range")
		}
		rule.sourcePortItems = append(rule.sourcePortItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.Port) > 0 {
		item := NewPortItem(false, options.Port)
		rule.destinationPortItems = append(rule.destinationPortItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.PortRange) > 0 {
		item, err := NewPortRangeItem(false, options.PortRange)
		if err != nil {
			return nil, E.Cause(err, "port_range")
		}
		rule.destinationPortItems = append(rule.destinationPortItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.ProcessName) > 0 {
		item := NewProcessItem(options.ProcessName)
		rule.items = append(rule.items, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.ProcessPath) > 0 {
		item := NewProcessPathItem(options.ProcessPath)
		rule.items = append(rule.items, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.ProcessPathRegex) > 0 {
		item, err := NewProcessPathRegexItem(options.ProcessPathRegex)
		if err != nil {
			return nil, E.Cause(err, "process_path_regex")
		}
		rule.items = append(rule.items, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.PackageName) > 0 {
		item := NewPackageNameItem(options.PackageName)
		rule.items = append(rule.items, item)
		rule.allItems = append(rule.allItems, item)
	}
	if len(options.PackageNameRegex) > 0 {
		item, err := NewPackageNameRegexItem(options.PackageNameRegex)
		if err != nil {
			return nil, E.Cause(err, "package_name_regex")
		}
		rule.items = append(rule.items, item)
		rule.allItems = append(rule.allItems, item)
	}
	if networkManager != nil {
		if len(options.NetworkType) > 0 {
			item := NewNetworkTypeItem(networkManager, common.Map(options.NetworkType, option.InterfaceType.Build))
			rule.items = append(rule.items, item)
			rule.allItems = append(rule.allItems, item)
		}
		if options.NetworkIsExpensive {
			item := NewNetworkIsExpensiveItem(networkManager)
			rule.items = append(rule.items, item)
			rule.allItems = append(rule.allItems, item)
		}
		if options.NetworkIsConstrained {
			item := NewNetworkIsConstrainedItem(networkManager)
			rule.items = append(rule.items, item)
			rule.allItems = append(rule.allItems, item)
		}
		if len(options.WIFISSID) > 0 {
			item := NewWIFISSIDItem(networkManager, options.WIFISSID)
			rule.items = append(rule.items, item)
			rule.allItems = append(rule.allItems, item)
		}
		if len(options.WIFIBSSID) > 0 {
			item := NewWIFIBSSIDItem(networkManager, options.WIFIBSSID)
			rule.items = append(rule.items, item)
			rule.allItems = append(rule.allItems, item)
		}
		if options.NetworkInterfaceAddress != nil && options.NetworkInterfaceAddress.Size() > 0 {
			item := NewNetworkInterfaceAddressItem(networkManager, options.NetworkInterfaceAddress)
			rule.items = append(rule.items, item)
			rule.allItems = append(rule.allItems, item)
		}
		if len(options.DefaultInterfaceAddress) > 0 {
			item := NewDefaultInterfaceAddressItem(networkManager, options.DefaultInterfaceAddress)
			rule.items = append(rule.items, item)
			rule.allItems = append(rule.allItems, item)
		}
	}
	if len(options.AdGuardDomain) > 0 {
		item := NewAdGuardDomainItem(options.AdGuardDomain)
		rule.destinationAddressItems = append(rule.destinationAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	} else if options.AdGuardDomainMatcher != nil {
		item := NewRawAdGuardDomainItem(options.AdGuardDomainMatcher)
		rule.destinationAddressItems = append(rule.destinationAddressItems, item)
		rule.allItems = append(rule.allItems, item)
	}
	switch {
	case len(rule.destinationAddressItems)+len(rule.destinationIPCIDRItems)+len(rule.sourceAddressItems) > 0:
		rule.ruleCount = headlessRuleEntryCount(options)
	case len(rule.allItems) == 0:
		rule.ruleCount = 1
	case len(rule.allItems) == len(rule.sourcePortItems):
		rule.ruleCount = uint64(len(options.SourcePort) + len(options.SourcePortRange))
	case len(rule.allItems) == len(rule.destinationPortItems):
		rule.ruleCount = uint64(len(options.Port) + len(options.PortRange))
	case len(rule.allItems) == 1:
		rule.ruleCount = max(headlessRuleConditionCount(options, networkManager != nil), 1)
	default:
		rule.ruleCount = 1
	}
	return rule, nil
}

func (r *DefaultHeadlessRule) RuleCount() uint64 {
	return r.ruleCount
}

// Count address entries independently of the other conditions in the rule.
// Binary matchers retain normalized entries, not the original source lists.
func headlessRuleEntryCount(options option.DefaultHeadlessRule) uint64 {
	count := uint64(len(options.Domain)) + uint64(len(options.DomainSuffix))
	if count == 0 && options.DomainMatcher != nil {
		count = succinctSetKeyCount(options.DomainMatcher.Mmap().Leaves)
	}
	count += uint64(len(options.DomainKeyword)) + uint64(len(options.DomainRegex))
	if len(options.SourceIPCIDR) > 0 {
		count += uint64(len(options.SourceIPCIDR))
	} else if options.SourceIPSet != nil {
		// One range can require multiple CIDR prefixes.
		count += options.SourceIPSet.PrefixCount()
	}
	if len(options.IPCIDR) > 0 {
		count += uint64(len(options.IPCIDR))
	} else if options.IPSet != nil {
		count += options.IPSet.PrefixCount()
	}
	if len(options.AdGuardDomain) > 0 {
		count += uint64(len(options.AdGuardDomain))
	} else if options.AdGuardDomainMatcher != nil {
		count += succinctSetKeyCount(options.AdGuardDomainMatcher.Mmap().Leaves)
	}
	return count
}

func succinctSetKeyCount(leaves []uint64) uint64 {
	var count uint64
	for _, word := range leaves {
		count += uint64(bits.OnesCount64(word))
	}
	return count
}

func headlessRuleConditionCount(options option.DefaultHeadlessRule, hasNetworkManager bool) uint64 {
	count := len(options.QueryType) + len(options.Network) +
		len(options.ProcessName) + len(options.ProcessPath) + len(options.ProcessPathRegex) +
		len(options.PackageName) + len(options.PackageNameRegex)
	if hasNetworkManager {
		count += len(options.NetworkType) + len(options.WIFISSID) + len(options.WIFIBSSID) + len(options.DefaultInterfaceAddress)
	}
	return uint64(count)
}

func headlessRuleCount(rule adapter.HeadlessRule) uint64 {
	switch rule := rule.(type) {
	case *DefaultHeadlessRule:
		return rule.ruleCount
	case *LogicalHeadlessRule:
		return rule.ruleCount
	default:
		return 0
	}
}

var _ adapter.HeadlessRule = (*LogicalHeadlessRule)(nil)

type LogicalHeadlessRule struct {
	abstractLogicalRule
	ruleCount uint64
}

func NewLogicalHeadlessRule(ctx context.Context, options option.LogicalHeadlessRule) (*LogicalHeadlessRule, error) {
	r := &LogicalHeadlessRule{
		abstractLogicalRule: abstractLogicalRule{
			rules:  make([]adapter.HeadlessRule, len(options.Rules)),
			invert: options.Invert,
		},
	}
	switch options.Mode {
	case C.LogicalTypeAnd:
		r.mode = C.LogicalTypeAnd
	case C.LogicalTypeOr:
		r.mode = C.LogicalTypeOr
	default:
		return nil, E.New("unknown logical mode: ", options.Mode)
	}
	for i, subRule := range options.Rules {
		rule, err := NewHeadlessRule(ctx, subRule)
		if err != nil {
			return nil, E.Cause(err, "sub rule[", i, "]")
		}
		r.rules[i] = rule
		r.ruleCount += headlessRuleCount(rule)
	}
	return r, nil
}

func (r *LogicalHeadlessRule) RuleCount() uint64 {
	return r.ruleCount
}
