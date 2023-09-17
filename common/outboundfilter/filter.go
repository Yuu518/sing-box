package outboundfilter

import (
	E "github.com/sagernet/sing/common/exceptions"

	"github.com/dlclark/regexp2/v2"
)

func Match(tag string, exclude, include *regexp2.Regexp) (bool, error) {
	if exclude != nil {
		matched, err := exclude.MatchString(tag)
		if err != nil {
			return false, E.Cause(err, "match exclude for node ", tag)
		}
		if matched {
			return false, nil
		}
	}
	if include != nil {
		matched, err := include.MatchString(tag)
		if err != nil {
			return false, E.Cause(err, "match include for node ", tag)
		}
		return matched, nil
	}
	return true, nil
}
