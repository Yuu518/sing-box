package rule

import (
	"regexp"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
)

var _ RuleItem = (*AuthUserRegexItem)(nil)

type AuthUserRegexItem struct {
	matchers    []*regexp.Regexp
	description string
}

func NewAuthUserRegexItem(expressions []string) (*AuthUserRegexItem, error) {
	matchers := make([]*regexp.Regexp, 0, len(expressions))
	for i, expression := range expressions {
		matcher, err := regexp.Compile(expression)
		if err != nil {
			return nil, E.Cause(err, "parse auth_user_regex expression ", i)
		}
		matchers = append(matchers, matcher)
	}
	description := "auth_user_regex="
	if len(expressions) == 1 {
		description += expressions[0]
	} else {
		description += F.ToString("[", strings.Join(expressions, " "), "]")
	}
	return &AuthUserRegexItem{matchers: matchers, description: description}, nil
}

func (r *AuthUserRegexItem) Match(metadata *adapter.InboundContext) bool {
	if metadata.User == "" {
		return false
	}
	for _, matcher := range r.matchers {
		if matcher.MatchString(metadata.User) {
			return true
		}
	}
	return false
}

func (r *AuthUserRegexItem) String() string {
	return r.description
}
