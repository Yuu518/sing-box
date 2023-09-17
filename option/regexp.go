package option

import (
	"time"

	"github.com/sagernet/sing-box/schema"
	"github.com/sagernet/sing/common/json"

	"github.com/dlclark/regexp2/v2"
)

type Regexp struct {
	regexp *regexp2.Regexp
}

func (r *Regexp) Build() *regexp2.Regexp {
	if r == nil {
		return nil
	}
	return r.regexp
}

func (r *Regexp) MarshalJSON() ([]byte, error) {
	if r.Build() == nil {
		return []byte("null"), nil
	}
	return json.Marshal(r.regexp.String())
}

func (r *Regexp) UnmarshalJSON(content []byte) error {
	var pattern string
	err := json.Unmarshal(content, &pattern)
	if err != nil {
		return err
	}
	compiled, err := regexp2.Compile(pattern)
	if err != nil {
		return err
	}
	compiled.MatchTimeout = 100 * time.Millisecond
	r.regexp = compiled
	return nil
}

func (Regexp) DescribeSchema(builder schema.Builder) (*schema.Node, error) {
	return schema.StringNode(), nil
}
