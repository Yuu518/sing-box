package option

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/schema"
	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

func TestRegexpAdvancedSyntax(t *testing.T) {
	for _, test := range []struct {
		name    string
		pattern string
		accept  string
		reject  string
	}{
		{"negative lookahead", `^(?!.*(?i:DMIT)).*🇺🇸`, "🇺🇸 US node", "🇺🇸 dMiT node"},
		{"positive lookahead", `US(?= premium)`, "US premium", "US standard"},
		{"positive lookbehind", `(?<=US )premium`, "US premium", "JP premium"},
		{"negative lookbehind", `(?<!US )premium`, "JP premium", "US premium"},
		{"backreference", `^(\w+)-\1$`, "US-US", "US-JP"},
		{"named backreference", `^(?<region>\w+)-\k<region>$`, "US-US", "US-JP"},
	} {
		t.Run(test.name, func(t *testing.T) {
			content, err := json.Marshal(test.pattern)
			require.NoError(t, err)
			var expression Regexp
			require.NoError(t, json.Unmarshal(content, &expression))
			require.Equal(t, 100*time.Millisecond, expression.Build().MatchTimeout)
			matched, err := expression.Build().MatchString(test.accept)
			require.NoError(t, err)
			require.True(t, matched)
			matched, err = expression.Build().MatchString(test.reject)
			require.NoError(t, err)
			require.False(t, matched)
			encoded, err := json.Marshal(&expression)
			require.NoError(t, err)
			require.JSONEq(t, string(content), string(encoded))
		})
	}
}

func TestFilterRegexpOptions(t *testing.T) {
	for _, options := range []any{new(ProviderRemoteOptions), new(SelectorOutboundOptions), new(URLTestOutboundOptions)} {
		t.Run(reflect.TypeOf(options).Elem().Name(), func(t *testing.T) {
			content := `{"include":"^(?!.*(?i:DMIT)).*🇺🇸","exclude":"(?<=US )blocked"}`
			require.NoError(t, json.Unmarshal([]byte(content), options))
			encoded, err := json.Marshal(options)
			require.NoError(t, err)
			var values map[string]any
			require.NoError(t, json.Unmarshal(encoded, &values))
			require.Equal(t, `^(?!.*(?i:DMIT)).*🇺🇸`, values["include"])
			require.Equal(t, `(?<=US )blocked`, values["exclude"])
			for _, invalid := range []string{`{"include":"(?<="}`, `{"exclude":"["}`, `{"include":42}`} {
				require.Error(t, json.Unmarshal([]byte(invalid), options))
			}
		})
	}
}

func TestFilterRegexpOptionalAndEmpty(t *testing.T) {
	var options GroupCommonOption
	require.NoError(t, json.Unmarshal([]byte(`{"include":null}`), &options))
	require.Nil(t, options.Include.Build())
	require.Nil(t, options.Exclude.Build())
	require.NoError(t, json.Unmarshal([]byte(`{"include":"","exclude":""}`), &options))
	for _, expression := range []*Regexp{options.Include, options.Exclude} {
		matched, err := expression.Build().MatchString("node")
		require.NoError(t, err)
		require.True(t, matched)
	}
}

func TestFilterRegexpSchema(t *testing.T) {
	for _, valueType := range []reflect.Type{reflect.TypeFor[GroupCommonOption](), reflect.TypeFor[ProviderRemoteOptions]()} {
		content, err := schema.Generate(context.Background(), valueType)
		require.NoError(t, err)
		var root map[string]any
		require.NoError(t, json.Unmarshal(content, &root))
		var check func(map[string]any)
		check = func(node map[string]any) {
			if ref, loaded := node["$ref"].(string); loaded {
				definitions := root["$defs"].(map[string]any)
				check(definitions[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any))
				return
			}
			if branches, loaded := node["oneOf"].([]any); loaded {
				for _, branch := range branches {
					check(branch.(map[string]any))
				}
				return
			}
			properties := node["properties"].(map[string]any)
			for _, field := range []string{"include", "exclude"} {
				require.Equal(t, "string", properties[field].(map[string]any)["type"])
			}
		}
		check(root)
	}
}
