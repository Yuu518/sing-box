package option

import (
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

func TestInlineRuleSetRoundTrip(t *testing.T) {
	t.Parallel()
	for _, content := range []string{
		`{"tag":"test","rules":[{"domain":"example.com"}]}`,
		`{"type":"inline","tag":"test","rules":[{"domain":"example.com"}]}`,
	} {
		var ruleSet RuleSet
		require.NoError(t, json.Unmarshal([]byte(content), &ruleSet))
		require.Equal(t, C.RuleSetFormatSource, ruleSet.Format)
		encoded, err := json.Marshal(ruleSet)
		require.NoError(t, err)
		require.JSONEq(t, `{"tag":"test","rules":[{"domain":"example.com"}]}`, string(encoded))
		var restored RuleSet
		require.NoError(t, json.Unmarshal(encoded, &restored))
		require.Equal(t, ruleSet, restored)
	}
}

func TestRemoteRuleSetFormatInference(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name, url, path, format, expected string
	}{
		{"binary path", "https://example.com/download", "rules.srs", "", C.RuleSetFormatBinary},
		{"source path", "https://example.com/download", "rules.json", "", C.RuleSetFormatSource},
		{"URL takes precedence", "https://example.com/rules.json", "cache.srs", "", C.RuleSetFormatSource},
		{"explicit format", "https://example.com/rules.json", "cache.json", C.RuleSetFormatBinary, C.RuleSetFormatBinary},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			content, err := json.Marshal(map[string]string{
				"type": "remote", "tag": "test", "url": testCase.url, "path": testCase.path, "format": testCase.format,
			})
			require.NoError(t, err)
			var ruleSet RuleSet
			require.NoError(t, json.Unmarshal(content, &ruleSet))
			require.Equal(t, testCase.expected, ruleSet.Format)
			encoded, err := json.Marshal(ruleSet)
			require.NoError(t, err)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(encoded, &fields))
			if testCase.format == "" {
				require.NotContains(t, fields, "format")
			}
			var restored RuleSet
			require.NoError(t, json.Unmarshal(encoded, &restored))
			require.Equal(t, ruleSet, restored)
		})
	}
}

func TestRemoteRuleSetPathConflict(t *testing.T) {
	var ruleSet RuleSet
	err := json.Unmarshal([]byte(`{
		"type": "remote",
		"tag": "remote",
		"format": "source",
		"path": "remote.json",
		"initial_path": "initial.json",
		"url": "https://example.com/remote.json"
	}`), &ruleSet)
	require.ErrorContains(t, err, "path and initial_path are mutually exclusive")
}

func TestRemoteRuleSetMultipleTagsPath(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		path         string
		requireError bool
	}{
		{
			name: "empty",
		},
		{
			name: "placeholder",
			path: "{tag}.json",
		},
		{
			name:         "shared",
			path:         "shared.json",
			requireError: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			content := `{
				"type": "remote",
				"tag": ["a", "b"],
				"format": "source",
				"path": "` + testCase.path + `",
				"url": "https://example.com/{tag}.json"
			}`
			var ruleSet RuleSet
			err := json.Unmarshal([]byte(content), &ruleSet)
			if testCase.requireError {
				require.ErrorContains(t, err, "missing {tag} placeholder in path")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
