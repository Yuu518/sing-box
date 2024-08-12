package clashapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/constant"

	"github.com/stretchr/testify/require"
)

type ruleProviderTestRuleSet struct {
	adapter.RuleSet
	updateError error
	updated     bool
}

func (r *ruleProviderTestRuleSet) Name() string {
	return "remote"
}

func (r *ruleProviderTestRuleSet) Type() string {
	return constant.RuleSetTypeRemote
}

func (r *ruleProviderTestRuleSet) Format() string {
	return constant.RuleSetFormatSource
}

func (r *ruleProviderTestRuleSet) RuleCount() uint64 {
	return 1
}

func (r *ruleProviderTestRuleSet) UpdatedTime() time.Time {
	return time.Unix(0, 0).UTC()
}

func (r *ruleProviderTestRuleSet) Update(ctx context.Context) error {
	r.updated = true
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.updateError
}

type ruleProviderTestRouter struct {
	adapter.Router
	ruleSet adapter.RuleSet
}

func (r *ruleProviderTestRouter) RuleSets() []adapter.RuleSet {
	return []adapter.RuleSet{r.ruleSet}
}

func (r *ruleProviderTestRouter) RuleSet(name string) (adapter.RuleSet, bool) {
	if name != r.ruleSet.Name() {
		return nil, false
	}
	return r.ruleSet, true
}

func TestGetRuleProviderContentType(t *testing.T) {
	t.Parallel()

	router := ruleProviderRouter(&ruleProviderTestRouter{ruleSet: &ruleProviderTestRuleSet{}})
	request := httptest.NewRequest(http.MethodGet, "/remote", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.JSONEq(t, `{
		"name": "remote",
		"type": "Rule",
		"vehicleType": "REMOTE",
		"behavior": "SOURCE",
		"ruleCount": 1,
		"updatedAt": "1970-01-01T00:00:00+00:00"
	}`, response.Body.String())
}

func TestRuleProviderRouter(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		method      string
		path        string
		updateError error
		status      int
		updated     bool
	}{
		{name: "list", method: http.MethodGet, path: "/", status: http.StatusOK},
		{name: "escaped name", method: http.MethodGet, path: "/%72emote", status: http.StatusOK},
		{name: "missing", method: http.MethodGet, path: "/missing", status: http.StatusNotFound},
		{name: "update", method: http.MethodPut, path: "/remote", status: http.StatusNoContent, updated: true},
		{name: "update missing", method: http.MethodPut, path: "/missing", status: http.StatusNotFound},
		{name: "update failure", method: http.MethodPut, path: "/remote", updateError: errors.New("update failed"), status: http.StatusInternalServerError, updated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ruleSet := &ruleProviderTestRuleSet{updateError: test.updateError}
			router := ruleProviderRouter(&ruleProviderTestRouter{ruleSet: ruleSet})
			response := httptest.NewRecorder()

			router.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))

			require.Equal(t, test.status, response.Code)
			require.Equal(t, test.updated, ruleSet.updated)
			if test.name == "list" {
				require.Contains(t, response.Body.String(), `"providers":{"remote":`)
			}
			if test.updateError != nil {
				require.Contains(t, response.Body.String(), test.updateError.Error())
			}
		})
	}
}
