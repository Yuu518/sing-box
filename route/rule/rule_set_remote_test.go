package rule

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(request *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRemoteRuleSetType(t *testing.T) {
	t.Parallel()

	ruleSet, err := NewRemoteRuleSet(context.Background(), logger.NOP(), "remote", option.RuleSet{
		Type:   constant.RuleSetTypeRemote,
		Format: constant.RuleSetFormatSource,
		RemoteOptions: option.RemoteRuleSet{
			URL: "https://example.com/rules.json",
		},
	})
	require.NoError(t, err)
	require.Equal(t, constant.RuleSetTypeRemote, ruleSet.Type())
}

func TestRemoteRuleSetWaitsForConcurrentUpdate(t *testing.T) {
	t.Parallel()

	ruleSet, err := NewRemoteRuleSet(t.Context(), logger.NOP(), "remote", option.RuleSet{
		Type:   constant.RuleSetTypeRemote,
		Format: constant.RuleSetFormatSource,
		RemoteOptions: option.RemoteRuleSet{
			URL: "https://example.com/rules.json",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ruleSet.Close()) })
	requestStarted := make(chan struct{}, 2)
	releaseRequest := make(chan struct{})
	var requests atomic.Int32
	ruleSet.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		requestStarted <- struct{}{}
		<-releaseRequest
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"version": 1,
				"rules": [{"domain": ["example.com"]}]
			}`)),
			Header: make(http.Header),
		}, nil
	})}

	firstUpdate := make(chan error, 1)
	go func() {
		firstUpdate <- ruleSet.Update(t.Context())
	}()
	<-requestStarted

	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, ruleSet.Update(cancelledCtx), context.Canceled)

	secondUpdate := make(chan error, 1)
	go func() {
		secondUpdate <- ruleSet.Update(t.Context())
	}()
	select {
	case <-requestStarted:
		t.Fatal("concurrent update did not wait for the running update")
	case <-time.After(50 * time.Millisecond):
	}
	require.EqualValues(t, 1, requests.Load())
	close(releaseRequest)
	require.NoError(t, <-firstUpdate)
	require.NoError(t, <-secondUpdate)
	require.EqualValues(t, 2, requests.Load())
}

func TestRemoteRuleSetRejectsDirectoryPath(t *testing.T) {
	t.Parallel()

	_, err := NewRemoteRuleSet(t.Context(), logger.NOP(), "remote", option.RuleSet{
		Type:   constant.RuleSetTypeRemote,
		Format: constant.RuleSetFormatSource,
		Path:   t.TempDir(),
		RemoteOptions: option.RemoteRuleSet{
			URL: "https://example.com/rules.json",
		},
	})
	require.ErrorContains(t, err, "rule_set path is a directory")
}

func TestAbstractRuleSetMetadataConcurrentAccess(t *testing.T) {
	t.Parallel()

	ruleSet := &abstractRuleSet{}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := range 1000 {
			ruleSet.access.Lock()
			ruleSet.ruleCount = uint64(i)
			ruleSet.lastUpdated = ruleSet.lastUpdated.Add(1)
			ruleSet.access.Unlock()
		}
	}()
	for range 1000 {
		ruleSet.RuleCount()
		ruleSet.UpdatedTime()
	}
	<-writerDone
}
