package xboard

import (
	"context"
	"maps"
	"math"
	"sync"
	"time"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

type syncPolicy struct {
	Pull int64 `json:"pull_interval,omitempty"`
	Push int64 `json:"push_interval,omitempty"`
}

type reportCapabilities struct {
	Ack   bool `json:"report_ack,omitempty"`
	Parts bool `json:"report_parts,omitempty"`
}

type nodeControl struct {
	Policy       syncPolicy         `json:"sync"`
	Capabilities reportCapabilities `json:"capabilities"`
}

func splitNodeConfig(response map[string]any) (map[string]any, nodeControl, error) {
	var control nodeControl
	config := response
	if raw, exists := response["node_config"]; exists {
		config, _ = raw.(map[string]any)
		policy, _ := response["sync"].(map[string]any)
		capabilities, _ := response["capabilities"].(map[string]any)
		control = parseNodeControl(policy, capabilities)
	} else {
		if base, ok := config["base_config"].(map[string]any); ok {
			maps.Copy(config, base)
		}
		delete(config, "base_config")
		control = parseNodeControl(config, config)
		for _, key := range []string{"pull_interval", "push_interval", "report_ack", "report_parts"} {
			delete(config, key)
		}
	}
	if len(config) == 0 {
		return nil, nodeControl{}, E.New("missing xboard config")
	}
	return config, control, nil
}

func parseNodeControl(policy, capabilities map[string]any) nodeControl {
	pull, _ := intValue(policy, "pull_interval")
	push, _ := intValue(policy, "push_interval")
	ack, _ := boolValue(capabilities, "report_ack")
	parts, _ := boolValue(capabilities, "report_parts")
	return nodeControl{
		Policy:       syncPolicy{Pull: int64(pull), Push: int64(push)},
		Capabilities: reportCapabilities{Ack: ack, Parts: parts},
	}
}

type syncSchedule struct {
	poll syncInterval
	push syncInterval
}

func (s *syncSchedule) configure(options option.XboardPanelOptions, policy syncPolicy) {
	s.poll.set(time.Duration(options.PollInterval), policy.Pull)
	s.push.set(time.Duration(options.PushInterval), policy.Push)
}

type syncInterval struct {
	access   sync.Mutex
	duration time.Duration
	changed  chan struct{}
}

func (i *syncInterval) set(local time.Duration, seconds int64) {
	duration := local
	if duration <= 0 {
		duration = defaultInterval
		if seconds > 0 && seconds <= math.MaxInt64/int64(time.Second) {
			duration = time.Duration(seconds) * time.Second
		}
	}
	i.access.Lock()
	defer i.access.Unlock()
	if duration != i.duration {
		i.duration = duration
		if i.changed != nil {
			close(i.changed)
			i.changed = nil
		}
	}
}

func (i *syncInterval) current() (time.Duration, <-chan struct{}) {
	i.access.Lock()
	defer i.access.Unlock()
	if i.changed == nil {
		i.changed = make(chan struct{})
	}
	if i.duration == 0 {
		i.duration = defaultInterval
	}
	return i.duration, i.changed
}

func (i *syncInterval) run(ctx context.Context, wake <-chan struct{}, action func()) {
	for ctx.Err() == nil {
		duration, changed := i.current()
		timer := time.NewTimer(duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-changed:
			timer.Stop()
			continue
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
		action()
	}
}
