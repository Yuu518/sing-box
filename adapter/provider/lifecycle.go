package provider

import (
	"reflect"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tagname"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
)

func (a *Adapter) StartStage(stage adapter.StartStage) error {
	a.updateAccess.Lock()
	defer a.updateAccess.Unlock()
	if a.closed {
		return nil
	}
	a.started = true
	a.stage = stage
	a.membersAccess.RLock()
	members := append([]*member(nil), a.members...)
	a.membersAccess.RUnlock()
	ordered, cyclic := sortMembers(members)
	var failed []*member
	failedSet := make(map[*member]bool)
	failedNames := make(map[string]bool)
	for _, it := range cyclic {
		a.logger.Warn("circular outbound dependency for ", it.outbound.Tag(), ", remove this node")
		failed = append(failed, it)
		failedSet[it] = true
		failedNames[it.name] = true
	}
	byName := make(map[string]*member, len(members))
	for _, it := range members {
		byName[it.name] = it
	}
	for _, it := range ordered {
		var depFailed string
		for _, dep := range memberLocalDependencies(it, byName) {
			if failedNames[dep] {
				depFailed = dep
				break
			}
		}
		if depFailed != "" {
			a.logger.Error("dependency ", depFailed, " failed for ", it.outbound.Tag(), ", remove this node")
			failed = append(failed, it)
			failedSet[it] = true
			failedNames[it.name] = true
			continue
		}
		through, _ := a.memberTarget(it.endpoint)
		err := a.startMember(it, through)
		if err != nil {
			a.logger.Error(E.Cause(err, "start ", it.outbound.Tag()), ", remove this node")
			failed = append(failed, it)
			failedSet[it] = true
			failedNames[it.name] = true
		}
	}
	if len(failed) > 0 {
		a.membersAccess.Lock()
		a.members = common.Filter(a.members, func(it *member) bool {
			return !failedSet[it]
		})
		for _, it := range failed {
			delete(a.memberByTag, it.outbound.Tag())
			delete(a.memberByName, it.name)
			delete(a.memberByOutbound, it.outbound)
		}
		a.membersAccess.Unlock()
		a.UpdateGroups()
		a.closeMembers(failed)
	}
	if stage == adapter.StartStatePostStart && a.enabled {
		a.startHealthCheck()
	}
	return nil
}

func (a *Adapter) Update(outbounds []option.Outbound, endpoints []option.Endpoint) {
	a.updateAccess.Lock()
	defer a.updateAccess.Unlock()
	if a.closed {
		return
	}
	specs := a.resolveMembers(outbounds, endpoints)
	a.membersAccess.RLock()
	current := a.memberByName
	a.membersAccess.RUnlock()
	var (
		candidates = make([]*member, 0, len(specs))
		reused     = make(map[*member]bool)
		createdSet = make(map[*member]bool)
	)
	for _, spec := range specs {
		previous := current[spec.name]
		if previous != nil && previous.outbound.Tag() == spec.tag && previous.endpoint == spec.endpoint && previous.memberType == spec.memberType && reflect.DeepEqual(previous.options, spec.options) {
			reused[previous] = true
			candidates = append(candidates, previous)
			continue
		}
		created, err := a.createMember(spec)
		if err != nil {
			a.logger.Warn(E.Cause(err, "create ", spec.tag), ", skip this node")
			continue
		}
		createdSet[created] = true
		candidates = append(candidates, created)
	}
	ordered, cyclic := sortMembers(candidates)
	candidateByName := make(map[string]*member, len(candidates))
	for _, it := range candidates {
		candidateByName[it.name] = it
	}
	rejected := make(map[*member]bool, len(cyclic))
	rejectedNames := make(map[string]bool, len(cyclic))
	for _, it := range cyclic {
		a.logger.Warn("circular outbound dependency for ", it.outbound.Tag(), ", skip this node")
		rejected[it] = true
		rejectedNames[it.name] = true
		if createdSet[it] {
			it.close()
		}
	}
	stagingByName := make(map[string]*member, len(ordered))
	a.membersAccess.Lock()
	a.stagingByName = stagingByName
	a.membersAccess.Unlock()
	defer func() {
		a.membersAccess.Lock()
		a.stagingByName = nil
		a.membersAccess.Unlock()
	}()
	for _, it := range ordered {
		var depFailed string
		for _, dep := range memberLocalDependencies(it, candidateByName) {
			if rejectedNames[dep] {
				depFailed = dep
				break
			}
		}
		if depFailed != "" {
			a.logger.Warn("dependency ", depFailed, " unavailable for ", it.outbound.Tag(), ", skip this node")
			rejected[it] = true
			rejectedNames[it.name] = true
			if createdSet[it] {
				it.close()
			}
			continue
		}
		if createdSet[it] {
			if through, ok := a.memberTarget(it.endpoint); ok {
				err := a.startMember(it, through)
				if err != nil {
					a.logger.Warn(E.Cause(err, "start ", it.outbound.Tag()), ", skip this node")
					it.close()
					rejected[it] = true
					rejectedNames[it.name] = true
					continue
				}
			}
		}
		a.membersAccess.Lock()
		a.stagingByName[it.name] = it
		a.membersAccess.Unlock()
	}
	var (
		members          = make([]*member, 0, len(candidates))
		memberByTag      = make(map[string]*member, len(candidates))
		memberByName     = make(map[string]*member, len(candidates))
		memberByOutbound = make(map[adapter.Outbound]*member, len(candidates))
		activeReused     = make(map[*member]bool, len(reused))
	)
	for _, it := range candidates {
		if rejected[it] {
			continue
		}
		if createdSet[it] && a.history != nil {
			a.history.AddProviderOutbound(it.outbound)
		}
		if reused[it] {
			activeReused[it] = true
		}
		members = append(members, it)
		memberByTag[it.outbound.Tag()] = it
		memberByName[it.name] = it
		memberByOutbound[it.outbound] = it
	}
	var retired []*member
	for _, previous := range current {
		if !activeReused[previous] {
			retired = append(retired, previous)
		}
	}
	a.membersAccess.Lock()
	a.members = members
	a.memberByTag = memberByTag
	a.memberByName = memberByName
	a.memberByOutbound = memberByOutbound
	a.stagingByName = nil
	a.membersAccess.Unlock()
	a.UpdateGroups()
	a.closeMembers(retired)
	if a.enabled && a.started && a.stage >= adapter.StartStatePostStart {
		go a.HealthCheck(a.ctx)
	}
}

func memberLocalDependencies(it *member, byName map[string]*member) []string {
	if wrapper, ok := it.options.(option.DialerOptionsWrapper); ok && wrapper.TakeDialerOptions().DetourLocalOnly {
		return nil
	}
	var deps []string
	for _, dep := range it.outbound.Dependencies() {
		if _, ok := byName[dep]; ok {
			deps = append(deps, dep)
		}
	}
	return deps
}

func sortMembers(members []*member) ([]*member, []*member) {
	byName := make(map[string]*member, len(members))
	for _, it := range members {
		byName[it.name] = it
	}
	const (
		stateUnvisited = iota
		stateVisiting
		stateValid
		stateCyclic
	)
	state := make(map[string]uint8, len(members))
	ordered := make([]*member, 0, len(members))
	var cyclic []*member
	var visit func(it *member) bool
	visit = func(it *member) bool {
		switch state[it.name] {
		case stateValid:
			return true
		case stateVisiting, stateCyclic:
			return false
		}
		state[it.name] = stateVisiting
		for _, depName := range memberLocalDependencies(it, byName) {
			if depName == it.name || !visit(byName[depName]) {
				state[it.name] = stateCyclic
				cyclic = append(cyclic, it)
				return false
			}
		}
		state[it.name] = stateValid
		ordered = append(ordered, it)
		return true
	}
	for _, it := range members {
		if state[it.name] == stateUnvisited {
			visit(it)
		}
	}
	return ordered, cyclic
}

func (a *Adapter) resolveMembers(outbounds []option.Outbound, endpoints []option.Endpoint) []memberSpec {
	specs := make([]memberSpec, 0, len(outbounds)+len(endpoints))
	for i, it := range outbounds {
		tag := it.Tag
		if tag == "" {
			tag = F.ToString(a.providerTag, "/", i)
		}
		specs = append(specs, memberSpec{name: tag, memberType: it.Type, options: it.Options})
	}
	for i, it := range endpoints {
		tag := it.Tag
		if tag == "" {
			tag = F.ToString(a.providerTag, "/endpoint-", i)
		}
		specs = append(specs, memberSpec{name: tag, endpoint: true, memberType: it.Type, options: it.Options})
	}
	names := tagname.Deduplicate(common.Map(specs, func(it memberSpec) string { return it.name }))
	for i := range specs {
		if names[i] != specs[i].name {
			a.logger.Warn("duplicate node name ", specs[i].name, " in provider, renamed to ", names[i])
			specs[i].name = names[i]
		}
	}
	tags := names
	if a.providerManager != nil {
		tags = a.providerManager.AllocateTags(a.providerTag, names)
	}
	for i := range specs {
		specs[i].tag = tags[i]
		if tags[i] != names[i] {
			a.logger.Warn("node name ", names[i], " is used by another provider or outbound, renamed to ", tags[i])
		}
	}
	return specs
}

func (a *Adapter) createMember(spec memberSpec) (*member, error) {
	ctx := adapter.ContextWithOutboundScope(a.ctx, memberScope{a})
	ctx = adapter.WithContext(ctx, &adapter.InboundContext{
		Outbound: spec.tag,
	})
	var (
		outbound adapter.Outbound
		err      error
	)
	if spec.endpoint {
		if a.endpointRegistry == nil {
			return nil, E.New("missing endpoint registry")
		}
		logger := a.logFactory.NewLogger(F.ToString("endpoint/", spec.memberType, "[", spec.tag, "]"))
		outbound, err = a.endpointRegistry.Create(ctx, a.router, logger, spec.tag, spec.memberType, spec.options)
	} else {
		if a.outboundRegistry == nil {
			return nil, E.New("missing outbound registry")
		}
		logger := a.logFactory.NewLogger(F.ToString("outbound/", spec.memberType, "[", spec.tag, "]"))
		outbound, err = a.outboundRegistry.CreateOutbound(ctx, a.router, logger, spec.tag, spec.memberType, spec.options)
	}
	if err != nil {
		return nil, err
	}
	var domainResolver string
	if wrapper, ok := spec.options.(option.DialerOptionsWrapper); ok {
		if resolver := wrapper.TakeDialerOptions().DomainResolver; resolver != nil {
			domainResolver = resolver.Server
		}
	}
	return &member{
		name:           spec.name,
		outbound:       outbound,
		endpoint:       spec.endpoint,
		memberType:     spec.memberType,
		options:        spec.options,
		domainResolver: domainResolver,
	}, nil
}

func (a *Adapter) memberTarget(endpoint bool) (adapter.StartStage, bool) {
	if !a.started {
		return 0, false
	}
	if endpoint && a.stage == adapter.StartStateStart {
		return adapter.StartStatePostStart, true
	}
	return a.stage, true
}

func (a *Adapter) startMember(it *member, through adapter.StartStage) error {
	kind := "outbound/"
	if it.endpoint {
		kind = "endpoint/"
	}
	name := F.ToString(kind, it.outbound.Type(), "[", it.outbound.Tag(), "]")
	lifecycle, isLifecycle := it.outbound.(adapter.Lifecycle)
	for _, stage := range adapter.ListStartStages {
		if stage > through {
			break
		}
		if it.started && stage <= it.startedTill {
			continue
		}
		if isLifecycle {
			if it.scope == nil {
				it.scope = adapter.NewScope(a.ctx, a.logger)
			}
			err := it.scope.Start(name, lifecycle, stage)
			if err != nil {
				return err
			}
		}
		it.started = true
		it.startedTill = stage
	}
	return nil
}

func (a *Adapter) closeMembers(members []*member) error {
	var err error
	for _, it := range members {
		if a.history != nil {
			a.history.RemoveProviderOutbound(it.outbound)
		}
		closeErr := it.close()
		if closeErr != nil {
			a.logger.Error(E.Cause(closeErr, "close ", it.outbound.Tag()))
			err = E.Append(err, closeErr, func(err error) error {
				return E.Cause(err, "close ", it.outbound.Tag())
			})
		}
	}
	return err
}
