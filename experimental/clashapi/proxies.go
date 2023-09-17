package clashapi

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json/badjson"
	N "github.com/sagernet/sing/common/network"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func proxyRouter(server *Server, router adapter.Router) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getProxies(server))

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName(server))
		r.Get("/", getProxy(server))
		r.Get("/delay", getProxyDelay(server))
		r.Put("/", updateProxy)
	})
	return r
}

func parseProxyName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := getEscapeParam(r, "name")
		ctx := context.WithValue(r.Context(), CtxKeyProxyName, name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func findProxyByName(server *Server) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := r.Context().Value(CtxKeyProxyName).(string)
			proxy, exist := server.outbound.Outbound(name)
			if !exist {
				proxy, exist = findProviderProxy(server, name)
			}
			if !exist {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			ctx := context.WithValue(r.Context(), CtxKeyProxy, proxy)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

type namedProxy struct {
	name     string
	outbound adapter.Outbound
}

func providerProxies(server *Server) []namedProxy {
	var result []namedProxy
	used := make(map[string]bool)
	add := func(name string, detour adapter.Outbound) {
		if name == "" || used[name] {
			return
		}
		if _, loaded := server.outbound.Outbound(name); loaded {
			return
		}
		used[name] = true
		result = append(result, namedProxy{name, detour})
	}
	for _, detour := range server.outbound.Outbounds() {
		outboundGroup, isGroup := detour.(adapter.OutboundGroup)
		if !isGroup {
			continue
		}
		tags, members := group.Members(outboundGroup)
		for i := range tags {
			add(tags[i], members[i])
		}
	}
	if server.provider != nil {
		for _, provider := range server.provider.Providers() {
			for _, detour := range provider.Outbounds() {
				add(detour.Tag(), detour)
			}
		}
	}
	return result
}

func findProviderProxy(server *Server, name string) (adapter.Outbound, bool) {
	for _, it := range providerProxies(server) {
		if it.name == name {
			return it.outbound, true
		}
	}
	return nil, false
}

func proxyInfo(server *Server, detour adapter.Outbound) *badjson.JSONObject {
	return proxyInfoWithName(server, detour.Tag(), detour)
}

func proxyInfoWithName(server *Server, name string, detour adapter.Outbound) *badjson.JSONObject {
	var info badjson.JSONObject
	var clashType string
	switch detour.Type() {
	case C.TypeBlock:
		clashType = "Reject"
	default:
		clashType = C.ProxyDisplayName(detour.Type())
	}
	info.Put("type", clashType)
	info.Put("name", name)
	info.Put("udp", common.Contains(detour.Network(), N.NetworkUDP))
	delayHistory := server.urlTestHistory.LoadURLTestHistoryForOutbound(group.RealOutbound(detour, N.NetworkTCP))
	if delayHistory != nil {
		info.Put("history", []*adapter.URLTestHistory{delayHistory})
	} else {
		info.Put("history", []*adapter.URLTestHistory{})
	}
	if group, isGroup := detour.(adapter.OutboundGroup); isGroup {
		now := group.SelectedTag(N.NetworkTCP)
		info.Put("now", now)
		info.Put("all", group.All())
	}
	return &info
}

func getProxies(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var proxyMap badjson.JSONObject
		outbounds := common.Filter(server.outbound.Outbounds(), func(detour adapter.Outbound) bool {
			return detour.Tag() != ""
		})
		outbounds = append(outbounds, common.Map(common.Filter(server.endpoint.Endpoints(), func(detour adapter.Endpoint) bool {
			return detour.Tag() != ""
		}), func(it adapter.Endpoint) adapter.Outbound {
			return it
		})...)

		allProxies := make([]string, 0, len(outbounds))

		for _, detour := range outbounds {
			switch detour.Type() {
			case C.TypeDirect, C.TypeBlock, C.TypeDNS:
				continue
			}
			allProxies = append(allProxies, detour.Tag())
		}

		defaultTag := server.outbound.Default().Tag()

		sort.SliceStable(allProxies, func(i, j int) bool {
			return allProxies[i] == defaultTag
		})

		// fix clash dashboard
		proxyMap.Put("GLOBAL", map[string]any{
			"type":    "Fallback",
			"name":    "GLOBAL",
			"udp":     true,
			"history": []*adapter.URLTestHistory{},
			"all":     allProxies,
			"now":     defaultTag,
		})

		for i, detour := range outbounds {
			var tag string
			if detour.Tag() == "" {
				tag = F.ToString(i)
			} else {
				tag = detour.Tag()
			}
			proxyMap.Put(tag, proxyInfo(server, detour))
		}
		for _, it := range providerProxies(server) {
			proxyMap.Put(it.name, proxyInfoWithName(server, it.name, it.outbound))
		}
		var responseMap badjson.JSONObject
		responseMap.Put("proxies", &proxyMap)
		response, err := responseMap.MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

func getProxy(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		name := r.Context().Value(CtxKeyProxyName).(string)
		response, err := proxyInfoWithName(server, name, proxy).MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

type UpdateProxyRequest struct {
	Name string `json:"name"`
}

func updateProxy(w http.ResponseWriter, r *http.Request) {
	req := UpdateProxyRequest{}
	if err := render.DecodeJSON(r.Body, &req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
	selector, ok := proxy.(*group.Selector)
	if !ok {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("Must be a Selector"))
		return
	}

	if !selector.SelectOutbound(req.Name) {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("Selector update error: not found"))
		return
	}

	render.NoContent(w, r)
}

func groupContains(outboundGroup adapter.OutboundGroup, target adapter.Outbound, visited map[adapter.Outbound]bool) bool {
	_, members := group.Members(outboundGroup)
	for _, member := range members {
		if member == target || group.RealOutbound(member, N.NetworkTCP) == target {
			return true
		}
		memberGroup, isGroup := member.(adapter.OutboundGroup)
		if !isGroup || visited[member] {
			continue
		}
		visited[member] = true
		if groupContains(memberGroup, target, visited) {
			return true
		}
	}
	return false
}

func getProxyDelay(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		url := query.Get("url")
		if strings.HasPrefix(url, "http://") {
			url = ""
		}
		timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 16)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		proxy := group.RealOutbound(r.Context().Value(CtxKeyProxy).(adapter.Outbound), N.NetworkTCP)
		if proxy == nil {
			render.Status(r, http.StatusServiceUnavailable)
			render.JSON(w, r, newError("An error occurred in the delay test"))
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(timeout))
		defer cancel()

		delay, err := urltest.URLTest(ctx, url, proxy)
		defer func() {
			if err != nil {
				server.urlTestHistory.StoreURLTestHistoryForOutbound(proxy, nil)
			} else {
				server.urlTestHistory.StoreURLTestHistoryForOutbound(proxy, &adapter.URLTestHistory{
					Time:  time.Now(),
					Delay: delay,
				})
			}
			for _, detour := range server.outbound.Outbounds() {
				urlTestGroup, isURLTestGroup := detour.(adapter.URLTestGroup)
				if !isURLTestGroup {
					continue
				}
				if !groupContains(urlTestGroup, proxy, map[adapter.Outbound]bool{detour: true}) {
					continue
				}
				urlTestGroup.PerformUpdateCheck()
			}
		}()

		if ctx.Err() != nil {
			render.Status(r, http.StatusGatewayTimeout)
			render.JSON(w, r, ErrRequestTimeout)
			return
		}

		if err != nil || delay == 0 {
			render.Status(r, http.StatusServiceUnavailable)
			render.JSON(w, r, newError("An error occurred in the delay test"))
			return
		}

		render.JSON(w, r, render.M{
			"delay": delay,
		})
	}
}
