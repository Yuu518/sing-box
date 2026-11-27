package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds []string `json:"outbounds" reference:"outbound"`
	GroupProviderOptions
	Default                   string `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool   `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	Outbounds []string `json:"outbounds" reference:"outbound"`
	GroupProviderOptions
	URL                       string                  `json:"url,omitempty"`
	Interval                  badoption.Duration      `json:"interval,omitempty"`
	Tolerance                 uint16                  `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration      `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool                    `json:"interrupt_exist_connections,omitempty"`
	Fallback                  *URLTestFallbackOptions `json:"fallback,omitempty"`
}

type URLTestFallbackOptions struct {
	Enabled  bool               `json:"enabled,omitempty"`
	MaxDelay badoption.Duration `json:"max_delay,omitempty"`
}

type GroupProviderOptions struct {
	Providers       []string `json:"providers" reference:"provider"`
	Exclude         *Regexp  `json:"exclude,omitempty"`
	Include         *Regexp  `json:"include,omitempty"`
	UseAllProviders bool     `json:"use_all_providers,omitempty"`
}
