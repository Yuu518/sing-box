package option

import (
	"reflect"

	"github.com/sagernet/sing-box/schema"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"
)

type XboardServiceOptions struct {
	Panels      []XboardPanelOptions `json:"panels,omitempty"`
	Nodes       []XboardNodeOptions  `json:"nodes,omitempty"`
	CacheMaxAge badoption.Duration   `json:"cache_max_age,omitempty"`
}

func (o XboardServiceOptions) DescribeSchema(builder schema.Builder) (*schema.Node, error) {
	node := schema.StrictObject()
	err := builder.FlattenStruct(node, reflect.TypeFor[XboardServiceOptions]())
	if err != nil {
		return nil, err
	}
	node.Required = []string{"panels", "nodes"}
	return node, nil
}

type XboardPanelOptions struct {
	Tag          string             `json:"tag"`
	URL          string             `json:"url" examples:"https://panel.example.com"`
	Token        string             `json:"token"`
	HTTPClient   *HTTPClientOptions `json:"http_client,omitempty"`
	WebSocket    *bool              `json:"websocket,omitempty"`
	PollInterval badoption.Duration `json:"poll_interval,omitempty"`
	PushInterval badoption.Duration `json:"push_interval,omitempty"`
}

func (o XboardPanelOptions) DescribeSchema(builder schema.Builder) (*schema.Node, error) {
	return builder.Define("XboardPanelOptions", func() (*schema.Node, error) {
		node := schema.StrictObject()
		err := builder.FlattenStruct(node, reflect.TypeFor[XboardPanelOptions]())
		if err != nil {
			return nil, err
		}
		node.Required = []string{"tag", "url", "token"}
		return node, nil
	})
}

type XboardNodeOptions struct {
	Inbound    string                 `json:"inbound"`
	Panel      string                 `json:"panel"`
	NodeID     int                    `json:"node_id"`
	NodeType   string                 `json:"node_type,omitempty" enum:"shadowsocks,vmess,vless,trojan,hysteria,hysteria2,tuic,anytls,socks,http,naive"`
	Listen     string                 `json:"listen,omitempty"`
	ListenUnix string                 `json:"listen_unix,omitempty"`
	TLS        *InboundTLSOptions     `json:"tls,omitempty"`
	Transport  *V2RayTransportOptions `json:"transport,omitempty"`
}

func (o XboardNodeOptions) DescribeSchema(builder schema.Builder) (*schema.Node, error) {
	return builder.Define("XboardNodeOptions", func() (*schema.Node, error) {
		node := schema.StrictObject()
		err := builder.FlattenStruct(node, reflect.TypeFor[XboardNodeOptions]())
		if err != nil {
			return nil, err
		}
		nodeID := schema.IntegerNode()
		nodeID.Minimum = common.Ptr(int64(1))
		node.Properties.Put("node_id", nodeID)
		node.Required = []string{"panel", "node_id"}
		return node, nil
	})
}
