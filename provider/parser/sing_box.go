package parser

import (
	"context"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badjson"
)

type _SingBoxDocument struct {
	Outbounds []option.Outbound `json:"outbounds"`
	Endpoints []option.Endpoint `json:"endpoints"`
}
type SingBoxDocument _SingBoxDocument

func (o *SingBoxDocument) UnmarshalJSONContext(ctx context.Context, inputContent []byte) error {
	var content badjson.JSONObject
	err := content.UnmarshalJSONContext(ctx, inputContent)
	if err != nil {
		return err
	}
	var parseErr error
	outbounds, hasOutbounds := content.Get("outbounds")
	if hasOutbounds {
		array, ok := outbounds.(badjson.JSONArray)
		if !ok {
			return E.New("outbounds must be an array")
		}
		for i, outbound := range array {
			object, ok := outbound.(*badjson.JSONObject)
			if !ok || object == nil {
				parseErr = E.Append(parseErr, E.New("outbound[", i, "] must be an object"), func(err error) error { return err })
				continue
			}
			typeVal, loaded := object.Get("type")
			if !loaded {
				parseErr = E.Append(parseErr, E.New("missing type in outbound[", i, "]"), func(err error) error { return err })
				continue
			}
			outboundType, ok := typeVal.(string)
			if !ok || outboundType == "" {
				parseErr = E.Append(parseErr, E.New("type in outbound[", i, "] must be a non-empty string"), func(err error) error { return err })
				continue
			}
			switch outboundType {
			case C.TypeDirect, C.TypeBridge, C.TypeBlock, C.TypeDNS, C.TypeSelector, C.TypeURLTest:
				continue
			}
			itemBytes, err := object.MarshalJSONContext(ctx)
			if err != nil {
				parseErr = E.Append(parseErr, E.Cause(err, "outbound[", i, "]"), func(err error) error { return err })
				continue
			}
			var item option.Outbound
			err = json.UnmarshalContext(ctx, itemBytes, &item)
			if err != nil {
				parseErr = E.Append(parseErr, E.Cause(err, "outbound[", i, "]"), func(err error) error { return err })
				continue
			}
			o.Outbounds = append(o.Outbounds, item)
		}
	}
	endpoints, hasEndpoints := content.Get("endpoints")
	if hasEndpoints {
		array, ok := endpoints.(badjson.JSONArray)
		if !ok {
			return E.New("endpoints must be an array")
		}
		for i, endpoint := range array {
			object, ok := endpoint.(*badjson.JSONObject)
			if !ok || object == nil {
				parseErr = E.Append(parseErr, E.New("endpoint[", i, "] must be an object"), func(err error) error { return err })
				continue
			}
			typeVal, loaded := object.Get("type")
			if !loaded {
				parseErr = E.Append(parseErr, E.New("missing type in endpoint[", i, "]"), func(err error) error { return err })
				continue
			}
			endpointType, ok := typeVal.(string)
			if !ok || endpointType == "" {
				parseErr = E.Append(parseErr, E.New("type in endpoint[", i, "] must be a non-empty string"), func(err error) error { return err })
				continue
			}
			switch endpointType {
			case C.TypeOpenVPNServer, C.TypeMASQUEServer:
				continue
			}
			itemBytes, err := object.MarshalJSONContext(ctx)
			if err != nil {
				parseErr = E.Append(parseErr, E.Cause(err, "endpoint[", i, "]"), func(err error) error { return err })
				continue
			}
			var item option.Endpoint
			err = json.UnmarshalContext(ctx, itemBytes, &item)
			if err != nil {
				parseErr = E.Append(parseErr, E.Cause(err, "endpoint[", i, "]"), func(err error) error { return err })
				continue
			}
			o.Endpoints = append(o.Endpoints, item)
		}
	}
	if len(o.Outbounds) == 0 && len(o.Endpoints) == 0 && parseErr != nil {
		return parseErr
	}
	return nil
}

func ParseBoxSubscription(ctx context.Context, content string) ([]option.Outbound, []option.Endpoint, error) {
	options, err := json.UnmarshalExtendedContext[SingBoxDocument](ctx, []byte(content))
	if err != nil {
		return nil, nil, err
	}
	if len(options.Outbounds) == 0 && len(options.Endpoints) == 0 {
		return nil, nil, E.New("no servers found")
	}
	return options.Outbounds, options.Endpoints, nil
}
