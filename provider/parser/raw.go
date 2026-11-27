package parser

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func ParseRawSubscription(ctx context.Context, content string) ([]option.Outbound, []option.Endpoint, error) {
	if base64Content, err := DecodeBase64URLSafe(content); err == nil {
		servers, _ := parseRawSubscription(base64Content)
		if len(servers) > 0 {
			return servers, nil, nil
		}
	}
	outbounds, err := parseRawSubscription(content)
	return outbounds, nil, err
}

func parseRawSubscription(content string) ([]option.Outbound, error) {
	var servers []option.Outbound
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	linkList := strings.SplitSeq(content, "\n")
	for linkLine := range linkList {
		linkLine = strings.TrimSpace(linkLine)
		if linkLine == "" || strings.HasPrefix(linkLine, "#") {
			continue
		}
		server, err := ParseSubscriptionLink(linkLine)
		if err != nil {
			continue
		}
		servers = append(servers, server)
	}
	if len(servers) == 0 {
		return nil, E.New("no servers found")
	}
	return servers, nil
}

func DecodeBase64URLSafe(content string) (string, error) {
	s := strings.TrimSpace(content)
	if s == "" {
		return content, E.New("empty base64 content")
	}
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "+", "-")
	s = strings.TrimRight(s, "=")
	result, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return content, err
	}
	return string(result), nil
}
