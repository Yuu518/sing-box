package remote

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/sagernet/sing-box/adapter"
)

var subscriptionInfoRegexp = regexp.MustCompile(`(?i)(upload|download|total|expire)[\s\t]*=[\s\t]*(-?\d+(?:\.\d+)?(?:e[+-]?\d+)?);?`)

func getFirstLine(content string) (string, string) {
	lines := strings.Split(content, "\n")
	if len(lines) == 1 {
		return lines[0], ""
	}
	others := strings.Join(lines[1:], "\n")
	return lines[0], others
}

func parseCommentInfo(firstLine string) (adapter.SubscriptionInfo, bool) {
	trimmed := strings.TrimSpace(firstLine)
	if !strings.HasPrefix(trimmed, "#") {
		return adapter.SubscriptionInfo{}, false
	}
	return parseInfo(strings.TrimPrefix(trimmed, "#"))
}

func parseInfoNumber(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return n, true
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return int64(f), true
}

func parseInfo(infoStr string) (adapter.SubscriptionInfo, bool) {
	info := adapter.SubscriptionInfo{}
	if infoStr == "" {
		return info, false
	}
	matches := subscriptionInfoRegexp.FindAllStringSubmatch(infoStr, 4)
	if len(matches) == 0 {
		return info, false
	}
	var parsedAny bool
	for _, match := range matches {
		key := strings.ToLower(match[1])
		value, ok := parseInfoNumber(match[2])
		if !ok {
			continue
		}
		parsedAny = true
		switch key {
		case "upload":
			info.Upload = value
		case "download":
			info.Download = value
		case "total":
			info.Total = value
		case "expire":
			info.Expire = value
		default:
			return info, false
		}
	}
	return info, parsedAny
}
