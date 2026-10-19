package userkey

import (
	"strconv"
	"strings"
)

const separator = "\x00"

func Keys(names []string) []string {
	keys := make([]string, len(names))
	seen := make(map[string]bool, len(names))
	for index, name := range names {
		key := name
		if key == "" || seen[key] {
			key = name + separator + strconv.Itoa(index)
		}
		seen[key] = true
		keys[index] = key
	}
	return keys
}

func Split(key string) (name string, display string) {
	if index := strings.Index(key, separator); index >= 0 {
		name = key[:index]
		if name == "" {
			return "", key[index+len(separator):]
		}
		return name, name
	}
	return key, key
}
