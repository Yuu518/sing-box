package tagname

import F "github.com/sagernet/sing/common/format"

func Deduplicate(names []string) []string {
	return Allocate(names, nil, nil)
}

func Allocate(names []string, preferred map[string]string, taken map[string]bool) []string {
	result := make([]string, len(names))
	used := make(map[string]bool, len(names))
	for i, name := range names {
		tag, loaded := preferred[name]
		if loaded && tag != "" && !taken[tag] && !used[tag] {
			used[tag] = true
			result[i] = tag
		}
	}
	reserved := make(map[string]bool, len(names))
	for _, name := range names {
		reserved[name] = true
	}
	next := make(map[string]int)
	for i, name := range names {
		if result[i] != "" {
			continue
		}
		if !used[name] && !taken[name] {
			used[name] = true
			result[i] = name
			continue
		}
		for {
			next[name]++
			candidate := F.ToString(name, " (", next[name], ")")
			if !used[candidate] && !reserved[candidate] && !taken[candidate] {
				used[candidate] = true
				result[i] = candidate
				break
			}
		}
	}
	return result
}
