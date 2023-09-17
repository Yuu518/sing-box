package tagname

import F "github.com/sagernet/sing/common/format"

func Deduplicate(names []string) []string {
	reserved := make(map[string]bool, len(names))
	for _, name := range names {
		reserved[name] = true
	}
	used := make(map[string]bool, len(names))
	next := make(map[string]int)
	result := make([]string, len(names))
	for i, name := range names {
		if !used[name] {
			used[name] = true
			result[i] = name
			continue
		}
		for {
			next[name]++
			candidate := F.ToString(name, " (", next[name], ")")
			if !used[candidate] && !reserved[candidate] {
				used[candidate] = true
				result[i] = candidate
				break
			}
		}
	}
	return result
}
