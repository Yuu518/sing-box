package userkey

import "testing"

func TestKeysAreUniqueAndReversible(t *testing.T) {
	keys := Keys([]string{"alice", "", "bob", "alice"})
	seen := map[string]bool{}
	for _, key := range keys {
		if seen[key] {
			t.Fatalf("duplicate key %q in %q", key, keys)
		}
		seen[key] = true
	}
	cases := []struct {
		key     string
		name    string
		display string
	}{
		{keys[0], "alice", "alice"},
		{keys[1], "", "1"},
		{keys[2], "bob", "bob"},
		{keys[3], "alice", "alice"},
	}
	for _, c := range cases {
		name, display := Split(c.key)
		if name != c.name || display != c.display {
			t.Fatalf("Split(%q) = %q, %q; want %q, %q", c.key, name, display, c.name, c.display)
		}
	}
}
