package rule

import (
	"testing"

	"github.com/sagernet/sing-box/adapter"
)

func TestAuthUserRegexItem(t *testing.T) {
	item, err := NewAuthUserRegexItem([]string{"-surge$"})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]bool{
		"luca-surge":       true,
		"vk-surge":         true,
		"luca":             false,
		"surge-luca":       false,
		"":                 false,
		"user-surge-other": false,
	}
	for user, want := range cases {
		if got := item.Match(&adapter.InboundContext{User: user}); got != want {
			t.Errorf("Match(%q) = %v, want %v", user, got, want)
		}
	}
	if item.String() != "auth_user_regex=-surge$" {
		t.Errorf("unexpected description %q", item.String())
	}
}

func TestAuthUserRegexItemRejectsInvalidExpression(t *testing.T) {
	if _, err := NewAuthUserRegexItem([]string{"("}); err == nil {
		t.Fatal("invalid expression accepted")
	}
}
