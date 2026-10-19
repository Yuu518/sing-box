package realip

import (
	"context"
	"net/http"
	"testing"
)

func TestSourceAddressPrefersConfiguredHeader(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	request.RemoteAddr = "@"
	request.Header.Set("X-Forwarded-For", "1.1.1.1, 9.9.9.9")
	request.Header.Set("CF-Connecting-IP", "9.9.9.9")
	if got := SourceAddress(request, "CF-Connecting-IP"); got.Addr.String() != "9.9.9.9" {
		t.Fatalf("expected trusted header address, got %s", got)
	}
	if got := SourceAddress(request, ""); got.Addr.String() != "1.1.1.1" {
		t.Fatalf("expected legacy X-Forwarded-For behaviour without header, got %s", got)
	}
}

func TestSourceAddressFallsBackWhenHeaderMissing(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	request.RemoteAddr = "10.0.0.2:5555"
	got := SourceAddress(request, "CF-Connecting-IP")
	if got.Addr.String() != "10.0.0.2" || got.Port != 5555 {
		t.Fatalf("expected remote address fallback, got %s", got)
	}
}

func TestSourceAddressKeepsRemotePort(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	request.RemoteAddr = "127.0.0.1:4321"
	request.Header.Set("CF-Connecting-IP", "2001:db8::1")
	got := SourceAddress(request, "CF-Connecting-IP")
	if got.Addr.String() != "2001:db8::1" || got.Port != 4321 {
		t.Fatalf("unexpected source %s", got)
	}
}

func TestParseHeaderValueRejectsGarbage(t *testing.T) {
	if _, ok := ParseHeaderValue("not-an-ip"); ok {
		t.Fatal("garbage accepted")
	}
	if _, ok := ParseHeaderValue(""); ok {
		t.Fatal("empty accepted")
	}
}

func TestContextHeader(t *testing.T) {
	ctx := ContextWithHeader(context.Background(), "X-Real-IP")
	if HeaderFromContext(ctx) != "X-Real-IP" {
		t.Fatal("header not stored in context")
	}
	if HeaderFromContext(ContextWithHeader(context.Background(), "")) != "" {
		t.Fatal("empty header should not be stored")
	}
}
