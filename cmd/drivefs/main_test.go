package main

import (
	"context"
	"errors"
	"testing"

	"golang.org/x/oauth2"

	"google-drive-linux/internal/auth"
)

type staticTokenSource struct {
	token *oauth2.Token
	err   error
}

func (s staticTokenSource) Token() (*oauth2.Token, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.token, nil
}

func TestReloadableTokenSourceSwapsCredentialsWithoutRestart(t *testing.T) {
	var source reloadableTokenSource
	if _, err := source.Token(); !errors.Is(err, auth.ErrNotAuthenticated) {
		t.Fatalf("empty Token() error = %v, want ErrNotAuthenticated", err)
	}

	source.set(staticTokenSource{err: auth.ErrReauthorizationRequired})
	if _, err := source.Token(); !errors.Is(err, auth.ErrReauthorizationRequired) {
		t.Fatalf("expired Token() error = %v, want ErrReauthorizationRequired", err)
	}

	want := &oauth2.Token{AccessToken: "fresh-token", TokenType: "Bearer"}
	source.set(staticTokenSource{token: want})
	got, err := source.Token()
	if err != nil || got.AccessToken != want.AccessToken {
		t.Fatalf("reloaded Token() = %#v, err=%v; want %q", got, err, want.AccessToken)
	}
	_ = context.Background()
}
