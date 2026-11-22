package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"google-drive-linux/internal/secretstore"
)

const (
	testAccessToken  = "access-token-never-log"
	testRefreshToken = "refresh-token-never-log"
)

func TestAuthorizeUsesPKCEAndRejectsWrongState(t *testing.T) {
	provider := newFakeOAuthProvider(t)
	secrets := newMemorySecretStore()
	var browser BrowserOpener = browserFunc(func(ctx context.Context, target string) error {
		authorizationURL, err := url.Parse(target)
		if err != nil {
			return err
		}
		query := authorizationURL.Query()
		if query.Get("response_type") != "code" || query.Get("access_type") != "offline" || query.Get("prompt") != "consent" {
			t.Errorf("authorization query missing expected flow parameters: %v", query)
		}
		if query.Get("code_challenge_method") != "S256" || query.Get("code_challenge") == "" {
			t.Errorf("authorization query missing PKCE S256 challenge: %v", query)
		}
		provider.setChallenge(query.Get("code_challenge"))
		if !strings.Contains(query.Get("scope"), GoogleDriveScope) {
			t.Errorf("authorization scope = %q, want Drive scope", query.Get("scope"))
		}
		redirect := query.Get("redirect_uri")
		wrongStateURL := callbackURL(redirect, url.Values{
			"state": {"wrong-state"}, "code": {"attacker-code"},
		})
		wrongResponse, err := http.Get(wrongStateURL)
		if err != nil {
			return err
		}
		wrongBody, _ := io.ReadAll(wrongResponse.Body)
		_ = wrongResponse.Body.Close()
		if wrongResponse.StatusCode != http.StatusBadRequest || strings.Contains(string(wrongBody), "attacker-code") {
			t.Errorf("wrong-state callback response = %d %q", wrongResponse.StatusCode, wrongBody)
		}
		validURL := callbackURL(redirect, url.Values{
			"state": {query.Get("state")}, "code": {"valid-auth-code"},
		})
		response, err := http.Get(validURL)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "Authorization received") || strings.Contains(string(body), "valid-auth-code") {
			t.Errorf("valid callback response = %d %q", response.StatusCode, body)
		}
		return nil
	})
	service := newTestService(t, provider, secrets, browser, time.Second)
	token, err := service.Authorize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != testAccessToken || token.RefreshToken != testRefreshToken {
		t.Fatalf("authorization token = %#v", token)
	}
	stored, err := secrets.Get(context.Background(), "primary")
	if err != nil {
		t.Fatal(err)
	}
	var storedToken oauth2.Token
	if err := json.Unmarshal(stored, &storedToken); err != nil {
		t.Fatal(err)
	}
	if storedToken.AccessToken != testAccessToken || storedToken.RefreshToken != testRefreshToken {
		t.Fatalf("stored token = %#v", storedToken)
	}
	if provider.exchangeCount() != 1 {
		t.Fatalf("authorization exchange count = %d, want 1", provider.exchangeCount())
	}
}

func TestTokenExchangeFailureShowsSafeReason(t *testing.T) {
	for _, test := range []struct {
		code string
		want string
	}{
		{"invalid_client", "OAuth client"},
		{"invalid_grant", "rejected the code"},
		{"redirect_uri_mismatch", "Desktop app"},
		{"unexpected-token-value", "HTTP 400"},
	} {
		err := tokenExchangeFailure(&oauth2.RetrieveError{
			Response:  &http.Response{StatusCode: http.StatusBadRequest},
			ErrorCode: test.code, ErrorDescription: testAccessToken,
			Body: []byte(testRefreshToken),
		})
		if !errors.Is(err, ErrTokenExchange) || !strings.Contains(err.Error(), test.want) {
			t.Errorf("code %q: error = %v", test.code, err)
		}
		if strings.Contains(err.Error(), testAccessToken) || strings.Contains(err.Error(), testRefreshToken) || strings.Contains(err.Error(), "unexpected-token-value") {
			t.Errorf("code %q leaked provider data: %v", test.code, err)
		}
	}
}

func TestTokenExchangeFailureClassifiesInvalidRequestWithoutLeakingDescription(t *testing.T) {
	err := tokenExchangeFailure(&oauth2.RetrieveError{
		ErrorCode: "invalid_request", ErrorDescription: "missing client_secret " + testAccessToken,
		Body: []byte(testRefreshToken),
	})
	if !errors.Is(err, ErrTokenExchange) || !strings.Contains(err.Error(), "client secret") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), testAccessToken) || strings.Contains(err.Error(), testRefreshToken) {
		t.Fatalf("error leaked credentials: %v", err)
	}
}

func TestAuthorizeTimeoutAndDenial(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		service := newTestService(t, provider, newMemorySecretStore(), browserFunc(func(context.Context, string) error {
			return nil
		}), 25*time.Millisecond)
		_, err := service.Authorize(context.Background())
		if !errors.Is(err, ErrCallbackTimeout) {
			t.Fatalf("Authorize error = %v, want ErrCallbackTimeout", err)
		}
		if provider.exchangeCount() != 0 {
			t.Fatalf("timeout made %d token requests", provider.exchangeCount())
		}
	})

	t.Run("access denied", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		browser := browserFunc(func(_ context.Context, target string) error {
			query := mustParseURL(t, target).Query()
			response, err := http.Get(callbackURL(query.Get("redirect_uri"), url.Values{
				"state": {query.Get("state")}, "error": {"access_denied"},
			}))
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Errorf("denial callback status = %d, want 200", response.StatusCode)
			}
			return nil
		})
		service := newTestService(t, provider, newMemorySecretStore(), browser, time.Second)
		_, err := service.Authorize(context.Background())
		if !errors.Is(err, ErrAuthorizationDenied) {
			t.Fatalf("Authorize error = %v, want ErrAuthorizationDenied", err)
		}
	})
}

func TestExpiredTokensRefreshOrRequireReauthorization(t *testing.T) {
	t.Run("successful refresh is stored", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		secrets := newMemorySecretStore()
		secrets.tokens["primary"] = marshalTestToken(t, &oauth2.Token{
			AccessToken: "old-access-secret", RefreshToken: "rotate-this-refresh-secret",
			TokenType: "Bearer", Expiry: time.Now().Add(-time.Hour),
		})
		service := newTestService(t, provider, secrets, browserFunc(func(context.Context, string) error { return nil }), time.Second)
		source, err := service.TokenSource(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		refreshed, err := source.Token()
		if err != nil {
			t.Fatal(err)
		}
		if refreshed.AccessToken != "fresh-access-token" || refreshed.RefreshToken != "rotated-refresh-token" {
			t.Fatalf("refreshed token = %#v", refreshed)
		}
		stored, err := secrets.Get(context.Background(), "primary")
		if err != nil {
			t.Fatal(err)
		}
		var persisted oauth2.Token
		if err := json.Unmarshal(stored, &persisted); err != nil {
			t.Fatal(err)
		}
		if persisted.RefreshToken != "rotated-refresh-token" {
			t.Fatalf("persisted refresh token = %q, want rotated token", persisted.RefreshToken)
		}
	})

	t.Run("invalid grant requires reauthorization without leaking credentials", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		secrets := newMemorySecretStore()
		secrets.tokens["primary"] = marshalTestToken(t, &oauth2.Token{
			AccessToken: "expired-access-secret", RefreshToken: "expired-refresh-secret",
			TokenType: "Bearer", Expiry: time.Now().Add(-time.Hour),
		})
		service := newTestService(t, provider, secrets, browserFunc(func(context.Context, string) error { return nil }), time.Second)
		source, err := service.TokenSource(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = source.Token()
		if !errors.Is(err, ErrReauthorizationRequired) {
			t.Fatalf("Token error = %v, want ErrReauthorizationRequired", err)
		}
		for _, secret := range []string{"expired-access-secret", "expired-refresh-secret"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("token error exposed credential %q: %v", secret, err)
			}
		}
	})

	t.Run("expired token without refresh token requires reauthorization", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		secrets := newMemorySecretStore()
		secrets.tokens["primary"] = marshalTestToken(t, &oauth2.Token{
			AccessToken: "expired-access-only",
			TokenType:   "Bearer", Expiry: time.Now().Add(-time.Hour),
		})
		service := newTestService(t, provider, secrets, browserFunc(func(context.Context, string) error { return nil }), time.Second)
		source, err := service.TokenSource(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = source.Token()
		if !errors.Is(err, ErrReauthorizationRequired) {
			t.Fatalf("Token error = %v, want ErrReauthorizationRequired", err)
		}
	})
}

func TestCredentialStoreErrorsDoNotLeakOAuthTokens(t *testing.T) {
	provider := newFakeOAuthProvider(t)
	secrets := newMemorySecretStore()
	secrets.setError = errors.New("backend echoed " + testAccessToken)
	browser := browserFunc(func(_ context.Context, target string) error {
		query := mustParseURL(t, target).Query()
		provider.setChallenge(query.Get("code_challenge"))
		response, err := http.Get(callbackURL(query.Get("redirect_uri"), url.Values{
			"state": {query.Get("state")}, "code": {"valid-auth-code"},
		}))
		if err != nil {
			return err
		}
		_ = response.Body.Close()
		return nil
	})
	service := newTestService(t, provider, secrets, browser, time.Second)
	_, err := service.Authorize(context.Background())
	if !errors.Is(err, ErrCredentialStore) {
		t.Fatalf("Authorize error = %v, want ErrCredentialStore", err)
	}
	if strings.Contains(err.Error(), testAccessToken) || strings.Contains(err.Error(), testRefreshToken) {
		t.Fatalf("credential storage error exposed a token: %v", err)
	}
}

func TestRevokeDeletesCredentialOnlyAfterRemoteSuccess(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		secrets := newMemorySecretStore()
		secrets.tokens["primary"] = marshalTestToken(t, &oauth2.Token{
			AccessToken: "revoke-access", RefreshToken: "revoke-refresh", TokenType: "Bearer",
		})
		service := newTestService(t, provider, secrets, browserFunc(func(context.Context, string) error { return nil }), time.Second)
		if err := service.Revoke(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := secrets.Get(context.Background(), "primary"); !errors.Is(err, secretstore.ErrNotFound) {
			t.Fatalf("credential after revoke error = %v, want not found", err)
		}
		if provider.revokedToken() != "revoke-refresh" {
			t.Fatalf("remote revoked token = %q, want refresh token", provider.revokedToken())
		}
	})

	t.Run("already revoked invalid_token deletes local credential", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		provider.setRevokeResponse(http.StatusBadRequest, `{"error":"invalid_token"}`)
		secrets := newMemorySecretStore()
		secrets.tokens["primary"] = marshalTestToken(t, &oauth2.Token{
			AccessToken: "expired-access", RefreshToken: "revoked-refresh", TokenType: "Bearer",
		})
		service := newTestService(t, provider, secrets, browserFunc(func(context.Context, string) error { return nil }), time.Second)
		if err := service.Revoke(context.Background()); err != nil {
			t.Fatalf("Revoke error = %v, want nil for invalid_token", err)
		}
		if _, err := secrets.Get(context.Background(), "primary"); !errors.Is(err, secretstore.ErrNotFound) {
			t.Fatalf("credential after invalid_token revoke error = %v, want not found", err)
		}
	})

	t.Run("server failure retains credential", func(t *testing.T) {
		provider := newFakeOAuthProvider(t)
		provider.setRevokeStatus(http.StatusInternalServerError)
		secrets := newMemorySecretStore()
		secrets.tokens["primary"] = marshalTestToken(t, &oauth2.Token{
			AccessToken: "revoke-access", RefreshToken: "keep-refresh", TokenType: "Bearer",
		})
		service := newTestService(t, provider, secrets, browserFunc(func(context.Context, string) error { return nil }), time.Second)
		if err := service.Revoke(context.Background()); !errors.Is(err, ErrTokenRevocation) {
			t.Fatalf("Revoke error = %v, want ErrTokenRevocation", err)
		}
		if _, err := secrets.Get(context.Background(), "primary"); err != nil {
			t.Fatalf("credential was deleted after remote revoke failure: %v", err)
		}
	})
}

type browserFunc func(context.Context, string) error

func (f browserFunc) OpenURL(ctx context.Context, target string) error { return f(ctx, target) }

type memorySecretStore struct {
	mu       sync.Mutex
	tokens   map[string][]byte
	setError error
}

func newMemorySecretStore() *memorySecretStore {
	return &memorySecretStore{tokens: make(map[string][]byte)}
}

func (s *memorySecretStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.tokens[key]
	if !ok {
		return nil, secretstore.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *memorySecretStore) Set(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setError != nil {
		return s.setError
	}
	s.tokens[key] = append([]byte(nil), value...)
	return nil
}

func (s *memorySecretStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tokens[key]; !ok {
		return secretstore.ErrNotFound
	}
	delete(s.tokens, key)
	return nil
}

type fakeOAuthProvider struct {
	server       *httptest.Server
	mu           sync.Mutex
	challenge    string
	exchanges    int
	revoked      string
	revokeStatus int
	revokeBody   string
}

func newFakeOAuthProvider(t *testing.T) *fakeOAuthProvider {
	t.Helper()
	provider := &fakeOAuthProvider{revokeStatus: http.StatusOK}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/token":
			if err := request.ParseForm(); err != nil {
				http.Error(writer, "invalid form", http.StatusBadRequest)
				return
			}
			if request.Form.Get("client_id") != "test-desktop-client" || request.Header.Get("Authorization") != "" {
				http.Error(writer, `{"error":"invalid_request"}`, http.StatusBadRequest)
				return
			}
			switch request.Form.Get("grant_type") {
			case "authorization_code":
				provider.mu.Lock()
				provider.exchanges++
				challenge := provider.challenge
				provider.mu.Unlock()
				if request.Form.Get("code") != "valid-auth-code" || oauth2.S256ChallengeFromVerifier(request.Form.Get("code_verifier")) != challenge {
					http.Error(writer, `{"error":"invalid_grant"}`, http.StatusBadRequest)
					return
				}
				writeJSON(writer, http.StatusOK, map[string]any{
					"access_token": testAccessToken, "refresh_token": testRefreshToken,
					"token_type": "Bearer", "expires_in": 3600,
				})
			case "refresh_token":
				if request.Form.Get("refresh_token") == "expired-refresh-secret" {
					writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_grant", "error_description": testAccessToken})
					return
				}
				writeJSON(writer, http.StatusOK, map[string]any{
					"access_token": "fresh-access-token", "refresh_token": "rotated-refresh-token",
					"token_type": "Bearer", "expires_in": 3600,
				})
			default:
				http.Error(writer, `{"error":"unsupported_grant_type"}`, http.StatusBadRequest)
			}
		case "/revoke":
			if err := request.ParseForm(); err != nil {
				http.Error(writer, "invalid form", http.StatusBadRequest)
				return
			}
			provider.mu.Lock()
			provider.revoked = request.Form.Get("token")
			status := provider.revokeStatus
			body := provider.revokeBody
			provider.mu.Unlock()
			if body != "" {
				writer.Header().Set("Content-Type", "application/json")
			}
			writer.WriteHeader(status)
			if body != "" {
				_, _ = io.WriteString(writer, body)
			}
		default:
			http.NotFound(writer, request)
		}
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	provider.server = &httptest.Server{
		Config:   &http.Server{Handler: handler},
		Listener: listener,
	}
	provider.server.Start()
	t.Cleanup(provider.server.Close)
	return provider
}

func (p *fakeOAuthProvider) setChallenge(challenge string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.challenge = challenge
}

func (p *fakeOAuthProvider) exchangeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exchanges
}

func (p *fakeOAuthProvider) setRevokeStatus(status int) {
	p.setRevokeResponse(status, "")
}

func (p *fakeOAuthProvider) setRevokeResponse(status int, body string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.revokeStatus = status
	p.revokeBody = body
}

func (p *fakeOAuthProvider) revokedToken() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.revoked
}

func newTestService(t *testing.T, provider *fakeOAuthProvider, secrets secretstore.Store, browser BrowserOpener, callbackTimeout time.Duration) *Service {
	t.Helper()
	service, err := NewService(Config{
		ClientID: "test-desktop-client", AuthURL: provider.server.URL + "/authorize",
		TokenURL: provider.server.URL + "/token", RevokeURL: provider.server.URL + "/revoke",
		HTTPClient: provider.server.Client(), Browser: browser, Secrets: secrets,
		CallbackTimeout: callbackTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func callbackURL(redirect string, values url.Values) string {
	return redirect + "?" + values.Encode()
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func marshalTestToken(t *testing.T, token *oauth2.Token) []byte {
	t.Helper()
	encoded, err := json.Marshal(token)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
