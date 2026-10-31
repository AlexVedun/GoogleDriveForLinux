package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	"google-drive-linux/internal/secretstore"
)

const (
	GoogleDriveScope = "https://www.googleapis.com/auth/drive"
	googleAuthURL    = "https://accounts.google.com/o/oauth2/v2/auth"
	googleTokenURL   = "https://oauth2.googleapis.com/token"
	googleRevokeURL  = "https://oauth2.googleapis.com/revoke"
	defaultSecretKey = "primary"
)

var (
	ErrInvalidState            = errors.New("OAuth callback state is invalid")
	ErrInvalidCallback         = errors.New("OAuth callback is invalid")
	ErrCallbackTimeout         = errors.New("OAuth callback timed out")
	ErrAuthorizationDenied     = errors.New("Google authorization was denied")
	ErrAuthorizationFailed     = errors.New("Google authorization failed")
	ErrBrowserOpen             = errors.New("could not open the system browser")
	ErrTokenExchange           = errors.New("could not exchange the authorization code")
	ErrTokenRefresh            = errors.New("could not refresh the access token")
	ErrTokenRevocation         = errors.New("could not revoke the Google token")
	ErrCredentialStore         = errors.New("could not access the system credential store")
	ErrNotAuthenticated        = errors.New("no Google account is authorized")
	ErrReauthorizationRequired = errors.New("Google authorization must be renewed")
)

type BrowserOpener interface {
	OpenURL(ctx context.Context, target string) error
}

type Config struct {
	ClientID        string
	ClientSecret    string
	AuthURL         string
	TokenURL        string
	RevokeURL       string
	Scopes          []string
	CallbackTimeout time.Duration
	HTTPClient      *http.Client
	Browser         BrowserOpener
	Secrets         secretstore.Store
	SecretKey       string
}

type Service struct {
	oauth           oauth2.Config
	revokeURL       string
	callbackTimeout time.Duration
	httpTimeout     time.Duration
	httpClient      *http.Client
	browser         BrowserOpener
	secrets         secretstore.Store
	secretKey       string
}

type callbackResult struct {
	code string
	err  error
}

func NewService(config Config) (*Service, error) {
	if strings.TrimSpace(config.ClientID) == "" {
		return nil, errors.New("Google OAuth client ID must not be empty")
	}
	if config.Secrets == nil {
		return nil, errors.New("system secret store must not be nil")
	}
	if config.AuthURL == "" {
		config.AuthURL = googleAuthURL
	}
	if config.TokenURL == "" {
		config.TokenURL = googleTokenURL
	}
	if config.RevokeURL == "" {
		config.RevokeURL = googleRevokeURL
	}
	if len(config.Scopes) == 0 {
		config.Scopes = []string{GoogleDriveScope}
	}
	if config.CallbackTimeout <= 0 {
		config.CallbackTimeout = 3 * time.Minute
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	httpTimeout := config.HTTPClient.Timeout
	if httpTimeout <= 0 {
		httpTimeout = 30 * time.Second
	}
	if config.Browser == nil {
		config.Browser = SystemBrowser{}
	}
	if config.SecretKey == "" {
		config.SecretKey = defaultSecretKey
	}
	if strings.TrimSpace(config.SecretKey) == "" {
		return nil, errors.New("system secret key must not be empty")
	}

	return &Service{
		oauth: oauth2.Config{
			ClientID: config.ClientID, ClientSecret: config.ClientSecret,
			Endpoint: oauth2.Endpoint{AuthURL: config.AuthURL, TokenURL: config.TokenURL},
			Scopes:   append([]string(nil), config.Scopes...),
		},
		revokeURL: config.RevokeURL, callbackTimeout: config.CallbackTimeout,
		httpTimeout: httpTimeout, httpClient: config.HTTPClient, browser: config.Browser,
		secrets: config.Secrets, secretKey: config.SecretKey,
	}, nil
}

func (s *Service) Authorize(ctx context.Context) (*oauth2.Token, error) {
	if ctx == nil {
		return nil, errors.New("context must not be nil")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for OAuth callback: %w", err)
	}
	redirectURL := (&url.URL{Scheme: "http", Host: listener.Addr().String(), Path: "/oauth/callback"}).String()
	state, err := randomState()
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("create OAuth state: %w", err)
	}
	verifier := oauth2.GenerateVerifier()
	callback := make(chan callbackResult, 1)
	server := &http.Server{
		Handler:           callbackHandler(listener.Addr().String(), state, callback),
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    8 * 1024,
	}
	serveDone := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			err = nil
		}
		serveDone <- err
	}()
	defer func() {
		_ = server.Close()
		_ = listener.Close()
	}()

	oauthConfig := s.oauth
	oauthConfig.RedirectURL = redirectURL
	authorizationURL := oauthConfig.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oauth2.ApprovalForce,
		oauth2.S256ChallengeOption(verifier),
	)
	if err := s.browser.OpenURL(ctx, authorizationURL); err != nil {
		return nil, safeFailure(ErrBrowserOpen, err)
	}
	timer := time.NewTimer(s.callbackTimeout)
	defer timer.Stop()
	var result callbackResult
	select {
	case result = <-callback:
		if result.err != nil {
			return nil, result.err
		}
	case err := <-serveDone:
		if err != nil {
			return nil, fmt.Errorf("OAuth callback server stopped: %w", err)
		}
		return nil, ErrAuthorizationFailed
	case <-timer.C:
		return nil, ErrCallbackTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	exchangeContext, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()
	exchangeContext = context.WithValue(exchangeContext, oauth2.HTTPClient, s.httpClient)
	token, err := oauthConfig.Exchange(exchangeContext, result.code, oauth2.VerifierOption(verifier))
	if err != nil || token == nil || token.AccessToken == "" {
		return nil, tokenExchangeFailure(err)
	}
	if err := s.saveToken(exchangeContext, token); err != nil {
		return nil, err
	}
	return token, nil
}

func (s *Service) TokenSource(ctx context.Context) (oauth2.TokenSource, error) {
	if ctx == nil {
		return nil, errors.New("context must not be nil")
	}
	token, err := s.loadToken(ctx)
	if err != nil {
		return nil, err
	}
	oauthContext := context.WithValue(ctx, oauth2.HTTPClient, s.httpClient)
	return &persistingTokenSource{
		service: s, ctx: oauthContext, source: s.oauth.TokenSource(oauthContext, token), current: token,
	}, nil
}

// IsAuthenticated checks for a locally stored credential without contacting
// Google or attempting a token refresh.
func (s *Service) IsAuthenticated(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, errors.New("context must not be nil")
	}
	_, err := s.loadToken(ctx)
	if errors.Is(err, ErrNotAuthenticated) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) Revoke(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	token, err := s.loadToken(ctx)
	if errors.Is(err, ErrNotAuthenticated) {
		return nil
	}
	if err != nil {
		return err
	}
	revokeValue := token.RefreshToken
	if revokeValue == "" {
		revokeValue = token.AccessToken
	}
	if revokeValue != "" {
		requestContext, cancel := context.WithTimeout(ctx, s.httpTimeout)
		defer cancel()
		form := url.Values{"token": {revokeValue}}
		request, err := http.NewRequestWithContext(requestContext, http.MethodPost, s.revokeURL, strings.NewReader(form.Encode()))
		if err != nil {
			return safeFailure(ErrTokenRevocation, err)
		}
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response, err := s.httpClient.Do(request)
		if err != nil {
			return safeFailure(ErrTokenRevocation, err)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4*1024))
		_ = response.Body.Close()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return ErrTokenRevocation
		}
	}
	if err := s.secrets.Delete(ctx, s.secretKey); err != nil {
		return safeFailure(ErrCredentialStore, err)
	}
	return nil
}

func (s *Service) loadToken(ctx context.Context) (*oauth2.Token, error) {
	encoded, err := s.secrets.Get(ctx, s.secretKey)
	if errors.Is(err, secretstore.ErrNotFound) {
		return nil, ErrNotAuthenticated
	}
	if err != nil {
		return nil, safeFailure(ErrCredentialStore, err)
	}
	defer wipe(encoded)
	var token oauth2.Token
	if err := json.Unmarshal(encoded, &token); err != nil || (token.AccessToken == "" && token.RefreshToken == "") {
		return nil, safeFailure(ErrCredentialStore, err)
	}
	return &token, nil
}

func (s *Service) saveToken(ctx context.Context, token *oauth2.Token) error {
	encoded, err := json.Marshal(token)
	if err != nil {
		return safeFailure(ErrCredentialStore, err)
	}
	defer wipe(encoded)
	if err := s.secrets.Set(ctx, s.secretKey, encoded); err != nil {
		return safeFailure(ErrCredentialStore, err)
	}
	return nil
}

func callbackHandler(expectedHost, expectedState string, callback chan<- callbackResult) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/callback", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodGet || request.Host != expectedHost || !remoteIsLoopback(request.RemoteAddr) {
			http.Error(writer, "Invalid OAuth callback.", http.StatusBadRequest)
			return
		}
		query := request.URL.Query()
		states := query["state"]
		if len(states) != 1 || subtle.ConstantTimeCompare([]byte(states[0]), []byte(expectedState)) != 1 {
			http.Error(writer, "Invalid OAuth state.", http.StatusBadRequest)
			return
		}
		errorsFromProvider := query["error"]
		if len(errorsFromProvider) > 1 {
			http.Error(writer, "Invalid OAuth callback.", http.StatusBadRequest)
			return
		}
		if len(errorsFromProvider) == 1 && errorsFromProvider[0] != "" {
			result := callbackResult{err: ErrAuthorizationFailed}
			if errorsFromProvider[0] == "access_denied" {
				result.err = ErrAuthorizationDenied
			}
			writeCallbackResponse(writer, "Authorization was not granted. You may close this tab.", http.StatusOK)
			sendCallback(callback, result)
			return
		}
		codes := query["code"]
		if len(codes) != 1 || codes[0] == "" {
			http.Error(writer, "Invalid OAuth callback.", http.StatusBadRequest)
			return
		}
		writeCallbackResponse(writer, "Authorization received. You may close this tab.", http.StatusOK)
		sendCallback(callback, callbackResult{code: codes[0]})
	})
	return mux
}

func sendCallback(callback chan<- callbackResult, result callbackResult) {
	select {
	case callback <- result:
	default:
	}
}

func writeCallbackResponse(writer http.ResponseWriter, body string, status int) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, body)
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func remoteIsLoopback(remoteAddress string) bool {
	host, _, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func randomState() (string, error) {
	var state [32]byte
	if _, err := rand.Read(state[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(state[:]), nil
}

func wipe(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

type persistingTokenSource struct {
	mu      sync.Mutex
	service *Service
	ctx     context.Context
	source  oauth2.TokenSource
	current *oauth2.Token
}

func (s *persistingTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token, err := s.source.Token()
	if err != nil {
		var retrieveError *oauth2.RetrieveError
		if errors.As(err, &retrieveError) && retrieveError.ErrorCode == "invalid_grant" {
			return nil, safeFailure(ErrReauthorizationRequired, err)
		}
		return nil, safeFailure(ErrTokenRefresh, err)
	}
	if !sameToken(s.current, token) {
		if err := s.service.saveToken(s.ctx, token); err != nil {
			return nil, err
		}
		s.current = token
	}
	return token, nil
}

func sameToken(left, right *oauth2.Token) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.AccessToken == right.AccessToken && left.RefreshToken == right.RefreshToken &&
		left.TokenType == right.TokenType && left.Expiry.Equal(right.Expiry)
}

type safeError struct {
	marker error
}

func (e *safeError) Error() string {
	return e.marker.Error()
}

func (e *safeError) Is(target error) bool {
	return target == e.marker
}

func safeFailure(marker, _ error) error {
	return &safeError{marker: marker}
}

// Keep provider response bodies and descriptions out of the UI: they can
// contain credentials. OAuth error codes are only shown from this allowlist.
func tokenExchangeFailure(err error) error {
	var retrieve *oauth2.RetrieveError
	if errors.As(err, &retrieve) {
		switch retrieve.ErrorCode {
		case "invalid_client":
			return fmt.Errorf("%w: Google rejected the OAuth client; check the Desktop app client ID and its client secret", ErrTokenExchange)
		case "invalid_grant":
			return fmt.Errorf("%w: Google rejected the code; retry sign-in and check the OAuth client configuration", ErrTokenExchange)
		case "redirect_uri_mismatch":
			return fmt.Errorf("%w: Google rejected the redirect URI; use a Desktop app OAuth client", ErrTokenExchange)
		case "invalid_request", "unauthorized_client":
			return fmt.Errorf("%w: Google returned %s; check the OAuth client configuration", ErrTokenExchange, retrieve.ErrorCode)
		}
		if retrieve.Response != nil {
			return fmt.Errorf("%w: Google token endpoint returned HTTP %d", ErrTokenExchange, retrieve.Response.StatusCode)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: timed out contacting Google", ErrTokenExchange)
	}
	return ErrTokenExchange
}
