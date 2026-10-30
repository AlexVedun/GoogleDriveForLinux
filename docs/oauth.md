# OAuth setup and credential storage

The application uses Google's installed-application OAuth flow. Authorization opens the system browser and listens for the callback on an IPv4 loopback address bound to a random local port. Each attempt uses a cryptographically random `state` value and PKCE S256. The callback listener accepts only the expected path, host, state, and loopback peer, and it is closed after completion or timeout.

## Google Cloud prerequisites

Before using a real account:

1. Create or select a Google Cloud project and enable the Google Drive API.
2. Configure the OAuth consent screen and add the test account as a test user while the app is in testing mode.
3. Create an OAuth client with application type **Desktop app**. Keep the downloaded client configuration outside the repository and do not put account tokens in source files, command history, screenshots, or issue reports.
4. Review Google's current verification and restricted-scope requirements before distributing the application. This prototype requests `https://www.googleapis.com/auth/drive` because the planned filesystem reads and writes arbitrary files in My Drive.

The client ID is passed to `auth.NewService`. The optional client secret can be supplied through `auth.Config.ClientSecret` if the downloaded desktop-client configuration provides one. The application UI and its local settings flow are scheduled for a later implementation step; this package does not currently read environment variables or perform a live sign-in on startup.

```go
secrets, err := secretstore.NewSystemStore("google-drive-linux")
if err != nil {
	return err
}

oauth, err := auth.NewService(auth.Config{
	ClientID: clientID,
	Secrets:  secrets,
})
if err != nil {
	return err
}

token, err := oauth.Authorize(ctx)
```

## Credential handling

Access and refresh tokens are serialized only for transfer to the OS credential service. On Linux, `github.com/zalando/go-keyring` uses the Secret Service API over the session D-Bus. There is no file, SQLite, environment-variable, or in-memory fallback when that service is missing or locked. Credential-store failures are returned as a generic error without including D-Bus diagnostics or token values. SQLite remains reserved for non-secret account metadata and synchronization state.

Token refresh persists a rotated refresh token before returning the refreshed token to the caller. An `invalid_grant` response requires the user to authorize again. Sign-out revokes the refresh token when possible, then deletes the local keyring entry; if remote revocation fails, the local entry is retained so the failure is visible and retryable.

The GNOME Keyring and KDE Wallet/Secret Service paths still need live desktop checks. Automated tests use an in-memory fake keyring and a local fake OAuth provider; they do not contact Google or validate a real account, consent screen, unlocked keyring, or desktop browser.
