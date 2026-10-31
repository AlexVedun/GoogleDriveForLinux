# OAuth setup and credential storage

The application uses Google's installed-application OAuth flow. Authorization opens the system browser and listens for the callback on an IPv4 loopback address bound to a random local port. Each attempt uses a cryptographically random `state` value and PKCE S256. The callback listener accepts only the expected path, host, state, and loopback peer, and it is closed after completion or timeout.

## Google Cloud prerequisites

Before using a real account:

1. Create or select a Google Cloud project and enable the Google Drive API.
2. Configure the OAuth consent screen and add the test account as a test user while the app is in testing mode.
3. Create an OAuth client with application type **Desktop app**. Keep the downloaded client configuration outside the repository and do not put account tokens in source files, command history, screenshots, or issue reports.
4. Keep the consent screen in testing mode and use an explicit test-user list during development. Before a public release, review Google's current verification and restricted-scope requirements. This prototype requests `https://www.googleapis.com/auth/drive` because the filesystem must read and write arbitrary existing files in My Drive; the narrower `drive.file` scope is for files created, opened, or explicitly shared with the app through an app file picker and does not satisfy this filesystem's current scope.

Enter the Desktop app client ID in the Qt Settings tab and click **Save settings** before signing in. It is stored in the private `settings.json` file under the user's application config directory (normally `~/.config/google-drive-linux/`). The service reads the saved ID on startup and again when sign-in begins, so no restart is needed after saving. `GOOGLE_OAUTH_CLIENT_ID` remains an optional fallback when no ID is saved. If a private Secret Service is available, the service opens its sign-in flow from the Qt window. The optional client secret can be supplied through `auth.Config.ClientSecret` when a desktop-client configuration requires one; the packaged build does not embed a secret.

After authorization, `drivefs` creates the Drive API client, recovers the durable content journal, starts the initial My Drive index, drains queued operations, and downloads pinned files in the background. An already stored credential starts these workers when the service starts. Signing out unmounts the filesystem and stops the workers before revoking the token. The Qt account status reports when Drive asks for reauthorization; no successful live Google login has been verified in this repository's test environment.

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

## Public-release verification

Google classifies the full `drive` scope as restricted. A public app generally
needs OAuth scope verification unless it qualifies for a stated exception;
Google identifies backup-and-sync apps as a permitted application type for
Drive restricted scopes. Google requires an independent security assessment
when restricted user data is accessed from or through a third-party server.
This client is designed to call Google directly from the desktop, but the
actual OAuth submission must confirm the applicable review and assessment
requirements. Budget several weeks for restricted-scope review; it is outside
the engineering estimate in the project plan.

See Google's current [Drive scope guidance](https://developers.google.com/workspace/drive/api/guides/api-specific-auth), [restricted-scope verification requirements](https://developers.google.com/identity/protocols/oauth2/production-readiness/restricted-scope-verification), and [OAuth scopes list](https://developers.google.com/identity/protocols/oauth2/scopes).
