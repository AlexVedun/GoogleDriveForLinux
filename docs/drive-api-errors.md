# Drive API error handling

`internal/driveapi` turns Drive's HTTP status and structured `reason` values into stable error categories. The response's human-readable message is deliberately omitted because it can include filenames or other account data.

| Drive response | Application category | Adapter behavior |
|---|---|---|
| `401` | Authorization required | Returned as `ErrUnauthenticated`. The authenticated HTTP client can refresh through the OAuth token source; `invalid_grant` from token refresh is handled by `internal/auth` as reauthorization required. |
| `403` with a permission/policy reason, or no recognized rate/quota reason | Access denied | Returned as `ErrForbidden`; do not retry automatically. |
| `403 rateLimitExceeded` or `userRateLimitExceeded` | Rate limited | Retry safe reads with bounded exponential backoff and jitter; after attempts are exhausted, return `ErrRateLimited`. Mutations are returned to the durable sync worker without a blind retry. |
| `403 storageQuotaExceeded`, `dailyLimitExceeded`, `activeItemCreationLimitExceeded`, or `teamDriveFileLimitExceeded` | Drive quota exceeded | Returned as `ErrQuotaExceeded`; do not retry until the user or project quota changes. |
| `404` | Missing or inaccessible object | Returned as `ErrNotFound`. Drive does not distinguish deletion from lost access in this response. Expired resumable sessions use the narrower `ErrUploadSessionExpired`. |
| `410`, or a recognized invalid-page-token reason (`invalidPageToken`, `pageTokenExpired`, `startPageTokenExpired`, `fullSyncRequired`) | Changes cursor is invalid | Returned as `ErrInvalidPageToken`. The indexer drops its in-flight page cursor and performs a controlled reindex. Generic `400 badRequest` errors remain `ErrInvalidRequest`. |
| `429` | Rate limited | Safe reads retry with bounded backoff; exhausted attempts return `ErrRateLimited`. |
| `500`–`599`, transport reset, or request timeout | Temporary service/network failure | Safe reads retry. Mutations are not automatically repeated because the remote outcome may be unknown. For an interrupted resumable chunk, the caller must query the session status before sending bytes again. |
| Other `4xx` | Invalid request or unsupported operation | Returned as `ErrInvalidRequest`; do not retry unchanged input. |

Retries honor `Retry-After` when present, cap waits and attempts, and stop when the request context is canceled. The client limits concurrent requests and gives each request a context timeout. A streamed download keeps its request slot and timeout until the body is closed.

The adapter selects explicit `fields` projections for file and change resources. Incremental synchronization stores the cursor after each committed page and replaces the durable start token only after the final page. If that start token is rejected, the reindex captures a new start token before listing the Drive tree, and queued local operations remain visible while remote metadata is reconciled. It does not export Google's raw error message, access token, or opaque resumable-session URL in error strings. A session URL is accepted only when it has the configured upload host; this prevents a forged `Location` header from redirecting OAuth credentials or file bytes to another origin.

References: [Google Drive error handling](https://developers.google.com/workspace/drive/api/guides/handle-errors), [upload and resumable-session recovery](https://developers.google.com/workspace/drive/api/guides/manage-uploads), [partial response fields](https://developers.google.com/workspace/drive/api/guides/fields-parameter), [Drive changes](https://developers.google.com/workspace/drive/api/guides/manage-changes), and the [changes.list reference](https://developers.google.com/workspace/drive/api/reference/rest/v3/changes/list).
