package driveapi

import (
	"errors"
	"fmt"
)

var (
	ErrUnauthenticated      = errors.New("Drive authorization is required")
	ErrForbidden            = errors.New("Drive denied access to this operation")
	ErrNotFound             = errors.New("Drive object was not found or is not accessible")
	ErrRateLimited          = errors.New("Drive request was rate limited")
	ErrQuotaExceeded        = errors.New("Drive quota was exceeded")
	ErrTemporary            = errors.New("Drive service is temporarily unavailable")
	ErrInvalidRequest       = errors.New("Drive rejected the request")
	ErrInvalidPageToken     = errors.New("Drive change page token is invalid")
	ErrUnexpectedResponse   = errors.New("Drive returned an unexpected response")
	ErrUploadSessionExpired = errors.New("Drive upload session expired")
)

type FailureKind string

const (
	FailureUnauthenticated  FailureKind = "unauthenticated"
	FailureForbidden        FailureKind = "forbidden"
	FailureNotFound         FailureKind = "not_found"
	FailureRateLimited      FailureKind = "rate_limited"
	FailureQuotaExceeded    FailureKind = "quota_exceeded"
	FailureTemporary        FailureKind = "temporary"
	FailureInvalidRequest   FailureKind = "invalid_request"
	FailureInvalidPageToken FailureKind = "invalid_page_token"
)

// APIError intentionally omits Google's response message, which can contain
// filenames and other user data. Reasons are stable API classifications.
type APIError struct {
	StatusCode int
	Reasons    []string
	Kind       FailureKind
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Drive API request failed (%s, HTTP %d)", e.Kind, e.StatusCode)
}

func (e *APIError) Is(target error) bool {
	switch target {
	case ErrUnauthenticated:
		return e.Kind == FailureUnauthenticated
	case ErrForbidden:
		return e.Kind == FailureForbidden
	case ErrNotFound:
		return e.Kind == FailureNotFound
	case ErrRateLimited:
		return e.Kind == FailureRateLimited
	case ErrQuotaExceeded:
		return e.Kind == FailureQuotaExceeded
	case ErrTemporary:
		return e.Kind == FailureTemporary
	case ErrInvalidRequest:
		return e.Kind == FailureInvalidRequest
	case ErrInvalidPageToken:
		return e.Kind == FailureInvalidPageToken
	default:
		return false
	}
}

func newAPIError(status int, reasons []string) *APIError {
	kind := FailureInvalidRequest
	switch {
	case status == 401:
		kind = FailureUnauthenticated
	case status == 403:
		kind = FailureForbidden
	case status == 404:
		kind = FailureNotFound
	case status == 429:
		kind = FailureRateLimited
	case status >= 500 && status <= 599:
		kind = FailureTemporary
	case status >= 400 && status <= 499:
		kind = FailureInvalidRequest
	}
	for _, reason := range reasons {
		switch reason {
		case "rateLimitExceeded", "userRateLimitExceeded":
			kind = FailureRateLimited
		case "dailyLimitExceeded", "storageQuotaExceeded", "activeItemCreationLimitExceeded", "teamDriveFileLimitExceeded":
			kind = FailureQuotaExceeded
		case "invalidPageToken", "pageTokenExpired", "startPageTokenExpired", "fullSyncRequired":
			kind = FailureInvalidPageToken
		}
	}
	return &APIError{StatusCode: status, Reasons: append([]string(nil), reasons...), Kind: kind}
}
