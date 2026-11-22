package driveapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"google-drive-linux/internal/auth"
)

const (
	defaultAPIURL    = "https://www.googleapis.com/drive/v3"
	defaultUploadURL = "https://www.googleapis.com/upload/drive/v3"
	maxJSONResponse  = 16 << 20
	maxErrorResponse = 1 << 20
)

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type Config struct {
	HTTPClient     *http.Client
	BaseURL        string
	UploadURL      string
	RequestTimeout time.Duration
	MaxConcurrent  int
	Retry          RetryPolicy
}

type Client struct {
	httpClient     *http.Client
	baseURL        string
	uploadURL      string
	requestTimeout time.Duration
	maxAttempts    int
	baseDelay      time.Duration
	maxDelay       time.Duration
	semaphore      chan struct{}
}

type requestSpec struct {
	method        string
	url           string
	header        http.Header
	body          func() io.Reader
	retrySafe     bool
	contentLength int64
}

func NewClient(config Config) (*Client, error) {
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{}
	}
	if config.BaseURL == "" {
		config.BaseURL = defaultAPIURL
	}
	if config.UploadURL == "" {
		config.UploadURL = defaultUploadURL
	}
	baseURL, err := validateBaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Drive API base URL: %w", err)
	}
	uploadURL, err := validateBaseURL(config.UploadURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Drive upload URL: %w", err)
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = 4
	}
	if config.MaxConcurrent > 32 {
		return nil, errors.New("Drive request concurrency limit must not exceed 32")
	}
	if config.Retry.MaxAttempts <= 0 {
		config.Retry.MaxAttempts = 4
	}
	if config.Retry.MaxAttempts > 8 {
		return nil, errors.New("Drive retry attempts must not exceed 8")
	}
	if config.Retry.BaseDelay <= 0 {
		config.Retry.BaseDelay = 100 * time.Millisecond
	}
	if config.Retry.MaxDelay <= 0 {
		config.Retry.MaxDelay = 3 * time.Second
	}
	if config.Retry.MaxDelay < config.Retry.BaseDelay {
		return nil, errors.New("maximum retry delay must be at least the base delay")
	}
	clientCopy := *config.HTTPClient
	originalRedirect := clientCopy.CheckRedirect
	clientCopy.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > 0 && !sameOrigin(via[0].URL, request.URL) {
			return http.ErrUseLastResponse
		}
		if originalRedirect != nil {
			return originalRedirect(request, via)
		}
		if len(via) >= 10 {
			return errors.New("too many Drive redirects")
		}
		return nil
	}
	return &Client{
		httpClient: &clientCopy, baseURL: baseURL, uploadURL: uploadURL,
		requestTimeout: config.RequestTimeout, maxAttempts: config.Retry.MaxAttempts,
		baseDelay: config.Retry.BaseDelay, maxDelay: config.Retry.MaxDelay,
		semaphore: make(chan struct{}, config.MaxConcurrent),
	}, nil
}

func (c *Client) ListFiles(ctx context.Context, options FileListOptions) (FileList, error) {
	if options.PageSize < 0 || options.PageSize > 1000 {
		return FileList{}, errors.New("Drive page size must be between 0 and 1000")
	}
	if options.PageSize == 0 {
		options.PageSize = 1000
	}
	if options.Query == "" {
		options.Query = "trashed = false"
	}
	query := url.Values{
		"q": {options.Query}, "pageSize": {strconv.Itoa(options.PageSize)},
		"fields": {fileListFields}, "spaces": {"drive"},
	}
	if options.PageToken != "" {
		query.Set("pageToken", options.PageToken)
	}
	var page FileList
	err := c.jsonRequest(ctx, requestSpec{
		method: http.MethodGet, url: c.apiURL("files", query), retrySafe: true,
	}, &page)
	return page, err
}

func (c *Client) GetFile(ctx context.Context, fileID string) (File, error) {
	if err := validateID(fileID); err != nil {
		return File{}, err
	}
	var file File
	err := c.jsonRequest(ctx, requestSpec{
		method: http.MethodGet, url: c.apiURL("files/"+url.PathEscape(fileID), url.Values{"fields": {defaultFileFields}}),
		retrySafe: true,
	}, &file)
	return file, err
}

func (c *Client) CreateFile(ctx context.Context, metadata FileMetadata) (File, error) {
	var file File
	body, err := json.Marshal(metadata)
	if err != nil {
		return file, errors.New("invalid Drive file metadata")
	}
	query := url.Values{"fields": {defaultFileFields}}
	err = c.jsonRequest(ctx, requestSpec{
		method: http.MethodPost, url: c.apiURL("files", query),
		header: http.Header{"Content-Type": {"application/json"}},
		body:   func() io.Reader { return bytes.NewReader(body) },
	}, &file)
	return file, err
}

func (c *Client) UpdateFile(ctx context.Context, fileID string, patch FilePatch, addParents, removeParents []string) (File, error) {
	if err := validateID(fileID); err != nil {
		return File{}, err
	}
	if patch.Name == nil && patch.Trashed == nil && len(patch.AppProperties) == 0 && len(addParents) == 0 && len(removeParents) == 0 {
		return File{}, errors.New("Drive file update must change at least one field")
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return File{}, errors.New("invalid Drive file update")
	}
	query := url.Values{"fields": {defaultFileFields}}
	if len(addParents) != 0 {
		query.Set("addParents", strings.Join(addParents, ","))
	}
	if len(removeParents) != 0 {
		query.Set("removeParents", strings.Join(removeParents, ","))
	}
	var file File
	err = c.jsonRequest(ctx, requestSpec{
		method: http.MethodPatch, url: c.apiURL("files/"+url.PathEscape(fileID), query),
		header: http.Header{"Content-Type": {"application/json"}},
		body:   func() io.Reader { return bytes.NewReader(body) },
	}, &file)
	return file, err
}

func (c *Client) DownloadFile(ctx context.Context, fileID string) (Download, error) {
	if err := validateID(fileID); err != nil {
		return Download{}, err
	}
	query := url.Values{"alt": {"media"}}
	response, cleanup, err := c.perform(ctx, requestSpec{
		method: http.MethodGet, url: c.apiURL("files/"+url.PathEscape(fileID), query), retrySafe: true,
	})
	if err != nil {
		return Download{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		apiErr := parseAPIError(response)
		cleanup()
		return Download{}, apiErr
	}
	return Download{
		Body:        &leasedBody{ReadCloser: response.Body, cleanup: cleanup},
		ContentType: response.Header.Get("Content-Type"), ContentLength: response.ContentLength,
	}, nil
}

func (c *Client) TrashFile(ctx context.Context, fileID string) (File, error) {
	trashed := true
	return c.UpdateFile(ctx, fileID, FilePatch{Trashed: &trashed}, nil, nil)
}

func (c *Client) MoveRename(ctx context.Context, fileID, newName, addParentID, removeParentID string) (File, error) {
	patch := FilePatch{}
	if newName != "" {
		patch.Name = &newName
	}
	var addParents, removeParents []string
	if addParentID != "" {
		addParents = []string{addParentID}
	}
	if removeParentID != "" {
		removeParents = []string{removeParentID}
	}
	return c.UpdateFile(ctx, fileID, patch, addParents, removeParents)
}

func (c *Client) GetStartPageToken(ctx context.Context) (string, error) {
	var response struct {
		StartPageToken string `json:"startPageToken"`
	}
	err := c.jsonRequest(ctx, requestSpec{
		method: http.MethodGet, url: c.apiURL("changes/startPageToken", url.Values{"fields": {"startPageToken"}}),
		retrySafe: true,
	}, &response)
	if err == nil && response.StartPageToken == "" {
		err = ErrUnexpectedResponse
	}
	return response.StartPageToken, err
}

func (c *Client) ListChanges(ctx context.Context, pageToken string) (ChangeList, error) {
	if pageToken == "" {
		return ChangeList{}, errors.New("Drive change page token must not be empty")
	}
	query := url.Values{
		"pageToken": {pageToken}, "pageSize": {"1000"}, "spaces": {"drive"},
		"includeRemoved": {"true"}, "restrictToMyDrive": {"true"}, "fields": {changeListFields},
	}
	var page ChangeList
	err := c.jsonRequest(ctx, requestSpec{
		method: http.MethodGet, url: c.apiURL("changes", query), retrySafe: true,
	}, &page)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusGone && apiErr.Kind == FailureInvalidRequest {
		invalidToken := *apiErr
		invalidToken.Kind = FailureInvalidPageToken
		err = &invalidToken
	}
	return page, err
}

func (c *Client) GenerateIDs(ctx context.Context, count int) ([]string, error) {
	if count < 1 || count > 1000 {
		return nil, errors.New("Drive ID count must be between 1 and 1000")
	}
	query := url.Values{"count": {strconv.Itoa(count)}, "space": {"drive"}, "fields": {"ids"}}
	var response struct {
		IDs []string `json:"ids"`
	}
	if err := c.jsonRequest(ctx, requestSpec{
		method: http.MethodGet, url: c.apiURL("files/generateIds", query), retrySafe: true,
	}, &response); err != nil {
		return nil, err
	}
	if len(response.IDs) != count {
		return nil, ErrUnexpectedResponse
	}
	for _, id := range response.IDs {
		if id == "" {
			return nil, ErrUnexpectedResponse
		}
	}
	return response.IDs, nil
}

// StartResumableUpload returns the opaque session URL. Callers must keep it in
// the durable operation record and must never log or display it.
func (c *Client) StartResumableUpload(ctx context.Context, upload UploadRequest) (string, error) {
	if upload.Size < 0 {
		return "", errors.New("Drive upload size must not be negative")
	}
	if upload.FileID != "" {
		if err := validateID(upload.FileID); err != nil {
			return "", err
		}
	}
	if upload.FileID == "" && upload.Metadata.ID == "" {
		return "", errors.New("new Drive uploads require a pre-generated file ID")
	}
	if upload.FileID != "" && upload.Metadata.ID != "" && upload.FileID != upload.Metadata.ID {
		return "", errors.New("Drive upload file IDs do not match")
	}
	metadata := upload.Metadata
	if upload.FileID == "" {
		metadata.ID = upload.Metadata.ID
	} else {
		metadata.ID = ""
	}
	body, err := json.Marshal(metadata)
	if err != nil {
		return "", errors.New("invalid Drive upload metadata")
	}
	path := "files"
	method := http.MethodPost
	if upload.FileID != "" {
		path += "/" + url.PathEscape(upload.FileID)
		method = http.MethodPatch
	}
	query := url.Values{"uploadType": {"resumable"}, "fields": {defaultFileFields}}
	requestURL := c.uploadURL + "/" + path + "?" + query.Encode()
	header := http.Header{
		"Content-Type":            {"application/json"},
		"X-Upload-Content-Type":   {contentTypeOrDefault(upload.MIMEType)},
		"X-Upload-Content-Length": {strconv.FormatInt(upload.Size, 10)},
	}
	response, cleanup, err := c.perform(ctx, requestSpec{
		method: method, url: requestURL, header: header,
		body: func() io.Reader { return bytes.NewReader(body) },
	})
	if err != nil {
		return "", err
	}
	defer cleanup()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", parseAPIError(response)
	}
	location := response.Header.Get("Location")
	if location == "" {
		return "", ErrUnexpectedResponse
	}
	base, _ := url.Parse(requestURL)
	sessionURL, err := base.Parse(location)
	if err != nil || !sameOrigin(base, sessionURL) || sessionURL.User != nil || sessionURL.Fragment != "" {
		return "", ErrUnexpectedResponse
	}
	return sessionURL.String(), nil
}

func (c *Client) UploadChunk(ctx context.Context, sessionURL string, chunk UploadChunkRequest) (UploadProgress, error) {
	if chunk.Body == nil {
		return UploadProgress{}, errors.New("invalid Drive upload chunk range")
	}
	parsed, err := c.validateSessionURL(sessionURL)
	if err != nil {
		return UploadProgress{}, err
	}
	if chunk.Empty {
		if chunk.Total != 0 || chunk.Start != 0 || chunk.End != -1 {
			return UploadProgress{}, errors.New("invalid empty Drive upload range")
		}
		response, cleanup, err := c.perform(ctx, requestSpec{
			method: http.MethodPut, url: parsed.String(),
			header: http.Header{"Content-Type": {contentTypeOrDefault(chunk.MIMEType)}, "Content-Length": {"0"}},
			body:   func() io.Reader { return bytes.NewReader(nil) },
		})
		if err != nil {
			return UploadProgress{}, err
		}
		defer cleanup()
		return parseUploadProgress(response, 0)
	}
	if chunk.Start < 0 || chunk.End < chunk.Start || chunk.Total <= chunk.End {
		return UploadProgress{}, errors.New("invalid Drive upload chunk range")
	}
	length := chunk.End - chunk.Start + 1
	header := http.Header{
		"Content-Type":  {contentTypeOrDefault(chunk.MIMEType)},
		"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", chunk.Start, chunk.End, chunk.Total)},
	}
	reader := io.LimitReader(chunk.Body, length)
	response, cleanup, err := c.perform(ctx, requestSpec{
		method: http.MethodPut, url: parsed.String(), header: header,
		body: func() io.Reader { return reader }, contentLength: length,
	})
	if err != nil {
		return UploadProgress{}, err
	}
	defer cleanup()
	return parseUploadProgress(response, chunk.Total)
}

func (c *Client) QueryUploadStatus(ctx context.Context, sessionURL string, total int64) (UploadProgress, error) {
	if total < 0 {
		return UploadProgress{}, errors.New("Drive upload size must not be negative")
	}
	parsed, err := c.validateSessionURL(sessionURL)
	if err != nil {
		return UploadProgress{}, err
	}
	header := http.Header{"Content-Range": {fmt.Sprintf("bytes */%d", total)}}
	response, cleanup, err := c.perform(ctx, requestSpec{
		method: http.MethodPut, url: parsed.String(), header: header,
		body: func() io.Reader { return nil }, contentLength: 0, retrySafe: true,
	})
	if err != nil {
		return UploadProgress{}, err
	}
	defer cleanup()
	if response.StatusCode == http.StatusNotFound {
		return UploadProgress{}, ErrUploadSessionExpired
	}
	return parseUploadProgress(response, total)
}

func parseUploadProgress(response *http.Response, total int64) (UploadProgress, error) {
	if response.StatusCode == http.StatusPermanentRedirect {
		nextOffset, err := nextUploadOffset(response.Header.Get("Range"), total)
		if err != nil {
			return UploadProgress{}, err
		}
		return UploadProgress{NextOffset: nextOffset}, nil
	}
	if response.StatusCode == http.StatusNotFound {
		return UploadProgress{}, ErrUploadSessionExpired
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return UploadProgress{}, parseAPIError(response)
	}
	var file File
	if err := decodeJSON(response.Body, &file); err != nil || file.ID == "" {
		return UploadProgress{}, ErrUnexpectedResponse
	}
	return UploadProgress{Complete: true, NextOffset: total, File: &file}, nil
}

func nextUploadOffset(value string, total int64) (int64, error) {
	if value == "" {
		return 0, nil
	}
	if !strings.HasPrefix(value, "bytes=0-") {
		return 0, ErrUnexpectedResponse
	}
	last, err := strconv.ParseInt(strings.TrimPrefix(value, "bytes=0-"), 10, 64)
	if err != nil || last < 0 || last >= total {
		return 0, ErrUnexpectedResponse
	}
	return last + 1, nil
}

func (c *Client) validateSessionURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	base, _ := url.Parse(c.uploadURL)
	if err != nil || parsed == nil || !sameOrigin(base, parsed) || parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("invalid Drive upload session URL")
	}
	return parsed, nil
}

func (c *Client) jsonRequest(ctx context.Context, spec requestSpec, destination any) error {
	response, cleanup, err := c.perform(ctx, spec)
	if err != nil {
		return err
	}
	defer cleanup()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return parseAPIError(response)
	}
	if destination == nil {
		return nil
	}
	if err := decodeJSON(response.Body, destination); err != nil {
		return ErrUnexpectedResponse
	}
	return nil
}

func (c *Client) perform(ctx context.Context, spec requestSpec) (*http.Response, func(), error) {
	if ctx == nil {
		return nil, nil, errors.New("context must not be nil")
	}
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
		select {
		case c.semaphore <- struct{}{}:
		case <-requestCtx.Done():
			cancel()
			return nil, nil, ctxOrTemporary(ctx)
		}
		release := sync.OnceFunc(func() {
			<-c.semaphore
			cancel()
		})
		var body io.Reader
		if spec.body != nil {
			body = spec.body()
		}
		request, err := http.NewRequestWithContext(requestCtx, spec.method, spec.url, body)
		if err != nil {
			release()
			return nil, nil, ErrUnexpectedResponse
		}
		request.Header = spec.header.Clone()
		if request.Header == nil {
			request.Header = make(http.Header)
		}
		if spec.contentLength > 0 {
			request.ContentLength = spec.contentLength
		}
		response, requestErr := c.httpClient.Do(request)
		if requestErr != nil {
			release()
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			classified, retryable := classifyTransportError(requestErr)
			if retryable && spec.retrySafe && attempt < c.maxAttempts {
				if err := c.waitBeforeRetry(ctx, attempt, 0); err != nil {
					return nil, nil, err
				}
				continue
			}
			return nil, nil, classified
		}
		cleanup := sync.OnceFunc(func() {
			_ = response.Body.Close()
			release()
		})
		if spec.retrySafe && attempt < c.maxAttempts && response.StatusCode >= 400 {
			apiErr := parseAPIError(response)
			if errors.Is(apiErr, ErrRateLimited) || errors.Is(apiErr, ErrTemporary) {
				delay := retryAfter(response.Header.Get("Retry-After"), time.Now())
				cleanup()
				if err := c.waitBeforeRetry(ctx, attempt, delay); err != nil {
					return nil, nil, err
				}
				continue
			}
		}
		return response, cleanup, nil
	}
	return nil, nil, ErrTemporary
}

func (c *Client) waitBeforeRetry(ctx context.Context, attempt int, retryAfter time.Duration) error {
	delay := retryAfter
	if delay <= 0 {
		delay = c.baseDelay
		for count := 1; count < attempt && delay < c.maxDelay; count++ {
			if delay > c.maxDelay/2 {
				delay = c.maxDelay
				break
			}
			delay *= 2
		}
		if delay > c.maxDelay {
			delay = c.maxDelay
		}
		if delay > 0 {
			delay = time.Duration(rand.Int64N(int64(delay) + 1))
		}
	}
	if delay > c.maxDelay {
		delay = c.maxDelay
	}
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) apiURL(path string, query url.Values) string {
	value := c.baseURL + "/" + path
	if encoded := query.Encode(); encoded != "" {
		value += "?" + encoded
	}
	return value
}

func validateBaseURL(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("URL must be an absolute HTTP(S) URL without credentials or query")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func validateID(value string) error {
	if value == "" || strings.ContainsAny(value, "/?#") {
		return errors.New("Drive file ID is invalid")
	}
	return nil
}

func contentTypeOrDefault(value string) string {
	if value == "" {
		return "application/octet-stream"
	}
	return value
}

func sameOrigin(left, right *url.URL) bool {
	return left != nil && right != nil && strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

func parseAPIError(response *http.Response) *APIError {
	var body struct {
		Error struct {
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
			Details []struct {
				Reason string `json:"reason"`
			} `json:"details"`
		} `json:"error"`
	}
	encoded, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorResponse))
	response.Body = io.NopCloser(bytes.NewReader(encoded))
	_ = json.Unmarshal(encoded, &body)
	reasons := make([]string, 0, len(body.Error.Errors)+len(body.Error.Details))
	for _, item := range body.Error.Errors {
		if item.Reason != "" {
			reasons = append(reasons, item.Reason)
		}
	}
	for _, item := range body.Error.Details {
		if item.Reason != "" {
			reasons = append(reasons, item.Reason)
		}
	}
	return newAPIError(response.StatusCode, reasons)
}

func decodeJSON(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, maxJSONResponse))
	return decoder.Decode(destination)
}

func retryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if parsed, err := http.ParseTime(value); err == nil && parsed.After(now) {
		return parsed.Sub(now)
	}
	return 0
}

func classifyTransportError(requestErr error) (error, bool) {
	switch {
	case errors.Is(requestErr, auth.ErrReauthorizationRequired):
		return fmt.Errorf("%w: %w", ErrUnauthenticated, auth.ErrReauthorizationRequired), false
	case errors.Is(requestErr, auth.ErrNotAuthenticated):
		return fmt.Errorf("%w: %w", ErrUnauthenticated, auth.ErrNotAuthenticated), false
	case errors.Is(requestErr, auth.ErrCredentialStore):
		return fmt.Errorf("%w: %w", ErrTemporary, auth.ErrCredentialStore), false
	case errors.Is(requestErr, auth.ErrTokenRefresh):
		return fmt.Errorf("%w: %w", ErrTemporary, auth.ErrTokenRefresh), true
	default:
		return ErrTemporary, true
	}
}

func ctxOrTemporary(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrTemporary
}

type leasedBody struct {
	io.ReadCloser
	cleanup func()
}

func (b *leasedBody) Close() error {
	err := b.ReadCloser.Close()
	b.cleanup()
	return err
}
